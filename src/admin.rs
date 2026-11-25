//! [`duroxide::providers::ProviderAdmin`] management, deletion, and pruning operations.

use duroxide::Event;
use duroxide::providers::{
    DeleteInstanceResult, ExecutionInfo, InstanceFilter, InstanceInfo, ProviderAdmin,
    ProviderError, PruneOptions, PruneResult, QueueDepths, SystemMetrics,
};
use sqlx::Row;

use crate::error::sqlx_to_provider_error;
use crate::provider::{MysqlProvider, PoolResolver};

const DEFAULT_BULK_LIMIT: u32 = 1_000;

#[async_trait::async_trait]
impl<P: PoolResolver> ProviderAdmin for MysqlProvider<P> {
    async fn list_instances(&self) -> Result<Vec<String>, ProviderError> {
        let pool = self.acquire_pool("list_instances")?;
        let sql = format!(
            "SELECT instance_id FROM {} ORDER BY created_at DESC, seq_id DESC",
            self.tables.instances
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .fetch_all(&pool)
            .await
            .map_err(|e| sqlx_to_provider_error("list_instances", e))?;

        Ok(rows
            .into_iter()
            .filter_map(|r| r.try_get("instance_id").ok())
            .collect())
    }

    async fn list_instances_by_status(&self, status: &str) -> Result<Vec<String>, ProviderError> {
        let pool = self.acquire_pool("list_instances_by_status")?;
        let sql = format!(
            "SELECT i.instance_id \
             FROM {} i \
             JOIN {} e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id \
             WHERE e.status = ? \
             ORDER BY i.created_at DESC, i.seq_id DESC",
            self.tables.instances, self.tables.executions
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(status)
            .fetch_all(&pool)
            .await
            .map_err(|e| sqlx_to_provider_error("list_instances_by_status", e))?;

        Ok(rows
            .into_iter()
            .filter_map(|r| r.try_get("instance_id").ok())
            .collect())
    }

    async fn list_executions(&self, instance: &str) -> Result<Vec<u64>, ProviderError> {
        let pool = self.acquire_pool("list_executions")?;
        let sql = format!(
            "SELECT execution_id FROM {} WHERE instance_id = ? ORDER BY execution_id ASC",
            self.tables.executions
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(instance)
            .fetch_all(&pool)
            .await
            .map_err(|e| sqlx_to_provider_error("list_executions", e))?;

        Ok(rows
            .into_iter()
            .filter_map(|r| {
                r.try_get::<i64, _>("execution_id")
                    .ok()
                    .map(|v| v.max(0) as u64)
            })
            .collect())
    }

    async fn read_history_with_execution_id(
        &self,
        instance: &str,
        execution_id: u64,
    ) -> Result<Vec<Event>, ProviderError> {
        duroxide::providers::Provider::read_with_execution(self, instance, execution_id).await
    }

    async fn read_history(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
        let exec_id = self.latest_execution_id(instance).await?;
        self.read_history_with_execution_id(instance, exec_id).await
    }

    async fn latest_execution_id(&self, instance: &str) -> Result<u64, ProviderError> {
        let pool = self.acquire_pool("latest_execution_id")?;
        let sql = format!(
            "SELECT COALESCE(MAX(execution_id), 1) AS max_exec FROM {} WHERE instance_id = ?",
            self.tables.executions
        );
        let max_exec: i64 = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(instance)
            .fetch_one(&pool)
            .await
            .map_err(|e| sqlx_to_provider_error("latest_execution_id", e))?;

        Ok(max_exec.max(1) as u64)
    }

    async fn get_instance_info(&self, instance: &str) -> Result<InstanceInfo, ProviderError> {
        let pool = self.acquire_pool("get_instance_info")?;
        let sql = format!(
            "SELECT \
                i.instance_id, \
                i.orchestration_name, \
                i.orchestration_version, \
                i.current_execution_id, \
                i.created_at, \
                i.updated_at, \
                i.parent_instance_id, \
                e.status, \
                e.output \
             FROM {} i \
             LEFT JOIN {} e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id \
             WHERE i.instance_id = ?",
            self.tables.instances, self.tables.executions
        );

        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(instance)
            .fetch_optional(&pool)
            .await
            .map_err(|e| sqlx_to_provider_error("get_instance_info", e))?;

        let Some(row) = row else {
            return Err(ProviderError::permanent(
                "get_instance_info",
                format!("Instance {instance} not found"),
            ));
        };

        let instance_id: String = row
            .try_get("instance_id")
            .map_err(|e| sqlx_to_provider_error("get_instance_info", e))?;
        let orchestration_name: String = row
            .try_get("orchestration_name")
            .map_err(|e| sqlx_to_provider_error("get_instance_info", e))?;
        let orchestration_version: String = row
            .try_get::<Option<String>, _>("orchestration_version")
            .ok()
            .flatten()
            .unwrap_or_else(|| "unknown".to_string());
        let current_execution_id: i64 = row.try_get("current_execution_id").unwrap_or(1);
        let created_at: i64 = row.try_get("created_at").unwrap_or(0);
        let updated_at: i64 = row.try_get("updated_at").unwrap_or(0);
        let parent_instance_id: Option<String> = row.try_get("parent_instance_id").ok().flatten();
        let status: String = row
            .try_get::<Option<String>, _>("status")
            .ok()
            .flatten()
            .unwrap_or_else(|| "Unknown".to_string());
        let output: Option<String> = row.try_get("output").ok().flatten();

        Ok(InstanceInfo {
            instance_id,
            orchestration_name,
            orchestration_version,
            current_execution_id: current_execution_id.max(1) as u64,
            status,
            output,
            created_at: created_at.max(0) as u64,
            updated_at: updated_at.max(0) as u64,
            parent_instance_id,
        })
    }

    async fn get_execution_info(
        &self,
        instance: &str,
        execution_id: u64,
    ) -> Result<ExecutionInfo, ProviderError> {
        let pool = self.acquire_pool("get_execution_info")?;
        let sql = format!(
            "SELECT \
                e.execution_id, \
                e.status, \
                e.output, \
                e.started_at, \
                e.completed_at, \
                COUNT(h.event_id) AS event_count \
             FROM {} e \
             LEFT JOIN {} h ON e.instance_id = h.instance_id AND e.execution_id = h.execution_id \
             WHERE e.instance_id = ? AND e.execution_id = ? \
             GROUP BY e.instance_id, e.execution_id, e.status, e.output, e.started_at, e.completed_at",
            self.tables.executions, self.tables.history
        );

        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(instance)
            .bind(execution_id as i64)
            .fetch_optional(&pool)
            .await
            .map_err(|e| sqlx_to_provider_error("get_execution_info", e))?;

        let Some(row) = row else {
            return Err(ProviderError::permanent(
                "get_execution_info",
                format!("Execution {execution_id} not found for instance {instance}"),
            ));
        };

        let exec_id: i64 = row
            .try_get("execution_id")
            .map_err(|e| sqlx_to_provider_error("get_execution_info", e))?;
        let status: String = row
            .try_get("status")
            .map_err(|e| sqlx_to_provider_error("get_execution_info", e))?;
        let output: Option<String> = row.try_get("output").ok().flatten();
        let started_at: i64 = row.try_get("started_at").unwrap_or(0);
        let completed_at: Option<i64> = row.try_get("completed_at").ok().flatten();
        let event_count: i64 = row.try_get("event_count").unwrap_or(0);

        Ok(ExecutionInfo {
            execution_id: exec_id.max(1) as u64,
            status,
            output,
            started_at: started_at.max(0) as u64,
            completed_at: completed_at.map(|t| t.max(0) as u64),
            event_count: event_count.max(0) as usize,
        })
    }

    async fn get_system_metrics(&self) -> Result<SystemMetrics, ProviderError> {
        let pool = self.acquire_pool("get_system_metrics")?;
        let inst_sql = format!(
            "SELECT \
                COUNT(*) AS total_instances, \
                CAST(COALESCE(SUM(CASE WHEN e.status = 'Running' THEN 1 ELSE 0 END), 0) AS SIGNED) AS running_instances, \
                CAST(COALESCE(SUM(CASE WHEN e.status = 'Completed' THEN 1 ELSE 0 END), 0) AS SIGNED) AS completed_instances, \
                CAST(COALESCE(SUM(CASE WHEN e.status = 'Failed' THEN 1 ELSE 0 END), 0) AS SIGNED) AS failed_instances \
             FROM {} i \
             LEFT JOIN {} e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id",
            self.tables.instances, self.tables.executions
        );

        let row = sqlx::query(sqlx::AssertSqlSafe(inst_sql.as_str()))
            .fetch_one(&pool)
            .await
            .map_err(|e| sqlx_to_provider_error("get_system_metrics", e))?;

        let total_instances: i64 = row.try_get("total_instances").unwrap_or(0);
        let running_instances: i64 = row.try_get("running_instances").unwrap_or(0);
        let completed_instances: i64 = row.try_get("completed_instances").unwrap_or(0);
        let failed_instances: i64 = row.try_get("failed_instances").unwrap_or(0);

        let total_exec_sql = format!("SELECT COUNT(*) FROM {}", self.tables.executions);
        let total_executions: i64 =
            sqlx::query_scalar(sqlx::AssertSqlSafe(total_exec_sql.as_str()))
                .fetch_one(&pool)
                .await
                .map_err(|e| sqlx_to_provider_error("get_system_metrics", e))?;

        let total_events_sql = format!("SELECT COUNT(*) FROM {}", self.tables.history);
        let total_events: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(total_events_sql.as_str()))
            .fetch_one(&pool)
            .await
            .map_err(|e| sqlx_to_provider_error("get_system_metrics", e))?;

        Ok(SystemMetrics {
            total_instances: total_instances.max(0) as u64,
            total_executions: total_executions.max(0) as u64,
            running_instances: running_instances.max(0) as u64,
            completed_instances: completed_instances.max(0) as u64,
            failed_instances: failed_instances.max(0) as u64,
            total_events: total_events.max(0) as u64,
        })
    }

    async fn get_queue_depths(&self) -> Result<QueueDepths, ProviderError> {
        let pool = self.acquire_pool("get_queue_depths")?;
        let oq_sql = format!(
            "SELECT COUNT(*) FROM {} WHERE lock_token IS NULL",
            self.tables.orchestrator_queue
        );
        let oq: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(oq_sql.as_str()))
            .fetch_one(&pool)
            .await
            .map_err(|e| sqlx_to_provider_error("get_queue_depths", e))?;

        let wq_sql = format!(
            "SELECT COUNT(*) FROM {} WHERE lock_token IS NULL",
            self.tables.worker_queue
        );
        let wq: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(wq_sql.as_str()))
            .fetch_one(&pool)
            .await
            .map_err(|e| sqlx_to_provider_error("get_queue_depths", e))?;

        Ok(QueueDepths {
            orchestrator_queue: oq.max(0) as usize,
            worker_queue: wq.max(0) as usize,
            timer_queue: 0,
        })
    }

    async fn list_children(&self, instance_id: &str) -> Result<Vec<String>, ProviderError> {
        let pool = self.acquire_pool("list_children")?;
        let sql = format!(
            "SELECT instance_id FROM {} WHERE parent_instance_id = ? ORDER BY seq_id ASC",
            self.tables.instances
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(instance_id)
            .fetch_all(&pool)
            .await
            .map_err(|e| sqlx_to_provider_error("list_children", e))?;

        Ok(rows
            .into_iter()
            .filter_map(|r| r.try_get("instance_id").ok())
            .collect())
    }

    async fn get_parent_id(&self, instance_id: &str) -> Result<Option<String>, ProviderError> {
        let pool = self.acquire_pool("get_parent_id")?;
        let sql = format!(
            "SELECT parent_instance_id FROM {} WHERE instance_id = ?",
            self.tables.instances
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(instance_id)
            .fetch_optional(&pool)
            .await
            .map_err(|e| sqlx_to_provider_error("get_parent_id", e))?;

        match row {
            Some(r) => Ok(r.try_get("parent_instance_id").ok().flatten()),
            None => Err(ProviderError::permanent(
                "get_parent_id",
                format!("Instance {instance_id} not found"),
            )),
        }
    }

    async fn delete_instances_atomic(
        &self,
        ids: &[String],
        force: bool,
    ) -> Result<DeleteInstanceResult, ProviderError> {
        if ids.is_empty() {
            return Ok(DeleteInstanceResult::default());
        }

        // Deduplicate and sort IDs to maintain deterministic row-lock acquisition order.
        let mut sorted_ids: Vec<String> = ids.to_vec();
        sorted_ids.sort();
        sorted_ids.dedup();
        let placeholders = vec!["?"; sorted_ids.len()].join(", ");

        let pool = self.acquire_pool("delete_instances_atomic")?;
        let mut tx = pool
            .begin()
            .await
            .map_err(|e| sqlx_to_provider_error("delete_instances_atomic", e))?;

        // Lock Order Step 1: Lock matching instance_locks rows FIRST.
        let lock_il_sql = format!(
            "SELECT instance_id FROM {} WHERE instance_id IN ({placeholders}) ORDER BY instance_id ASC FOR UPDATE",
            self.tables.instance_locks
        );
        let mut lock_il_query = sqlx::query(sqlx::AssertSqlSafe(lock_il_sql.as_str()));
        for id in &sorted_ids {
            lock_il_query = lock_il_query.bind(id);
        }
        lock_il_query
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("delete_instances_atomic", e))?;

        // Lock matching instances rows and verify existence + status inside the transaction.
        let check_sql = format!(
            "SELECT i.instance_id, e.status \
             FROM {} i \
             LEFT JOIN {} e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id \
             WHERE i.instance_id IN ({placeholders}) \
             ORDER BY i.instance_id ASC FOR UPDATE",
            self.tables.instances, self.tables.executions
        );
        let mut check_query = sqlx::query(sqlx::AssertSqlSafe(check_sql.as_str()));
        for id in &sorted_ids {
            check_query = check_query.bind(id);
        }
        let found_rows = check_query
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("delete_instances_atomic", e))?;

        if !force {
            for row in &found_rows {
                let status: Option<String> = row.try_get("status").ok().flatten();
                if status.as_deref() == Some("Running") {
                    let inst_id: String = row.try_get("instance_id").unwrap_or_default();
                    let _ = tx.rollback().await;
                    return Err(ProviderError::permanent(
                        "delete_instances_atomic",
                        format!(
                            "Instance {inst_id} is still running. Use force=true to delete anyway, or cancel first."
                        ),
                    ));
                }
            }
        }

        // Orphan check inside transaction to prevent TOCTOU child creation races.
        let orphan_sql = format!(
            "SELECT instance_id, parent_instance_id FROM {} \
             WHERE parent_instance_id IN ({placeholders}) \
               AND instance_id NOT IN ({placeholders}) \
             LIMIT 1 FOR UPDATE",
            self.tables.instances
        );
        let mut orphan_query = sqlx::query(sqlx::AssertSqlSafe(orphan_sql.as_str()));
        for id in &sorted_ids {
            orphan_query = orphan_query.bind(id);
        }
        for id in &sorted_ids {
            orphan_query = orphan_query.bind(id);
        }
        if let Some(orphan_row) = orphan_query
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("delete_instances_atomic", e))?
        {
            let orphan_id: String = orphan_row.try_get("instance_id").unwrap_or_default();
            let parent_id: String = orphan_row.try_get("parent_instance_id").unwrap_or_default();
            let _ = tx.rollback().await;
            return Err(ProviderError::permanent(
                "delete_instances_atomic",
                format!(
                    "Cannot delete: instance {parent_id} has child {orphan_id} created after tree traversal."
                ),
            ));
        }

        let mut result = DeleteInstanceResult {
            instances_deleted: found_rows.len() as u64,
            ..Default::default()
        };

        // Delete instance_locks first so any concurrent in-flight ack fails its lock check.
        let del_locks_sql = format!(
            "DELETE FROM {} WHERE instance_id IN ({placeholders})",
            self.tables.instance_locks
        );
        let mut q = sqlx::query(sqlx::AssertSqlSafe(del_locks_sql.as_str()));
        for id in &sorted_ids {
            q = q.bind(id);
        }
        q.execute(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("delete_instances_atomic", e))?;

        let del_oq_sql = format!(
            "DELETE FROM {} WHERE instance_id IN ({placeholders})",
            self.tables.orchestrator_queue
        );
        let mut q = sqlx::query(sqlx::AssertSqlSafe(del_oq_sql.as_str()));
        for id in &sorted_ids {
            q = q.bind(id);
        }
        let oq_deleted = q
            .execute(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("delete_instances_atomic", e))?
            .rows_affected();

        let del_wq_sql = format!(
            "DELETE FROM {} WHERE instance_id IN ({placeholders})",
            self.tables.worker_queue
        );
        let mut q = sqlx::query(sqlx::AssertSqlSafe(del_wq_sql.as_str()));
        for id in &sorted_ids {
            q = q.bind(id);
        }
        let wq_deleted = q
            .execute(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("delete_instances_atomic", e))?
            .rows_affected();
        result.queue_messages_deleted = oq_deleted + wq_deleted;

        let del_hist_sql = format!(
            "DELETE FROM {} WHERE instance_id IN ({placeholders})",
            self.tables.history
        );
        let mut q = sqlx::query(sqlx::AssertSqlSafe(del_hist_sql.as_str()));
        for id in &sorted_ids {
            q = q.bind(id);
        }
        result.events_deleted = q
            .execute(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("delete_instances_atomic", e))?
            .rows_affected();

        let del_exec_sql = format!(
            "DELETE FROM {} WHERE instance_id IN ({placeholders})",
            self.tables.executions
        );
        let mut q = sqlx::query(sqlx::AssertSqlSafe(del_exec_sql.as_str()));
        for id in &sorted_ids {
            q = q.bind(id);
        }
        result.executions_deleted = q
            .execute(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("delete_instances_atomic", e))?
            .rows_affected();

        let del_kvs_sql = format!(
            "DELETE FROM {} WHERE instance_id IN ({placeholders})",
            self.tables.kv_store
        );
        let mut q = sqlx::query(sqlx::AssertSqlSafe(del_kvs_sql.as_str()));
        for id in &sorted_ids {
            q = q.bind(id);
        }
        q.execute(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("delete_instances_atomic", e))?;

        let del_kvd_sql = format!(
            "DELETE FROM {} WHERE instance_id IN ({placeholders})",
            self.tables.kv_delta
        );
        let mut q = sqlx::query(sqlx::AssertSqlSafe(del_kvd_sql.as_str()));
        for id in &sorted_ids {
            q = q.bind(id);
        }
        q.execute(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("delete_instances_atomic", e))?;

        let del_inst_sql = format!(
            "DELETE FROM {} WHERE instance_id IN ({placeholders})",
            self.tables.instances
        );
        let mut q = sqlx::query(sqlx::AssertSqlSafe(del_inst_sql.as_str()));
        for id in &sorted_ids {
            q = q.bind(id);
        }
        q.execute(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("delete_instances_atomic", e))?;

        tx.commit()
            .await
            .map_err(|e| sqlx_to_provider_error("delete_instances_atomic", e))?;

        Ok(result)
    }

    async fn delete_instance_bulk(
        &self,
        filter: InstanceFilter,
    ) -> Result<DeleteInstanceResult, ProviderError> {
        let mut sql = format!(
            "SELECT i.instance_id \
             FROM {} i \
             LEFT JOIN {} e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id \
             WHERE i.parent_instance_id IS NULL \
               AND e.status IN ('Completed', 'Failed', 'ContinuedAsNew')",
            self.tables.instances, self.tables.executions
        );

        if let Some(ref ids) = filter.instance_ids {
            if ids.is_empty() {
                return Ok(DeleteInstanceResult::default());
            }
            let placeholders = vec!["?"; ids.len()].join(", ");
            sql.push_str(&format!(" AND i.instance_id IN ({placeholders})"));
        }

        if filter.completed_before.is_some() {
            sql.push_str(" AND e.completed_at < ?");
        }

        sql.push_str(" ORDER BY i.created_at ASC, i.seq_id ASC LIMIT ?");

        let limit = filter.limit.unwrap_or(DEFAULT_BULK_LIMIT);
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        if let Some(ref ids) = filter.instance_ids {
            for id in ids {
                query = query.bind(id);
            }
        }
        if let Some(cb) = filter.completed_before {
            query = query.bind(cb as i64);
        }
        query = query.bind(limit);

        let pool = self.acquire_pool("delete_instance_bulk")?;
        let rows = query
            .fetch_all(&pool)
            .await
            .map_err(|e| sqlx_to_provider_error("delete_instance_bulk", e))?;

        let roots: Vec<String> = rows
            .into_iter()
            .filter_map(|r| r.try_get("instance_id").ok())
            .collect();

        let mut total = DeleteInstanceResult::default();
        for root_id in &roots {
            let tree = self.get_instance_tree(root_id).await?;
            let sub = self.delete_instances_atomic(&tree.all_ids, true).await?;
            total.instances_deleted += sub.instances_deleted;
            total.executions_deleted += sub.executions_deleted;
            total.events_deleted += sub.events_deleted;
            total.queue_messages_deleted += sub.queue_messages_deleted;
        }

        Ok(total)
    }

    async fn prune_executions(
        &self,
        instance_id: &str,
        options: PruneOptions,
    ) -> Result<PruneResult, ProviderError> {
        let pool = self.acquire_pool("prune_executions")?;
        let mut tx = pool
            .begin()
            .await
            .map_err(|e| sqlx_to_provider_error("prune_executions", e))?;

        let inst_sql = format!(
            "SELECT current_execution_id FROM {} WHERE instance_id = ? FOR UPDATE",
            self.tables.instances
        );
        let Some(inst_row) = sqlx::query(sqlx::AssertSqlSafe(inst_sql.as_str()))
            .bind(instance_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("prune_executions", e))?
        else {
            let _ = tx.rollback().await;
            return Err(ProviderError::permanent(
                "prune_executions",
                format!("Instance {instance_id} not found"),
            ));
        };

        let current_exec_id: i64 = inst_row.try_get("current_execution_id").unwrap_or(1);

        // Fetch all executions ordered newest-first and select prune targets in Rust
        // (avoids MySQL's `LIMIT & IN/ALL/ANY/SOME subquery` restriction).
        let execs_sql = format!(
            "SELECT execution_id, status, completed_at FROM {} \
             WHERE instance_id = ? ORDER BY execution_id DESC FOR UPDATE",
            self.tables.executions
        );
        let exec_rows = sqlx::query(sqlx::AssertSqlSafe(execs_sql.as_str()))
            .bind(instance_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("prune_executions", e))?;

        let keep_n = options.keep_last.unwrap_or(0) as usize;
        let mut to_prune = Vec::new();
        for (idx, row) in exec_rows.iter().enumerate() {
            let exec_id: i64 = row.try_get("execution_id").unwrap_or(0);
            let status: String = row.try_get("status").unwrap_or_default();
            let completed_at: Option<i64> = row.try_get("completed_at").ok().flatten();

            // Never prune current execution or Running executions, or within top `keep_last`.
            if exec_id == current_exec_id || status == "Running" || idx < keep_n {
                continue;
            }

            if let Some(cutoff) = options.completed_before {
                let Some(done_at) = completed_at else {
                    continue;
                };
                if done_at >= cutoff as i64 {
                    continue;
                }
            }

            to_prune.push(exec_id);
        }

        if to_prune.is_empty() {
            tx.commit()
                .await
                .map_err(|e| sqlx_to_provider_error("prune_executions", e))?;
            return Ok(PruneResult {
                instances_processed: 1,
                ..Default::default()
            });
        }

        let placeholders = vec!["?"; to_prune.len()].join(", ");
        let del_hist_sql = format!(
            "DELETE FROM {} WHERE instance_id = ? AND execution_id IN ({placeholders})",
            self.tables.history
        );
        let mut hq = sqlx::query(sqlx::AssertSqlSafe(del_hist_sql.as_str())).bind(instance_id);
        for exec_id in &to_prune {
            hq = hq.bind(*exec_id);
        }
        let events_deleted = hq
            .execute(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("prune_executions", e))?
            .rows_affected();

        let del_exec_sql = format!(
            "DELETE FROM {} WHERE instance_id = ? AND execution_id IN ({placeholders})",
            self.tables.executions
        );
        let mut eq = sqlx::query(sqlx::AssertSqlSafe(del_exec_sql.as_str())).bind(instance_id);
        for exec_id in &to_prune {
            eq = eq.bind(*exec_id);
        }
        let executions_deleted = eq
            .execute(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("prune_executions", e))?
            .rows_affected();

        tx.commit()
            .await
            .map_err(|e| sqlx_to_provider_error("prune_executions", e))?;

        Ok(PruneResult {
            instances_processed: 1,
            executions_deleted,
            events_deleted,
        })
    }

    async fn prune_executions_bulk(
        &self,
        filter: InstanceFilter,
        options: PruneOptions,
    ) -> Result<PruneResult, ProviderError> {
        let mut sql = format!(
            "SELECT i.instance_id \
             FROM {} i \
             LEFT JOIN {} e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id \
             WHERE 1=1",
            self.tables.instances, self.tables.executions
        );

        if let Some(ref ids) = filter.instance_ids {
            if ids.is_empty() {
                return Ok(PruneResult::default());
            }
            let placeholders = vec!["?"; ids.len()].join(", ");
            sql.push_str(&format!(" AND i.instance_id IN ({placeholders})"));
        }

        if filter.completed_before.is_some() {
            sql.push_str(" AND e.completed_at < ?");
        }

        sql.push_str(" ORDER BY i.created_at ASC, i.seq_id ASC LIMIT ?");
        let limit = filter.limit.unwrap_or(DEFAULT_BULK_LIMIT);

        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()));
        if let Some(ref ids) = filter.instance_ids {
            for id in ids {
                query = query.bind(id);
            }
        }
        if let Some(cb) = filter.completed_before {
            query = query.bind(cb as i64);
        }
        query = query.bind(limit);

        let pool = self.acquire_pool("prune_executions_bulk")?;
        let rows = query
            .fetch_all(&pool)
            .await
            .map_err(|e| sqlx_to_provider_error("prune_executions_bulk", e))?;

        let instance_ids: Vec<String> = rows
            .into_iter()
            .filter_map(|r| r.try_get("instance_id").ok())
            .collect();

        let mut total = PruneResult::default();
        for inst_id in &instance_ids {
            let sub = self.prune_executions(inst_id, options.clone()).await?;
            total.instances_processed += sub.instances_processed;
            total.executions_deleted += sub.executions_deleted;
            total.events_deleted += sub.events_deleted;
        }

        Ok(total)
    }
}
