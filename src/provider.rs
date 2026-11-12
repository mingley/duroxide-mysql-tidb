//! Core [`duroxide::providers::Provider`] implementation for MySQL 8.0+ and TiDB.

use std::collections::HashMap;
use std::time::Duration;

use duroxide::providers::{
    DispatcherCapabilityFilter, ExecutionMetadata, KvEntry, OrchestrationItem, Provider,
    ProviderAdmin, ProviderError, ScheduledActivityIdentifier, SessionFetchConfig, TagFilter,
    WorkItem,
};
use duroxide::{Event, EventKind, SystemStats};
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use sqlx::{Executor, MySql, Row, Transaction};
use tracing::{debug, warn};

use crate::config::MysqlProviderConfig;
use crate::error::{
    InitError, is_duplicate_key_error, is_transient_conflict, sqlx_to_provider_error,
};
use crate::id::{next_seq_id, now_millis, offset_millis, timestamp_after};
use crate::schema::{SchemaNames, run_migrations};

const MAX_TX_RETRIES: usize = 4;

/// MySQL 8.0+ and TiDB durable storage provider for [`duroxide`].
#[derive(Debug, Clone)]
pub struct MysqlProvider {
    pub(crate) pool: MySqlPool,
    pub(crate) tables: SchemaNames,
    pub(crate) candidate_batch_size: u32,
}

impl MysqlProvider {
    /// Connects to a MySQL 8.0+ or TiDB database URL with default pool settings and runs
    /// `CREATE TABLE IF NOT EXISTS` schema initialization.
    ///
    /// # Errors
    ///
    /// Returns [`InitError`] if URL parsing, connection, or DDL execution fails.
    pub async fn new(database_url: &str) -> Result<Self, InitError> {
        let config = MysqlProviderConfig::from_url(database_url)?;
        Self::new_with_config(config).await
    }

    /// Connects with an optional table prefix for multi-tenant isolation in a shared database.
    ///
    /// # Errors
    ///
    /// Returns [`InitError`] if `table_prefix` is invalid, or if connection/DDL fails.
    pub async fn new_with_prefix(
        database_url: &str,
        table_prefix: &str,
    ) -> Result<Self, InitError> {
        let config = MysqlProviderConfig::from_url(database_url)?.table_prefix(table_prefix);
        Self::new_with_config(config).await
    }

    /// Constructs a provider from a customized [`MysqlProviderConfig`].
    ///
    /// # Errors
    ///
    /// Returns [`InitError`] if `table_prefix` is invalid, pool initialization fails,
    /// or `auto_migrate` is enabled and DDL execution fails.
    pub async fn new_with_config(config: MysqlProviderConfig) -> Result<Self, InitError> {
        let tables = SchemaNames::new(&config.table_prefix)?;
        let set_rc = config.set_read_committed;

        let pool = MySqlPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(config.acquire_timeout)
            .idle_timeout(config.idle_timeout)
            .max_lifetime(config.max_lifetime)
            .after_connect(move |conn, _meta| {
                Box::pin(async move {
                    if set_rc {
                        conn.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                            .await?;
                    }
                    Ok(())
                })
            })
            .connect_with(config.connect_options)
            .await?;

        Self::from_pool_internal(
            pool,
            tables,
            config.table_prefix,
            config.auto_migrate,
            config.candidate_batch_size,
        )
        .await
    }

    /// Wraps an existing [`MySqlPool`] with optional `table_prefix` and `auto_migrate`.
    ///
    /// # Errors
    ///
    /// Returns [`InitError`] if `table_prefix` is invalid or `auto_migrate` fails.
    pub async fn from_pool(
        pool: MySqlPool,
        table_prefix: &str,
        auto_migrate: bool,
    ) -> Result<Self, InitError> {
        let tables = SchemaNames::new(table_prefix)?;
        Self::from_pool_internal(pool, tables, table_prefix.to_string(), auto_migrate, 8).await
    }

    async fn from_pool_internal(
        pool: MySqlPool,
        tables: SchemaNames,
        prefix: String,
        auto_migrate: bool,
        candidate_batch_size: u32,
    ) -> Result<Self, InitError> {
        if auto_migrate {
            run_migrations(&pool, &prefix).await?;
        }
        Ok(Self {
            pool,
            tables,
            candidate_batch_size,
        })
    }

    /// Returns a reference to the underlying `sqlx::MySqlPool`.
    #[must_use]
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    fn generate_lock_token() -> String {
        format!("lock_{}", uuid::Uuid::new_v4())
    }

    pub(crate) fn extract_orchestrator_instance(item: &WorkItem) -> Option<&str> {
        match item {
            WorkItem::StartOrchestration { instance, .. }
            | WorkItem::ActivityCompleted { instance, .. }
            | WorkItem::ActivityFailed { instance, .. }
            | WorkItem::TimerFired { instance, .. }
            | WorkItem::ExternalRaised { instance, .. }
            | WorkItem::QueueMessage { instance, .. }
            | WorkItem::CancelInstance { instance, .. }
            | WorkItem::ContinueAsNew { instance, .. } => Some(instance.as_str()),
            #[cfg(feature = "replay-version-test")]
            WorkItem::ExternalRaised2 { instance, .. } => Some(instance.as_str()),
            WorkItem::SubOrchCompleted {
                parent_instance, ..
            }
            | WorkItem::SubOrchFailed {
                parent_instance, ..
            } => Some(parent_instance.as_str()),
            WorkItem::ActivityExecute { .. } => None,
        }
    }

    fn event_type_name(kind: &EventKind) -> &'static str {
        match kind {
            EventKind::OrchestrationStarted { .. } => "OrchestrationStarted",
            EventKind::OrchestrationCompleted { .. } => "OrchestrationCompleted",
            EventKind::OrchestrationFailed { .. } => "OrchestrationFailed",
            EventKind::OrchestrationContinuedAsNew { .. } => "OrchestrationContinuedAsNew",
            EventKind::ActivityScheduled { .. } => "ActivityScheduled",
            EventKind::ActivityCompleted { .. } => "ActivityCompleted",
            EventKind::ActivityFailed { .. } => "ActivityFailed",
            EventKind::ActivityCancelRequested { .. } => "ActivityCancelRequested",
            EventKind::TimerCreated { .. } => "TimerCreated",
            EventKind::TimerFired { .. } => "TimerFired",
            EventKind::ExternalSubscribed { .. } => "ExternalSubscribed",
            EventKind::ExternalEvent { .. } => "ExternalEvent",
            EventKind::SubOrchestrationScheduled { .. } => "SubOrchestrationScheduled",
            EventKind::SubOrchestrationCompleted { .. } => "SubOrchestrationCompleted",
            EventKind::SubOrchestrationFailed { .. } => "SubOrchestrationFailed",
            EventKind::SubOrchestrationCancelRequested { .. } => "SubOrchestrationCancelRequested",
            EventKind::OrchestrationCancelRequested { .. } => "OrchestrationCancelRequested",
            EventKind::ExternalSubscribedCancelled { .. } => "ExternalSubscribedCancelled",
            EventKind::QueueSubscribed { .. } => "ExternalSubscribedPersistent",
            EventKind::QueueEventDelivered { .. } => "ExternalEventPersistent",
            EventKind::QueueSubscriptionCancelled { .. } => "ExternalSubscribedPersistentCancelled",
            EventKind::OrchestrationChained { .. } => "OrchestrationChained",
            EventKind::CustomStatusUpdated { .. } => "CustomStatusUpdated",
            EventKind::KeyValueSet { .. } => "KeyValueSet",
            EventKind::KeyValueCleared { .. } => "KeyValueCleared",
            EventKind::KeyValuesCleared => "KeyValuesCleared",
            #[cfg(feature = "replay-version-test")]
            EventKind::ExternalSubscribed2 { .. } => "ExternalSubscribed2",
            #[cfg(feature = "replay-version-test")]
            EventKind::ExternalEvent2 { .. } => "ExternalEvent2",
        }
    }

    fn build_tag_clause(filter: &TagFilter) -> (String, Vec<String>) {
        match filter {
            TagFilter::DefaultOnly => ("q.tag IS NULL".to_string(), Vec::new()),
            TagFilter::Tags(set) => {
                let mut tags: Vec<String> = set.iter().cloned().collect();
                tags.sort();
                let placeholders = vec!["?"; tags.len()].join(", ");
                (format!("q.tag IN ({placeholders})"), tags)
            }
            TagFilter::DefaultAnd(set) => {
                let mut tags: Vec<String> = set.iter().cloned().collect();
                tags.sort();
                let placeholders = vec!["?"; tags.len()].join(", ");
                (
                    format!("(q.tag IS NULL OR q.tag IN ({placeholders}))"),
                    tags,
                )
            }
            TagFilter::Any => ("1=1".to_string(), Vec::new()),
            TagFilter::None => ("1=0".to_string(), Vec::new()),
        }
    }

    fn pack_semver(v: &semver::Version) -> i64 {
        (v.major as i64)
            .saturating_mul(1_000_000)
            .saturating_add((v.minor as i64).saturating_mul(1_000))
            .saturating_add(v.patch as i64)
    }

    async fn read_history_in_tx(
        &self,
        tx: &mut Transaction<'_, MySql>,
        instance: &str,
        execution_id: Option<u64>,
    ) -> Result<Vec<Event>, String> {
        let exec_id: i64 = match execution_id {
            Some(id) => id as i64,
            None => {
                let sql = format!(
                    "SELECT COALESCE(MAX(execution_id), 1) FROM {} WHERE instance_id = ?",
                    self.tables.executions
                );
                sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(instance)
                    .fetch_one(&mut **tx)
                    .await
                    .map_err(|e| e.to_string())?
            }
        };

        let sql = format!(
            "SELECT event_data FROM {} WHERE instance_id = ? AND execution_id = ? ORDER BY event_id ASC",
            self.tables.history
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(instance)
            .bind(exec_id)
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| e.to_string())?;

        let mut events = Vec::with_capacity(rows.len());
        for (idx, row) in rows.iter().enumerate() {
            let event_data: String = row.try_get("event_data").map_err(|e| e.to_string())?;
            let event: Event = serde_json::from_str(&event_data).map_err(|e| {
                format!(
                    "Failed to deserialize history event at position {idx} for instance '{instance}' execution {exec_id}: {e}"
                )
            })?;
            events.push(event);
        }
        Ok(events)
    }

    async fn get_kv_snapshot_in_tx(
        &self,
        tx: &mut Transaction<'_, MySql>,
        instance: &str,
    ) -> Result<HashMap<String, KvEntry>, ProviderError> {
        let sql = format!(
            "SELECT kv_key, value, last_updated_at_ms FROM {} WHERE instance_id = ?",
            self.tables.kv_store
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(instance)
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| sqlx_to_provider_error("get_kv_snapshot_in_tx", e))?;

        let mut map = HashMap::with_capacity(rows.len());
        for row in rows {
            let key: String = row
                .try_get("kv_key")
                .map_err(|e| sqlx_to_provider_error("get_kv_snapshot_in_tx", e))?;
            let value: String = row
                .try_get("value")
                .map_err(|e| sqlx_to_provider_error("get_kv_snapshot_in_tx", e))?;
            let ts: i64 = row
                .try_get("last_updated_at_ms")
                .map_err(|e| sqlx_to_provider_error("get_kv_snapshot_in_tx", e))?;
            map.insert(
                key,
                KvEntry {
                    value,
                    last_updated_at_ms: ts.max(0) as u64,
                },
            );
        }
        Ok(map)
    }

    async fn append_history_in_tx(
        &self,
        tx: &mut Transaction<'_, MySql>,
        instance: &str,
        execution_id: u64,
        events: &[Event],
    ) -> Result<(), ProviderError> {
        if events.is_empty() {
            return Ok(());
        }
        for event in events {
            if event.event_id() == 0 {
                return Err(ProviderError::permanent(
                    "append_history",
                    "event_id must be set by runtime (got 0)",
                ));
            }
        }

        let created_at = now_millis();
        let sql = format!(
            "INSERT INTO {} (instance_id, execution_id, event_id, event_type, event_data, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            self.tables.history
        );

        for event in events {
            let event_type = Self::event_type_name(&event.kind);
            let event_data = serde_json::to_string(event).map_err(|e| {
                ProviderError::permanent(
                    "append_history",
                    format!("Event serialization error: {e}"),
                )
            })?;
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .bind(instance)
                .bind(execution_id as i64)
                .bind(event.event_id() as i64)
                .bind(event_type)
                .bind(event_data)
                .bind(created_at)
                .execute(&mut **tx)
                .await
                .map_err(|e| {
                    if is_duplicate_key_error(&e) {
                        ProviderError::permanent(
                            "ack_orchestration_item",
                            format!("Duplicate event_id {}: {e}", event.event_id()),
                        )
                    } else {
                        sqlx_to_provider_error("ack_orchestration_item", e)
                    }
                })?;
        }

        Ok(())
    }

    async fn try_claim_orchestration_instance(
        &self,
        instance_id: &str,
        lock_timeout: Duration,
        version_bounds: Option<(i64, i64)>,
    ) -> Result<Option<(OrchestrationItem, String, u32)>, ProviderError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| sqlx_to_provider_error("fetch_orchestration_item", e))?;

        // 1. Lock Order Step 1: Acquire/steal instance_locks row FIRST.
        let select_lock_sql = format!(
            "SELECT locked_until FROM {} WHERE instance_id = ? FOR UPDATE",
            self.tables.instance_locks
        );
        let existing_lock = sqlx::query(sqlx::AssertSqlSafe(select_lock_sql.as_str()))
            .bind(instance_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("fetch_orchestration_item", e))?;

        let now_ms = now_millis();
        let lock_token = Self::generate_lock_token();
        let locked_until = offset_millis(now_ms, lock_timeout);

        if let Some(lock_row) = existing_lock {
            let existing_until: i64 = lock_row
                .try_get("locked_until")
                .map_err(|e| sqlx_to_provider_error("fetch_orchestration_item", e))?;
            if existing_until > now_ms {
                let _ = tx.rollback().await;
                return Ok(None);
            }
            let steal_sql = format!(
                "UPDATE {} SET lock_token = ?, locked_until = ?, locked_at = ? WHERE instance_id = ?",
                self.tables.instance_locks
            );
            sqlx::query(sqlx::AssertSqlSafe(steal_sql.as_str()))
                .bind(&lock_token)
                .bind(locked_until)
                .bind(now_ms)
                .bind(instance_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| sqlx_to_provider_error("fetch_orchestration_item", e))?;
        } else {
            let insert_lock_sql = format!(
                "INSERT INTO {} (instance_id, lock_token, locked_until, locked_at) VALUES (?, ?, ?, ?)",
                self.tables.instance_locks
            );
            match sqlx::query(sqlx::AssertSqlSafe(insert_lock_sql.as_str()))
                .bind(instance_id)
                .bind(&lock_token)
                .bind(locked_until)
                .bind(now_ms)
                .execute(&mut *tx)
                .await
            {
                Ok(_) => {}
                Err(e) if is_duplicate_key_error(&e) || is_transient_conflict(&e) => {
                    let _ = tx.rollback().await;
                    return Ok(None);
                }
                Err(e) => {
                    let _ = tx.rollback().await;
                    return Err(sqlx_to_provider_error("fetch_orchestration_item", e));
                }
            }
        }

        // Re-verify capability filter under instance lock so concurrent metadata updates cannot slip by,
        // and incompatible instances are rolled back without leaving any lock or attempt increment.
        if let Some((min_packed, max_packed)) = version_bounds {
            let cap_sql = format!(
                "SELECT e.duroxide_version_major, e.duroxide_version_minor, e.duroxide_version_patch \
                 FROM {} i \
                 JOIN {} e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id \
                 WHERE i.instance_id = ?",
                self.tables.instances, self.tables.executions
            );
            if let Some(vrow) = sqlx::query(sqlx::AssertSqlSafe(cap_sql.as_str()))
                .bind(instance_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| sqlx_to_provider_error("fetch_orchestration_item", e))?
            {
                let major: Option<i64> = vrow.try_get("duroxide_version_major").ok().flatten();
                let minor: Option<i64> = vrow.try_get("duroxide_version_minor").ok().flatten();
                let patch: Option<i64> = vrow.try_get("duroxide_version_patch").ok().flatten();
                if let (Some(maj), Some(min), Some(pat)) = (major, minor, patch) {
                    let packed = maj * 1_000_000 + min * 1_000 + pat;
                    if packed < min_packed || packed > max_packed {
                        let _ = tx.rollback().await;
                        return Ok(None);
                    }
                }
            }
        }

        // 2. Lock Order Step 2: Tag all currently visible orchestrator_queue messages for this instance.
        let tag_msgs_sql = format!(
            "UPDATE {} SET lock_token = ?, locked_until = ?, attempt_count = attempt_count + 1 \
             WHERE instance_id = ? AND visible_at <= ?",
            self.tables.orchestrator_queue
        );
        let tagged = sqlx::query(sqlx::AssertSqlSafe(tag_msgs_sql.as_str()))
            .bind(&lock_token)
            .bind(locked_until)
            .bind(instance_id)
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("fetch_orchestration_item", e))?;

        if tagged.rows_affected() == 0 {
            let _ = tx.rollback().await;
            return Ok(None);
        }

        let fetch_msgs_sql = format!(
            "SELECT id, work_item, attempt_count FROM {} WHERE instance_id = ? AND lock_token = ? ORDER BY id ASC",
            self.tables.orchestrator_queue
        );
        let msg_rows = sqlx::query(sqlx::AssertSqlSafe(fetch_msgs_sql.as_str()))
            .bind(instance_id)
            .bind(&lock_token)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("fetch_orchestration_item", e))?;

        if msg_rows.is_empty() {
            let _ = tx.rollback().await;
            return Ok(None);
        }

        let mut max_attempt_count: u32 = 0;
        let mut work_items: Vec<WorkItem> = Vec::with_capacity(msg_rows.len());
        for row in &msg_rows {
            if let Ok(ac) = row.try_get::<i64, _>("attempt_count") {
                max_attempt_count = max_attempt_count.max(ac.max(0) as u32);
            }
            if let Ok(raw) = row.try_get::<String, _>("work_item")
                && let Ok(parsed) = serde_json::from_str::<WorkItem>(&raw)
            {
                work_items.push(parsed);
            }
        }

        if work_items.is_empty() {
            // Malformed queue entries: drop locked poison entries cleanly and release lock.
            let del_q_sql = format!(
                "DELETE FROM {} WHERE instance_id = ? AND lock_token = ?",
                self.tables.orchestrator_queue
            );
            sqlx::query(sqlx::AssertSqlSafe(del_q_sql.as_str()))
                .bind(instance_id)
                .bind(&lock_token)
                .execute(&mut *tx)
                .await
                .map_err(|e| sqlx_to_provider_error("fetch_orchestration_item", e))?;
            let del_lock_sql = format!(
                "DELETE FROM {} WHERE instance_id = ? AND lock_token = ?",
                self.tables.instance_locks
            );
            sqlx::query(sqlx::AssertSqlSafe(del_lock_sql.as_str()))
                .bind(instance_id)
                .bind(&lock_token)
                .execute(&mut *tx)
                .await
                .map_err(|e| sqlx_to_provider_error("fetch_orchestration_item", e))?;
            tx.commit()
                .await
                .map_err(|e| sqlx_to_provider_error("fetch_orchestration_item", e))?;
            return Ok(None);
        }

        // 3. Load instance metadata & history.
        let inst_sql = format!(
            "SELECT orchestration_name, orchestration_version, current_execution_id FROM {} WHERE instance_id = ?",
            self.tables.instances
        );
        let inst_row = sqlx::query(sqlx::AssertSqlSafe(inst_sql.as_str()))
            .bind(instance_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("fetch_orchestration_item", e))?;

        let (orchestration_name, orchestration_version, current_execution_id, history) =
            if let Some(info) = inst_row {
                let name: String = info.try_get("orchestration_name").map_err(|e| {
                    ProviderError::permanent(
                        "fetch_orchestration_item",
                        format!("Failed to decode orchestration_name: {e}"),
                    )
                })?;
                let version: Option<String> = info.try_get("orchestration_version").ok().flatten();
                let exec_id: i64 = info.try_get("current_execution_id").map_err(|e| {
                    ProviderError::permanent(
                        "fetch_orchestration_item",
                        format!("Failed to decode current_execution_id: {e}"),
                    )
                })?;

                match self
                    .read_history_in_tx(&mut tx, instance_id, Some(exec_id.max(1) as u64))
                    .await
                {
                    Ok(hist) => {
                        let resolved_ver = version.unwrap_or_else(|| "unknown".to_string());
                        (name, resolved_ver, exec_id.max(1) as u64, hist)
                    }
                    Err(err_str) => {
                        let error_msg = format!("Failed to deserialize history: {err_str}");
                        warn!(
                            instance = %instance_id,
                            error = %error_msg,
                            "History deserialization failed; returning item with history_error"
                        );
                        let resolved_ver = version.unwrap_or_else(|| "unknown".to_string());
                        tx.commit()
                            .await
                            .map_err(|e| sqlx_to_provider_error("fetch_orchestration_item", e))?;
                        return Ok(Some((
                            OrchestrationItem {
                                instance: instance_id.to_string(),
                                orchestration_name: name,
                                execution_id: exec_id.max(1) as u64,
                                version: resolved_ver,
                                history: Vec::new(),
                                messages: work_items,
                                history_error: Some(error_msg),
                                kv_snapshot: HashMap::new(),
                            },
                            lock_token,
                            max_attempt_count,
                        )));
                    }
                }
            } else {
                let hist = self
                    .read_history_in_tx(&mut tx, instance_id, None)
                    .await
                    .unwrap_or_default();

                if let Some((name, version)) = hist.iter().find_map(|e| {
                    if let EventKind::OrchestrationStarted { name, version, .. } = &e.kind {
                        Some((name.clone(), version.clone()))
                    } else {
                        None
                    }
                }) {
                    (name, version, 1u64, hist)
                } else if let Some((orchestration, version)) =
                    work_items.iter().find_map(|item| match item {
                        WorkItem::StartOrchestration {
                            orchestration,
                            version,
                            ..
                        }
                        | WorkItem::ContinueAsNew {
                            orchestration,
                            version,
                            ..
                        } => Some((orchestration.clone(), version.clone())),
                        _ => None,
                    })
                {
                    (
                        orchestration,
                        version.unwrap_or_else(|| "unknown".to_string()),
                        1u64,
                        Vec::new(),
                    )
                } else {
                    let all_queue_messages = work_items
                        .iter()
                        .all(|item| matches!(item, WorkItem::QueueMessage { .. }));
                    if all_queue_messages {
                        warn!(
                            instance = %instance_id,
                            message_count = work_items.len(),
                            "Dropping orphan queue messages enqueued before orchestration started"
                        );
                        let del_q_sql = format!(
                            "DELETE FROM {} WHERE instance_id = ? AND lock_token = ?",
                            self.tables.orchestrator_queue
                        );
                        sqlx::query(sqlx::AssertSqlSafe(del_q_sql.as_str()))
                            .bind(instance_id)
                            .bind(&lock_token)
                            .execute(&mut *tx)
                            .await
                            .map_err(|e| sqlx_to_provider_error("fetch_orchestration_item", e))?;
                        let del_lock_sql = format!(
                            "DELETE FROM {} WHERE instance_id = ? AND lock_token = ?",
                            self.tables.instance_locks
                        );
                        sqlx::query(sqlx::AssertSqlSafe(del_lock_sql.as_str()))
                            .bind(instance_id)
                            .bind(&lock_token)
                            .execute(&mut *tx)
                            .await
                            .map_err(|e| sqlx_to_provider_error("fetch_orchestration_item", e))?;
                        tx.commit()
                            .await
                            .map_err(|e| sqlx_to_provider_error("fetch_orchestration_item", e))?;
                        return Ok(None);
                    }

                    let _ = tx.rollback().await;
                    return Ok(None);
                }
            };

        let kv_snapshot = self.get_kv_snapshot_in_tx(&mut tx, instance_id).await?;

        tx.commit()
            .await
            .map_err(|e| sqlx_to_provider_error("fetch_orchestration_item", e))?;

        debug!(
            instance = %instance_id,
            messages = work_items.len(),
            history_len = history.len(),
            "Fetched orchestration item"
        );

        Ok(Some((
            OrchestrationItem {
                instance: instance_id.to_string(),
                orchestration_name,
                execution_id: current_execution_id,
                version: orchestration_version,
                history,
                messages: work_items,
                history_error: None,
                kv_snapshot,
            },
            lock_token,
            max_attempt_count,
        )))
    }

    async fn resolve_instance_by_lock_token(
        &self,
        operation: &str,
        lock_token: &str,
    ) -> Result<Option<String>, ProviderError> {
        let sql = format!(
            "SELECT instance_id FROM {} WHERE lock_token = ? LIMIT 1",
            self.tables.instance_locks
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(lock_token)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| sqlx_to_provider_error(operation, e))?;

        match row {
            Some(r) => {
                let id: String = r
                    .try_get("instance_id")
                    .map_err(|e| sqlx_to_provider_error(operation, e))?;
                Ok(Some(id))
            }
            None => Ok(None),
        }
    }

    async fn try_claim_worker_candidate(
        &self,
        id: i64,
        candidate_session_id: Option<&str>,
        lock_timeout: Duration,
        session_cfg: Option<&SessionFetchConfig>,
    ) -> Result<Option<(WorkItem, String, u32)>, ProviderError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| sqlx_to_provider_error("fetch_work_item", e))?;

        let now_ms = now_millis();

        // Lock Order: `sessions` FIRST (when session-bound), `worker_queue` SECOND.
        if let Some(sid) = candidate_session_id {
            let Some(cfg) = session_cfg else {
                let _ = tx.rollback().await;
                return Ok(None);
            };
            let session_locked_until = offset_millis(now_ms, cfg.lock_timeout);
            let select_sess_sql = format!(
                "SELECT worker_id, locked_until FROM {} WHERE session_id = ? FOR UPDATE",
                self.tables.sessions
            );
            let existing_sess = sqlx::query(sqlx::AssertSqlSafe(select_sess_sql.as_str()))
                .bind(sid)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| sqlx_to_provider_error("fetch_work_item", e))?;

            if let Some(srow) = existing_sess {
                let owner: String = srow
                    .try_get("worker_id")
                    .map_err(|e| sqlx_to_provider_error("fetch_work_item", e))?;
                let until: i64 = srow
                    .try_get("locked_until")
                    .map_err(|e| sqlx_to_provider_error("fetch_work_item", e))?;
                if until > now_ms && owner != cfg.owner_id {
                    let _ = tx.rollback().await;
                    return Ok(None);
                }
                let update_sess_sql = format!(
                    "UPDATE {} SET worker_id = ?, locked_until = ?, last_activity_at = ? WHERE session_id = ?",
                    self.tables.sessions
                );
                sqlx::query(sqlx::AssertSqlSafe(update_sess_sql.as_str()))
                    .bind(&cfg.owner_id)
                    .bind(session_locked_until)
                    .bind(now_ms)
                    .bind(sid)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| sqlx_to_provider_error("fetch_work_item", e))?;
            } else {
                let insert_sess_sql = format!(
                    "INSERT INTO {} (session_id, worker_id, locked_until, last_activity_at) VALUES (?, ?, ?, ?)",
                    self.tables.sessions
                );
                match sqlx::query(sqlx::AssertSqlSafe(insert_sess_sql.as_str()))
                    .bind(sid)
                    .bind(&cfg.owner_id)
                    .bind(session_locked_until)
                    .bind(now_ms)
                    .execute(&mut *tx)
                    .await
                {
                    Ok(_) => {}
                    Err(e) if is_duplicate_key_error(&e) || is_transient_conflict(&e) => {
                        let _ = tx.rollback().await;
                        return Ok(None);
                    }
                    Err(e) => {
                        let _ = tx.rollback().await;
                        return Err(sqlx_to_provider_error("fetch_work_item", e));
                    }
                }
            }
        }

        let lock_token = Self::generate_lock_token();
        let locked_until = offset_millis(now_ms, lock_timeout);

        let claim_sql = format!(
            "UPDATE {} \
             SET lock_token = ?, locked_until = ?, attempt_count = attempt_count + 1 \
             WHERE id = ? AND visible_at <= ? AND (lock_token IS NULL OR locked_until <= ?)",
            self.tables.worker_queue
        );
        let claim_res = match sqlx::query(sqlx::AssertSqlSafe(claim_sql.as_str()))
            .bind(&lock_token)
            .bind(locked_until)
            .bind(id)
            .bind(now_ms)
            .bind(now_ms)
            .execute(&mut *tx)
            .await
        {
            Ok(res) => res,
            Err(e) if is_transient_conflict(&e) => {
                let _ = tx.rollback().await;
                return Ok(None);
            }
            Err(e) => {
                let _ = tx.rollback().await;
                return Err(sqlx_to_provider_error("fetch_work_item", e));
            }
        };

        if claim_res.rows_affected() == 0 {
            let _ = tx.rollback().await;
            return Ok(None);
        }

        let read_claimed_sql = format!(
            "SELECT work_item, attempt_count FROM {} WHERE id = ? AND lock_token = ?",
            self.tables.worker_queue
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(read_claimed_sql.as_str()))
            .bind(id)
            .bind(&lock_token)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("fetch_work_item", e))?;

        let Some(row) = row else {
            let _ = tx.rollback().await;
            return Ok(None);
        };

        let raw_item: String = row
            .try_get("work_item")
            .map_err(|e| sqlx_to_provider_error("fetch_work_item", e))?;
        let attempt_count: i64 = row
            .try_get("attempt_count")
            .map_err(|e| sqlx_to_provider_error("fetch_work_item", e))?;

        let parsed: WorkItem = match serde_json::from_str(&raw_item) {
            Ok(item) => item,
            Err(e) => {
                let _ = tx.rollback().await;
                return Err(ProviderError::permanent(
                    "fetch_work_item",
                    format!("Deserialization error: {e}"),
                ));
            }
        };

        tx.commit()
            .await
            .map_err(|e| sqlx_to_provider_error("fetch_work_item", e))?;

        Ok(Some((parsed, lock_token, attempt_count.max(1) as u32)))
    }
}

#[async_trait::async_trait]
impl Provider for MysqlProvider {
    fn name(&self) -> &str {
        "mysql-tidb"
    }

    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn fetch_orchestration_item(
        &self,
        lock_timeout: Duration,
        _poll_timeout: Duration,
        filter: Option<&DispatcherCapabilityFilter>,
    ) -> Result<Option<(OrchestrationItem, String, u32)>, ProviderError> {
        let version_bounds = if let Some(cap_filter) = filter {
            match cap_filter.supported_duroxide_versions.first() {
                Some(range) => Some((Self::pack_semver(&range.min), Self::pack_semver(&range.max))),
                None => return Ok(None),
            }
        } else {
            None
        };

        let now_ms = now_millis();
        let batch_limit = self.candidate_batch_size;

        // Fetch distinct candidate instances ordered by their earliest visible message id.
        let candidates: Vec<String> = if let Some((min_packed, max_packed)) = version_bounds {
            let sql = format!(
                "SELECT q.instance_id, MIN(q.id) AS min_id \
                 FROM {} q \
                 LEFT JOIN {} il ON q.instance_id = il.instance_id \
                 LEFT JOIN {} i ON q.instance_id = i.instance_id \
                 LEFT JOIN {} e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id \
                 WHERE q.visible_at <= ? \
                   AND (il.instance_id IS NULL OR il.locked_until <= ?) \
                   AND ( \
                     e.duroxide_version_major IS NULL \
                     OR (e.duroxide_version_major * 1000000 + e.duroxide_version_minor * 1000 + e.duroxide_version_patch) BETWEEN ? AND ? \
                   ) \
                 GROUP BY q.instance_id \
                 ORDER BY min_id ASC \
                 LIMIT ?",
                self.tables.orchestrator_queue,
                self.tables.instance_locks,
                self.tables.instances,
                self.tables.executions
            );
            let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .bind(now_ms)
                .bind(now_ms)
                .bind(min_packed)
                .bind(max_packed)
                .bind(batch_limit)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| sqlx_to_provider_error("fetch_orchestration_item", e))?;
            rows.iter()
                .filter_map(|r| r.try_get::<String, _>("instance_id").ok())
                .collect()
        } else {
            let sql = format!(
                "SELECT q.instance_id, MIN(q.id) AS min_id \
                 FROM {} q \
                 LEFT JOIN {} il ON q.instance_id = il.instance_id \
                 WHERE q.visible_at <= ? \
                   AND (il.instance_id IS NULL OR il.locked_until <= ?) \
                 GROUP BY q.instance_id \
                 ORDER BY min_id ASC \
                 LIMIT ?",
                self.tables.orchestrator_queue, self.tables.instance_locks
            );
            let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .bind(now_ms)
                .bind(now_ms)
                .bind(batch_limit)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| sqlx_to_provider_error("fetch_orchestration_item", e))?;
            rows.iter()
                .filter_map(|r| r.try_get::<String, _>("instance_id").ok())
                .collect()
        };

        for candidate in candidates {
            if let Some(claimed) = self
                .try_claim_orchestration_instance(&candidate, lock_timeout, version_bounds)
                .await?
            {
                return Ok(Some(claimed));
            }
        }

        Ok(None)
    }

    async fn ack_orchestration_item(
        &self,
        lock_token: &str,
        execution_id: u64,
        history_delta: Vec<Event>,
        worker_items: Vec<WorkItem>,
        orchestrator_items: Vec<WorkItem>,
        metadata: ExecutionMetadata,
        cancelled_activities: Vec<ScheduledActivityIdentifier>,
    ) -> Result<(), ProviderError> {
        let Some(instance_id) = self
            .resolve_instance_by_lock_token("ack_orchestration_item", lock_token)
            .await?
        else {
            return Err(ProviderError::permanent(
                "ack_orchestration_item",
                "Invalid lock token",
            ));
        };

        for attempt in 0..MAX_TX_RETRIES {
            let res = self
                .ack_orchestration_item_once(
                    &instance_id,
                    lock_token,
                    execution_id,
                    &history_delta,
                    &worker_items,
                    &orchestrator_items,
                    &metadata,
                    &cancelled_activities,
                )
                .await;
            match res {
                Ok(()) => return Ok(()),
                Err(e) if e.is_retryable() && attempt + 1 < MAX_TX_RETRIES => {
                    tokio::time::sleep(Duration::from_millis(2 * (attempt as u64 + 1))).await;
                }
                Err(e) => return Err(e),
            }
        }
        Err(ProviderError::retryable(
            "ack_orchestration_item",
            "Transient database conflict persisted across retries",
        ))
    }

    async fn abandon_orchestration_item(
        &self,
        lock_token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        let Some(instance_id) = self
            .resolve_instance_by_lock_token("abandon_orchestration_item", lock_token)
            .await?
        else {
            return Err(ProviderError::permanent(
                "abandon_orchestration_item",
                "Invalid lock token",
            ));
        };

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| sqlx_to_provider_error("abandon_orchestration_item", e))?;

        // Step 1: Lock & delete instance_locks row FIRST.
        let lock_sql = format!(
            "SELECT lock_token FROM {} WHERE instance_id = ? AND lock_token = ? FOR UPDATE",
            self.tables.instance_locks
        );
        let held = sqlx::query(sqlx::AssertSqlSafe(lock_sql.as_str()))
            .bind(&instance_id)
            .bind(lock_token)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("abandon_orchestration_item", e))?;

        if held.is_none() {
            let _ = tx.rollback().await;
            return Err(ProviderError::permanent(
                "abandon_orchestration_item",
                "Invalid lock token",
            ));
        }

        // Step 2: Update only the orchestrator_queue rows tagged by this fetch, releasing their lock_token.
        match (ignore_attempt, delay) {
            (true, Some(d)) => {
                let visible_at = timestamp_after(d);
                let sql = format!(
                    "UPDATE {} SET lock_token = NULL, locked_until = NULL, visible_at = ?, \
                     attempt_count = IF(attempt_count > 0, attempt_count - 1, 0) \
                     WHERE instance_id = ? AND lock_token = ?",
                    self.tables.orchestrator_queue
                );
                sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(visible_at)
                    .bind(&instance_id)
                    .bind(lock_token)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| sqlx_to_provider_error("abandon_orchestration_item", e))?;
            }
            (true, None) => {
                let sql = format!(
                    "UPDATE {} SET lock_token = NULL, locked_until = NULL, \
                     attempt_count = IF(attempt_count > 0, attempt_count - 1, 0) \
                     WHERE instance_id = ? AND lock_token = ?",
                    self.tables.orchestrator_queue
                );
                sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(&instance_id)
                    .bind(lock_token)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| sqlx_to_provider_error("abandon_orchestration_item", e))?;
            }
            (false, Some(d)) => {
                let visible_at = timestamp_after(d);
                let sql = format!(
                    "UPDATE {} SET lock_token = NULL, locked_until = NULL, visible_at = ? \
                     WHERE instance_id = ? AND lock_token = ?",
                    self.tables.orchestrator_queue
                );
                sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(visible_at)
                    .bind(&instance_id)
                    .bind(lock_token)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| sqlx_to_provider_error("abandon_orchestration_item", e))?;
            }
            (false, None) => {
                let sql = format!(
                    "UPDATE {} SET lock_token = NULL, locked_until = NULL \
                     WHERE instance_id = ? AND lock_token = ?",
                    self.tables.orchestrator_queue
                );
                sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(&instance_id)
                    .bind(lock_token)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| sqlx_to_provider_error("abandon_orchestration_item", e))?;
            }
        }

        let del_lock_sql = format!(
            "DELETE FROM {} WHERE instance_id = ? AND lock_token = ?",
            self.tables.instance_locks
        );
        let removed = sqlx::query(sqlx::AssertSqlSafe(del_lock_sql.as_str()))
            .bind(&instance_id)
            .bind(lock_token)
            .execute(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("abandon_orchestration_item", e))?;

        if removed.rows_affected() == 0 {
            let _ = tx.rollback().await;
            return Err(ProviderError::permanent(
                "abandon_orchestration_item",
                "Invalid lock token",
            ));
        }

        tx.commit()
            .await
            .map_err(|e| sqlx_to_provider_error("abandon_orchestration_item", e))?;

        Ok(())
    }

    async fn renew_orchestration_item_lock(
        &self,
        token: &str,
        extend_for: Duration,
    ) -> Result<(), ProviderError> {
        let Some(instance_id) = self
            .resolve_instance_by_lock_token("renew_orchestration_item_lock", token)
            .await?
        else {
            return Err(ProviderError::permanent(
                "renew_orchestration_item_lock",
                "Lock token invalid, expired, or already acked",
            ));
        };

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| sqlx_to_provider_error("renew_orchestration_item_lock", e))?;

        // Step 1: Lock `instance_locks` FIRST, then read `now_ms` after waiting for lock.
        let lock_sql = format!(
            "SELECT locked_until FROM {} WHERE instance_id = ? AND lock_token = ? FOR UPDATE",
            self.tables.instance_locks
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(lock_sql.as_str()))
            .bind(&instance_id)
            .bind(token)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("renew_orchestration_item_lock", e))?;

        let now_ms = now_millis();
        let Some(row) = row else {
            let _ = tx.rollback().await;
            return Err(ProviderError::permanent(
                "renew_orchestration_item_lock",
                "Lock token invalid, expired, or already acked",
            ));
        };

        let current_until: i64 = row
            .try_get("locked_until")
            .map_err(|e| sqlx_to_provider_error("renew_orchestration_item_lock", e))?;

        if current_until <= now_ms {
            let _ = tx.rollback().await;
            return Err(ProviderError::permanent(
                "renew_orchestration_item_lock",
                "Lock token invalid, expired, or already acked",
            ));
        }

        // Never shorten an existing lease.
        let new_until = current_until.max(offset_millis(now_ms, extend_for));

        let update_il_sql = format!(
            "UPDATE {} SET locked_until = ? WHERE instance_id = ? AND lock_token = ?",
            self.tables.instance_locks
        );
        let updated = sqlx::query(sqlx::AssertSqlSafe(update_il_sql.as_str()))
            .bind(new_until)
            .bind(&instance_id)
            .bind(token)
            .execute(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("renew_orchestration_item_lock", e))?;

        if updated.rows_affected() == 0 {
            let _ = tx.rollback().await;
            return Err(ProviderError::permanent(
                "renew_orchestration_item_lock",
                "Lock token invalid, expired, or already acked",
            ));
        }

        // Step 2: Extend matching orchestrator_queue rows SECOND.
        let update_oq_sql = format!(
            "UPDATE {} SET locked_until = ? WHERE instance_id = ? AND lock_token = ?",
            self.tables.orchestrator_queue
        );
        sqlx::query(sqlx::AssertSqlSafe(update_oq_sql.as_str()))
            .bind(new_until)
            .bind(&instance_id)
            .bind(token)
            .execute(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("renew_orchestration_item_lock", e))?;

        tx.commit()
            .await
            .map_err(|e| sqlx_to_provider_error("renew_orchestration_item_lock", e))?;

        Ok(())
    }

    async fn read(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
        let exec_sql = format!(
            "SELECT COALESCE(MAX(execution_id), 1) FROM {} WHERE instance_id = ?",
            self.tables.executions
        );
        let exec_id: i64 = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(exec_sql.as_str()))
            .bind(instance)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| sqlx_to_provider_error("read", e))?;

        self.read_with_execution(instance, exec_id.max(1) as u64)
            .await
    }

    async fn read_with_execution(
        &self,
        instance: &str,
        execution_id: u64,
    ) -> Result<Vec<Event>, ProviderError> {
        let sql = format!(
            "SELECT event_data FROM {} WHERE instance_id = ? AND execution_id = ? ORDER BY event_id ASC",
            self.tables.history
        );
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(instance)
            .bind(execution_id as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| sqlx_to_provider_error("read_with_execution", e))?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let raw: String = row.try_get("event_data").map_err(|e| {
                ProviderError::permanent(
                    "read_with_execution",
                    format!("Failed to read event_data: {e}"),
                )
            })?;
            let event: Event = serde_json::from_str(&raw).map_err(|e| {
                ProviderError::permanent(
                    "read_with_execution",
                    format!("Failed to deserialize event: {e}"),
                )
            })?;
            events.push(event);
        }
        Ok(events)
    }

    async fn append_with_execution(
        &self,
        instance: &str,
        execution_id: u64,
        new_events: Vec<Event>,
    ) -> Result<(), ProviderError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| sqlx_to_provider_error("append_with_execution", e))?;

        self.append_history_in_tx(&mut tx, instance, execution_id, &new_events)
            .await?;

        tx.commit()
            .await
            .map_err(|e| sqlx_to_provider_error("append_with_execution", e))?;
        Ok(())
    }

    async fn enqueue_for_orchestrator(
        &self,
        item: WorkItem,
        delay: Option<Duration>,
    ) -> Result<(), ProviderError> {
        let Some(instance) = Self::extract_orchestrator_instance(&item) else {
            return Err(ProviderError::permanent(
                "enqueue_for_orchestrator",
                "Invalid work item type for orchestrator queue",
            ));
        };

        let raw = serde_json::to_string(&item).map_err(|e| {
            ProviderError::permanent(
                "enqueue_for_orchestrator",
                format!("Serialization error: {e}"),
            )
        })?;

        let now_ms = now_millis();
        let visible_at = match (&item, delay) {
            (_, Some(d)) => offset_millis(now_ms, d),
            (WorkItem::TimerFired { fire_at_ms, .. }, None) => *fire_at_ms as i64,
            (_, None) => now_ms,
        };
        let id = next_seq_id();

        let sql = format!(
            "INSERT INTO {} (id, instance_id, work_item, visible_at, created_at) VALUES (?, ?, ?, ?, ?)",
            self.tables.orchestrator_queue
        );
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(id)
            .bind(instance)
            .bind(raw)
            .bind(visible_at)
            .bind(now_ms)
            .execute(&self.pool)
            .await
            .map_err(|e| sqlx_to_provider_error("enqueue_for_orchestrator", e))?;

        Ok(())
    }

    async fn enqueue_for_worker(&self, item: WorkItem) -> Result<(), ProviderError> {
        let (activity_instance, activity_execution_id, activity_id, session_id, tag) = match &item {
            WorkItem::ActivityExecute {
                instance,
                execution_id,
                id,
                session_id,
                tag,
                ..
            } => (
                Some(instance.as_str()),
                Some(*execution_id as i64),
                Some(*id as i64),
                session_id.as_deref(),
                tag.as_deref(),
            ),
            _ => (None, None, None, None, None),
        };

        let raw = serde_json::to_string(&item).map_err(|e| {
            ProviderError::permanent("enqueue_for_worker", format!("Serialization error: {e}"))
        })?;
        let now_ms = now_millis();
        let id = next_seq_id();

        let sql = format!(
            "INSERT INTO {} (id, work_item, visible_at, created_at, instance_id, execution_id, activity_id, session_id, tag) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            self.tables.worker_queue
        );
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(id)
            .bind(raw)
            .bind(now_ms)
            .bind(now_ms)
            .bind(activity_instance)
            .bind(activity_execution_id)
            .bind(activity_id)
            .bind(session_id)
            .bind(tag)
            .execute(&self.pool)
            .await
            .map_err(|e| sqlx_to_provider_error("enqueue_for_worker", e))?;

        Ok(())
    }

    async fn fetch_work_item(
        &self,
        lock_timeout: Duration,
        _poll_timeout: Duration,
        session: Option<&SessionFetchConfig>,
        tag_filter: &TagFilter,
    ) -> Result<Option<(WorkItem, String, u32)>, ProviderError> {
        if matches!(tag_filter, TagFilter::None) {
            return Ok(None);
        }

        let now_ms = now_millis();
        let (tag_clause, tag_values) = Self::build_tag_clause(tag_filter);
        let batch_limit = self.candidate_batch_size;

        let candidate_rows = if let Some(cfg) = session {
            let sql = format!(
                "SELECT q.id, q.session_id \
                 FROM {} q \
                 LEFT JOIN {} s ON s.session_id = q.session_id AND s.locked_until > ? \
                 WHERE q.visible_at <= ? \
                   AND (q.lock_token IS NULL OR q.locked_until <= ?) \
                   AND (q.session_id IS NULL OR s.worker_id = ? OR s.session_id IS NULL) \
                   AND ({tag_clause}) \
                 ORDER BY q.id ASC \
                 LIMIT ?",
                self.tables.worker_queue, self.tables.sessions
            );
            let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .bind(now_ms)
                .bind(now_ms)
                .bind(now_ms)
                .bind(&cfg.owner_id);
            for tag in &tag_values {
                query = query.bind(tag.as_str());
            }
            query = query.bind(batch_limit);
            query
                .fetch_all(&self.pool)
                .await
                .map_err(|e| sqlx_to_provider_error("fetch_work_item", e))?
        } else {
            let sql = format!(
                "SELECT q.id, q.session_id \
                 FROM {} q \
                 WHERE q.visible_at <= ? \
                   AND (q.lock_token IS NULL OR q.locked_until <= ?) \
                   AND q.session_id IS NULL \
                   AND ({tag_clause}) \
                 ORDER BY q.id ASC \
                 LIMIT ?",
                self.tables.worker_queue
            );
            let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .bind(now_ms)
                .bind(now_ms);
            for tag in &tag_values {
                query = query.bind(tag.as_str());
            }
            query = query.bind(batch_limit);
            query
                .fetch_all(&self.pool)
                .await
                .map_err(|e| sqlx_to_provider_error("fetch_work_item", e))?
        };

        for row in candidate_rows {
            let Ok(id) = row.try_get::<i64, _>("id") else {
                continue;
            };
            let sess_id: Option<String> = row.try_get("session_id").ok().flatten();
            if let Some(claimed) = self
                .try_claim_worker_candidate(id, sess_id.as_deref(), lock_timeout, session)
                .await?
            {
                return Ok(Some(claimed));
            }
        }

        Ok(None)
    }

    async fn ack_work_item(
        &self,
        token: &str,
        completion: Option<WorkItem>,
    ) -> Result<(), ProviderError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| sqlx_to_provider_error("ack_work_item", e))?;

        let find_sql = format!(
            "SELECT id, session_id, locked_until FROM {} WHERE lock_token = ? FOR UPDATE",
            self.tables.worker_queue
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(find_sql.as_str()))
            .bind(token)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("ack_work_item", e))?;

        let now_ms = now_millis();
        let Some(row) = row else {
            let _ = tx.rollback().await;
            return Err(ProviderError::permanent(
                "ack_work_item",
                "Activity was cancelled or lock expired (worker queue row not found)",
            ));
        };

        let id: i64 = row
            .try_get("id")
            .map_err(|e| sqlx_to_provider_error("ack_work_item", e))?;
        let session_id: Option<String> = row.try_get("session_id").ok().flatten();
        let locked_until: Option<i64> = row.try_get("locked_until").ok().flatten();

        if locked_until.unwrap_or(0) <= now_ms {
            let _ = tx.rollback().await;
            return Err(ProviderError::permanent(
                "ack_work_item",
                "Activity lock expired before acknowledgement",
            ));
        }

        let del_sql = format!(
            "DELETE FROM {} WHERE id = ? AND lock_token = ?",
            self.tables.worker_queue
        );
        let del_res = sqlx::query(sqlx::AssertSqlSafe(del_sql.as_str()))
            .bind(id)
            .bind(token)
            .execute(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("ack_work_item", e))?;

        if del_res.rows_affected() == 0 {
            let _ = tx.rollback().await;
            return Err(ProviderError::permanent(
                "ack_work_item",
                "Activity was cancelled or lock expired",
            ));
        }

        if let Some(comp) = completion {
            let instance = match &comp {
                WorkItem::ActivityCompleted { instance, .. }
                | WorkItem::ActivityFailed { instance, .. } => instance.as_str(),
                _ => {
                    let _ = tx.rollback().await;
                    return Err(ProviderError::permanent(
                        "ack_work_item",
                        "Invalid completion type for worker ack",
                    ));
                }
            };
            let raw = serde_json::to_string(&comp).map_err(|e| {
                ProviderError::permanent("ack_work_item", format!("Serialization error: {e}"))
            })?;
            let qid = next_seq_id();
            let ins_sql = format!(
                "INSERT INTO {} (id, instance_id, work_item, visible_at, created_at) VALUES (?, ?, ?, ?, ?)",
                self.tables.orchestrator_queue
            );
            sqlx::query(sqlx::AssertSqlSafe(ins_sql.as_str()))
                .bind(qid)
                .bind(instance)
                .bind(raw)
                .bind(now_ms)
                .bind(now_ms)
                .execute(&mut *tx)
                .await
                .map_err(|e| sqlx_to_provider_error("ack_work_item", e))?;
        }

        tx.commit()
            .await
            .map_err(|e| sqlx_to_provider_error("ack_work_item", e))?;

        // Best-effort session activity timestamp update AFTER commit so we never invert
        // the `sessions -> worker_queue` transaction lock order.
        if let Some(sid) = session_id {
            let sess_sql = format!(
                "UPDATE {} SET last_activity_at = ? WHERE session_id = ? AND locked_until > ?",
                self.tables.sessions
            );
            let _ = sqlx::query(sqlx::AssertSqlSafe(sess_sql.as_str()))
                .bind(now_ms)
                .bind(&sid)
                .bind(now_ms)
                .execute(&self.pool)
                .await;
        }

        Ok(())
    }

    async fn renew_work_item_lock(
        &self,
        token: &str,
        extend_for: Duration,
    ) -> Result<(), ProviderError> {
        let find_sql = format!(
            "SELECT id, session_id, locked_until FROM {} WHERE lock_token = ? LIMIT 1",
            self.tables.worker_queue
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(find_sql.as_str()))
            .bind(token)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| sqlx_to_provider_error("renew_work_item_lock", e))?;

        let Some(row) = row else {
            return Err(ProviderError::permanent(
                "renew_work_item_lock",
                "Lock renewal failed - activity was cancelled or lock expired",
            ));
        };

        let id: i64 = row
            .try_get("id")
            .map_err(|e| sqlx_to_provider_error("renew_work_item_lock", e))?;
        let session_id: Option<String> = row.try_get("session_id").ok().flatten();
        let current_until: i64 = row
            .try_get::<Option<i64>, _>("locked_until")
            .ok()
            .flatten()
            .unwrap_or(0);

        let now_ms = now_millis();
        if current_until <= now_ms {
            return Err(ProviderError::permanent(
                "renew_work_item_lock",
                "Lock renewal failed - lock expired",
            ));
        }

        let new_until = current_until.max(offset_millis(now_ms, extend_for));
        let upd_sql = format!(
            "UPDATE {} SET locked_until = ? WHERE id = ? AND lock_token = ? AND locked_until > ?",
            self.tables.worker_queue
        );
        let res = sqlx::query(sqlx::AssertSqlSafe(upd_sql.as_str()))
            .bind(new_until)
            .bind(id)
            .bind(token)
            .bind(now_ms)
            .execute(&self.pool)
            .await
            .map_err(|e| sqlx_to_provider_error("renew_work_item_lock", e))?;

        if res.rows_affected() == 0 {
            return Err(ProviderError::permanent(
                "renew_work_item_lock",
                "Lock renewal failed - activity was cancelled or lock expired",
            ));
        }

        if let Some(sid) = session_id {
            let sess_sql = format!(
                "UPDATE {} SET last_activity_at = ? WHERE session_id = ? AND locked_until > ?",
                self.tables.sessions
            );
            let _ = sqlx::query(sqlx::AssertSqlSafe(sess_sql.as_str()))
                .bind(now_ms)
                .bind(&sid)
                .bind(now_ms)
                .execute(&self.pool)
                .await;
        }

        Ok(())
    }

    async fn abandon_work_item(
        &self,
        token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        let now_ms = now_millis();
        let visible_at = match delay {
            Some(d) => offset_millis(now_ms, d),
            None => now_ms,
        };

        let sql = if ignore_attempt {
            format!(
                "UPDATE {} SET lock_token = NULL, locked_until = NULL, visible_at = ?, \
                 attempt_count = IF(attempt_count > 0, attempt_count - 1, 0) \
                 WHERE lock_token = ?",
                self.tables.worker_queue
            )
        } else {
            format!(
                "UPDATE {} SET lock_token = NULL, locked_until = NULL, visible_at = ? \
                 WHERE lock_token = ?",
                self.tables.worker_queue
            )
        };

        let res = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(visible_at)
            .bind(token)
            .execute(&self.pool)
            .await
            .map_err(|e| sqlx_to_provider_error("abandon_work_item", e))?;

        if res.rows_affected() == 0 {
            return Err(ProviderError::permanent(
                "abandon_work_item",
                "Invalid lock token or already acked",
            ));
        }

        Ok(())
    }

    async fn renew_session_lock(
        &self,
        owner_ids: &[&str],
        extend_for: Duration,
        idle_timeout: Duration,
    ) -> Result<usize, ProviderError> {
        if owner_ids.is_empty() {
            return Ok(0);
        }

        let now_ms = now_millis();
        let locked_until = offset_millis(now_ms, extend_for);
        let idle_ms = idle_timeout.as_millis().min(i64::MAX as u128) as i64;
        let idle_cutoff = now_ms.saturating_sub(idle_ms);

        let placeholders = vec!["?"; owner_ids.len()].join(", ");
        let sql = format!(
            "UPDATE {} SET locked_until = GREATEST(locked_until, ?) \
             WHERE worker_id IN ({placeholders}) \
               AND locked_until > ? \
               AND last_activity_at > ?",
            self.tables.sessions
        );

        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str())).bind(locked_until);
        for owner in owner_ids {
            query = query.bind(*owner);
        }
        query = query.bind(now_ms).bind(idle_cutoff);

        let res = query
            .execute(&self.pool)
            .await
            .map_err(|e| sqlx_to_provider_error("renew_session_lock", e))?;

        Ok(res.rows_affected() as usize)
    }

    async fn cleanup_orphaned_sessions(
        &self,
        _idle_timeout: Duration,
    ) -> Result<usize, ProviderError> {
        let now_ms = now_millis();
        let sql = format!(
            "DELETE s FROM {} s \
             LEFT JOIN {} wq ON wq.session_id = s.session_id \
             WHERE s.locked_until < ? AND wq.id IS NULL",
            self.tables.sessions, self.tables.worker_queue
        );

        let res = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(now_ms)
            .execute(&self.pool)
            .await
            .map_err(|e| sqlx_to_provider_error("cleanup_orphaned_sessions", e))?;

        Ok(res.rows_affected() as usize)
    }

    fn as_management_capability(&self) -> Option<&dyn ProviderAdmin> {
        Some(self as &dyn ProviderAdmin)
    }

    async fn get_custom_status(
        &self,
        instance: &str,
        last_seen_version: u64,
    ) -> Result<Option<(Option<String>, u64)>, ProviderError> {
        let sql = format!(
            "SELECT custom_status, custom_status_version FROM {} \
             WHERE instance_id = ? AND custom_status_version > ?",
            self.tables.instances
        );
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(instance)
            .bind(last_seen_version as i64)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| sqlx_to_provider_error("get_custom_status", e))?;

        match row {
            Some(r) => {
                let status: Option<String> = r.try_get("custom_status").ok().flatten();
                let ver: i64 = r.try_get("custom_status_version").unwrap_or(0);
                Ok(Some((status, ver.max(0) as u64)))
            }
            None => Ok(None),
        }
    }

    async fn get_kv_value(
        &self,
        instance: &str,
        key: &str,
    ) -> Result<Option<String>, ProviderError> {
        let delta_sql = format!(
            "SELECT value FROM {} WHERE instance_id = ? AND kv_key = ?",
            self.tables.kv_delta
        );
        if let Some(drow) = sqlx::query(sqlx::AssertSqlSafe(delta_sql.as_str()))
            .bind(instance)
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| sqlx_to_provider_error("get_kv_value", e))?
        {
            let value: Option<String> = drow
                .try_get("value")
                .map_err(|e| sqlx_to_provider_error("get_kv_value", e))?;
            return Ok(value);
        }

        let store_sql = format!(
            "SELECT value FROM {} WHERE instance_id = ? AND kv_key = ?",
            self.tables.kv_store
        );
        let srow = sqlx::query(sqlx::AssertSqlSafe(store_sql.as_str()))
            .bind(instance)
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| sqlx_to_provider_error("get_kv_value", e))?;

        match srow {
            Some(r) => {
                let value: String = r
                    .try_get("value")
                    .map_err(|e| sqlx_to_provider_error("get_kv_value", e))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    async fn get_kv_all_values(
        &self,
        instance: &str,
    ) -> Result<HashMap<String, String>, ProviderError> {
        let store_sql = format!(
            "SELECT kv_key, value FROM {} WHERE instance_id = ?",
            self.tables.kv_store
        );
        let store_rows = sqlx::query(sqlx::AssertSqlSafe(store_sql.as_str()))
            .bind(instance)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| sqlx_to_provider_error("get_kv_all_values", e))?;

        let mut map = HashMap::with_capacity(store_rows.len());
        for row in store_rows {
            let k: String = row
                .try_get("kv_key")
                .map_err(|e| sqlx_to_provider_error("get_kv_all_values", e))?;
            let v: String = row
                .try_get("value")
                .map_err(|e| sqlx_to_provider_error("get_kv_all_values", e))?;
            map.insert(k, v);
        }

        let delta_sql = format!(
            "SELECT kv_key, value FROM {} WHERE instance_id = ?",
            self.tables.kv_delta
        );
        let delta_rows = sqlx::query(sqlx::AssertSqlSafe(delta_sql.as_str()))
            .bind(instance)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| sqlx_to_provider_error("get_kv_all_values", e))?;

        for row in delta_rows {
            let k: String = row
                .try_get("kv_key")
                .map_err(|e| sqlx_to_provider_error("get_kv_all_values", e))?;
            let v: Option<String> = row
                .try_get("value")
                .map_err(|e| sqlx_to_provider_error("get_kv_all_values", e))?;
            match v {
                Some(val) => {
                    map.insert(k, val);
                }
                None => {
                    map.remove(&k);
                }
            }
        }

        Ok(map)
    }

    async fn get_instance_stats(
        &self,
        instance: &str,
    ) -> Result<Option<SystemStats>, ProviderError> {
        let inst_sql = format!(
            "SELECT current_execution_id FROM {} WHERE instance_id = ?",
            self.tables.instances
        );
        let Some(inst_row) = sqlx::query(sqlx::AssertSqlSafe(inst_sql.as_str()))
            .bind(instance)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| sqlx_to_provider_error("get_instance_stats", e))?
        else {
            return Ok(None);
        };

        let exec_id: i64 = inst_row
            .try_get("current_execution_id")
            .map_err(|e| sqlx_to_provider_error("get_instance_stats", e))?;

        let hist_sql = format!(
            "SELECT COUNT(*) AS cnt, CAST(COALESCE(SUM(OCTET_LENGTH(event_data)), 0) AS SIGNED) AS size_bytes \
             FROM {} WHERE instance_id = ? AND execution_id = ?",
            self.tables.history
        );
        let hist_row = sqlx::query(sqlx::AssertSqlSafe(hist_sql.as_str()))
            .bind(instance)
            .bind(exec_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| sqlx_to_provider_error("get_instance_stats", e))?;

        let history_event_count: i64 = hist_row
            .try_get("cnt")
            .map_err(|e| sqlx_to_provider_error("get_instance_stats", e))?;
        let history_size_bytes: i64 = hist_row
            .try_get("size_bytes")
            .map_err(|e| sqlx_to_provider_error("get_instance_stats", e))?;

        let merged_kv = self.get_kv_all_values(instance).await?;
        let kv_user_key_count = merged_kv.len() as u64;
        let kv_total_value_bytes: u64 = merged_kv.values().map(|v| v.len() as u64).sum();

        let first_event_sql = format!(
            "SELECT event_data FROM {} WHERE instance_id = ? AND execution_id = ? AND event_id = 1",
            self.tables.history
        );
        let first_row = sqlx::query(sqlx::AssertSqlSafe(first_event_sql.as_str()))
            .bind(instance)
            .bind(exec_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| sqlx_to_provider_error("get_instance_stats", e))?;

        let queue_pending_count = match first_row {
            Some(row) => {
                let raw: String = row
                    .try_get("event_data")
                    .map_err(|e| sqlx_to_provider_error("get_instance_stats", e))?;
                let event: Event = serde_json::from_str(&raw).map_err(|e| {
                    ProviderError::permanent(
                        "get_instance_stats",
                        format!("Failed to deserialize OrchestrationStarted event: {e}"),
                    )
                })?;
                match event.kind {
                    EventKind::OrchestrationStarted {
                        carry_forward_events: Some(ref events),
                        ..
                    } => events.len() as u64,
                    _ => 0,
                }
            }
            None => 0,
        };

        Ok(Some(SystemStats {
            history_event_count: history_event_count.max(0) as u64,
            history_size_bytes: history_size_bytes.max(0) as u64,
            queue_pending_count,
            kv_user_key_count,
            kv_total_value_bytes,
        }))
    }
}

impl MysqlProvider {
    #[allow(clippy::too_many_arguments)]
    async fn ack_orchestration_item_once(
        &self,
        instance_id: &str,
        lock_token: &str,
        execution_id: u64,
        history_delta: &[Event],
        worker_items: &[WorkItem],
        orchestrator_items: &[WorkItem],
        metadata: &ExecutionMetadata,
        cancelled_activities: &[ScheduledActivityIdentifier],
    ) -> Result<(), ProviderError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;

        // Step 1: Lock `instance_locks` FIRST and validate token + lease expiry after lock wait.
        let lock_sql = format!(
            "SELECT locked_until FROM {} WHERE instance_id = ? AND lock_token = ? FOR UPDATE",
            self.tables.instance_locks
        );
        let lock_row = sqlx::query(sqlx::AssertSqlSafe(lock_sql.as_str()))
            .bind(instance_id)
            .bind(lock_token)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;

        let now_ms = now_millis();
        let Some(lock_row) = lock_row else {
            let _ = tx.rollback().await;
            return Err(ProviderError::permanent(
                "ack_orchestration_item",
                "Invalid lock token",
            ));
        };
        let locked_until: i64 = lock_row
            .try_get("locked_until")
            .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;
        if locked_until <= now_ms {
            let _ = tx.rollback().await;
            return Err(ProviderError::permanent(
                "ack_orchestration_item",
                "Instance lock expired",
            ));
        }

        // Step 2: Delete consumed orchestrator_queue messages tagged with our lock_token.
        let del_q_sql = format!(
            "DELETE FROM {} WHERE instance_id = ? AND lock_token = ?",
            self.tables.orchestrator_queue
        );
        sqlx::query(sqlx::AssertSqlSafe(del_q_sql.as_str()))
            .bind(instance_id)
            .bind(lock_token)
            .execute(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;

        // Step 3: Create or update instance metadata when provided by runtime.
        if let Some(name) = &metadata.orchestration_name {
            let seq = next_seq_id();
            let upsert_inst_sql = format!(
                "INSERT INTO {} \
                 (instance_id, orchestration_name, orchestration_version, current_execution_id, created_at, updated_at, seq_id, parent_instance_id) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
                 ON DUPLICATE KEY UPDATE \
                 orchestration_name = ?, \
                 orchestration_version = COALESCE(?, orchestration_version), \
                 current_execution_id = GREATEST(current_execution_id, ?), \
                 updated_at = ?",
                self.tables.instances
            );
            sqlx::query(sqlx::AssertSqlSafe(upsert_inst_sql.as_str()))
                .bind(instance_id)
                .bind(name)
                .bind(metadata.orchestration_version.as_deref())
                .bind(execution_id as i64)
                .bind(now_ms)
                .bind(now_ms)
                .bind(seq)
                .bind(metadata.parent_instance_id.as_deref())
                .bind(name)
                .bind(metadata.orchestration_version.as_deref())
                .bind(execution_id as i64)
                .bind(now_ms)
                .execute(&mut *tx)
                .await
                .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;
        } else {
            let bump_inst_sql = format!(
                "UPDATE {} SET current_execution_id = GREATEST(current_execution_id, ?), updated_at = ? WHERE instance_id = ?",
                self.tables.instances
            );
            sqlx::query(sqlx::AssertSqlSafe(bump_inst_sql.as_str()))
                .bind(execution_id as i64)
                .bind(now_ms)
                .bind(instance_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;
        }

        // Step 4: Idempotently ensure execution row exists without overwriting existing status.
        let ensure_exec_sql = format!(
            "INSERT INTO {} (instance_id, execution_id, status, started_at) \
             VALUES (?, ?, 'Running', ?) \
             ON DUPLICATE KEY UPDATE instance_id = instance_id",
            self.tables.executions
        );
        sqlx::query(sqlx::AssertSqlSafe(ensure_exec_sql.as_str()))
            .bind(instance_id)
            .bind(execution_id as i64)
            .bind(now_ms)
            .execute(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;

        if let Some(pinned) = &metadata.pinned_duroxide_version {
            let pin_sql = format!(
                "UPDATE {} SET duroxide_version_major = ?, duroxide_version_minor = ?, duroxide_version_patch = ? \
                 WHERE instance_id = ? AND execution_id = ?",
                self.tables.executions
            );
            sqlx::query(sqlx::AssertSqlSafe(pin_sql.as_str()))
                .bind(pinned.major as i64)
                .bind(pinned.minor as i64)
                .bind(pinned.patch as i64)
                .bind(instance_id)
                .bind(execution_id as i64)
                .execute(&mut *tx)
                .await
                .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;
        }

        // Step 5: Append history_delta events (duplicate event_id fails & rolls back entire turn).
        self.append_history_in_tx(&mut tx, instance_id, execution_id, history_delta)
            .await?;

        // Step 6: Update execution status/output when provided in metadata.
        if let Some(status) = &metadata.status {
            let is_terminal =
                status == "Completed" || status == "Failed" || status == "ContinuedAsNew";
            if is_terminal {
                let status_sql = format!(
                    "UPDATE {} SET status = ?, output = ?, completed_at = ? \
                     WHERE instance_id = ? AND execution_id = ?",
                    self.tables.executions
                );
                sqlx::query(sqlx::AssertSqlSafe(status_sql.as_str()))
                    .bind(status)
                    .bind(metadata.output.as_deref())
                    .bind(now_ms)
                    .bind(instance_id)
                    .bind(execution_id as i64)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;
            } else {
                let status_sql = format!(
                    "UPDATE {} SET status = ?, output = ? \
                     WHERE instance_id = ? AND execution_id = ?",
                    self.tables.executions
                );
                sqlx::query(sqlx::AssertSqlSafe(status_sql.as_str()))
                    .bind(status)
                    .bind(metadata.output.as_deref())
                    .bind(instance_id)
                    .bind(execution_id as i64)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;
            }
        }

        // Step 7: Materialize custom_status updates from history_delta.
        let custom_status_from_delta = history_delta.iter().rev().find_map(|e| match &e.kind {
            EventKind::CustomStatusUpdated { status } => Some(status.clone()),
            _ => None,
        });
        match custom_status_from_delta {
            Some(Some(cs)) => {
                let cs_sql = format!(
                    "UPDATE {} SET custom_status = ?, custom_status_version = custom_status_version + 1, updated_at = ? \
                     WHERE instance_id = ?",
                    self.tables.instances
                );
                sqlx::query(sqlx::AssertSqlSafe(cs_sql.as_str()))
                    .bind(cs)
                    .bind(now_ms)
                    .bind(instance_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;
            }
            Some(None) => {
                let cs_sql = format!(
                    "UPDATE {} SET custom_status = NULL, custom_status_version = custom_status_version + 1, updated_at = ? \
                     WHERE instance_id = ?",
                    self.tables.instances
                );
                sqlx::query(sqlx::AssertSqlSafe(cs_sql.as_str()))
                    .bind(now_ms)
                    .bind(instance_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;
            }
            None => {}
        }

        // Step 8: Materialize KV mutations into kv_delta, then merge into kv_store on terminal turn.
        for event in history_delta {
            match &event.kind {
                EventKind::KeyValueSet {
                    key,
                    value,
                    last_updated_at_ms,
                } => {
                    let kv_sql = format!(
                        "INSERT INTO {} (instance_id, kv_key, value, last_updated_at_ms) \
                         VALUES (?, ?, ?, ?) \
                         ON DUPLICATE KEY UPDATE value = ?, last_updated_at_ms = ?",
                        self.tables.kv_delta
                    );
                    let ts = *last_updated_at_ms as i64;
                    sqlx::query(sqlx::AssertSqlSafe(kv_sql.as_str()))
                        .bind(instance_id)
                        .bind(key)
                        .bind(value)
                        .bind(ts)
                        .bind(value)
                        .bind(ts)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;
                }
                EventKind::KeyValueCleared { key } => {
                    let kv_sql = format!(
                        "INSERT INTO {} (instance_id, kv_key, value, last_updated_at_ms) \
                         VALUES (?, ?, NULL, ?) \
                         ON DUPLICATE KEY UPDATE value = NULL, last_updated_at_ms = ?",
                        self.tables.kv_delta
                    );
                    sqlx::query(sqlx::AssertSqlSafe(kv_sql.as_str()))
                        .bind(instance_id)
                        .bind(key)
                        .bind(now_ms)
                        .bind(now_ms)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;
                }
                EventKind::KeyValuesCleared => {
                    let upd_delta_sql = format!(
                        "UPDATE {} SET value = NULL, last_updated_at_ms = ? WHERE instance_id = ?",
                        self.tables.kv_delta
                    );
                    sqlx::query(sqlx::AssertSqlSafe(upd_delta_sql.as_str()))
                        .bind(now_ms)
                        .bind(instance_id)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;

                    let store_keys_sql = format!(
                        "SELECT kv_key FROM {} WHERE instance_id = ?",
                        self.tables.kv_store
                    );
                    let store_keys = sqlx::query(sqlx::AssertSqlSafe(store_keys_sql.as_str()))
                        .bind(instance_id)
                        .fetch_all(&mut *tx)
                        .await
                        .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;

                    let ins_tomb_sql = format!(
                        "INSERT INTO {} (instance_id, kv_key, value, last_updated_at_ms) \
                         VALUES (?, ?, NULL, ?) \
                         ON DUPLICATE KEY UPDATE instance_id = instance_id",
                        self.tables.kv_delta
                    );
                    for krow in store_keys {
                        let k: String = krow
                            .try_get("kv_key")
                            .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;
                        sqlx::query(sqlx::AssertSqlSafe(ins_tomb_sql.as_str()))
                            .bind(instance_id)
                            .bind(k)
                            .bind(now_ms)
                            .execute(&mut *tx)
                            .await
                            .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;
                    }
                }
                _ => {}
            }
        }

        let is_terminal = metadata
            .status
            .as_deref()
            .is_some_and(|s| s == "Completed" || s == "ContinuedAsNew" || s == "Failed");
        if is_terminal {
            let read_delta_sql = format!(
                "SELECT kv_key, value, last_updated_at_ms FROM {} WHERE instance_id = ?",
                self.tables.kv_delta
            );
            let delta_rows = sqlx::query(sqlx::AssertSqlSafe(read_delta_sql.as_str()))
                .bind(instance_id)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;

            let upsert_store_sql = format!(
                "INSERT INTO {} (instance_id, kv_key, value, last_updated_at_ms) \
                 VALUES (?, ?, ?, ?) \
                 ON DUPLICATE KEY UPDATE value = ?, last_updated_at_ms = ?",
                self.tables.kv_store
            );
            let del_store_sql = format!(
                "DELETE FROM {} WHERE instance_id = ? AND kv_key = ?",
                self.tables.kv_store
            );

            for drow in delta_rows {
                let k: String = drow
                    .try_get("kv_key")
                    .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;
                let v: Option<String> = drow
                    .try_get("value")
                    .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;
                let ts: i64 = drow
                    .try_get("last_updated_at_ms")
                    .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;

                if let Some(val) = v {
                    sqlx::query(sqlx::AssertSqlSafe(upsert_store_sql.as_str()))
                        .bind(instance_id)
                        .bind(&k)
                        .bind(&val)
                        .bind(ts)
                        .bind(&val)
                        .bind(ts)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;
                } else {
                    sqlx::query(sqlx::AssertSqlSafe(del_store_sql.as_str()))
                        .bind(instance_id)
                        .bind(&k)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;
                }
            }

            let clear_delta_sql =
                format!("DELETE FROM {} WHERE instance_id = ?", self.tables.kv_delta);
            sqlx::query(sqlx::AssertSqlSafe(clear_delta_sql.as_str()))
                .bind(instance_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;
        }

        // Step 9: Enqueue worker_items BEFORE deleting cancelled_activities (critical ordering).
        if !worker_items.is_empty() {
            let ins_wq_sql = format!(
                "INSERT INTO {} (id, work_item, visible_at, created_at, instance_id, execution_id, activity_id, session_id, tag) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                self.tables.worker_queue
            );
            for item in worker_items {
                let (act_inst, act_exec, act_id, sess_id, tag) = match item {
                    WorkItem::ActivityExecute {
                        instance,
                        execution_id,
                        id,
                        session_id,
                        tag,
                        ..
                    } => (
                        Some(instance.as_str()),
                        Some(*execution_id as i64),
                        Some(*id as i64),
                        session_id.as_deref(),
                        tag.as_deref(),
                    ),
                    _ => (None, None, None, None, None),
                };
                let raw = serde_json::to_string(item).map_err(|e| {
                    ProviderError::permanent(
                        "ack_orchestration_item",
                        format!("Serialization error: {e}"),
                    )
                })?;
                sqlx::query(sqlx::AssertSqlSafe(ins_wq_sql.as_str()))
                    .bind(next_seq_id())
                    .bind(raw)
                    .bind(now_ms)
                    .bind(now_ms)
                    .bind(act_inst)
                    .bind(act_exec)
                    .bind(act_id)
                    .bind(sess_id)
                    .bind(tag)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;
            }
        }

        // Step 10: Delete cancelled_activities via multi-column tuple IN clause.
        if !cancelled_activities.is_empty() {
            let tuples = vec!["(?, ?, ?)"; cancelled_activities.len()].join(", ");
            let cancel_sql = format!(
                "DELETE FROM {} WHERE (instance_id, execution_id, activity_id) IN ({tuples})",
                self.tables.worker_queue
            );
            let mut query = sqlx::query(sqlx::AssertSqlSafe(cancel_sql.as_str()));
            for act in cancelled_activities {
                query = query
                    .bind(&act.instance)
                    .bind(act.execution_id as i64)
                    .bind(act.activity_id as i64);
            }
            query
                .execute(&mut *tx)
                .await
                .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;
        }

        // Step 11: Enqueue follow-up orchestrator_items.
        if !orchestrator_items.is_empty() {
            let ins_oq_sql = format!(
                "INSERT INTO {} (id, instance_id, work_item, visible_at, created_at) VALUES (?, ?, ?, ?, ?)",
                self.tables.orchestrator_queue
            );
            for item in orchestrator_items {
                let Some(target_instance) = Self::extract_orchestrator_instance(item) else {
                    continue;
                };
                let raw = serde_json::to_string(item).map_err(|e| {
                    ProviderError::permanent(
                        "ack_orchestration_item",
                        format!("Serialization error: {e}"),
                    )
                })?;
                let visible_at = match item {
                    WorkItem::TimerFired { fire_at_ms, .. } => *fire_at_ms as i64,
                    _ => now_ms,
                };
                sqlx::query(sqlx::AssertSqlSafe(ins_oq_sql.as_str()))
                    .bind(next_seq_id())
                    .bind(target_instance)
                    .bind(raw)
                    .bind(visible_at)
                    .bind(now_ms)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;
            }
        }

        // Step 12: Re-verify lease did not expire during turn commit and delete lock row.
        let final_now = now_millis();
        let del_lock_sql = format!(
            "DELETE FROM {} WHERE instance_id = ? AND lock_token = ? AND locked_until > ?",
            self.tables.instance_locks
        );
        let del_lock = sqlx::query(sqlx::AssertSqlSafe(del_lock_sql.as_str()))
            .bind(instance_id)
            .bind(lock_token)
            .bind(final_now)
            .execute(&mut *tx)
            .await
            .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;

        if del_lock.rows_affected() == 0 {
            let _ = tx.rollback().await;
            return Err(ProviderError::permanent(
                "ack_orchestration_item",
                "Instance lock expired",
            ));
        }

        tx.commit()
            .await
            .map_err(|e| sqlx_to_provider_error("ack_orchestration_item", e))?;

        Ok(())
    }
}
