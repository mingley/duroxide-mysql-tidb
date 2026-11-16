//! Pure-Rust, zero-C in-memory [`Provider`] and [`ProviderAdmin`] implementation.
//!
//! Enabled via the optional `memory` Cargo feature for fast unit and integration testing
//! without requiring a running MySQL, TiDB, or SQLite instance.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use duroxide::providers::{
    DeleteInstanceResult, DispatcherCapabilityFilter, ExecutionInfo, ExecutionMetadata,
    InstanceFilter, InstanceInfo, KvEntry, OrchestrationItem, Provider, ProviderAdmin,
    ProviderError, PruneOptions, PruneResult, QueueDepths, ScheduledActivityIdentifier,
    SessionFetchConfig, SystemMetrics, TagFilter, WorkItem,
};
use duroxide::{Event, EventKind, SystemStats};
use tracing::{debug, warn};

use crate::id::{next_seq_id, now_millis, offset_millis, timestamp_after};
use crate::provider::MysqlProvider;

const DEFAULT_BULK_LIMIT: u32 = 1_000;

#[derive(Debug, Clone)]
struct InstanceRecord {
    instance_id: String,
    orchestration_name: String,
    orchestration_version: Option<String>,
    current_execution_id: u64,
    custom_status: Option<String>,
    custom_status_version: u64,
    created_at: i64,
    updated_at: i64,
    seq_id: i64,
    parent_instance_id: Option<String>,
}

#[derive(Debug, Clone)]
struct ExecutionRecord {
    execution_id: u64,
    status: String,
    output: Option<String>,
    pinned_version: Option<semver::Version>,
    started_at: i64,
    completed_at: Option<i64>,
}

#[derive(Debug, Clone)]
struct OrchestratorQueueEntry {
    instance_id: String,
    work_item: String,
    visible_at: i64,
    lock_token: Option<String>,
    locked_until: Option<i64>,
    attempt_count: u32,
}

#[derive(Debug, Clone)]
struct WorkerQueueEntry {
    work_item: String,
    visible_at: i64,
    lock_token: Option<String>,
    locked_until: Option<i64>,
    attempt_count: u32,
    instance_id: Option<String>,
    execution_id: Option<u64>,
    activity_id: Option<u64>,
    session_id: Option<String>,
    tag: Option<String>,
}

#[derive(Debug, Clone)]
struct InstanceLock {
    lock_token: String,
    locked_until: i64,
}

#[derive(Debug, Clone)]
struct SessionRecord {
    session_id: String,
    worker_id: String,
    locked_until: i64,
    last_activity_at: i64,
}

#[derive(Debug, Clone)]
struct KvDeltaEntry {
    value: Option<String>,
    last_updated_at_ms: u64,
}

type PreparedWorkerItem = (
    String,
    Option<String>,
    Option<u64>,
    Option<u64>,
    Option<String>,
    Option<String>,
);

#[derive(Debug, Default)]
struct MemoryState {
    instances: HashMap<String, InstanceRecord>,
    executions: HashMap<String, BTreeMap<u64, ExecutionRecord>>,
    history: HashMap<String, BTreeMap<u64, BTreeMap<u64, String>>>,
    orchestrator_queue: BTreeMap<i64, OrchestratorQueueEntry>,
    worker_queue: BTreeMap<i64, WorkerQueueEntry>,
    instance_locks: HashMap<String, InstanceLock>,
    sessions: HashMap<String, SessionRecord>,
    kv_store: HashMap<String, HashMap<String, KvEntry>>,
    kv_delta: HashMap<String, HashMap<String, KvDeltaEntry>>,
}

/// Pure-Rust, zero-C in-memory storage provider for [`duroxide`].
///
/// `MemoryProvider` implements the full [`Provider`] and [`ProviderAdmin`] contracts
/// in memory using an `Arc<Mutex<...>>`, making cloning cheap while sharing state across
/// dispatchers and clients within a process.
///
/// Enable the `memory` Cargo feature (optionally with `default-features = false` for a
/// zero-C build) to use this type:
///
/// ```toml
/// [dev-dependencies]
/// duroxide-mysql-tidb = { version = "0.1", default-features = false, features = ["memory"] }
/// ```
///
/// # Example
///
/// ```rust
/// use std::sync::Arc;
/// use std::time::Duration;
/// use duroxide::{
///     ActivityContext, Client, OrchestrationContext, OrchestrationRegistry, OrchestrationStatus,
/// };
/// use duroxide::runtime::{self, registry::ActivityRegistry};
/// use duroxide_mysql_tidb::MemoryProvider;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let store = Arc::new(MemoryProvider::new());
///
/// let activities = ActivityRegistry::builder()
///     .register("Greet", |_ctx: ActivityContext, name: String| async move {
///         Ok(format!("Hello, {name}!"))
///     })
///     .build();
///
/// let orchestrations = OrchestrationRegistry::builder()
///     .register("HelloFlow", |ctx: OrchestrationContext, name: String| async move {
///         ctx.schedule_activity("Greet", name).await
///     })
///     .build();
///
/// let rt = runtime::Runtime::start_with_store(store.clone(), activities, orchestrations).await;
/// let client = Client::new(store);
///
/// client.start_orchestration("mem-1", "HelloFlow", "World").await?;
/// let status = client.wait_for_orchestration("mem-1", Duration::from_secs(5)).await?;
/// assert!(matches!(status, OrchestrationStatus::Completed { .. }));
///
/// rt.shutdown(None).await;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default)]
pub struct MemoryProvider {
    state: Arc<Mutex<MemoryState>>,
}

impl MemoryProvider {
    /// Creates a new, empty in-memory Duroxide storage provider.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn lock_state(&self) -> MutexGuard<'_, MemoryState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn generate_lock_token() -> String {
        format!("lock_{}", uuid::Uuid::new_v4())
    }

    fn deserialize_history_map(
        instance: &str,
        exec_id: u64,
        events_map: Option<&BTreeMap<u64, String>>,
    ) -> Result<Vec<Event>, String> {
        let Some(events_map) = events_map else {
            return Ok(Vec::new());
        };
        let mut events = Vec::with_capacity(events_map.len());
        for (idx, raw) in events_map.values().enumerate() {
            let event: Event = serde_json::from_str(raw).map_err(|e| {
                format!(
                    "Failed to deserialize history event at position {idx} for instance '{instance}' execution {exec_id}: {e}"
                )
            })?;
            events.push(event);
        }
        Ok(events)
    }

    fn validate_and_serialize_events(
        operation: &str,
        existing: Option<&BTreeMap<u64, String>>,
        events: &[Event],
    ) -> Result<Vec<(u64, String)>, ProviderError> {
        let mut seen = HashSet::with_capacity(events.len());
        let mut prepared = Vec::with_capacity(events.len());
        for event in events {
            let event_id = event.event_id();
            if event_id == 0 {
                return Err(ProviderError::permanent(
                    "append_history",
                    "event_id must be set by runtime (got 0)",
                ));
            }
            if existing.is_some_and(|m| m.contains_key(&event_id)) || !seen.insert(event_id) {
                return Err(ProviderError::permanent(
                    operation,
                    format!("Duplicate event_id {event_id}"),
                ));
            }
            let raw = serde_json::to_string(event).map_err(|e| {
                ProviderError::permanent(operation, format!("Event serialization error: {e}"))
            })?;
            prepared.push((event_id, raw));
        }
        Ok(prepared)
    }

    fn prepare_worker_item(
        operation: &str,
        item: &WorkItem,
    ) -> Result<PreparedWorkerItem, ProviderError> {
        let (instance_id, execution_id, activity_id, session_id, tag) = match item {
            WorkItem::ActivityExecute {
                instance,
                execution_id,
                id,
                session_id,
                tag,
                ..
            } => (
                Some(instance.clone()),
                Some(*execution_id),
                Some(*id),
                session_id.clone(),
                tag.clone(),
            ),
            _ => (None, None, None, None, None),
        };
        let raw = serde_json::to_string(item).map_err(|e| {
            ProviderError::permanent(operation, format!("Serialization error: {e}"))
        })?;
        Ok((raw, instance_id, execution_id, activity_id, session_id, tag))
    }

    fn read_merged_kv(state: &MemoryState, instance: &str) -> HashMap<String, String> {
        let mut map: HashMap<String, String> = state
            .kv_store
            .get(instance)
            .map(|s| {
                s.iter()
                    .map(|(k, v)| (k.clone(), v.value.clone()))
                    .collect()
            })
            .unwrap_or_default();

        if let Some(delta) = state.kv_delta.get(instance) {
            for (k, entry) in delta {
                match &entry.value {
                    Some(val) => {
                        map.insert(k.clone(), val.clone());
                    }
                    None => {
                        map.remove(k);
                    }
                }
            }
        }
        map
    }

    fn prune_instance_locked(
        state: &mut MemoryState,
        instance_id: &str,
        options: &PruneOptions,
    ) -> Result<PruneResult, ProviderError> {
        let Some(inst) = state.instances.get(instance_id) else {
            return Err(ProviderError::permanent(
                "prune_executions",
                format!("Instance {instance_id} not found"),
            ));
        };
        let current_exec_id = inst.current_execution_id;

        let Some(exec_map) = state.executions.get(instance_id) else {
            return Ok(PruneResult {
                instances_processed: 1,
                ..Default::default()
            });
        };

        let keep_n = options.keep_last.unwrap_or(0) as usize;
        let mut to_prune = Vec::new();
        for (idx, (&exec_id, exec)) in exec_map.iter().rev().enumerate() {
            if exec_id == current_exec_id || exec.status == "Running" || idx < keep_n {
                continue;
            }
            if let Some(cutoff) = options.completed_before {
                let Some(done_at) = exec.completed_at else {
                    continue;
                };
                if done_at >= cutoff as i64 {
                    continue;
                }
            }
            to_prune.push(exec_id);
        }

        if to_prune.is_empty() {
            return Ok(PruneResult {
                instances_processed: 1,
                ..Default::default()
            });
        }

        let mut events_deleted: u64 = 0;
        if let Some(hist_map) = state.history.get_mut(instance_id) {
            for exec_id in &to_prune {
                if let Some(events) = hist_map.remove(exec_id) {
                    events_deleted += events.len() as u64;
                }
            }
        }

        let mut executions_deleted: u64 = 0;
        if let Some(exec_map) = state.executions.get_mut(instance_id) {
            for exec_id in &to_prune {
                if exec_map.remove(exec_id).is_some() {
                    executions_deleted += 1;
                }
            }
        }

        Ok(PruneResult {
            instances_processed: 1,
            executions_deleted,
            events_deleted,
        })
    }

    fn delete_instances_atomic_locked(
        state: &mut MemoryState,
        ids: &[String],
        force: bool,
    ) -> Result<DeleteInstanceResult, ProviderError> {
        if ids.is_empty() {
            return Ok(DeleteInstanceResult::default());
        }

        let mut sorted_ids: Vec<String> = ids.to_vec();
        sorted_ids.sort();
        sorted_ids.dedup();
        let id_set: HashSet<&str> = sorted_ids.iter().map(String::as_str).collect();

        if !force {
            for id in &sorted_ids {
                if let Some(inst) = state.instances.get(id.as_str()) {
                    let status = state
                        .executions
                        .get(id.as_str())
                        .and_then(|em| em.get(&inst.current_execution_id))
                        .map(|e| e.status.as_str());
                    if status == Some("Running") {
                        return Err(ProviderError::permanent(
                            "delete_instances_atomic",
                            format!(
                                "Instance {id} is still running. Use force=true to delete anyway, or cancel first."
                            ),
                        ));
                    }
                }
            }
        }

        for inst in state.instances.values() {
            if let Some(ref parent_id) = inst.parent_instance_id
                && id_set.contains(parent_id.as_str())
                && !id_set.contains(inst.instance_id.as_str())
            {
                return Err(ProviderError::permanent(
                    "delete_instances_atomic",
                    format!(
                        "Cannot delete: instance {parent_id} has child {} created after tree traversal.",
                        inst.instance_id
                    ),
                ));
            }
        }

        let mut result = DeleteInstanceResult::default();

        for id in &sorted_ids {
            state.instance_locks.remove(id.as_str());
        }

        let before_oq = state.orchestrator_queue.len();
        state
            .orchestrator_queue
            .retain(|_, q| !id_set.contains(q.instance_id.as_str()));
        let oq_deleted = (before_oq - state.orchestrator_queue.len()) as u64;

        let before_wq = state.worker_queue.len();
        state.worker_queue.retain(|_, q| {
            !q.instance_id
                .as_deref()
                .is_some_and(|inst_id| id_set.contains(inst_id))
        });
        let wq_deleted = (before_wq - state.worker_queue.len()) as u64;
        result.queue_messages_deleted = oq_deleted + wq_deleted;

        for id in &sorted_ids {
            if let Some(hist_execs) = state.history.remove(id.as_str()) {
                result.events_deleted += hist_execs.values().map(|em| em.len() as u64).sum::<u64>();
            }
            if let Some(exec_map) = state.executions.remove(id.as_str()) {
                result.executions_deleted += exec_map.len() as u64;
            }
            state.kv_store.remove(id.as_str());
            state.kv_delta.remove(id.as_str());
            if state.instances.remove(id.as_str()).is_some() {
                result.instances_deleted += 1;
            }
        }

        Ok(result)
    }

    #[doc(hidden)]
    pub fn corrupt_instance_history(&self, instance: &str) {
        let mut state = self.lock_state();
        if let Some(execs) = state.history.get_mut(instance) {
            for events in execs.values_mut() {
                for raw in events.values_mut() {
                    *raw = "NOT_VALID_JSON{{{".to_string();
                }
            }
        }
    }

    #[doc(hidden)]
    #[must_use]
    pub fn get_max_attempt_count(&self, instance: &str) -> u32 {
        let state = self.lock_state();
        state
            .orchestrator_queue
            .values()
            .filter(|q| q.instance_id == instance)
            .map(|q| q.attempt_count)
            .max()
            .unwrap_or(0)
    }
}

#[async_trait::async_trait]
impl Provider for MemoryProvider {
    fn name(&self) -> &str {
        "memory"
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
        let version_range = if let Some(cap_filter) = filter {
            match cap_filter.supported_duroxide_versions.first() {
                Some(range) => Some(range),
                None => return Ok(None),
            }
        } else {
            None
        };

        let mut state = self.lock_state();
        let now_ms = now_millis();

        let mut seen = HashSet::new();
        let mut candidates = Vec::new();
        for entry in state.orchestrator_queue.values() {
            if entry.visible_at > now_ms {
                continue;
            }
            if !seen.insert(entry.instance_id.clone()) {
                continue;
            }
            if state
                .instance_locks
                .get(&entry.instance_id)
                .is_some_and(|il| il.locked_until > now_ms)
            {
                continue;
            }
            if let Some(range) = version_range
                && let Some(inst) = state.instances.get(&entry.instance_id)
                && let Some(pinned) = state
                    .executions
                    .get(&entry.instance_id)
                    .and_then(|em| em.get(&inst.current_execution_id))
                    .and_then(|e| e.pinned_version.as_ref())
                && !range.contains(pinned)
            {
                continue;
            }
            candidates.push(entry.instance_id.clone());
        }

        for instance_id in candidates {
            let visible_ids: Vec<i64> = state
                .orchestrator_queue
                .iter()
                .filter(|(_, q)| q.instance_id == instance_id && q.visible_at <= now_ms)
                .map(|(&id, _)| id)
                .collect();

            if visible_ids.is_empty() {
                continue;
            }

            let mut work_items = Vec::with_capacity(visible_ids.len());
            for id in &visible_ids {
                if let Some(q) = state.orchestrator_queue.get(id)
                    && let Ok(parsed) = serde_json::from_str::<WorkItem>(&q.work_item)
                {
                    work_items.push(parsed);
                }
            }

            if work_items.is_empty() {
                for id in &visible_ids {
                    state.orchestrator_queue.remove(id);
                }
                state.instance_locks.remove(&instance_id);
                continue;
            }

            if !state.instances.contains_key(&instance_id) {
                let max_exec = state
                    .executions
                    .get(&instance_id)
                    .and_then(|em| em.keys().next_back().copied())
                    .unwrap_or(1);
                let hist = Self::deserialize_history_map(
                    &instance_id,
                    max_exec,
                    state
                        .history
                        .get(&instance_id)
                        .and_then(|hm| hm.get(&max_exec)),
                )
                .unwrap_or_default();

                let resolved_unregistered = if let Some((name, version)) =
                    hist.iter().find_map(|e| {
                        if let EventKind::OrchestrationStarted { name, version, .. } = &e.kind {
                            Some((name.clone(), version.clone()))
                        } else {
                            None
                        }
                    }) {
                    Some((name, version, 1u64, hist))
                } else {
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
                        } => Some((
                            orchestration.clone(),
                            version.clone().unwrap_or_else(|| "unknown".to_string()),
                            1u64,
                            Vec::new(),
                        )),
                        _ => None,
                    })
                };

                let Some((orchestration_name, orchestration_version, exec_id, history)) =
                    resolved_unregistered
                else {
                    let all_queue_messages = work_items
                        .iter()
                        .all(|item| matches!(item, WorkItem::QueueMessage { .. }));
                    if all_queue_messages {
                        warn!(
                            instance = %instance_id,
                            message_count = work_items.len(),
                            "Dropping orphan queue messages enqueued before orchestration started"
                        );
                        for id in &visible_ids {
                            state.orchestrator_queue.remove(id);
                        }
                        state.instance_locks.remove(&instance_id);
                    }
                    continue;
                };

                let lock_token = Self::generate_lock_token();
                let locked_until = offset_millis(now_ms, lock_timeout);
                state.instance_locks.insert(
                    instance_id.clone(),
                    InstanceLock {
                        lock_token: lock_token.clone(),
                        locked_until,
                    },
                );

                let mut max_attempt_count: u32 = 0;
                for id in &visible_ids {
                    if let Some(q) = state.orchestrator_queue.get_mut(id) {
                        q.lock_token = Some(lock_token.clone());
                        q.locked_until = Some(locked_until);
                        q.attempt_count = q.attempt_count.saturating_add(1);
                        max_attempt_count = max_attempt_count.max(q.attempt_count);
                    }
                }

                let kv_snapshot = state
                    .kv_store
                    .get(&instance_id)
                    .cloned()
                    .unwrap_or_default();

                return Ok(Some((
                    OrchestrationItem {
                        instance: instance_id,
                        orchestration_name,
                        execution_id: exec_id,
                        version: orchestration_version,
                        history,
                        messages: work_items,
                        history_error: None,
                        kv_snapshot,
                    },
                    lock_token,
                    max_attempt_count,
                )));
            }

            let Some(inst) = state.instances.get(&instance_id) else {
                continue;
            };
            let orchestration_name = inst.orchestration_name.clone();
            let orchestration_version = inst
                .orchestration_version
                .clone()
                .unwrap_or_else(|| "unknown".to_string());
            let current_execution_id = inst.current_execution_id.max(1);

            let lock_token = Self::generate_lock_token();
            let locked_until = offset_millis(now_ms, lock_timeout);
            state.instance_locks.insert(
                instance_id.clone(),
                InstanceLock {
                    lock_token: lock_token.clone(),
                    locked_until,
                },
            );

            let mut max_attempt_count: u32 = 0;
            for id in &visible_ids {
                if let Some(q) = state.orchestrator_queue.get_mut(id) {
                    q.lock_token = Some(lock_token.clone());
                    q.locked_until = Some(locked_until);
                    q.attempt_count = q.attempt_count.saturating_add(1);
                    max_attempt_count = max_attempt_count.max(q.attempt_count);
                }
            }

            let history_result = Self::deserialize_history_map(
                &instance_id,
                current_execution_id,
                state
                    .history
                    .get(&instance_id)
                    .and_then(|hm| hm.get(&current_execution_id)),
            );

            match history_result {
                Ok(history) => {
                    let kv_snapshot = state
                        .kv_store
                        .get(&instance_id)
                        .cloned()
                        .unwrap_or_default();
                    debug!(
                        instance = %instance_id,
                        messages = work_items.len(),
                        history_len = history.len(),
                        "Fetched orchestration item"
                    );
                    return Ok(Some((
                        OrchestrationItem {
                            instance: instance_id,
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
                    )));
                }
                Err(err_str) => {
                    let error_msg = format!("Failed to deserialize history: {err_str}");
                    warn!(
                        instance = %instance_id,
                        error = %error_msg,
                        "History deserialization failed; returning item with history_error"
                    );
                    return Ok(Some((
                        OrchestrationItem {
                            instance: instance_id,
                            orchestration_name,
                            execution_id: current_execution_id,
                            version: orchestration_version,
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
        let mut state = self.lock_state();
        let now_ms = now_millis();

        let Some((instance_id, locked_until)) = state
            .instance_locks
            .iter()
            .find(|(_, il)| il.lock_token == lock_token)
            .map(|(inst_id, il)| (inst_id.clone(), il.locked_until))
        else {
            return Err(ProviderError::permanent(
                "ack_orchestration_item",
                "Invalid lock token",
            ));
        };

        if locked_until <= now_ms {
            return Err(ProviderError::permanent(
                "ack_orchestration_item",
                "Instance lock expired",
            ));
        }

        let existing_events = state
            .history
            .get(&instance_id)
            .and_then(|hm| hm.get(&execution_id));
        let prepared_events = Self::validate_and_serialize_events(
            "ack_orchestration_item",
            existing_events,
            &history_delta,
        )?;

        let mut prepared_worker_items = Vec::with_capacity(worker_items.len());
        for item in &worker_items {
            prepared_worker_items.push(Self::prepare_worker_item("ack_orchestration_item", item)?);
        }

        let mut prepared_orchestrator_items = Vec::with_capacity(orchestrator_items.len());
        for item in &orchestrator_items {
            let Some(target_instance) = MysqlProvider::extract_orchestrator_instance(item) else {
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
            prepared_orchestrator_items.push((target_instance.to_string(), raw, visible_at));
        }

        // All validations succeeded; mutate state atomically.
        state.orchestrator_queue.retain(|_, q| {
            !(q.instance_id == instance_id && q.lock_token.as_deref() == Some(lock_token))
        });

        if let Some(name) = &metadata.orchestration_name {
            if let Some(inst) = state.instances.get_mut(&instance_id) {
                inst.orchestration_name.clone_from(name);
                if let Some(ver) = &metadata.orchestration_version {
                    inst.orchestration_version = Some(ver.clone());
                }
                inst.current_execution_id = inst.current_execution_id.max(execution_id);
                inst.updated_at = now_ms;
            } else {
                state.instances.insert(
                    instance_id.clone(),
                    InstanceRecord {
                        instance_id: instance_id.clone(),
                        orchestration_name: name.clone(),
                        orchestration_version: metadata.orchestration_version.clone(),
                        current_execution_id: execution_id,
                        custom_status: None,
                        custom_status_version: 0,
                        created_at: now_ms,
                        updated_at: now_ms,
                        seq_id: next_seq_id(),
                        parent_instance_id: metadata.parent_instance_id.clone(),
                    },
                );
            }
        } else if let Some(inst) = state.instances.get_mut(&instance_id) {
            inst.current_execution_id = inst.current_execution_id.max(execution_id);
            inst.updated_at = now_ms;
        }

        {
            let exec_map = state.executions.entry(instance_id.clone()).or_default();
            let exec = exec_map
                .entry(execution_id)
                .or_insert_with(|| ExecutionRecord {
                    execution_id,
                    status: "Running".to_string(),
                    output: None,
                    pinned_version: None,
                    started_at: now_ms,
                    completed_at: None,
                });
            if let Some(pinned) = &metadata.pinned_duroxide_version {
                exec.pinned_version = Some(pinned.clone());
            }
            if let Some(status) = &metadata.status {
                let is_terminal =
                    status == "Completed" || status == "Failed" || status == "ContinuedAsNew";
                exec.status.clone_from(status);
                exec.output.clone_from(&metadata.output);
                if is_terminal {
                    exec.completed_at = Some(now_ms);
                }
            }
        }

        if !prepared_events.is_empty() {
            let events_map = state
                .history
                .entry(instance_id.clone())
                .or_default()
                .entry(execution_id)
                .or_default();
            for (eid, raw) in prepared_events {
                events_map.insert(eid, raw);
            }
        }

        let custom_status_from_delta = history_delta.iter().rev().find_map(|e| match &e.kind {
            EventKind::CustomStatusUpdated { status } => Some(status.clone()),
            _ => None,
        });
        if let Some(cs_update) = custom_status_from_delta
            && let Some(inst) = state.instances.get_mut(&instance_id)
        {
            inst.custom_status = cs_update;
            inst.custom_status_version = inst.custom_status_version.saturating_add(1);
            inst.updated_at = now_ms;
        }

        let now_u64 = now_ms.max(0) as u64;
        for event in &history_delta {
            match &event.kind {
                EventKind::KeyValueSet {
                    key,
                    value,
                    last_updated_at_ms,
                } => {
                    state
                        .kv_delta
                        .entry(instance_id.clone())
                        .or_default()
                        .insert(
                            key.clone(),
                            KvDeltaEntry {
                                value: Some(value.clone()),
                                last_updated_at_ms: *last_updated_at_ms,
                            },
                        );
                }
                EventKind::KeyValueCleared { key } => {
                    state
                        .kv_delta
                        .entry(instance_id.clone())
                        .or_default()
                        .insert(
                            key.clone(),
                            KvDeltaEntry {
                                value: None,
                                last_updated_at_ms: now_u64,
                            },
                        );
                }
                EventKind::KeyValuesCleared => {
                    let store_keys: Vec<String> = state
                        .kv_store
                        .get(&instance_id)
                        .map(|s| s.keys().cloned().collect())
                        .unwrap_or_default();
                    let delta = state.kv_delta.entry(instance_id.clone()).or_default();
                    for entry in delta.values_mut() {
                        entry.value = None;
                        entry.last_updated_at_ms = now_u64;
                    }
                    for key in store_keys {
                        delta.entry(key).or_insert(KvDeltaEntry {
                            value: None,
                            last_updated_at_ms: now_u64,
                        });
                    }
                }
                _ => {}
            }
        }

        let is_terminal = metadata
            .status
            .as_deref()
            .is_some_and(|s| s == "Completed" || s == "ContinuedAsNew" || s == "Failed");
        if is_terminal && let Some(delta_entries) = state.kv_delta.remove(&instance_id) {
            let store = state.kv_store.entry(instance_id.clone()).or_default();
            for (k, d_entry) in delta_entries {
                if let Some(val) = d_entry.value {
                    store.insert(
                        k,
                        KvEntry {
                            value: val,
                            last_updated_at_ms: d_entry.last_updated_at_ms,
                        },
                    );
                } else {
                    store.remove(&k);
                }
            }
            if store.is_empty() {
                state.kv_store.remove(&instance_id);
            }
        }

        for (raw, act_inst, act_exec, act_id, sess_id, tag) in prepared_worker_items {
            state.worker_queue.insert(
                next_seq_id(),
                WorkerQueueEntry {
                    work_item: raw,
                    visible_at: now_ms,
                    lock_token: None,
                    locked_until: None,
                    attempt_count: 0,
                    instance_id: act_inst,
                    execution_id: act_exec,
                    activity_id: act_id,
                    session_id: sess_id,
                    tag,
                },
            );
        }

        if !cancelled_activities.is_empty() {
            state.worker_queue.retain(|_, q| {
                let Some(ref q_inst) = q.instance_id else {
                    return true;
                };
                let Some(q_exec) = q.execution_id else {
                    return true;
                };
                let Some(q_act) = q.activity_id else {
                    return true;
                };
                !cancelled_activities.iter().any(|c| {
                    c.instance == *q_inst && c.execution_id == q_exec && c.activity_id == q_act
                })
            });
        }

        for (target_instance, raw, visible_at) in prepared_orchestrator_items {
            state.orchestrator_queue.insert(
                next_seq_id(),
                OrchestratorQueueEntry {
                    instance_id: target_instance,
                    work_item: raw,
                    visible_at,
                    lock_token: None,
                    locked_until: None,
                    attempt_count: 0,
                },
            );
        }

        state.instance_locks.remove(&instance_id);
        Ok(())
    }

    async fn abandon_orchestration_item(
        &self,
        lock_token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        let mut state = self.lock_state();
        let Some(instance_id) = state
            .instance_locks
            .iter()
            .find(|(_, il)| il.lock_token == lock_token)
            .map(|(inst_id, _)| inst_id.clone())
        else {
            return Err(ProviderError::permanent(
                "abandon_orchestration_item",
                "Invalid lock token",
            ));
        };

        let delayed_visible_at = delay.map(timestamp_after);
        for q in state.orchestrator_queue.values_mut() {
            if q.instance_id == instance_id && q.lock_token.as_deref() == Some(lock_token) {
                q.lock_token = None;
                q.locked_until = None;
                if let Some(vis) = delayed_visible_at {
                    q.visible_at = vis;
                }
                if ignore_attempt {
                    q.attempt_count = q.attempt_count.saturating_sub(1);
                }
            }
        }

        state.instance_locks.remove(&instance_id);
        Ok(())
    }

    async fn renew_orchestration_item_lock(
        &self,
        token: &str,
        extend_for: Duration,
    ) -> Result<(), ProviderError> {
        let mut state = self.lock_state();
        let now_ms = now_millis();

        let Some((instance_id, current_until)) = state
            .instance_locks
            .iter()
            .find(|(_, il)| il.lock_token == token)
            .map(|(inst_id, il)| (inst_id.clone(), il.locked_until))
        else {
            return Err(ProviderError::permanent(
                "renew_orchestration_item_lock",
                "Lock token invalid, expired, or already acked",
            ));
        };

        if current_until <= now_ms {
            return Err(ProviderError::permanent(
                "renew_orchestration_item_lock",
                "Lock token invalid, expired, or already acked",
            ));
        }

        let new_until = current_until.max(offset_millis(now_ms, extend_for));
        if let Some(il) = state.instance_locks.get_mut(&instance_id) {
            il.locked_until = new_until;
        }
        for q in state.orchestrator_queue.values_mut() {
            if q.instance_id == instance_id && q.lock_token.as_deref() == Some(token) {
                q.locked_until = Some(new_until);
            }
        }

        Ok(())
    }

    async fn read(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
        let state = self.lock_state();
        let exec_id = state
            .executions
            .get(instance)
            .and_then(|em| em.keys().next_back().copied())
            .unwrap_or(1)
            .max(1);
        Self::deserialize_history_map(
            instance,
            exec_id,
            state.history.get(instance).and_then(|hm| hm.get(&exec_id)),
        )
        .map_err(|e| ProviderError::permanent("read", e))
    }

    async fn read_with_execution(
        &self,
        instance: &str,
        execution_id: u64,
    ) -> Result<Vec<Event>, ProviderError> {
        let state = self.lock_state();
        Self::deserialize_history_map(
            instance,
            execution_id,
            state
                .history
                .get(instance)
                .and_then(|hm| hm.get(&execution_id)),
        )
        .map_err(|e| ProviderError::permanent("read_with_execution", e))
    }

    async fn append_with_execution(
        &self,
        instance: &str,
        execution_id: u64,
        new_events: Vec<Event>,
    ) -> Result<(), ProviderError> {
        if new_events.is_empty() {
            return Ok(());
        }
        let mut state = self.lock_state();
        let existing = state
            .history
            .get(instance)
            .and_then(|hm| hm.get(&execution_id));
        let prepared =
            Self::validate_and_serialize_events("append_with_execution", existing, &new_events)?;
        let events_map = state
            .history
            .entry(instance.to_string())
            .or_default()
            .entry(execution_id)
            .or_default();
        for (eid, raw) in prepared {
            events_map.insert(eid, raw);
        }
        Ok(())
    }

    async fn enqueue_for_orchestrator(
        &self,
        item: WorkItem,
        delay: Option<Duration>,
    ) -> Result<(), ProviderError> {
        let Some(instance) = MysqlProvider::extract_orchestrator_instance(&item) else {
            return Err(ProviderError::permanent(
                "enqueue_for_orchestrator",
                "Invalid work item type for orchestrator queue",
            ));
        };
        let instance_id = instance.to_string();
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

        let mut state = self.lock_state();
        state.orchestrator_queue.insert(
            next_seq_id(),
            OrchestratorQueueEntry {
                instance_id,
                work_item: raw,
                visible_at,
                lock_token: None,
                locked_until: None,
                attempt_count: 0,
            },
        );
        Ok(())
    }

    async fn enqueue_for_worker(&self, item: WorkItem) -> Result<(), ProviderError> {
        let (raw, instance_id, execution_id, activity_id, session_id, tag) =
            Self::prepare_worker_item("enqueue_for_worker", &item)?;
        let now_ms = now_millis();

        let mut state = self.lock_state();
        state.worker_queue.insert(
            next_seq_id(),
            WorkerQueueEntry {
                work_item: raw,
                visible_at: now_ms,
                lock_token: None,
                locked_until: None,
                attempt_count: 0,
                instance_id,
                execution_id,
                activity_id,
                session_id,
                tag,
            },
        );
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

        let mut state = self.lock_state();
        let now_ms = now_millis();

        let matched_id = state
            .worker_queue
            .iter()
            .find(|(_, q)| {
                if q.visible_at > now_ms {
                    return false;
                }
                if q.lock_token.is_some() && q.locked_until.unwrap_or(0) > now_ms {
                    return false;
                }
                if !tag_filter.matches(q.tag.as_deref()) {
                    return false;
                }
                match (session, q.session_id.as_deref()) {
                    (_, None) => true,
                    (None, Some(_)) => false,
                    (Some(cfg), Some(sid)) => state
                        .sessions
                        .get(sid)
                        .is_none_or(|s| s.locked_until <= now_ms || s.worker_id == cfg.owner_id),
                }
            })
            .map(|(&id, _)| id);

        let Some(id) = matched_id else {
            return Ok(None);
        };

        let Some(entry_snapshot) = state.worker_queue.get(&id) else {
            return Ok(None);
        };
        let parsed: WorkItem = serde_json::from_str(&entry_snapshot.work_item).map_err(|e| {
            ProviderError::permanent("fetch_work_item", format!("Deserialization error: {e}"))
        })?;
        let candidate_session_id = entry_snapshot.session_id.clone();

        if let Some(sid) = candidate_session_id {
            let Some(cfg) = session else {
                return Ok(None);
            };
            let session_locked_until = offset_millis(now_ms, cfg.lock_timeout);
            state.sessions.insert(
                sid.clone(),
                SessionRecord {
                    session_id: sid,
                    worker_id: cfg.owner_id.clone(),
                    locked_until: session_locked_until,
                    last_activity_at: now_ms,
                },
            );
        }

        let lock_token = Self::generate_lock_token();
        let locked_until = offset_millis(now_ms, lock_timeout);
        let Some(entry) = state.worker_queue.get_mut(&id) else {
            return Ok(None);
        };
        entry.lock_token = Some(lock_token.clone());
        entry.locked_until = Some(locked_until);
        entry.attempt_count = entry.attempt_count.saturating_add(1);
        let attempt = entry.attempt_count.max(1);

        Ok(Some((parsed, lock_token, attempt)))
    }

    async fn ack_work_item(
        &self,
        token: &str,
        completion: Option<WorkItem>,
    ) -> Result<(), ProviderError> {
        let mut state = self.lock_state();
        let now_ms = now_millis();

        let Some((id, session_id, locked_until)) = state
            .worker_queue
            .iter()
            .find(|(_, q)| q.lock_token.as_deref() == Some(token))
            .map(|(&id, q)| (id, q.session_id.clone(), q.locked_until.unwrap_or(0)))
        else {
            return Err(ProviderError::permanent(
                "ack_work_item",
                "Activity was cancelled or lock expired (worker queue row not found)",
            ));
        };

        if locked_until <= now_ms {
            return Err(ProviderError::permanent(
                "ack_work_item",
                "Activity lock expired before acknowledgement",
            ));
        }

        let prepared_completion = if let Some(comp) = completion {
            let instance = match &comp {
                WorkItem::ActivityCompleted { instance, .. }
                | WorkItem::ActivityFailed { instance, .. } => instance.clone(),
                _ => {
                    return Err(ProviderError::permanent(
                        "ack_work_item",
                        "Invalid completion type for worker ack",
                    ));
                }
            };
            let raw = serde_json::to_string(&comp).map_err(|e| {
                ProviderError::permanent("ack_work_item", format!("Serialization error: {e}"))
            })?;
            Some((instance, raw))
        } else {
            None
        };

        state.worker_queue.remove(&id);

        if let Some((instance, raw)) = prepared_completion {
            state.orchestrator_queue.insert(
                next_seq_id(),
                OrchestratorQueueEntry {
                    instance_id: instance,
                    work_item: raw,
                    visible_at: now_ms,
                    lock_token: None,
                    locked_until: None,
                    attempt_count: 0,
                },
            );
        }

        if let Some(sid) = session_id
            && let Some(sess) = state.sessions.get_mut(&sid)
            && sess.locked_until > now_ms
        {
            sess.last_activity_at = now_ms;
        }

        Ok(())
    }

    async fn renew_work_item_lock(
        &self,
        token: &str,
        extend_for: Duration,
    ) -> Result<(), ProviderError> {
        let mut state = self.lock_state();
        let now_ms = now_millis();

        let Some(entry) = state
            .worker_queue
            .values_mut()
            .find(|q| q.lock_token.as_deref() == Some(token))
        else {
            return Err(ProviderError::permanent(
                "renew_work_item_lock",
                "Lock renewal failed - activity was cancelled or lock expired",
            ));
        };

        let current_until = entry.locked_until.unwrap_or(0);
        if current_until <= now_ms {
            return Err(ProviderError::permanent(
                "renew_work_item_lock",
                "Lock renewal failed - lock expired",
            ));
        }

        entry.locked_until = Some(current_until.max(offset_millis(now_ms, extend_for)));
        let session_id = entry.session_id.clone();

        if let Some(sid) = session_id
            && let Some(sess) = state.sessions.get_mut(&sid)
            && sess.locked_until > now_ms
        {
            sess.last_activity_at = now_ms;
        }

        Ok(())
    }

    async fn abandon_work_item(
        &self,
        token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        let mut state = self.lock_state();
        let now_ms = now_millis();
        let visible_at = match delay {
            Some(d) => offset_millis(now_ms, d),
            None => now_ms,
        };

        let Some(entry) = state
            .worker_queue
            .values_mut()
            .find(|q| q.lock_token.as_deref() == Some(token))
        else {
            return Err(ProviderError::permanent(
                "abandon_work_item",
                "Invalid lock token or already acked",
            ));
        };

        entry.lock_token = None;
        entry.locked_until = None;
        entry.visible_at = visible_at;
        if ignore_attempt {
            entry.attempt_count = entry.attempt_count.saturating_sub(1);
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

        let mut state = self.lock_state();
        let now_ms = now_millis();
        let locked_until = offset_millis(now_ms, extend_for);
        let idle_ms = idle_timeout.as_millis().min(i64::MAX as u128) as i64;
        let idle_cutoff = now_ms.saturating_sub(idle_ms);

        let mut renewed = 0usize;
        for sess in state.sessions.values_mut() {
            if owner_ids.contains(&sess.worker_id.as_str())
                && sess.locked_until > now_ms
                && sess.last_activity_at > idle_cutoff
            {
                sess.locked_until = sess.locked_until.max(locked_until);
                renewed += 1;
            }
        }

        Ok(renewed)
    }

    async fn cleanup_orphaned_sessions(
        &self,
        _idle_timeout: Duration,
    ) -> Result<usize, ProviderError> {
        let mut state = self.lock_state();
        let now_ms = now_millis();

        let active_session_ids: HashSet<String> = state
            .worker_queue
            .values()
            .filter_map(|q| q.session_id.clone())
            .collect();

        let before = state.sessions.len();
        state.sessions.retain(|_, s| {
            !(s.locked_until < now_ms && !active_session_ids.contains(&s.session_id))
        });

        Ok(before - state.sessions.len())
    }

    fn as_management_capability(&self) -> Option<&dyn ProviderAdmin> {
        Some(self as &dyn ProviderAdmin)
    }

    async fn get_custom_status(
        &self,
        instance: &str,
        last_seen_version: u64,
    ) -> Result<Option<(Option<String>, u64)>, ProviderError> {
        let state = self.lock_state();
        match state.instances.get(instance) {
            Some(inst) if inst.custom_status_version > last_seen_version => Ok(Some((
                inst.custom_status.clone(),
                inst.custom_status_version,
            ))),
            _ => Ok(None),
        }
    }

    async fn get_kv_value(
        &self,
        instance: &str,
        key: &str,
    ) -> Result<Option<String>, ProviderError> {
        let state = self.lock_state();
        if let Some(delta_entry) = state.kv_delta.get(instance).and_then(|d| d.get(key)) {
            return Ok(delta_entry.value.clone());
        }
        Ok(state
            .kv_store
            .get(instance)
            .and_then(|s| s.get(key))
            .map(|e| e.value.clone()))
    }

    async fn get_kv_all_values(
        &self,
        instance: &str,
    ) -> Result<HashMap<String, String>, ProviderError> {
        let state = self.lock_state();
        Ok(Self::read_merged_kv(&state, instance))
    }

    async fn get_instance_stats(
        &self,
        instance: &str,
    ) -> Result<Option<SystemStats>, ProviderError> {
        let state = self.lock_state();
        let Some(inst) = state.instances.get(instance) else {
            return Ok(None);
        };
        let exec_id = inst.current_execution_id;

        let events_map = state.history.get(instance).and_then(|hm| hm.get(&exec_id));
        let history_event_count = events_map.map_or(0u64, |m| m.len() as u64);
        let history_size_bytes = events_map.map_or(0u64, |m| {
            m.values().map(|raw| raw.len() as u64).sum::<u64>()
        });

        let merged_kv = Self::read_merged_kv(&state, instance);
        let kv_user_key_count = merged_kv.len() as u64;
        let kv_total_value_bytes: u64 = merged_kv.values().map(|v| v.len() as u64).sum();

        let queue_pending_count = match events_map.and_then(|m| m.get(&1)) {
            Some(raw) => {
                let event: Event = serde_json::from_str(raw).map_err(|e| {
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
            history_event_count,
            history_size_bytes,
            queue_pending_count,
            kv_user_key_count,
            kv_total_value_bytes,
        }))
    }
}

#[async_trait::async_trait]
impl ProviderAdmin for MemoryProvider {
    async fn list_instances(&self) -> Result<Vec<String>, ProviderError> {
        let state = self.lock_state();
        let mut records: Vec<&InstanceRecord> = state.instances.values().collect();
        records.sort_by_key(|i| (Reverse(i.created_at), Reverse(i.seq_id)));
        Ok(records.into_iter().map(|i| i.instance_id.clone()).collect())
    }

    async fn list_instances_by_status(&self, status: &str) -> Result<Vec<String>, ProviderError> {
        let state = self.lock_state();
        let mut records: Vec<&InstanceRecord> = state
            .instances
            .values()
            .filter(|i| {
                state
                    .executions
                    .get(&i.instance_id)
                    .and_then(|em| em.get(&i.current_execution_id))
                    .is_some_and(|e| e.status == status)
            })
            .collect();
        records.sort_by_key(|i| (Reverse(i.created_at), Reverse(i.seq_id)));
        Ok(records.into_iter().map(|i| i.instance_id.clone()).collect())
    }

    async fn list_executions(&self, instance: &str) -> Result<Vec<u64>, ProviderError> {
        let state = self.lock_state();
        Ok(state
            .executions
            .get(instance)
            .map(|em| em.keys().copied().collect())
            .unwrap_or_default())
    }

    async fn read_history_with_execution_id(
        &self,
        instance: &str,
        execution_id: u64,
    ) -> Result<Vec<Event>, ProviderError> {
        Provider::read_with_execution(self, instance, execution_id).await
    }

    async fn read_history(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
        let exec_id = self.latest_execution_id(instance).await?;
        self.read_history_with_execution_id(instance, exec_id).await
    }

    async fn latest_execution_id(&self, instance: &str) -> Result<u64, ProviderError> {
        let state = self.lock_state();
        Ok(state
            .executions
            .get(instance)
            .and_then(|em| em.keys().next_back().copied())
            .unwrap_or(1)
            .max(1))
    }

    async fn get_instance_info(&self, instance: &str) -> Result<InstanceInfo, ProviderError> {
        let state = self.lock_state();
        let Some(inst) = state.instances.get(instance) else {
            return Err(ProviderError::permanent(
                "get_instance_info",
                format!("Instance {instance} not found"),
            ));
        };
        let exec = state
            .executions
            .get(instance)
            .and_then(|em| em.get(&inst.current_execution_id));

        Ok(InstanceInfo {
            instance_id: inst.instance_id.clone(),
            orchestration_name: inst.orchestration_name.clone(),
            orchestration_version: inst
                .orchestration_version
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
            current_execution_id: inst.current_execution_id.max(1),
            status: exec
                .map(|e| e.status.clone())
                .unwrap_or_else(|| "Unknown".to_string()),
            output: exec.and_then(|e| e.output.clone()),
            created_at: inst.created_at.max(0) as u64,
            updated_at: inst.updated_at.max(0) as u64,
            parent_instance_id: inst.parent_instance_id.clone(),
        })
    }

    async fn get_execution_info(
        &self,
        instance: &str,
        execution_id: u64,
    ) -> Result<ExecutionInfo, ProviderError> {
        let state = self.lock_state();
        let Some(exec) = state
            .executions
            .get(instance)
            .and_then(|em| em.get(&execution_id))
        else {
            return Err(ProviderError::permanent(
                "get_execution_info",
                format!("Execution {execution_id} not found for instance {instance}"),
            ));
        };
        let event_count = state
            .history
            .get(instance)
            .and_then(|hm| hm.get(&execution_id))
            .map_or(0, BTreeMap::len);

        Ok(ExecutionInfo {
            execution_id: exec.execution_id.max(1),
            status: exec.status.clone(),
            output: exec.output.clone(),
            started_at: exec.started_at.max(0) as u64,
            completed_at: exec.completed_at.map(|t| t.max(0) as u64),
            event_count,
        })
    }

    async fn get_system_metrics(&self) -> Result<SystemMetrics, ProviderError> {
        let state = self.lock_state();
        let total_instances = state.instances.len() as u64;
        let mut running_instances = 0u64;
        let mut completed_instances = 0u64;
        let mut failed_instances = 0u64;

        for inst in state.instances.values() {
            if let Some(exec) = state
                .executions
                .get(&inst.instance_id)
                .and_then(|em| em.get(&inst.current_execution_id))
            {
                match exec.status.as_str() {
                    "Running" => running_instances += 1,
                    "Completed" => completed_instances += 1,
                    "Failed" => failed_instances += 1,
                    _ => {}
                }
            }
        }

        let total_executions: u64 = state.executions.values().map(|em| em.len() as u64).sum();
        let total_events: u64 = state
            .history
            .values()
            .flat_map(|hm| hm.values())
            .map(|em| em.len() as u64)
            .sum();

        Ok(SystemMetrics {
            total_instances,
            total_executions,
            running_instances,
            completed_instances,
            failed_instances,
            total_events,
        })
    }

    async fn get_queue_depths(&self) -> Result<QueueDepths, ProviderError> {
        let state = self.lock_state();
        let orchestrator_queue = state
            .orchestrator_queue
            .values()
            .filter(|q| q.lock_token.is_none())
            .count();
        let worker_queue = state
            .worker_queue
            .values()
            .filter(|q| q.lock_token.is_none())
            .count();

        Ok(QueueDepths {
            orchestrator_queue,
            worker_queue,
            timer_queue: 0,
        })
    }

    async fn list_children(&self, instance_id: &str) -> Result<Vec<String>, ProviderError> {
        let state = self.lock_state();
        let mut children: Vec<&InstanceRecord> = state
            .instances
            .values()
            .filter(|i| i.parent_instance_id.as_deref() == Some(instance_id))
            .collect();
        children.sort_by_key(|i| i.seq_id);
        Ok(children
            .into_iter()
            .map(|i| i.instance_id.clone())
            .collect())
    }

    async fn get_parent_id(&self, instance_id: &str) -> Result<Option<String>, ProviderError> {
        let state = self.lock_state();
        match state.instances.get(instance_id) {
            Some(inst) => Ok(inst.parent_instance_id.clone()),
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
        let mut state = self.lock_state();
        Self::delete_instances_atomic_locked(&mut state, ids, force)
    }

    async fn delete_instance_bulk(
        &self,
        filter: InstanceFilter,
    ) -> Result<DeleteInstanceResult, ProviderError> {
        if let Some(ref ids) = filter.instance_ids
            && ids.is_empty()
        {
            return Ok(DeleteInstanceResult::default());
        }

        let roots: Vec<String> = {
            let state = self.lock_state();
            let id_allowlist: Option<HashSet<&str>> = filter
                .instance_ids
                .as_ref()
                .map(|ids| ids.iter().map(String::as_str).collect());

            let mut matched: Vec<&InstanceRecord> = state
                .instances
                .values()
                .filter(|inst| {
                    if inst.parent_instance_id.is_some() {
                        return false;
                    }
                    if let Some(ref allow) = id_allowlist
                        && !allow.contains(inst.instance_id.as_str())
                    {
                        return false;
                    }
                    let Some(exec) = state
                        .executions
                        .get(&inst.instance_id)
                        .and_then(|em| em.get(&inst.current_execution_id))
                    else {
                        return false;
                    };
                    if !matches!(
                        exec.status.as_str(),
                        "Completed" | "Failed" | "ContinuedAsNew"
                    ) {
                        return false;
                    }
                    if let Some(cb) = filter.completed_before {
                        let Some(done_at) = exec.completed_at else {
                            return false;
                        };
                        if done_at >= cb as i64 {
                            return false;
                        }
                    }
                    true
                })
                .collect();

            matched.sort_by_key(|i| (i.created_at, i.seq_id));
            let limit = filter.limit.unwrap_or(DEFAULT_BULK_LIMIT) as usize;
            matched
                .into_iter()
                .take(limit)
                .map(|i| i.instance_id.clone())
                .collect()
        };

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
        let mut state = self.lock_state();
        Self::prune_instance_locked(&mut state, instance_id, &options)
    }

    async fn prune_executions_bulk(
        &self,
        filter: InstanceFilter,
        options: PruneOptions,
    ) -> Result<PruneResult, ProviderError> {
        if let Some(ref ids) = filter.instance_ids
            && ids.is_empty()
        {
            return Ok(PruneResult::default());
        }

        let mut state = self.lock_state();
        let id_allowlist: Option<HashSet<&str>> = filter
            .instance_ids
            .as_ref()
            .map(|ids| ids.iter().map(String::as_str).collect());

        let mut matched: Vec<(&str, i64, i64)> = state
            .instances
            .values()
            .filter(|inst| {
                if let Some(ref allow) = id_allowlist
                    && !allow.contains(inst.instance_id.as_str())
                {
                    return false;
                }
                if let Some(cb) = filter.completed_before {
                    let done_at = state
                        .executions
                        .get(&inst.instance_id)
                        .and_then(|em| em.get(&inst.current_execution_id))
                        .and_then(|e| e.completed_at);
                    let Some(done_at) = done_at else {
                        return false;
                    };
                    if done_at >= cb as i64 {
                        return false;
                    }
                }
                true
            })
            .map(|i| (i.instance_id.as_str(), i.created_at, i.seq_id))
            .collect();

        matched.sort_by_key(|&(_, created_at, seq_id)| (created_at, seq_id));
        let limit = filter.limit.unwrap_or(DEFAULT_BULK_LIMIT) as usize;
        let instance_ids: Vec<String> = matched
            .into_iter()
            .take(limit)
            .map(|(id, _, _)| id.to_string())
            .collect();

        let mut total = PruneResult::default();
        for inst_id in &instance_ids {
            let sub = Self::prune_instance_locked(&mut state, inst_id, &options)?;
            total.instances_processed += sub.instances_processed;
            total.executions_deleted += sub.executions_deleted;
            total.events_deleted += sub.events_deleted;
        }

        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_provider_identity_and_empty_reads() -> Result<(), Box<dyn std::error::Error>> {
        let provider = MemoryProvider::new();
        assert_eq!(provider.name(), "memory");
        assert_eq!(provider.version(), env!("CARGO_PKG_VERSION"));
        assert!(provider.as_management_capability().is_some());
        assert!(provider.read("missing").await?.is_empty());
        assert!(provider.get_instance_stats("missing").await?.is_none());
        Ok(())
    }

    #[test]
    fn recovers_from_poisoned_mutex() {
        let provider = MemoryProvider::new();
        let clone = provider.clone();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _guard = clone.lock_state();
            assert_eq!(1, 2);
        }));
        assert_eq!(provider.get_max_attempt_count("any"), 0);
    }

    #[tokio::test]
    async fn drops_corrupted_orchestrator_queue_json_cleanly()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider = MemoryProvider::new();
        {
            let mut state = provider.lock_state();
            state.orchestrator_queue.insert(
                next_seq_id(),
                OrchestratorQueueEntry {
                    instance_id: "bad-json-inst".to_string(),
                    work_item: "NOT_VALID_WORK_ITEM_JSON{{{".to_string(),
                    visible_at: 0,
                    lock_token: None,
                    locked_until: None,
                    attempt_count: 0,
                },
            );
        }

        let fetched = provider
            .fetch_orchestration_item(Duration::from_secs(5), Duration::ZERO, None)
            .await?;
        assert!(fetched.is_none());
        let depths = provider.get_queue_depths().await?;
        assert_eq!(depths.orchestrator_queue, 0);
        Ok(())
    }
}
