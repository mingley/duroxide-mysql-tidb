#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::{IsolatedMysqlTestFactory, SharedMysqlTestFactory};
use duroxide::provider_validations as pv;

macro_rules! isolated_validation {
    ($test_name:ident, $func:path) => {
        #[tokio::test]
        async fn $test_name() {
            let Some(factory) = IsolatedMysqlTestFactory::new_or_skip().await else {
                return;
            };
            $func(&factory).await;
            factory.finish().await;
        }
    };
}

macro_rules! shared_validation {
    ($test_name:ident, $func:path) => {
        #[tokio::test]
        async fn $test_name() {
            let Some(factory) = SharedMysqlTestFactory::new_or_skip().await else {
                return;
            };
            $func(&factory).await;
            factory.finish().await;
        }
    };
}

// === Instance Creation ===
isolated_validation!(
    instance_creation_via_metadata,
    pv::test_instance_creation_via_metadata
);
isolated_validation!(
    no_instance_creation_on_enqueue,
    pv::test_no_instance_creation_on_enqueue
);
isolated_validation!(null_version_handling, pv::test_null_version_handling);
isolated_validation!(
    sub_orchestration_instance_creation,
    pv::test_sub_orchestration_instance_creation
);

// === Atomicity ===
isolated_validation!(
    atomicity_failure_rollback,
    pv::test_atomicity_failure_rollback
);
isolated_validation!(
    multi_operation_atomic_ack,
    pv::test_multi_operation_atomic_ack
);
isolated_validation!(
    lock_released_only_on_successful_ack,
    pv::test_lock_released_only_on_successful_ack
);
isolated_validation!(
    concurrent_ack_prevention,
    pv::test_concurrent_ack_prevention
);

// === Error Handling ===
isolated_validation!(
    invalid_lock_token_on_ack,
    pv::test_invalid_lock_token_on_ack
);
isolated_validation!(
    duplicate_event_id_rejection,
    pv::test_duplicate_event_id_rejection
);
isolated_validation!(
    missing_instance_metadata,
    pv::test_missing_instance_metadata
);
isolated_validation!(
    corrupted_serialization_data,
    pv::test_corrupted_serialization_data
);
isolated_validation!(
    lock_expiration_during_ack,
    pv::test_lock_expiration_during_ack
);
shared_validation!(
    read_corrupted_history_returns_error,
    pv::test_read_corrupted_history_returns_error
);
shared_validation!(
    read_with_execution_corrupted_history_returns_error,
    pv::test_read_with_execution_corrupted_history_returns_error
);

// === Instance Locking ===
isolated_validation!(exclusive_instance_lock, pv::test_exclusive_instance_lock);
isolated_validation!(lock_token_uniqueness, pv::test_lock_token_uniqueness);
isolated_validation!(
    invalid_lock_token_rejection,
    pv::test_invalid_lock_token_rejection
);
isolated_validation!(
    concurrent_instance_fetching,
    pv::test_concurrent_instance_fetching
);
isolated_validation!(
    completions_arriving_during_lock_blocked,
    pv::test_completions_arriving_during_lock_blocked
);
isolated_validation!(
    cross_instance_lock_isolation,
    pv::test_cross_instance_lock_isolation
);
isolated_validation!(
    message_tagging_during_lock,
    pv::test_message_tagging_during_lock
);
isolated_validation!(
    ack_only_affects_locked_messages,
    pv::test_ack_only_affects_locked_messages
);
isolated_validation!(
    multi_threaded_lock_contention,
    pv::test_multi_threaded_lock_contention
);
isolated_validation!(
    multi_threaded_no_duplicate_processing,
    pv::test_multi_threaded_no_duplicate_processing
);
isolated_validation!(
    multi_threaded_lock_expiration_recovery,
    pv::test_multi_threaded_lock_expiration_recovery
);

// === Lock Expiration & Renewal ===
isolated_validation!(
    lock_expires_after_timeout,
    pv::test_lock_expires_after_timeout
);
isolated_validation!(
    abandon_releases_lock_immediately,
    pv::test_abandon_releases_lock_immediately
);
isolated_validation!(
    abandon_work_item_releases_lock,
    pv::test_abandon_work_item_releases_lock
);
isolated_validation!(
    abandon_work_item_with_delay,
    pv::test_abandon_work_item_with_delay
);
isolated_validation!(lock_renewal_on_ack, pv::test_lock_renewal_on_ack);
isolated_validation!(
    concurrent_lock_attempts_respect_expiration,
    pv::test_concurrent_lock_attempts_respect_expiration
);
isolated_validation!(
    orchestration_lock_renewal_after_expiration,
    pv::test_orchestration_lock_renewal_after_expiration
);
isolated_validation!(
    worker_ack_fails_after_lock_expiry,
    pv::test_worker_ack_fails_after_lock_expiry
);
isolated_validation!(
    worker_lock_renewal_success,
    pv::test_worker_lock_renewal_success
);
isolated_validation!(
    worker_lock_renewal_invalid_token,
    pv::test_worker_lock_renewal_invalid_token
);
isolated_validation!(
    worker_lock_renewal_after_expiration,
    pv::test_worker_lock_renewal_after_expiration
);
isolated_validation!(
    worker_lock_renewal_extends_timeout,
    pv::test_worker_lock_renewal_extends_timeout
);
isolated_validation!(
    worker_lock_renewal_after_ack,
    pv::test_worker_lock_renewal_after_ack
);

// === Multi-Execution (ContinueAsNew) ===
isolated_validation!(execution_isolation, pv::test_execution_isolation);
isolated_validation!(
    latest_execution_detection,
    pv::test_latest_execution_detection
);
isolated_validation!(execution_id_sequencing, pv::test_execution_id_sequencing);
isolated_validation!(
    continue_as_new_creates_new_execution,
    pv::test_continue_as_new_creates_new_execution
);
isolated_validation!(
    execution_history_persistence,
    pv::test_execution_history_persistence
);

// === Queue Semantics ===
isolated_validation!(
    worker_queue_fifo_ordering,
    pv::test_worker_queue_fifo_ordering
);
isolated_validation!(
    worker_peek_lock_semantics,
    pv::test_worker_peek_lock_semantics
);
isolated_validation!(worker_ack_atomicity, pv::test_worker_ack_atomicity);
isolated_validation!(timer_delayed_visibility, pv::test_timer_delayed_visibility);
isolated_validation!(lost_lock_token_handling, pv::test_lost_lock_token_handling);
isolated_validation!(
    worker_item_immediate_visibility,
    pv::test_worker_item_immediate_visibility
);
isolated_validation!(
    worker_delayed_visibility_skips_future_items,
    pv::test_worker_delayed_visibility_skips_future_items
);
isolated_validation!(
    orphan_queue_messages_dropped,
    pv::test_orphan_queue_messages_dropped
);

// === Management & Introspection ===
isolated_validation!(mgmt_list_instances, pv::test_list_instances);
isolated_validation!(
    mgmt_list_instances_by_status,
    pv::test_list_instances_by_status
);
isolated_validation!(mgmt_list_executions, pv::test_list_executions);
isolated_validation!(mgmt_get_instance_info, pv::test_get_instance_info);
isolated_validation!(mgmt_get_execution_info, pv::test_get_execution_info);
isolated_validation!(mgmt_get_system_metrics, pv::test_get_system_metrics);
isolated_validation!(mgmt_get_queue_depths, pv::test_get_queue_depths);
isolated_validation!(
    mgmt_instance_stats_nonexistent,
    pv::test_get_instance_stats_nonexistent
);
isolated_validation!(
    mgmt_instance_stats_history,
    pv::test_get_instance_stats_history
);
isolated_validation!(mgmt_instance_stats_kv, pv::test_get_instance_stats_kv);
isolated_validation!(
    mgmt_instance_stats_kv_delta_only,
    pv::test_get_instance_stats_kv_delta_only
);
isolated_validation!(
    mgmt_instance_stats_kv_merged,
    pv::test_get_instance_stats_kv_merged
);
isolated_validation!(
    mgmt_instance_stats_carry_forward,
    pv::test_get_instance_stats_carry_forward
);

// === Short Polling ===
#[tokio::test]
async fn short_poll_returns_immediately() {
    use duroxide::provider_validations::ProviderFactory;
    let Some(factory) = IsolatedMysqlTestFactory::new_or_skip().await else {
        return;
    };
    let provider = factory.create_provider().await;
    pv::long_polling::test_short_poll_returns_immediately(
        &*provider,
        factory.short_poll_threshold(),
    )
    .await;
    factory.finish().await;
}

#[tokio::test]
async fn short_poll_work_item_returns_immediately() {
    use duroxide::provider_validations::ProviderFactory;
    let Some(factory) = IsolatedMysqlTestFactory::new_or_skip().await else {
        return;
    };
    let provider = factory.create_provider().await;
    pv::long_polling::test_short_poll_work_item_returns_immediately(
        &*provider,
        factory.short_poll_threshold(),
    )
    .await;
    factory.finish().await;
}

#[tokio::test]
async fn fetch_respects_timeout_upper_bound() {
    use duroxide::provider_validations::ProviderFactory;
    let Some(factory) = IsolatedMysqlTestFactory::new_or_skip().await else {
        return;
    };
    let provider = factory.create_provider().await;
    pv::long_polling::test_fetch_respects_timeout_upper_bound(&*provider).await;
    factory.finish().await;
}

// === Poison Message & Attempt Tracking ===
isolated_validation!(
    orch_attempt_starts_at_one,
    pv::poison_message::orchestration_attempt_count_starts_at_one
);
isolated_validation!(
    orch_attempt_increments_on_refetch,
    pv::poison_message::orchestration_attempt_count_increments_on_refetch
);
isolated_validation!(
    worker_attempt_starts_at_one,
    pv::poison_message::worker_attempt_count_starts_at_one
);
isolated_validation!(
    worker_attempt_increments_on_lock_expiry,
    pv::poison_message::worker_attempt_count_increments_on_lock_expiry
);
isolated_validation!(
    attempt_count_is_per_message,
    pv::poison_message::attempt_count_is_per_message
);
isolated_validation!(
    abandon_work_item_ignore_attempt_decrements,
    pv::poison_message::abandon_work_item_ignore_attempt_decrements
);
isolated_validation!(
    abandon_orchestration_item_ignore_attempt_decrements,
    pv::poison_message::abandon_orchestration_item_ignore_attempt_decrements
);
isolated_validation!(
    ignore_attempt_never_goes_negative,
    pv::poison_message::ignore_attempt_never_goes_negative
);
isolated_validation!(
    max_attempt_count_across_message_batch,
    pv::poison_message::max_attempt_count_across_message_batch
);
isolated_validation!(
    delayed_abandon_preserves_unlocked_rows,
    pv::poison_message::orchestration_delayed_abandon_preserves_unlocked_rows
);
isolated_validation!(
    ignore_attempt_preserves_hidden_start,
    pv::poison_message::orchestration_ignore_attempt_preserves_hidden_start
);

// === Cancellation & Lock Stealing ===
isolated_validation!(
    ack_work_item_fails_when_entry_deleted,
    pv::test_ack_work_item_fails_when_entry_deleted
);
isolated_validation!(
    ack_work_item_none_deletes_without_enqueue,
    pv::test_ack_work_item_none_deletes_without_enqueue
);
isolated_validation!(
    batch_cancellation_deletes_multiple,
    pv::test_batch_cancellation_deletes_multiple_activities
);
isolated_validation!(
    cancelled_activities_deleted,
    pv::test_cancelled_activities_deleted_from_worker_queue
);
isolated_validation!(
    cancelling_nonexistent_is_idempotent,
    pv::test_cancelling_nonexistent_activities_is_idempotent
);
isolated_validation!(
    fetch_missing_state_when_deleted,
    pv::test_fetch_returns_missing_state_when_instance_deleted
);
isolated_validation!(
    fetch_running_state_for_active,
    pv::test_fetch_returns_running_state_for_active_orchestration
);
isolated_validation!(
    fetch_terminal_state_when_completed,
    pv::test_fetch_returns_terminal_state_when_orchestration_completed
);
isolated_validation!(
    fetch_terminal_state_when_can,
    pv::test_fetch_returns_terminal_state_when_orchestration_continued_as_new
);
isolated_validation!(
    fetch_terminal_state_when_failed,
    pv::test_fetch_returns_terminal_state_when_orchestration_failed
);
isolated_validation!(
    orphan_activity_after_force_delete,
    pv::test_orphan_activity_after_instance_force_deletion
);
isolated_validation!(
    renew_fails_when_entry_deleted,
    pv::test_renew_fails_when_entry_deleted
);
isolated_validation!(
    renew_missing_when_instance_deleted,
    pv::test_renew_returns_missing_when_instance_deleted
);
isolated_validation!(
    renew_running_when_active,
    pv::test_renew_returns_running_when_orchestration_active
);
isolated_validation!(
    renew_terminal_when_completed,
    pv::test_renew_returns_terminal_when_orchestration_completed
);
isolated_validation!(
    same_activity_in_worker_and_cancelled_noop,
    pv::test_same_activity_in_worker_items_and_cancelled_is_noop
);

// === Deletion, Cascade, Bulk Delete, Prune ===
isolated_validation!(
    delete_terminal_instances,
    pv::deletion::test_delete_terminal_instances
);
isolated_validation!(
    delete_running_rejected_force_succeeds,
    pv::deletion::test_delete_running_rejected_force_succeeds
);
isolated_validation!(
    delete_nonexistent_instance,
    pv::deletion::test_delete_nonexistent_instance
);
isolated_validation!(
    delete_cleans_queues_and_locks,
    pv::deletion::test_delete_cleans_queues_and_locks
);
isolated_validation!(
    cascade_delete_hierarchy,
    pv::deletion::test_cascade_delete_hierarchy
);
isolated_validation!(delete_list_children, pv::deletion::test_list_children);
isolated_validation!(
    delete_get_parent_id,
    pv::deletion::test_delete_get_parent_id
);
isolated_validation!(
    delete_get_instance_tree,
    pv::deletion::test_delete_get_instance_tree
);
isolated_validation!(
    delete_instances_atomic,
    pv::deletion::test_delete_instances_atomic
);
isolated_validation!(
    delete_instances_atomic_force,
    pv::deletion::test_delete_instances_atomic_force
);
isolated_validation!(
    delete_instances_atomic_orphan_detection,
    pv::deletion::test_delete_instances_atomic_orphan_detection
);
isolated_validation!(
    force_delete_prevents_ack_recreation,
    pv::deletion::test_force_delete_prevents_ack_recreation
);
isolated_validation!(
    stale_activity_after_delete_recreate,
    pv::deletion::test_stale_activity_after_delete_recreate
);

isolated_validation!(
    prune_options_combinations,
    pv::prune::test_prune_options_combinations
);
isolated_validation!(prune_safety, pv::prune::test_prune_safety);
isolated_validation!(prune_bulk, pv::prune::test_prune_bulk);
isolated_validation!(
    prune_bulk_includes_running,
    pv::prune::test_prune_bulk_includes_running_instances
);

isolated_validation!(
    bulk_delete_filter_combinations,
    pv::bulk_deletion::test_delete_instance_bulk_filter_combinations
);
isolated_validation!(
    bulk_delete_safety_and_limits,
    pv::bulk_deletion::test_delete_instance_bulk_safety_and_limits
);
isolated_validation!(
    bulk_delete_completed_before_filter,
    pv::bulk_deletion::test_delete_instance_bulk_completed_before_filter
);
isolated_validation!(
    bulk_delete_cascades_to_children,
    pv::bulk_deletion::test_delete_instance_bulk_cascades_to_children
);

// === Capability Filtering ===
isolated_validation!(
    cap_filter_none_returns_any,
    pv::capability_filtering::test_fetch_with_filter_none_returns_any_item
);
isolated_validation!(
    cap_filter_compatible_returns_item,
    pv::capability_filtering::test_fetch_with_compatible_filter_returns_item
);
isolated_validation!(
    cap_filter_incompatible_skips_item,
    pv::capability_filtering::test_fetch_with_incompatible_filter_skips_item
);
isolated_validation!(
    cap_filter_skips_incompatible_selects_compatible,
    pv::capability_filtering::test_fetch_filter_skips_incompatible_selects_compatible
);
isolated_validation!(
    cap_filter_does_not_lock_skipped,
    pv::capability_filtering::test_fetch_filter_does_not_lock_skipped_instances
);
isolated_validation!(
    cap_filter_null_pinned_always_compatible,
    pv::capability_filtering::test_fetch_filter_null_pinned_version_always_compatible
);
isolated_validation!(
    cap_filter_boundary_versions,
    pv::capability_filtering::test_fetch_filter_boundary_versions
);
isolated_validation!(
    cap_pinned_stored_via_ack_metadata,
    pv::capability_filtering::test_pinned_version_stored_via_ack_metadata
);
isolated_validation!(
    cap_pinned_immutable_across_acks,
    pv::capability_filtering::test_pinned_version_immutable_across_ack_cycles
);
isolated_validation!(
    cap_can_gets_own_pinned_version,
    pv::capability_filtering::test_continue_as_new_execution_gets_own_pinned_version
);
isolated_validation!(
    cap_empty_supported_versions_returns_nothing,
    pv::capability_filtering::test_filter_with_empty_supported_versions_returns_nothing
);
isolated_validation!(
    cap_single_range_only_uses_first_range,
    pv::capability_filtering::test_fetch_single_range_only_uses_first_range
);
isolated_validation!(
    cap_concurrent_filtered_fetch_no_double_lock,
    pv::capability_filtering::test_concurrent_filtered_fetch_no_double_lock
);
isolated_validation!(
    cap_ack_stores_pinned_via_metadata_update,
    pv::capability_filtering::test_ack_stores_pinned_version_via_metadata_update
);
isolated_validation!(
    cap_updates_pinned_when_told,
    pv::capability_filtering::test_provider_updates_pinned_version_when_told
);
shared_validation!(
    cap_corrupted_history_filtered_vs_unfiltered,
    pv::capability_filtering::test_fetch_corrupted_history_filtered_vs_unfiltered
);
shared_validation!(
    cap_deserialization_error_increments_attempt,
    pv::capability_filtering::test_fetch_deserialization_error_increments_attempt_count
);
shared_validation!(
    cap_deserialization_error_reaches_poison,
    pv::capability_filtering::test_fetch_deserialization_error_eventually_reaches_poison
);
shared_validation!(
    cap_filter_applied_before_history_deser,
    pv::capability_filtering::test_fetch_filter_applied_before_history_deserialization
);
shared_validation!(
    cap_ack_appends_event_to_corrupted_history,
    pv::capability_filtering::test_ack_appends_event_to_corrupted_history
);

// === Sessions ===
isolated_validation!(
    sess_non_session_fetchable,
    pv::sessions::test_non_session_items_fetchable_by_any_worker
);
isolated_validation!(
    sess_claimable_when_no_session,
    pv::sessions::test_session_item_claimable_when_no_session
);
isolated_validation!(
    sess_affinity_same_worker,
    pv::sessions::test_session_affinity_same_worker
);
isolated_validation!(
    sess_affinity_blocks_other_worker,
    pv::sessions::test_session_affinity_blocks_other_worker
);
isolated_validation!(
    sess_different_sessions_different_workers,
    pv::sessions::test_different_sessions_different_workers
);
isolated_validation!(
    sess_mixed_session_and_non_session,
    pv::sessions::test_mixed_session_and_non_session_items
);
isolated_validation!(
    sess_none_skips_session_items,
    pv::sessions::test_none_session_skips_session_items
);
isolated_validation!(
    sess_some_returns_all_items,
    pv::sessions::test_some_session_returns_all_items
);
isolated_validation!(
    sess_non_session_returned_with_cfg,
    pv::sessions::test_non_session_items_returned_with_session_config
);
isolated_validation!(
    sess_claimable_after_expiry,
    pv::sessions::test_session_claimable_after_lock_expiry
);
isolated_validation!(
    sess_takeover_after_expiry,
    pv::sessions::test_session_takeover_after_lock_expiry
);
isolated_validation!(
    sess_original_reclaims_expired,
    pv::sessions::test_original_worker_reclaims_expired_session
);
isolated_validation!(
    sess_act_lock_expires_sess_valid_same_worker,
    pv::sessions::test_activity_lock_expires_session_lock_valid_same_worker_refetches
);
isolated_validation!(
    sess_both_locks_expire_diff_worker,
    pv::sessions::test_both_locks_expire_different_worker_claims
);
isolated_validation!(
    sess_lock_expires_act_valid_ack_succeeds,
    pv::sessions::test_session_lock_expires_activity_lock_valid_ack_succeeds
);
isolated_validation!(
    sess_lock_expires_new_owner_redelivery,
    pv::sessions::test_session_lock_expires_new_owner_gets_redelivery
);
isolated_validation!(
    sess_lock_expires_same_worker_reacquires,
    pv::sessions::test_session_lock_expires_same_worker_reacquires
);
isolated_validation!(
    sess_renew_active,
    pv::sessions::test_renew_session_lock_active
);
isolated_validation!(
    sess_renew_extends_past_original,
    pv::sessions::test_session_lock_renewal_extends_past_original_timeout
);
isolated_validation!(
    sess_renew_after_expiry_zero,
    pv::sessions::test_renew_session_lock_after_expiry_returns_zero
);
isolated_validation!(
    sess_renew_skips_idle,
    pv::sessions::test_renew_session_lock_skips_idle
);
isolated_validation!(
    sess_renew_no_sessions,
    pv::sessions::test_renew_session_lock_no_sessions
);
isolated_validation!(
    sess_ack_updates_last_activity,
    pv::sessions::test_ack_updates_session_last_activity
);
isolated_validation!(
    sess_renew_work_item_updates_last_activity,
    pv::sessions::test_renew_work_item_updates_session_last_activity
);
isolated_validation!(
    sess_cleanup_removes_expired_no_items,
    pv::sessions::test_cleanup_removes_expired_no_items
);
isolated_validation!(
    sess_cleanup_keeps_with_pending,
    pv::sessions::test_cleanup_keeps_sessions_with_pending_items
);
isolated_validation!(
    sess_cleanup_keeps_active,
    pv::sessions::test_cleanup_keeps_active_sessions
);
isolated_validation!(
    sess_cleanup_then_recreates,
    pv::sessions::test_cleanup_then_new_item_recreates_session
);
isolated_validation!(
    sess_concurrent_claim_only_one_wins,
    pv::sessions::test_concurrent_session_claim_only_one_wins
);
isolated_validation!(
    sess_items_processed_in_order,
    pv::sessions::test_session_items_processed_in_order
);
isolated_validation!(
    sess_shared_worker_id_any_caller,
    pv::sessions::test_shared_worker_id_any_caller_can_fetch_owned_session
);
isolated_validation!(
    sess_abandoned_retryable,
    pv::sessions::test_abandoned_session_item_retryable
);
isolated_validation!(
    sess_abandoned_ignore_attempt,
    pv::sessions::test_abandoned_session_item_ignore_attempt
);

// === Custom Status ===
isolated_validation!(custom_status_set, pv::custom_status::test_custom_status_set);
isolated_validation!(
    custom_status_clear,
    pv::custom_status::test_custom_status_clear
);
isolated_validation!(
    custom_status_none_preserves,
    pv::custom_status::test_custom_status_none_preserves
);
isolated_validation!(
    custom_status_version_increments,
    pv::custom_status::test_custom_status_version_increments
);
isolated_validation!(
    custom_status_polling_no_change,
    pv::custom_status::test_custom_status_polling_no_change
);
isolated_validation!(
    custom_status_nonexistent,
    pv::custom_status::test_custom_status_nonexistent_instance
);
isolated_validation!(
    custom_status_default_new_instance,
    pv::custom_status::test_custom_status_default_on_new_instance
);

// === Tag Filtering ===
isolated_validation!(
    tag_default_only,
    pv::tag_filtering::test_default_only_fetches_untagged
);
isolated_validation!(
    tag_specific_only,
    pv::tag_filtering::test_tags_fetches_only_matching
);
isolated_validation!(
    tag_default_and,
    pv::tag_filtering::test_default_and_fetches_untagged_and_matching
);
isolated_validation!(
    tag_any_fetches_all,
    pv::tag_filtering::test_any_filter_fetches_everything
);
isolated_validation!(
    tag_none_returns_nothing,
    pv::tag_filtering::test_none_filter_returns_nothing
);
isolated_validation!(tag_multi_filter, pv::tag_filtering::test_multi_tag_filter);
isolated_validation!(
    tag_round_trip,
    pv::tag_filtering::test_tag_round_trip_preservation
);
isolated_validation!(
    tag_preserved_through_ack,
    pv::tag_filtering::test_tag_preserved_through_ack_orchestration_item
);
isolated_validation!(
    tag_survives_abandon,
    pv::tag_filtering::test_tag_survives_abandon_and_refetch
);
isolated_validation!(
    tag_multi_runtime_isolation,
    pv::tag_filtering::test_multi_runtime_tag_isolation
);

// === KV Store & KV Delta ===
isolated_validation!(kv_set_and_get, pv::kv_store::test_kv_set_and_get);
isolated_validation!(kv_overwrite, pv::kv_store::test_kv_overwrite);
isolated_validation!(kv_clear_single, pv::kv_store::test_kv_clear_single);
isolated_validation!(kv_clear_all, pv::kv_store::test_kv_clear_all);
isolated_validation!(kv_get_nonexistent, pv::kv_store::test_kv_get_nonexistent);
isolated_validation!(
    kv_snapshot_in_fetch,
    pv::kv_store::test_kv_snapshot_in_fetch
);
isolated_validation!(kv_snapshot_empty, pv::kv_store::test_kv_snapshot_empty);
isolated_validation!(
    kv_instance_isolation,
    pv::kv_store::test_kv_instance_isolation
);
isolated_validation!(
    kv_execution_id_tracking,
    pv::kv_store::test_kv_execution_id_tracking
);
isolated_validation!(
    kv_cross_execution_overwrite,
    pv::kv_store::test_kv_cross_execution_overwrite
);
isolated_validation!(
    kv_cross_execution_remove_readd,
    pv::kv_store::test_kv_cross_execution_remove_readd
);
isolated_validation!(
    kv_prune_preserves_overwritten,
    pv::kv_store::test_kv_prune_preserves_overwritten
);
isolated_validation!(
    kv_prune_preserves_all_keys,
    pv::kv_store::test_kv_prune_preserves_all_keys
);
isolated_validation!(
    kv_delete_instance_cascades,
    pv::kv_store::test_kv_delete_instance_cascades
);
isolated_validation!(
    kv_clear_nonexistent_key,
    pv::kv_store::test_kv_clear_nonexistent_key
);
isolated_validation!(
    kv_get_unknown_instance,
    pv::kv_store::test_kv_get_unknown_instance
);
isolated_validation!(kv_set_after_clear, pv::kv_store::test_kv_set_after_clear);
isolated_validation!(kv_empty_value, pv::kv_store::test_kv_empty_value);
isolated_validation!(kv_large_value, pv::kv_store::test_kv_large_value);
isolated_validation!(
    kv_special_chars_in_key,
    pv::kv_store::test_kv_special_chars_in_key
);
isolated_validation!(
    kv_snapshot_after_clear_single,
    pv::kv_store::test_kv_snapshot_after_clear_single
);
isolated_validation!(
    kv_snapshot_after_clear_all,
    pv::kv_store::test_kv_snapshot_after_clear_all
);
isolated_validation!(
    kv_snapshot_cross_execution,
    pv::kv_store::test_kv_snapshot_cross_execution
);
isolated_validation!(
    kv_prune_current_protected,
    pv::kv_store::test_kv_prune_current_execution_protected
);
isolated_validation!(
    kv_delete_with_children,
    pv::kv_store::test_kv_delete_instance_with_children
);
isolated_validation!(kv_clear_isolation, pv::kv_store::test_kv_clear_isolation);
isolated_validation!(
    kv_delta_snapshot_excludes_current,
    pv::kv_store::test_kv_delta_snapshot_excludes_current_execution
);
isolated_validation!(
    kv_delta_snapshot_includes_completed,
    pv::kv_store::test_kv_delta_snapshot_includes_completed_execution
);
isolated_validation!(
    kv_delta_client_reads_merged,
    pv::kv_store::test_kv_delta_client_reads_merged
);
isolated_validation!(
    kv_delta_tombstone_overrides_store,
    pv::kv_store::test_kv_delta_tombstone_overrides_store
);
isolated_validation!(
    kv_delta_clear_all_tombstones_store,
    pv::kv_store::test_kv_delta_clear_all_tombstones_store
);
isolated_validation!(
    kv_delta_merged_on_completion,
    pv::kv_store::test_kv_delta_merged_on_completion
);
isolated_validation!(
    kv_delta_merged_on_can,
    pv::kv_store::test_kv_delta_merged_on_can
);
isolated_validation!(
    kv_delta_delete_cascades,
    pv::kv_store::test_kv_delta_delete_instance_cascades
);
isolated_validation!(
    kv_delta_prune_untouched_survives,
    pv::kv_store::test_kv_delta_prune_untouched_key_survives
);

// === Race & Replay Version Conformance ===
isolated_validation!(
    race_queue_cancellation_replay,
    pv::race_replay::test_queue_race_cancellation_replay
);
isolated_validation!(
    race_queue_replay_version_stamp_roundtrip,
    pv::race_replay::test_queue_replay_version_stamp_roundtrip
);
isolated_validation!(
    race_positional_wait_replay,
    pv::race_replay::test_positional_wait_race_replay
);
isolated_validation!(
    race_can_queue_replay,
    pv::race_replay::test_continue_as_new_queue_race_replay
);
isolated_validation!(
    race_can_duplicate_start,
    pv::race_replay::test_continue_as_new_duplicate_start
);
isolated_validation!(
    race_can_unregistered_backoff,
    pv::race_replay::test_continue_as_new_unregistered_backoff
);
isolated_validation!(
    race_can_poisoned_successor_own_exec,
    pv::race_replay::test_continue_as_new_poisoned_successor_is_own_execution
);
isolated_validation!(
    race_duplicate_start_preserves_pinned_handler,
    pv::race_replay::test_duplicate_start_preserves_pinned_handler
);
isolated_validation!(
    race_legacy_queue_decision_preserved,
    pv::race_replay::test_legacy_queue_race_decision_preserved
);

#[tokio::test]
async fn race_can_transition_delivery_legacy_stamp() {
    let Some(factory) = IsolatedMysqlTestFactory::new_or_skip().await else {
        return;
    };
    pv::race_replay::test_continue_as_new_transition_delivery(&factory, "0.1.30").await;
    factory.finish().await;
}

#[tokio::test]
async fn race_can_transition_delivery_new_stamp() {
    let Some(factory) = IsolatedMysqlTestFactory::new_or_skip().await else {
        return;
    };
    pv::race_replay::test_continue_as_new_transition_delivery(&factory, "0.1.31").await;
    factory.finish().await;
}
