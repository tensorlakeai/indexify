pub mod contexts;
pub mod migration_trait;
pub mod registry;
#[cfg(test)]
mod testing;

// migrations
// Diptanu - Leaving these here so that we can see
// how to write new migrations
mod v1_fake_migration;
//mod v2_invocation_ctx_timestamps;
//mod v3_invocation_ctx_secondary_index;
//mod v4_drop_executors;
//mod v5_allocation_keys;
//mod v6_clean_orphaned_tasks;
//mod v7_reset_allocated_tasks;
//mod v8_rebuild_invocation_ctx_secondary_index;
// Add new migrations mod here
mod v10_allocation_output_event_format;
mod v11_sandbox_data_model_changes;
mod v12_slim_allocation_output_event;
mod v13_reencode_json_as_bincode;
mod v14_normalize_request_ctx;
mod v15_split_container_pools;
mod v16_sandbox_pending_reason;
mod v17_sandbox_snapshot_field;
mod v18_fix_corrupt_request_finished_events;
mod v9_separate_executor_and_app_state_changes;
