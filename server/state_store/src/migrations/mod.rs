pub mod contexts;
pub mod migration_trait;
pub mod registry;
#[cfg(test)]
mod testing;

// migrations
mod v1_task_status;
mod v2_invocation_ctx_timestamps;
mod v3_invocation_ctx_secondary_index;
mod v4_drop_executors;
mod v5_allocation_keys;
mod v6_clean_orphaned_tasks;
mod v7_reset_allocated_tasks;
mod v8_rebuild_invocation_ctx_secondary_index;
// Add new migrations mod here
