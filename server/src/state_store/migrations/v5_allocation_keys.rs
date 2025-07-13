use anyhow::Result;
use tracing::info;

use super::{contexts::MigrationContext, migration_trait::Migration};
use crate::state_store::state_machine::IndexifyObjectsColumns;

/// Migration to reformat allocation keys and clean up orphaned allocations
#[derive(Clone)]
pub struct V5AllocationKeysMigration {}

impl Migration for V5AllocationKeysMigration {
    fn version(&self) -> u64 {
        5
    }

    fn name(&self) -> &'static str {
        "Reformat allocation keys and clean orphaned allocations"
    }

    fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        // First migrate allocations to new key format and clean orphaned ones
        self.migrate_allocations(ctx)?;

        // Then clean orphaned tasks
        self.clean_orphaned_tasks(ctx)?;

        Ok(())
    }

    fn box_clone(&self) -> Box<dyn Migration> {
        Box::new(self.clone())
    }
}

impl V5AllocationKeysMigration {
    fn migrate_allocations(&self, ctx: &MigrationContext) -> Result<()> {
        let mut num_total_allocations: usize = 0;
        let mut num_migrated_allocations: usize = 0;
        let mut num_deleted_allocations: usize = 0;

        // We need to collect all keys and values first since we'll be modifying the CF
        let mut allocations_to_process = Vec::new();

        ctx.iterate_cf(&IndexifyObjectsColumns::Allocations, |key, value| {
            num_total_allocations += 1;
            allocations_to_process.push((key.to_vec(), value.to_vec()));
            Ok(())
        })?;

        // Process allocations
        for (key, val_bytes) in allocations_to_process {
            let allocation: serde_json::Value = ctx.parse_json(&val_bytes)?;

            // Extract fields for new key
            let namespace = ctx.get_string_val(&allocation, "namespace")?;
            let compute_graph = ctx.get_string_val(&allocation, "compute_graph")?;
            let invocation_id = ctx.get_string_val(&allocation, "invocation_id")?;
            let compute_fn = ctx.get_string_val(&allocation, "compute_fn")?;
            let task_id = ctx.get_string_val(&allocation, "task_id")?;
            let executor_id = ctx.get_string_val(&allocation, "executor_id")?;

            // Create new allocation key
            let new_allocation_key = format!(
                "{}|{}|{}|{}|{}|{}",
                namespace, compute_graph, invocation_id, compute_fn, task_id, executor_id
            );

            // Delete the old allocation
            ctx.txn
                .delete_cf(ctx.cf(&IndexifyObjectsColumns::Allocations), &key)?;

            // Check if the allocation is orphaned by ensuring it has a graph invocation ctx
            let invocation_ctx_key = format!("{}|{}|{}", namespace, compute_graph, invocation_id);

            if ctx
                .db
                .get_cf(
                    ctx.cf(&IndexifyObjectsColumns::GraphInvocationCtx),
                    invocation_ctx_key.as_bytes(),
                )?
                .is_some()
            {
                // Re-insert with new key
                ctx.txn.put_cf(
                    ctx.cf(&IndexifyObjectsColumns::Allocations),
                    new_allocation_key.as_bytes(),
                    &val_bytes,
                )?;
                num_migrated_allocations += 1;
            } else {
                // Allocation is orphaned, don't re-insert
                num_deleted_allocations += 1;
            }
        }

        info!(
            "Migrated {} allocations and deleted {} orphaned allocations from {} total allocations",
            num_migrated_allocations, num_deleted_allocations, num_total_allocations
        );

        Ok(())
    }

    fn clean_orphaned_tasks(&self, ctx: &MigrationContext) -> Result<()> {
        let mut num_total_tasks: usize = 0;
        let mut num_deleted_tasks: usize = 0;

        // Collect tasks to delete
        let mut tasks_to_delete = Vec::new();

        ctx.iterate_cf(&IndexifyObjectsColumns::Tasks, |key, value| {
            num_total_tasks += 1;

            let task: serde_json::Value = ctx.parse_json(&value)?;

            let namespace = ctx.get_string_val(&task, "namespace")?;
            let compute_graph = ctx.get_string_val(&task, "compute_graph_name")?;
            let invocation_id = ctx.get_string_val(&task, "invocation_id")?;

            // Check if the task is orphaned
            let invocation_ctx_key = format!("{}|{}|{}", namespace, compute_graph, invocation_id);

            if ctx
                .db
                .get_cf(
                    ctx.cf(&IndexifyObjectsColumns::GraphInvocationCtx),
                    invocation_ctx_key.as_bytes(),
                )?
                .is_none()
            {
                // Task is orphaned, mark for deletion
                tasks_to_delete.push(key.to_vec());
            }

            Ok(())
        })?;

        // Delete orphaned tasks
        for key in tasks_to_delete {
            ctx.txn
                .delete_cf(ctx.cf(&IndexifyObjectsColumns::Tasks), &key)?;
            num_deleted_tasks += 1;
        }

        info!(
            "Deleted {} orphaned tasks out of {}",
            num_deleted_tasks, num_total_tasks
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::state_store::migrations::testing::MigrationTestBuilder;

    #[test]
    fn test_v5_migration() -> Result<()> {
        let migration = V5AllocationKeysMigration {};

        MigrationTestBuilder::new()
            .with_column_family(IndexifyObjectsColumns::Allocations.as_ref())
            .with_column_family(IndexifyObjectsColumns::Tasks.as_ref())
            .with_column_family(IndexifyObjectsColumns::GraphInvocationCtx.as_ref())
            .run_test(
                &migration,
                |db| {
                    // Setup: Create test allocations and tasks

                    // First create invocation contexts to reference
                    let contexts = vec![
                        (
                            b"test_ns|test_graph|inv1".to_vec(),
                            json!({
                                "namespace": "test_ns",
                                "compute_graph_name": "test_graph",
                                "invocation_id": "inv1",
                                "graph_version": "1",
                                "completed": false
                            }),
                        ),
                        // No inv2 - to test orphaned references
                    ];

                    for (key, value) in &contexts {
                        db.put_cf(
                            IndexifyObjectsColumns::GraphInvocationCtx.cf_db(db),
                            key,
                            serde_json::to_vec(value)?.as_slice(),
                        )?;
                    }

                    // Create allocations - some with old-style keys
                    let allocations = vec![
                        (
                            b"allocation1".to_vec(),
                            json!({
                                "id": "allocation1",
                                "namespace": "test_ns",
                                "compute_graph": "test_graph",
                                "invocation_id": "inv1",
                                "compute_fn": "test_fn1",
                                "task_id": "task1",
                                "executor_id": "exec1"
                            }),
                        ),
                        (
                            b"allocation2".to_vec(),
                            json!({
                                "id": "allocation2",
                                "namespace": "test_ns",
                                "compute_graph": "test_graph",
                                "invocation_id": "inv2", // Orphaned
                                "compute_fn": "test_fn2",
                                "task_id": "task2",
                                "executor_id": "exec2"
                            }),
                        ),
                    ];

                    for (key, value) in &allocations {
                        db.put_cf(
                            IndexifyObjectsColumns::Allocations.cf_db(db),
                            key,
                            serde_json::to_vec(value)?.as_slice(),
                        )?;
                    }

                    // Create tasks - some valid, some orphaned
                    let tasks = vec![
                        (
                            b"test_ns|test_graph|inv1|test_fn1|task1".to_vec(),
                            json!({
                                "id": "task1",
                                "namespace": "test_ns",
                                "compute_graph_name": "test_graph",
                                "invocation_id": "inv1",
                                "compute_fn_name": "test_fn1"
                            }),
                        ),
                        (
                            b"test_ns|test_graph|inv2|test_fn2|task2".to_vec(), // Orphaned
                            json!({
                                "id": "task2",
                                "namespace": "test_ns",
                                "compute_graph_name": "test_graph",
                                "invocation_id": "inv2",
                                "compute_fn_name": "test_fn2"
                            }),
                        ),
                    ];

                    for (key, value) in &tasks {
                        db.put_cf(
                            IndexifyObjectsColumns::Tasks.cf_db(db),
                            key,
                            serde_json::to_vec(value)?.as_slice(),
                        )?;
                    }

                    Ok(())
                },
                |db| {
                    // Verify: Check that allocations were migrated correctly and orphaned items
                    // removed

                    // Old allocation keys should be gone
                    assert!(db
                        .get_cf(
                            IndexifyObjectsColumns::Allocations.cf_db(db),
                            b"allocation1"
                        )?
                        .is_none());

                    assert!(db
                        .get_cf(
                            IndexifyObjectsColumns::Allocations.cf_db(db),
                            b"allocation2"
                        )?
                        .is_none());

                    // Valid allocation should be migrated with new key format
                    let new_key = b"test_ns|test_graph|inv1|test_fn1|task1|exec1";
                    let migrated_allocation =
                        db.get_cf(IndexifyObjectsColumns::Allocations.cf_db(db), new_key)?;

                    assert!(
                        migrated_allocation.is_some(),
                        "Valid allocation should be migrated with new key"
                    );

                    // Orphaned allocation should not exist
                    let orphaned_key = b"test_ns|test_graph|inv2|test_fn2|task2|exec2";
                    let orphaned_allocation =
                        db.get_cf(IndexifyObjectsColumns::Allocations.cf_db(db), orphaned_key)?;

                    assert!(
                        orphaned_allocation.is_none(),
                        "Orphaned allocation should be deleted"
                    );

                    // Valid task should still exist
                    let valid_task = db.get_cf(
                        IndexifyObjectsColumns::Tasks.cf_db(db),
                        b"test_ns|test_graph|inv1|test_fn1|task1",
                    )?;

                    assert!(valid_task.is_some(), "Valid task should still exist");

                    // Orphaned task should be deleted
                    let orphaned_task = db.get_cf(
                        IndexifyObjectsColumns::Tasks.cf_db(db),
                        b"test_ns|test_graph|inv2|test_fn2|task2",
                    )?;

                    assert!(orphaned_task.is_none(), "Orphaned task should be deleted");

                    Ok(())
                },
            )?;

        Ok(())
    }
}
