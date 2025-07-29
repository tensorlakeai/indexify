use anyhow::Result;
use tracing::info;

use super::{contexts::MigrationContext, migration_trait::Migration};
use crate::state_store::state_machine::IndexifyObjectsColumns;

/// Migration to reset pending tasks and remove allocations
#[derive(Clone)]
pub struct V7ResetAllocatedTasksMigration {}

impl Migration for V7ResetAllocatedTasksMigration {
    fn version(&self) -> u64 {
        7
    }

    fn name(&self) -> &'static str {
        "Reset allocated tasks and drop allocations"
    }

    fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        let mut num_total_allocations: usize = 0;
        let mut num_deleted_allocations: usize = 0;
        let mut num_updated_tasks: usize = 0;

        // Collect all allocations
        let mut allocations_to_process = Vec::new();

        ctx.iterate_cf(&IndexifyObjectsColumns::Allocations, |key, value| {
            num_total_allocations += 1;
            allocations_to_process.push((key.to_vec(), value.to_vec()));
            Ok(())
        })?;

        // Process each allocation
        for (key, val_bytes) in allocations_to_process {
            let allocation: serde_json::Value = ctx.parse_json(&val_bytes)?;

            // Extract task information from the allocation
            let namespace = ctx.get_string_val(&allocation, "namespace")?;
            let compute_graph = ctx.get_string_val(&allocation, "compute_graph")?;
            let invocation_id = ctx.get_string_val(&allocation, "invocation_id")?;
            let compute_fn = ctx.get_string_val(&allocation, "compute_fn")?;
            let task_id = ctx.get_string_val(&allocation, "task_id")?;

            // Construct the task key
            let task_key =
                format!("{namespace}|{compute_graph}|{invocation_id}|{compute_fn}|{task_id}");

            // Update the task status to Pending
            let updated = ctx.update_json(
                &IndexifyObjectsColumns::Tasks,
                task_key.as_bytes(),
                |task| {
                    if let Some(task_obj) = task.as_object_mut() {
                        task_obj.insert(
                            "status".to_string(),
                            serde_json::Value::String("Pending".to_string()),
                        );
                        num_updated_tasks += 1;
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                },
            )?;

            if !updated {
                info!("Task not found for allocation: {}", task_key);
            }

            // Delete the allocation
            ctx.txn
                .delete_cf(ctx.cf(&IndexifyObjectsColumns::Allocations), &key)?;
            num_deleted_allocations += 1;
        }

        info!(
            "Dropped {} allocations and updated {} tasks out of {} total allocations",
            num_deleted_allocations, num_updated_tasks, num_total_allocations
        );

        Ok(())
    }

    fn box_clone(&self) -> Box<dyn Migration> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::state_store::migrations::testing::MigrationTestBuilder;

    #[test]
    fn test_v6_migration() -> Result<()> {
        let migration = V7ResetAllocatedTasksMigration {};

        MigrationTestBuilder::new()
            .with_column_family(IndexifyObjectsColumns::Allocations.as_ref())
            .with_column_family(IndexifyObjectsColumns::Tasks.as_ref())
            .run_test(
                &migration,
                |db| {
                    // Setup: Create test allocations and tasks

                    // Create tasks with different statuses
                    let tasks = vec![
                        (
                            b"test_ns|test_graph|inv1|test_fn1|task1".to_vec(),
                            json!({
                                "id": "task1",
                                "namespace": "test_ns",
                                "compute_graph_name": "test_graph",
                                "invocation_id": "inv1",
                                "compute_fn_name": "test_fn1",
                                "status": "Running"
                            }),
                        ),
                        (
                            b"test_ns|test_graph|inv1|test_fn2|task2".to_vec(),
                            json!({
                                "id": "task2",
                                "namespace": "test_ns",
                                "compute_graph_name": "test_graph",
                                "invocation_id": "inv1",
                                "compute_fn_name": "test_fn2",
                                "status": "Running"
                            }),
                        ),
                        // This task has no allocation, should remain unchanged
                        (
                            b"test_ns|test_graph|inv1|test_fn3|task3".to_vec(),
                            json!({
                                "id": "task3",
                                "namespace": "test_ns",
                                "compute_graph_name": "test_graph",
                                "invocation_id": "inv1",
                                "compute_fn_name": "test_fn3",
                                "status": "Running"
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

                    // Create allocations
                    let allocations = vec![
                        (
                            b"test_ns|test_graph|inv1|test_fn1|task1|exec1".to_vec(),
                            json!({
                                "namespace": "test_ns",
                                "compute_graph": "test_graph",
                                "invocation_id": "inv1",
                                "compute_fn": "test_fn1",
                                "task_id": "task1",
                                "executor_id": "exec1"
                            }),
                        ),
                        (
                            b"test_ns|test_graph|inv1|test_fn2|task2|exec2".to_vec(),
                            json!({
                                "namespace": "test_ns",
                                "compute_graph": "test_graph",
                                "invocation_id": "inv1",
                                "compute_fn": "test_fn2",
                                "task_id": "task2",
                                "executor_id": "exec2"
                            }),
                        ),
                        // This allocation has no task, it should be deleted without error
                        (
                            b"test_ns|test_graph|inv1|test_fn4|task4|exec1".to_vec(),
                            json!({
                                "namespace": "test_ns",
                                "compute_graph": "test_graph",
                                "invocation_id": "inv1",
                                "compute_fn": "test_fn4",
                                "task_id": "task4",
                                "executor_id": "exec1"
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

                    Ok(())
                },
                |db| {
                    // Verify: All allocations should be deleted, tasks should be updated

                    // Check no allocations remain
                    let all_allocations = db
                        .iterator_cf(
                            IndexifyObjectsColumns::Allocations.cf_db(db),
                            rocksdb::IteratorMode::Start,
                        )
                        .collect::<Vec<_>>();

                    assert!(
                        all_allocations.is_empty(),
                        "All allocations should be deleted"
                    );

                    // Check task statuses were updated properly
                    let check_task_status = |key: &[u8], expected_status: &str| -> Result<()> {
                        let bytes = db
                            .get_cf(IndexifyObjectsColumns::Tasks.cf_db(db), key)?
                            .unwrap();
                        let task: serde_json::Value = serde_json::from_slice(&bytes)?;
                        assert_eq!(task["status"].as_str().unwrap(), expected_status);
                        Ok(())
                    };

                    // Task1 and Task2 should be Pending
                    check_task_status(b"test_ns|test_graph|inv1|test_fn1|task1", "Pending")?;
                    check_task_status(b"test_ns|test_graph|inv1|test_fn2|task2", "Pending")?;

                    // Task3 should still be Running (unchanged)
                    check_task_status(b"test_ns|test_graph|inv1|test_fn3|task3", "Running")?;

                    Ok(())
                },
            )?;

        Ok(())
    }
}
