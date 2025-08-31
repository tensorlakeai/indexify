use anyhow::Result;
use tracing::info;

use super::{contexts::MigrationContext, migration_trait::Migration};
use crate::state_store::state_machine::IndexifyObjectsColumns;

#[derive(Clone)]
pub struct V6CleanOrphanedTasksMigration {}

impl Migration for V6CleanOrphanedTasksMigration {
    fn version(&self) -> u64 {
        6
    }

    fn name(&self) -> &'static str {
        "Clean orphaned tasks"
    }

    fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        let mut num_deleted_tasks = 0;
        let mut num_total_tasks = 0;

        // We need to collect keys first since we'll be modifying the collection
        let mut orphaned_task_keys = Vec::new();

        ctx.iterate_cf(&IndexifyObjectsColumns::Tasks, |key, value| {
            num_total_tasks += 1;

            let task: serde_json::Value = serde_json::from_slice(value)
                .map_err(|e| anyhow::anyhow!("error deserializing Tasks json bytes, {:#?}", e))?;

            let namespace = ctx.get_string_val(&task, "namespace")?;
            let compute_graph = ctx.get_string_val(&task, "compute_graph_name")?;
            let invocation_id = ctx.get_string_val(&task, "invocation_id")?;

            // Check if the task is orphaned by ensuring it has a graph invocation
            let invocation_ctx_key = format!("{namespace}|{compute_graph}|{invocation_id}");

            if ctx
                .db
                .get_cf(
                    ctx.cf(&IndexifyObjectsColumns::GraphInvocationCtx),
                    invocation_ctx_key.as_bytes(),
                )?
                .is_none()
            {
                // Mark the task for deletion
                orphaned_task_keys.push(key.to_vec());
            }

            Ok(())
        })?;

        // Delete the orphaned tasks
        for key in orphaned_task_keys {
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
    fn test_v5_clean_orphaned_tasks() -> Result<()> {
        let migration = V6CleanOrphanedTasksMigration {};

        MigrationTestBuilder::new()
            .with_column_family(IndexifyObjectsColumns::Tasks.as_ref())
            .with_column_family(IndexifyObjectsColumns::GraphInvocationCtx.as_ref())
            .run_test(
                &migration,
                |db| {
                    // Setup: Create tasks with different invocation statuses
                    let tasks = vec![
                        (
                            b"test_ns|test_graph|invocation1|test_fn|task1".to_vec(),
                            json!({
                                "id": "task1",
                                "namespace": "test_ns",
                                "compute_graph_name": "test_graph",
                                "invocation_id": "invocation1",
                                "compute_fn_name": "test_fn",
                                "input_node_output_key": "test_input",
                                "graph_version": "1",
                                "outcome": "Success",
                                "creation_time_ns": 0,
                            }),
                        ),
                        (
                            b"test_ns|test_graph|invocation2|test_fn|task2".to_vec(),
                            json!({
                                "id": "task2",
                                "namespace": "test_ns",
                                "compute_graph_name": "test_graph",
                                "invocation_id": "invocation2", // Orphaned
                                "compute_fn_name": "test_fn",
                                "input_node_output_key": "test_input",
                                "graph_version": "1",
                                "outcome": "Failure",
                                "creation_time_ns": 0,
                            }),
                        ),
                    ];

                    for (key, value) in &tasks {
                        db.put_cf(
                            db.column_family(IndexifyObjectsColumns::Tasks.as_ref()),
                            key,
                            serde_json::to_vec(value)?.as_slice(),
                        )?;
                    }

                    // Create invocation context only for invocation1
                    let invocation_ctx = json!({
                        "namespace": "test_ns",
                        "compute_graph_name": "test_graph",
                        "graph_version": "1",
                        "invocation_id": "invocation1",
                        "completed": false,
                        "outcome": "Undefined",
                        "outstanding_tasks": 0,
                        "fn_task_analytics": {}
                    });

                    let key = format!(
                        "{}|{}|{}",
                        invocation_ctx["namespace"].as_str().unwrap(),
                        invocation_ctx["compute_graph_name"].as_str().unwrap(),
                        invocation_ctx["invocation_id"].as_str().unwrap()
                    );

                    db.put_cf(
                        db.column_family(IndexifyObjectsColumns::GraphInvocationCtx.as_ref()),
                        key,
                        serde_json::to_vec(&invocation_ctx)?.as_slice(),
                    )?;

                    Ok(())
                },
                |db| {
                    // Verify: Check that orphaned task was deleted

                    // Task1 (not orphaned) should still exist
                    let cf = db.column_family(IndexifyObjectsColumns::Tasks.as_ref());
                    let task1_key = b"test_ns|test_graph|invocation1|test_fn|task1";
                    let task1_exists = db.get_cf(cf, task1_key)?.is_some();

                    assert!(task1_exists, "Task1 should still exist");

                    // Task2 (orphaned) should be deleted
                    let task2_key = b"test_ns|test_graph|invocation2|test_fn|task2";
                    let task2_exists = db.get_cf(cf, task2_key)?.is_some();

                    assert!(!task2_exists, "Task2 should be deleted");

                    Ok(())
                },
            )?;

        Ok(())
    }
}
