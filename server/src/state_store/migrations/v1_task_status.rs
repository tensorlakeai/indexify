use anyhow::Result;
use tracing::info;

use super::{contexts::MigrationContext, migration_trait::Migration};
use crate::state_store::state_machine::IndexifyObjectsColumns;

#[derive(Clone)]
pub struct V1TaskStatusMigration {}

impl Migration for V1TaskStatusMigration {
    fn version(&self) -> u64 {
        1
    }

    fn name(&self) -> &'static str {
        "Add status field to tasks"
    }

    fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        let mut num_total_tasks: usize = 0;
        let mut num_migrated_tasks: usize = 0;

        ctx.iterate(&IndexifyObjectsColumns::Tasks, |key, _value| {
            num_total_tasks += 1;

            ctx.update_json(&IndexifyObjectsColumns::Tasks, key, |task_json| {
                let task_obj = task_json
                    .as_object_mut()
                    .ok_or_else(|| anyhow::anyhow!("unexpected task JSON value"))?;

                let outcome = task_obj
                    .get("outcome")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("unexpected task outcome JSON value"))?;

                let status_undefined = match task_obj.get("status") {
                    Some(serde_json::Value::String(status)) => status.is_empty(),
                    Some(serde_json::Value::Null) => true,
                    None => true,
                    _ => false,
                };

                if status_undefined {
                    num_migrated_tasks += 1;
                    if outcome == "Success" || outcome == "Failure" {
                        task_obj.insert(
                            "status".to_string(),
                            serde_json::Value::String("Completed".to_string()),
                        );
                    } else {
                        task_obj.insert(
                            "status".to_string(),
                            serde_json::Value::String("Pending".to_string()),
                        );
                    }

                    Ok(true) // Task was modified
                } else {
                    Ok(false) // No changes needed
                }
            })?;

            Ok(())
        })?;

        info!(
            "Migrated {}/{} tasks: added status field",
            num_migrated_tasks, num_total_tasks
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
    fn test_v1_migration() -> Result<()> {
        let migration = V1TaskStatusMigration {};

        MigrationTestBuilder::new()
            .with_column_family(IndexifyObjectsColumns::Tasks.as_ref())
            .run_test(
                &migration,
                |db| {
                    // Setup: Insert test tasks with different outcomes and no status field
                    let tasks = vec![
                        (
                            b"test_ns|test_graph|test_invoc|test_fn|task1".to_vec(),
                            json!({
                                "id": "task1",
                                "namespace": "test_ns",
                                "compute_graph_name": "test_graph",
                                "invocation_id": "test_invoc",
                                "compute_fn_name": "test_fn",
                                "outcome": "Success"
                            }),
                        ),
                        (
                            b"test_ns|test_graph|test_invoc|test_fn|task2".to_vec(),
                            json!({
                                "id": "task2",
                                "namespace": "test_ns",
                                "compute_graph_name": "test_graph",
                                "invocation_id": "test_invoc",
                                "compute_fn_name": "test_fn",
                                "outcome": "Failure"
                            }),
                        ),
                        (
                            b"test_ns|test_graph|test_invoc|test_fn|task3".to_vec(),
                            json!({
                                "id": "task3",
                                "namespace": "test_ns",
                                "compute_graph_name": "test_graph",
                                "invocation_id": "test_invoc",
                                "compute_fn_name": "test_fn",
                                "outcome": "Unknown"
                            }),
                        ),
                        (
                            b"test_ns|test_graph|test_invoc|test_fn|task4".to_vec(),
                            json!({
                                "id": "task4",
                                "namespace": "test_ns",
                                "compute_graph_name": "test_graph",
                                "invocation_id": "test_invoc",
                                "compute_fn_name": "test_fn",
                                "outcome": "Success",
                                "status": "AlreadySet"
                            }),
                        ),
                    ];

                    for (key, value) in tasks {
                        db.put(
                            IndexifyObjectsColumns::Tasks.as_ref(),
                            &key,
                            serde_json::to_vec(&value)?.as_slice(),
                        )?;
                    }

                    Ok(())
                },
                |db| {
                    // Verify: Check that status fields were added properly
                    let verify_status = |key: &[u8], expected_status: &str| -> Result<()> {
                        let cf = IndexifyObjectsColumns::Tasks.as_ref();
                        let bytes = db.get(cf, key)?.unwrap();
                        let task: serde_json::Value = serde_json::from_slice(&bytes)?;
                        assert_eq!(task["status"].as_str().unwrap(), expected_status);
                        Ok(())
                    };

                    verify_status(b"test_ns|test_graph|test_invoc|test_fn|task1", "Completed")?;
                    verify_status(b"test_ns|test_graph|test_invoc|test_fn|task2", "Completed")?;
                    verify_status(b"test_ns|test_graph|test_invoc|test_fn|task3", "Pending")?;
                    verify_status(b"test_ns|test_graph|test_invoc|test_fn|task4", "AlreadySet")?;

                    Ok(())
                },
            )?;

        Ok(())
    }
}
