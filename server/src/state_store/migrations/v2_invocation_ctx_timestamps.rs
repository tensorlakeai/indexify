use anyhow::Result;
use tracing::info;

use super::{contexts::MigrationContext, migration_trait::Migration};
use crate::state_store::state_machine::IndexifyObjectsColumns;

#[derive(Clone)]
pub struct V2InvocationTimestampsMigration {}

impl Migration for V2InvocationTimestampsMigration {
    fn version(&self) -> u64 {
        2
    }

    fn name(&self) -> &'static str {
        "Add timestamps to invocation contexts"
    }

    fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        let mut num_total_invocation_ctx: usize = 0;
        let mut num_migrated_invocation_ctx: usize = 0;

        ctx.iterate(
            &IndexifyObjectsColumns::GraphInvocationCtx,
            |key, _value| {
                num_total_invocation_ctx += 1;
                let key_str = String::from_utf8_lossy(key);

                ctx.update_json(
                    &IndexifyObjectsColumns::GraphInvocationCtx,
                    key,
                    |invocation_ctx| {
                        let cf = IndexifyObjectsColumns::GraphInvocations.as_ref();
                        let invocation_bytes = ctx.db.get(cf, key)?.ok_or_else(|| {
                            anyhow::anyhow!("invocation not found for invocation ctx: {}", key_str)
                        })?;

                        let invocation = ctx.parse_json(&invocation_bytes)?;

                        let created_at = invocation
                            .get("created_at")
                            .and_then(|v| v.as_u64())
                            .ok_or_else(|| {
                                anyhow::anyhow!("created_at not found in invocation: {}", key_str)
                            })?;

                        let ctx_obj = invocation_ctx.as_object_mut().ok_or_else(|| {
                            anyhow::anyhow!("unexpected invocation ctx JSON value {}", key_str)
                        })?;

                        ctx_obj.insert(
                            "created_at".to_string(),
                            serde_json::Value::Number(serde_json::Number::from(created_at)),
                        );

                        num_migrated_invocation_ctx += 1;
                        Ok(true)
                    },
                )?;

                Ok(())
            },
        )?;

        info!(
            "Migrated {}/{} invocation contexts: added timestamps",
            num_migrated_invocation_ctx, num_total_invocation_ctx
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
    fn test_v2_migration() -> Result<()> {
        let migration = V2InvocationTimestampsMigration {};

        MigrationTestBuilder::new()
            .with_column_family(IndexifyObjectsColumns::GraphInvocationCtx.as_ref())
            .with_column_family(IndexifyObjectsColumns::GraphInvocations.as_ref())
            .run_test(
                &migration,
                |db| {
                    // Setup: Insert test invocations and contexts
                    let invocations = vec![
                        (
                            b"test_ns|test_graph|inv1".to_vec(),
                            json!({
                                "id": "inv1",
                                "namespace": "test_ns",
                                "compute_graph_name": "test_graph",
                                "created_at": 1000,
                                "payload": {
                                    "path": "test_path",
                                    "size": 123
                                }
                            }),
                        ),
                        (
                            b"test_ns|test_graph|inv2".to_vec(),
                            json!({
                                "id": "inv2",
                                "namespace": "test_ns",
                                "compute_graph_name": "test_graph",
                                "created_at": 2000,
                                "payload": {
                                    "path": "test_path2",
                                    "size": 456
                                }
                            }),
                        ),
                    ];

                    let contexts = vec![
                        (
                            b"test_ns|test_graph|inv1".to_vec(),
                            json!({
                                "namespace": "test_ns",
                                "compute_graph_name": "test_graph",
                                "invocation_id": "inv1",
                                "graph_version": "1",
                                "completed": false,
                                "outcome": "Undefined",
                                "outstanding_tasks": 0
                            }),
                        ),
                        (
                            b"test_ns|test_graph|inv2".to_vec(),
                            json!({
                                "namespace": "test_ns",
                                "compute_graph_name": "test_graph",
                                "invocation_id": "inv2",
                                "graph_version": "1",
                                "completed": false,
                                "outcome": "Undefined",
                                "outstanding_tasks": 0
                            }),
                        ),
                    ];

                    for (key, value) in invocations {
                        db.put(
                            IndexifyObjectsColumns::GraphInvocations.as_ref(),
                            &key,
                            serde_json::to_vec(&value)?.as_slice(),
                        )?;
                    }

                    for (key, value) in contexts {
                        db.put(
                            IndexifyObjectsColumns::GraphInvocationCtx.as_ref(),
                            &key,
                            serde_json::to_vec(&value)?.as_slice(),
                        )?;
                    }

                    Ok(())
                },
                |db| {
                    // Verify: Check that timestamps were added to contexts
                    let verify_timestamp = |key: &[u8], expected_timestamp: u64| -> Result<()> {
                        let cf = IndexifyObjectsColumns::GraphInvocationCtx.as_ref();
                        let bytes = db.get(cf, key)?.unwrap();
                        let ctx_json: serde_json::Value = serde_json::from_slice(&bytes)?;
                        assert_eq!(ctx_json["created_at"].as_u64().unwrap(), expected_timestamp);
                        Ok(())
                    };

                    verify_timestamp(b"test_ns|test_graph|inv1", 1000)?;
                    verify_timestamp(b"test_ns|test_graph|inv2", 2000)?;

                    Ok(())
                },
            )?;

        Ok(())
    }
}
