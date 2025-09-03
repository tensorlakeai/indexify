use anyhow::{Context, Result};
use tracing::info;

use super::{
    contexts::{MigrationContext, PrepareContext},
    migration_trait::Migration,
};
use crate::state_store::{driver::rocksdb::RocksDBDriver, state_machine::IndexifyObjectsColumns};

#[derive(Clone)]
pub struct V3SecondaryIndexesMigration {}

impl Migration for V3SecondaryIndexesMigration {
    fn version(&self) -> u64 {
        3
    }

    fn name(&self) -> &'static str {
        "Create invocation context secondary indexes"
    }

    fn prepare(&self, ctx: &PrepareContext) -> Result<RocksDBDriver> {
        // Check if secondary index CF already exists, if not create it
        let existing_cfs = ctx.list_cfs()?;

        if !existing_cfs.contains(
            &IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex
                .as_ref()
                .to_string(),
        ) {
            ctx.reopen_with_cf_operations(|db| {
                info!("Creating secondary index column family");
                db.create(
                    IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex.as_ref(),
                    &rocksdb::Options::default(),
                )?;
                Ok(())
            })
        } else {
            info!("Secondary index column family already exists");
            ctx.open_db()
        }
    }

    fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        // First clear any existing secondary indexes
        let deleted_count =
            ctx.truncate_cf(&IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex)?;
        if deleted_count > 0 {
            info!("Cleared {} existing secondary index entries", deleted_count);
        }

        let mut num_total_invocation_ctx: usize = 0;
        let mut num_indexed_invocation_ctx: usize = 0;

        ctx.iterate(
            &IndexifyObjectsColumns::GraphInvocationCtx,
            |_key, value| {
                num_total_invocation_ctx += 1;

                // Parse the invocation context as JSON
                let invocation_ctx: serde_json::Value = serde_json::from_slice(value)
                    .context("Error parsing GraphInvocationCtx as JSON")?;

                // Create secondary index key
                let secondary_index_key = create_secondary_index_key(&invocation_ctx)?;

                // Store the secondary index (key -> empty value)
                ctx.txn.put(
                    IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex.as_ref(),
                    &secondary_index_key,
                    [],
                )?;

                num_indexed_invocation_ctx += 1;
                Ok(())
            },
        )?;

        info!(
            "Created secondary indexes for {}/{} invocation contexts",
            num_indexed_invocation_ctx, num_total_invocation_ctx
        );

        Ok(())
    }

    fn box_clone(&self) -> Box<dyn Migration> {
        Box::new(self.clone())
    }
}

/// Create a secondary index key from a JSON representation of
/// GraphInvocationCtx
/// Format: namespace|compute_graph_name|created_at_bytes|invocation_id
fn create_secondary_index_key(invocation_ctx: &serde_json::Value) -> Result<Vec<u8>> {
    let namespace = invocation_ctx["namespace"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Missing namespace in invocation context"))?;

    let compute_graph_name = invocation_ctx["compute_graph_name"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Missing compute_graph_name in invocation context"))?;

    let created_at = invocation_ctx["created_at"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("Missing or invalid created_at in invocation context"))?;

    let invocation_id = invocation_ctx["invocation_id"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Missing invocation_id in invocation context"))?;

    let mut key = Vec::new();
    key.extend_from_slice(namespace.as_bytes());
    key.push(b'|');
    key.extend_from_slice(compute_graph_name.as_bytes());
    key.push(b'|');
    key.extend_from_slice(&created_at.to_be_bytes());
    key.push(b'|');
    key.extend_from_slice(invocation_id.as_bytes());

    Ok(key)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::state_store::{driver::Reader, migrations::testing::MigrationTestBuilder};

    #[test]
    fn test_v3_migration() -> Result<()> {
        let migration = V3SecondaryIndexesMigration {};

        MigrationTestBuilder::new()
            .with_column_family(IndexifyObjectsColumns::GraphInvocationCtx.as_ref())
            .run_test(
                &migration,
                |db| {
                    // Setup: Create test invocation contexts
                    let contexts = vec![
                        json!({
                            "namespace": "test_ns",
                            "compute_graph_name": "graph1",
                            "graph_version": "1",
                            "invocation_id": "inv1",
                            "created_at": 1000,
                            "completed": false,
                            "outcome": "Success",
                            "outstanding_tasks": 0,
                            "fn_task_analytics": {}
                        }),
                        json!({
                            "namespace": "test_ns",
                            "compute_graph_name": "graph1",
                            "graph_version": "1",
                            "invocation_id": "inv2",
                            "created_at": 2000,
                            "completed": true,
                            "outcome": "Success",
                            "outstanding_tasks": 0,
                            "fn_task_analytics": {}
                        }),
                        json!({
                            "namespace": "other_ns",
                            "compute_graph_name": "graph2",
                            "graph_version": "1",
                            "invocation_id": "inv3",
                            "created_at": 3000,
                            "completed": false,
                            "outcome": "Success",
                            "outstanding_tasks": 0,
                            "fn_task_analytics": {}
                        }),
                    ];

                    for ctx_obj in &contexts {
                        let key = format!(
                            "{}|{}|{}",
                            ctx_obj["namespace"].as_str().unwrap(),
                            ctx_obj["compute_graph_name"].as_str().unwrap(),
                            ctx_obj["invocation_id"].as_str().unwrap()
                        );
                        let encoded = serde_json::to_vec(ctx_obj).unwrap();
                        db.put(
                            IndexifyObjectsColumns::GraphInvocationCtx.as_ref(),
                            key,
                            &encoded,
                        )?;
                    }

                    Ok(())
                },
                |db| {
                    // Verify secondary indexes were created
                    let contexts = vec![
                        json!({
                            "namespace": "test_ns",
                            "compute_graph_name": "graph1",
                            "graph_version": "1",
                            "invocation_id": "inv1",
                            "created_at": 1000,
                            "completed": false,
                            "outcome": "Success",
                            "outstanding_tasks": 0,
                            "fn_task_analytics": {}
                        }),
                        json!({
                            "namespace": "test_ns",
                            "compute_graph_name": "graph1",
                            "graph_version": "1",
                            "invocation_id": "inv2",
                            "created_at": 2000,
                            "completed": true,
                            "outcome": "Success",
                            "outstanding_tasks": 0,
                            "fn_task_analytics": {}
                        }),
                        json!({
                            "namespace": "other_ns",
                            "compute_graph_name": "graph2",
                            "graph_version": "1",
                            "invocation_id": "inv3",
                            "created_at": 3000,
                            "completed": false,
                            "outcome": "Success",
                            "outstanding_tasks": 0,
                            "fn_task_analytics": {}
                        }),
                    ];

                    // Check secondary index CF was created
                    let iter = db.iter(
                        IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex.as_ref(),
                        Default::default(),
                    );
                    assert!(!iter.collect::<Vec<_>>().is_empty());

                    // Check secondary indexes were created
                    for ctx_obj in &contexts {
                        let secondary_key = create_secondary_index_key(ctx_obj)?;
                        let exists = db
                            .get(
                                IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex.as_ref(),
                                &secondary_key,
                            )?
                            .is_some();

                        assert!(exists, "Secondary index not found for {ctx_obj:?}");
                    }

                    // Count secondary indexes
                    let secondary_indexes = db
                        .iter(
                            IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex.as_ref(),
                            Default::default(),
                        )
                        .collect::<Vec<_>>();

                    assert_eq!(
                        secondary_indexes.len(),
                        3,
                        "Should have 3 secondary indexes"
                    );

                    Ok(())
                },
            )?;

        Ok(())
    }
}
