use anyhow::{Context, Result};
use tracing::info;

use super::{
    contexts::{MigrationContext, PrepareContext},
    migration_trait::Migration,
};
use crate::state_store::{driver::rocksdb::RocksDBDriver, state_machine::IndexifyObjectsColumns};

/// Migration to rebuild the invocation context secondary indexes by dropping
/// and recreating the column family
#[derive(Clone)]
pub struct V8RebuildInvocationCtxSecondaryIndexMigration {}

impl Migration for V8RebuildInvocationCtxSecondaryIndexMigration {
    fn version(&self) -> u64 {
        8
    }

    fn name(&self) -> &'static str {
        "Rebuild invocation context secondary indexes"
    }

    fn prepare(&self, ctx: &PrepareContext) -> Result<RocksDBDriver> {
        // Drop and recreate the secondary index column family instead of truncating
        info!("Rebuilding secondary index column family");

        ctx.reopen_with_cf_operations(|db| {
            // Drop if exists
            let cf_name = IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex.as_ref();
            info!("Dropping secondary index column family");
            db.drop(cf_name)?;

            // Create fresh
            info!("Creating new secondary index column family");
            db.create(cf_name, &Default::default())?;

            Ok(())
        })
    }

    fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        let mut num_total_invocation_ctx: usize = 0;
        let mut num_migrated_invocation_ctx: usize = 0;

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

                num_migrated_invocation_ctx += 1;
                Ok(())
            },
        )?;

        info!(
            "Rebuilt secondary indexes for {}/{} invocation contexts",
            num_migrated_invocation_ctx, num_total_invocation_ctx
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
    fn test_v7_migration() -> Result<()> {
        let migration = V8RebuildInvocationCtxSecondaryIndexMigration {};

        MigrationTestBuilder::new()
            .with_column_family(IndexifyObjectsColumns::GraphInvocationCtx.as_ref())
            .with_column_family(IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex.as_ref())
            .run_test(
                &migration,
                |db| {
                    // Setup: Create invocation contexts and invalid secondary indexes

                    // Create contexts
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

                    // Create some invalid secondary indexes (to be replaced)
                    db.put(
                        IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex.as_ref(),
                        b"invalid_index_1",
                        [],
                    )?;

                    db.put(
                        IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex.as_ref(),
                        b"invalid_index_2",
                        [],
                    )?;

                    Ok(())
                },
                |db| {
                    // Verify: Invalid secondary indexes should be gone, correct ones created

                    // Check invalid indexes are gone
                    assert!(db
                        .get(
                            IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex.as_ref(),
                            b"invalid_index_1"
                        )?
                        .is_none());

                    assert!(db
                        .get(
                            IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex.as_ref(),
                            b"invalid_index_2"
                        )?
                        .is_none());

                    // Check correct indexes are present
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

                    let cf = IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex.as_ref();
                    for ctx_obj in &contexts {
                        let secondary_key = create_secondary_index_key(ctx_obj)?;
                        let exists = db.get(cf, &secondary_key)?.is_some();

                        assert!(exists, "Secondary index not found for {ctx_obj:?}");
                    }

                    // Check total count of secondary indexes
                    let secondary_indexes = db.iter(cf, Default::default()).collect::<Vec<_>>();

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
