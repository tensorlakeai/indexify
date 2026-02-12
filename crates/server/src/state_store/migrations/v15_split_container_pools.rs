use anyhow::Result;
use async_trait::async_trait;
use tracing::info;

use super::{
    contexts::{MigrationContext, PrepareContext},
    migration_trait::Migration,
};
use crate::{
    data_model::ContainerPool,
    state_store::{
        driver::{Writer, rocksdb::RocksDBDriver},
        serializer::{StateStoreEncode, StateStoreEncoder},
        state_machine::IndexifyObjectsColumns,
    },
};

/// Migration to split the single `ContainerPools` column family into
/// `FunctionPools` and `SandboxPools`, and rewrite pool ID format.
///
/// Before this migration:
/// - All pools stored in `ContainerPools` CF
/// - Function pool IDs: `fn:{namespace}:{app}:{function}:{version}`
/// - Pool key: `{namespace}|{pool_id}` (with `fn:…` prefix in pool_id)
///
/// After this migration:
/// - Function pools in `FunctionPools` CF with pool ID `{app}|{fn}|{ver}`
/// - Sandbox pools in `SandboxPools` CF with their original pool ID
/// - `ContainerPools` CF emptied (kept for RocksDB compatibility, dropped later)
#[derive(Clone)]
pub struct V15SplitContainerPools;

/// The old function pool ID prefix used before this migration.
const OLD_FN_PREFIX: &str = "fn:";

#[async_trait]
impl Migration for V15SplitContainerPools {
    fn version(&self) -> u64 {
        15
    }

    fn name(&self) -> &'static str {
        "Split ContainerPools into FunctionPools and SandboxPools"
    }

    async fn prepare(&self, ctx: &PrepareContext) -> Result<RocksDBDriver> {
        let existing_cfs = ctx.list_cfs().unwrap_or_default();
        ctx.reopen_with_cf_operations(|db| {
            Box::pin(async move {
                let fn_cf = IndexifyObjectsColumns::FunctionPools.to_string();
                if !existing_cfs.contains(&fn_cf) {
                    db.create(&fn_cf, &Default::default()).await?;
                }

                let sb_cf = IndexifyObjectsColumns::SandboxPools.to_string();
                if !existing_cfs.contains(&sb_cf) {
                    db.create(&sb_cf, &Default::default()).await?;
                }

                Ok(())
            })
        })
        .await
    }

    async fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        let mut total: usize = 0;
        let mut fn_pools: usize = 0;
        let mut sb_pools: usize = 0;

        // Collect all entries from the old ContainerPools CF
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        ctx.iterate(&IndexifyObjectsColumns::ContainerPools, |key, value| {
            entries.push((key.to_vec(), value.to_vec()));
            Ok(())
        })
        .await?;

        for (old_key_bytes, value_bytes) in &entries {
            total += 1;

            let old_key = String::from_utf8_lossy(old_key_bytes);
            // Old key format: "{namespace}|{pool_id}"
            let Some((namespace, old_pool_id)) = old_key.split_once('|') else {
                info!("V15: skipping entry with unexpected key format: {old_key}");
                continue;
            };

            // Decode the pool value (may be JSON or bincode from V13)
            let mut pool: ContainerPool = StateStoreEncoder::decode(&value_bytes)?;

            if old_pool_id.starts_with(OLD_FN_PREFIX) {
                // Function pool: old ID = "fn:{ns}:{app}:{fn}:{ver}"
                // New ID = "{app}|{fn}|{ver}"
                let inner = &old_pool_id[OLD_FN_PREFIX.len()..];
                // Skip the namespace segment (first colon-separated part)
                let new_pool_id = if let Some((_ns_part, rest)) = inner.split_once(':') {
                    // rest = "{app}:{fn}:{ver}" → replace colons with pipes
                    rest.replace(':', "|")
                } else {
                    // Unexpected format — keep as-is
                    info!(
                        "V15: function pool with unexpected ID format: {old_pool_id}, keeping as-is"
                    );
                    old_pool_id.to_string()
                };

                pool.id = crate::data_model::ContainerPoolId::new(new_pool_id.clone());
                pool.pool_type = crate::data_model::ContainerPoolType::Function;
                let new_key = format!("{}|{}", namespace, new_pool_id);
                let encoded = StateStoreEncoder::encode(&pool)?;
                ctx.txn
                    .put(
                        IndexifyObjectsColumns::FunctionPools.as_ref(),
                        new_key.as_bytes(),
                        &encoded,
                    )
                    .await?;
                fn_pools += 1;
            } else {
                // Sandbox pool: keep ID as-is, just move to SandboxPools CF
                pool.pool_type = crate::data_model::ContainerPoolType::Sandbox;
                let new_key = format!("{}|{}", namespace, old_pool_id);
                let encoded = StateStoreEncoder::encode(&pool)?;
                ctx.txn
                    .put(
                        IndexifyObjectsColumns::SandboxPools.as_ref(),
                        new_key.as_bytes(),
                        &encoded,
                    )
                    .await?;
                sb_pools += 1;
            }

            // Delete from old CF
            ctx.txn
                .delete(
                    IndexifyObjectsColumns::ContainerPools.as_ref(),
                    old_key_bytes,
                )
                .await?;
        }

        info!(
            "V15 split ContainerPools: {total} total, {fn_pools} function pools, {sb_pools} sandbox pools"
        );

        Ok(())
    }

    fn box_clone(&self) -> Box<dyn Migration> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator;

    use super::*;
    use crate::{
        data_model::{ContainerPoolBuilder, ContainerPoolId, ContainerPoolType, ContainerResources},
        state_store::{
            driver::{Reader, Writer},
            migrations::testing::MigrationTestBuilder,
            state_machine::IndexifyObjectsColumns,
        },
    };

    #[tokio::test]
    async fn test_v15_splits_container_pools() -> Result<()> {
        let migration = V15SplitContainerPools;

        // Create a function pool in the old format
        let fn_pool = ContainerPoolBuilder::default()
            .id(ContainerPoolId::new(
                "fn:test_ns:my_app:my_fn:v1".to_string(),
            ))
            .namespace("test_ns".to_string())
            .pool_type(ContainerPoolType::Sandbox) // old default, will be corrected
            .image(String::new())
            .resources(ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 256,
                gpu: None,
                ephemeral_disk_mb: 0,
            })
            .build()?;
        let fn_pool_bytes = StateStoreEncoder::encode(&fn_pool)?;
        let fn_pool_key = b"test_ns|fn:test_ns:my_app:my_fn:v1";

        // Create a sandbox pool in the old format (no fn: prefix)
        let sb_pool = ContainerPoolBuilder::default()
            .id(ContainerPoolId::new("my_sandbox_pool".to_string()))
            .namespace("test_ns".to_string())
            .pool_type(ContainerPoolType::Sandbox)
            .image("my-image:latest".to_string())
            .resources(ContainerResources {
                cpu_ms_per_sec: 500,
                memory_mb: 128,
                gpu: None,
                ephemeral_disk_mb: 0,
            })
            .build()?;
        let sb_pool_bytes = StateStoreEncoder::encode(&sb_pool)?;
        let sb_pool_key = b"test_ns|my_sandbox_pool";

        let mut builder = MigrationTestBuilder::new();
        for cf in IndexifyObjectsColumns::iter() {
            builder = builder.with_column_family(cf.as_ref());
        }

        builder
            .run_test(
                &migration,
                |db| {
                    Box::pin(async move {
                        // Insert into old ContainerPools CF
                        db.put(
                            IndexifyObjectsColumns::ContainerPools.as_ref(),
                            fn_pool_key,
                            &fn_pool_bytes,
                        )
                        .await?;
                        db.put(
                            IndexifyObjectsColumns::ContainerPools.as_ref(),
                            sb_pool_key,
                            &sb_pool_bytes,
                        )
                        .await?;
                        Ok(())
                    })
                },
                |db| {
                    Box::pin(async move {
                        // Old CF should be empty
                        let old_fn = db
                            .get(
                                IndexifyObjectsColumns::ContainerPools.as_ref(),
                                fn_pool_key,
                            )
                            .await?;
                        assert!(old_fn.is_none(), "old ContainerPools entry should be deleted");

                        let old_sb = db
                            .get(
                                IndexifyObjectsColumns::ContainerPools.as_ref(),
                                sb_pool_key,
                            )
                            .await?;
                        assert!(old_sb.is_none(), "old ContainerPools entry should be deleted");

                        // Function pool should be in FunctionPools with new key format
                        let new_fn_key = b"test_ns|my_app|my_fn|v1";
                        let fn_result = db
                            .get(
                                IndexifyObjectsColumns::FunctionPools.as_ref(),
                                new_fn_key,
                            )
                            .await?
                            .expect("function pool should exist in FunctionPools");
                        let fn_decoded: ContainerPool =
                            StateStoreEncoder::decode(&fn_result)?;
                        assert_eq!(fn_decoded.id.get(), "my_app|my_fn|v1");
                        assert_eq!(fn_decoded.namespace, "test_ns");
                        assert_eq!(fn_decoded.pool_type, ContainerPoolType::Function);

                        // Sandbox pool should be in SandboxPools with same key
                        let sb_result = db
                            .get(
                                IndexifyObjectsColumns::SandboxPools.as_ref(),
                                sb_pool_key,
                            )
                            .await?
                            .expect("sandbox pool should exist in SandboxPools");
                        let sb_decoded: ContainerPool =
                            StateStoreEncoder::decode(&sb_result)?;
                        assert_eq!(sb_decoded.id.get(), "my_sandbox_pool");
                        assert_eq!(sb_decoded.namespace, "test_ns");
                        assert_eq!(sb_decoded.pool_type, ContainerPoolType::Sandbox);

                        Ok(())
                    })
                },
            )
            .await?;

        Ok(())
    }
}
