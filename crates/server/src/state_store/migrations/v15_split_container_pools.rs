use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::info;

use super::{
    contexts::{MigrationContext, PrepareContext},
    migration_trait::Migration,
};
use crate::{
    data_model::{
        ContainerPool,
        ContainerPoolBuilder,
        ContainerPoolId,
        ContainerPoolType,
        ContainerResources,
        NetworkPolicy,
    },
    state_store::{
        driver::{Writer, rocksdb::RocksDBDriver},
        serializer::{StateStoreEncode, StateStoreEncoder},
        state_machine::IndexifyObjectsColumns,
    },
};

/// Legacy ContainerPool layout matching the postcard schema written by V13.
/// Does NOT have the `pool_type` field that was added later.
#[derive(Debug, Deserialize, Serialize)]
struct LegacyContainerPool {
    id: ContainerPoolId,
    namespace: String,
    image: String,
    resources: ContainerResources,
    entrypoint: Option<Vec<String>>,
    network_policy: Option<NetworkPolicy>,
    secret_names: Vec<String>,
    timeout_secs: u64,
    min_containers: Option<u32>,
    max_containers: Option<u32>,
    buffer_containers: Option<u32>,
    created_at: u64,
    created_at_clock: Option<u64>,
    updated_at_clock: Option<u64>,
}

impl LegacyContainerPool {
    /// Convert to the current ContainerPool with the given new ID and pool
    /// type.
    fn into_pool(self, new_id: ContainerPoolId, pool_type: ContainerPoolType) -> ContainerPool {
        ContainerPoolBuilder::default()
            .id(new_id)
            .namespace(self.namespace)
            .pool_type(pool_type)
            .image(self.image)
            .resources(self.resources)
            .entrypoint(self.entrypoint)
            .network_policy(self.network_policy)
            .secret_names(self.secret_names)
            .timeout_secs(self.timeout_secs)
            .min_containers(self.min_containers)
            .max_containers(self.max_containers)
            .buffer_containers(self.buffer_containers)
            .created_at(self.created_at)
            .build()
            .expect("all required fields provided")
    }
}

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
/// - `ContainerPools` CF emptied (kept for RocksDB compatibility, dropped
///   later)
#[derive(Clone)]
pub struct V15SplitContainerPools;

/// The old function pool ID prefix used before this migration.
const OLD_FN_PREFIX: &str = "fn:";

/// Synthetic standalone sandbox pool ID prefix — these are discarded.
const OLD_SANDBOX_PREFIX: &str = "sandbox:";

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
                // Ensure ContainerPools exists (may be missing in some backups)
                let container_cf = IndexifyObjectsColumns::ContainerPools.to_string();
                if !existing_cfs.contains(&container_cf) {
                    db.create(&container_cf, &Default::default()).await?;
                }

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
        let mut discarded: usize = 0;

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

            if old_pool_id.starts_with(OLD_SANDBOX_PREFIX) {
                // Synthetic standalone sandbox pool — discard, no real pool object
                // existed for these.
                discarded += 1;
            } else if let Some(inner) = old_pool_id.strip_prefix(OLD_FN_PREFIX) {
                // Decode using legacy layout (no pool_type field) since the old
                // postcard data was written before pool_type was added.
                let legacy: LegacyContainerPool = StateStoreEncoder::decode(value_bytes)?;

                // Function pool: old ID = "fn:{ns}:{app}:{fn}:{ver}"
                // New ID = "{app}|{fn}|{ver}"
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

                let pool = legacy.into_pool(
                    ContainerPoolId::new(new_pool_id.clone()),
                    ContainerPoolType::Function,
                );
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
                // User-created sandbox pool: keep ID as-is, move to SandboxPools CF
                let legacy: LegacyContainerPool = StateStoreEncoder::decode(value_bytes)?;
                let pool = legacy.into_pool(
                    ContainerPoolId::new(old_pool_id.to_string()),
                    ContainerPoolType::Sandbox,
                );
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
            "V15 split ContainerPools: {total} total, {fn_pools} function pools, {sb_pools} sandbox pools, {discarded} discarded standalone sandbox pools"
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
        data_model::{ContainerPoolId, ContainerPoolType, ContainerResources},
        state_store::{
            driver::{Reader, Writer},
            migrations::testing::MigrationTestBuilder,
            state_machine::IndexifyObjectsColumns,
        },
    };

    #[tokio::test]
    async fn test_v15_splits_container_pools() -> Result<()> {
        let migration = V15SplitContainerPools;

        // Encode using the LEGACY layout (no pool_type) to match real production data
        let fn_pool = LegacyContainerPool {
            id: ContainerPoolId::new("fn:test_ns:my_app:my_fn:v1".to_string()),
            namespace: "test_ns".to_string(),
            image: String::new(),
            resources: ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 256,
                gpu: None,
                ephemeral_disk_mb: 0,
            },
            entrypoint: None,
            network_policy: None,
            secret_names: vec![],
            timeout_secs: 0,
            min_containers: None,
            max_containers: None,
            buffer_containers: None,
            created_at: 1000,
            created_at_clock: None,
            updated_at_clock: None,
        };
        let fn_pool_bytes = StateStoreEncoder::encode(&fn_pool)?;
        let fn_pool_key = b"test_ns|fn:test_ns:my_app:my_fn:v1";

        // Create a sandbox pool in the old format (no fn: prefix)
        let sb_pool = LegacyContainerPool {
            id: ContainerPoolId::new("my_sandbox_pool".to_string()),
            namespace: "test_ns".to_string(),
            image: "my-image:latest".to_string(),
            resources: ContainerResources {
                cpu_ms_per_sec: 500,
                memory_mb: 128,
                gpu: None,
                ephemeral_disk_mb: 0,
            },
            entrypoint: None,
            network_policy: None,
            secret_names: vec![],
            timeout_secs: 0,
            min_containers: None,
            max_containers: None,
            buffer_containers: None,
            created_at: 2000,
            created_at_clock: None,
            updated_at_clock: None,
        };
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
                            .get(IndexifyObjectsColumns::ContainerPools.as_ref(), fn_pool_key)
                            .await?;
                        assert!(
                            old_fn.is_none(),
                            "old ContainerPools entry should be deleted"
                        );

                        let old_sb = db
                            .get(IndexifyObjectsColumns::ContainerPools.as_ref(), sb_pool_key)
                            .await?;
                        assert!(
                            old_sb.is_none(),
                            "old ContainerPools entry should be deleted"
                        );

                        // Function pool should be in FunctionPools with new key format
                        let new_fn_key = b"test_ns|my_app|my_fn|v1";
                        let fn_result = db
                            .get(IndexifyObjectsColumns::FunctionPools.as_ref(), new_fn_key)
                            .await?
                            .expect("function pool should exist in FunctionPools");
                        let fn_decoded: ContainerPool = StateStoreEncoder::decode(&fn_result)?;
                        assert_eq!(fn_decoded.id.get(), "my_app|my_fn|v1");
                        assert_eq!(fn_decoded.namespace, "test_ns");
                        assert_eq!(fn_decoded.pool_type, ContainerPoolType::Function);

                        // Sandbox pool should be in SandboxPools with same key
                        let sb_result = db
                            .get(IndexifyObjectsColumns::SandboxPools.as_ref(), sb_pool_key)
                            .await?
                            .expect("sandbox pool should exist in SandboxPools");
                        let sb_decoded: ContainerPool = StateStoreEncoder::decode(&sb_result)?;
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
