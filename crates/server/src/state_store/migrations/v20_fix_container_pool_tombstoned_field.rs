use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::{contexts::MigrationContext, migration_trait::Migration};
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
        serializer::{StateStoreEncode, StateStoreEncoder},
        state_machine::IndexifyObjectsColumns,
    },
};

/// Legacy ContainerPool layout from before `tombstoned` was added.
#[derive(Debug, Deserialize, Serialize)]
struct LegacyContainerPool {
    id: ContainerPoolId,
    namespace: String,
    #[serde(default = "default_pool_type")]
    pool_type: ContainerPoolType,
    image: String,
    resources: ContainerResources,
    #[serde(default)]
    entrypoint: Option<Vec<String>>,
    #[serde(default)]
    network_policy: Option<NetworkPolicy>,
    #[serde(default)]
    secret_names: Vec<String>,
    #[serde(default)]
    timeout_secs: u64,
    #[serde(default)]
    min_containers: Option<u32>,
    #[serde(default)]
    max_containers: Option<u32>,
    #[serde(default)]
    buffer_containers: Option<u32>,
    created_at: u64,
    #[serde(default)]
    created_at_clock: Option<u64>,
    #[serde(default)]
    updated_at_clock: Option<u64>,
    // NOTE: no tombstoned field
}

fn default_pool_type() -> ContainerPoolType {
    ContainerPoolType::Sandbox
}

fn repair_pool_from_legacy(legacy: LegacyContainerPool) -> ContainerPool {
    ContainerPoolBuilder::default()
        .id(legacy.id)
        .namespace(legacy.namespace)
        .pool_type(legacy.pool_type)
        .image(legacy.image)
        .resources(legacy.resources)
        .entrypoint(legacy.entrypoint)
        .network_policy(legacy.network_policy)
        .secret_names(legacy.secret_names)
        .timeout_secs(legacy.timeout_secs)
        .min_containers(legacy.min_containers)
        .max_containers(legacy.max_containers)
        .buffer_containers(legacy.buffer_containers)
        .created_at(legacy.created_at)
        .tombstoned(false)
        .created_at_clock(legacy.created_at_clock)
        .updated_at_clock(legacy.updated_at_clock)
        .build()
        .expect("all required fields provided")
}

async fn repair_cf(
    ctx: &MigrationContext,
    cf: IndexifyObjectsColumns,
) -> Result<(usize, usize, usize)> {
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    ctx.iterate(&cf, |key, value| {
        entries.push((key.to_vec(), value.to_vec()));
        Ok(())
    })
    .await?;

    let total = entries.len();
    let mut recovered: usize = 0;
    let mut deleted: usize = 0;

    for (key, value) in &entries {
        if StateStoreEncoder::decode::<ContainerPool>(value).is_ok() {
            continue;
        }

        match StateStoreEncoder::decode::<LegacyContainerPool>(value) {
            Ok(legacy) => {
                let repaired = repair_pool_from_legacy(legacy);
                let encoded = StateStoreEncoder::encode(&repaired)?;
                ctx.txn.put(cf.as_ref(), key, &encoded).await?;
                recovered += 1;
            }
            Err(err) => {
                warn!(
                    cf = %cf,
                    key = %String::from_utf8_lossy(key),
                    error = ?err,
                    "V20 deleting unreadable container pool row"
                );
                ctx.txn.delete(cf.as_ref(), key).await?;
                deleted += 1;
            }
        }
    }

    Ok((total, recovered, deleted))
}

/// Migration to repair ContainerPool records written before `tombstoned` was
/// added.
///
/// Postcard is positional; adding fields is not backward-compatible.
#[derive(Clone)]
pub struct V20FixContainerPoolTombstonedField;

#[async_trait]
impl Migration for V20FixContainerPoolTombstonedField {
    fn version(&self) -> u64 {
        20
    }

    fn name(&self) -> &'static str {
        "Fix ContainerPool postcard schema (add tombstoned default)"
    }

    async fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        let (fn_total, fn_recovered, fn_deleted) =
            repair_cf(ctx, IndexifyObjectsColumns::FunctionPools).await?;
        let (sb_total, sb_recovered, sb_deleted) =
            repair_cf(ctx, IndexifyObjectsColumns::SandboxPools).await?;

        info!(
            fn_total,
            fn_recovered,
            fn_deleted,
            sb_total,
            sb_recovered,
            sb_deleted,
            "V20 container pool repair complete"
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
        data_model::ContainerPoolKey,
        state_store::{
            driver::{Reader, Writer},
            migrations::testing::MigrationTestBuilder,
            state_machine::IndexifyObjectsColumns,
        },
    };

    #[tokio::test]
    async fn test_v20_repairs_legacy_container_pool_rows() -> Result<()> {
        let migration = V20FixContainerPoolTombstonedField;

        let legacy_function_pool = LegacyContainerPool {
            id: ContainerPoolId::new("app|fn|v1"),
            namespace: "ns".to_string(),
            pool_type: ContainerPoolType::Function,
            image: "python:3.12".to_string(),
            resources: ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            },
            entrypoint: None,
            network_policy: None,
            secret_names: vec![],
            timeout_secs: 300,
            min_containers: Some(0),
            max_containers: Some(4),
            buffer_containers: Some(1),
            created_at: 111,
            created_at_clock: Some(11),
            updated_at_clock: Some(12),
        };
        let legacy_function_bytes = StateStoreEncoder::encode(&legacy_function_pool)?;
        assert!(
            StateStoreEncoder::decode::<ContainerPool>(&legacy_function_bytes).is_err(),
            "legacy container pool bytes should fail current decode before migration"
        );

        let legacy_sandbox_pool = LegacyContainerPool {
            id: ContainerPoolId::new("sandbox_pool_1"),
            namespace: "ns".to_string(),
            pool_type: ContainerPoolType::Sandbox,
            image: "python:3.12".to_string(),
            resources: ContainerResources {
                cpu_ms_per_sec: 500,
                memory_mb: 256,
                ephemeral_disk_mb: 512,
                gpu: None,
            },
            entrypoint: Some(vec!["python".to_string()]),
            network_policy: None,
            secret_names: vec!["secret_a".to_string()],
            timeout_secs: 60,
            min_containers: Some(1),
            max_containers: Some(2),
            buffer_containers: Some(0),
            created_at: 222,
            created_at_clock: Some(21),
            updated_at_clock: Some(22),
        };
        let legacy_sandbox_bytes = StateStoreEncoder::encode(&legacy_sandbox_pool)?;

        let current_pool = ContainerPoolBuilder::default()
            .id(ContainerPoolId::new("sandbox_pool_current"))
            .namespace("ns".to_string())
            .pool_type(ContainerPoolType::Sandbox)
            .image("python:3.12".to_string())
            .resources(ContainerResources {
                cpu_ms_per_sec: 250,
                memory_mb: 128,
                ephemeral_disk_mb: 256,
                gpu: None,
            })
            .timeout_secs(30)
            .tombstoned(true)
            .build()
            .expect("valid current pool");
        let current_bytes = StateStoreEncoder::encode(&current_pool)?;

        let corrupt_bytes = vec![0x01, 0xAA, 0xBB];

        let legacy_function_key = ContainerPoolKey::new("ns", &legacy_function_pool.id)
            .to_string()
            .into_bytes();
        let legacy_sandbox_key = ContainerPoolKey::new("ns", &legacy_sandbox_pool.id)
            .to_string()
            .into_bytes();
        let current_key = ContainerPoolKey::new("ns", &current_pool.id)
            .to_string()
            .into_bytes();
        let corrupt_key = ContainerPoolKey::new("ns", &ContainerPoolId::new("corrupt_pool"))
            .to_string()
            .into_bytes();

        let mut builder = MigrationTestBuilder::new();
        for cf in IndexifyObjectsColumns::iter() {
            builder = builder.with_column_family(cf.as_ref());
        }

        builder
            .run_test(
                &migration,
                |db| {
                    let legacy_function_key = legacy_function_key.clone();
                    let legacy_sandbox_key = legacy_sandbox_key.clone();
                    let current_key = current_key.clone();
                    let corrupt_key = corrupt_key.clone();
                    let legacy_function_bytes = legacy_function_bytes.clone();
                    let legacy_sandbox_bytes = legacy_sandbox_bytes.clone();
                    let current_bytes = current_bytes.clone();
                    let corrupt_bytes = corrupt_bytes.clone();
                    Box::pin(async move {
                        db.put(
                            IndexifyObjectsColumns::FunctionPools.as_ref(),
                            &legacy_function_key,
                            &legacy_function_bytes,
                        )
                        .await?;
                        db.put(
                            IndexifyObjectsColumns::SandboxPools.as_ref(),
                            &legacy_sandbox_key,
                            &legacy_sandbox_bytes,
                        )
                        .await?;
                        db.put(
                            IndexifyObjectsColumns::SandboxPools.as_ref(),
                            &current_key,
                            &current_bytes,
                        )
                        .await?;
                        db.put(
                            IndexifyObjectsColumns::SandboxPools.as_ref(),
                            &corrupt_key,
                            &corrupt_bytes,
                        )
                        .await?;
                        Ok(())
                    })
                },
                |db| {
                    let legacy_function_key = legacy_function_key.clone();
                    let legacy_sandbox_key = legacy_sandbox_key.clone();
                    let current_key = current_key.clone();
                    let corrupt_key = corrupt_key.clone();
                    Box::pin(async move {
                        let repaired_function = db
                            .get(
                                IndexifyObjectsColumns::FunctionPools.as_ref(),
                                &legacy_function_key,
                            )
                            .await?
                            .expect("function pool should be present after repair");
                        let repaired_function: ContainerPool =
                            StateStoreEncoder::decode(&repaired_function)?;
                        assert_eq!(repaired_function.id, legacy_function_pool.id);
                        assert!(!repaired_function.tombstoned);
                        assert_eq!(repaired_function.created_at, 111);
                        assert_eq!(repaired_function.pool_type, ContainerPoolType::Function);

                        let repaired_sandbox = db
                            .get(
                                IndexifyObjectsColumns::SandboxPools.as_ref(),
                                &legacy_sandbox_key,
                            )
                            .await?
                            .expect("sandbox pool should be present after repair");
                        let repaired_sandbox: ContainerPool =
                            StateStoreEncoder::decode(&repaired_sandbox)?;
                        assert_eq!(repaired_sandbox.id, legacy_sandbox_pool.id);
                        assert!(!repaired_sandbox.tombstoned);
                        assert_eq!(repaired_sandbox.created_at, 222);
                        assert_eq!(repaired_sandbox.pool_type, ContainerPoolType::Sandbox);

                        let preserved_current = db
                            .get(IndexifyObjectsColumns::SandboxPools.as_ref(), &current_key)
                            .await?
                            .expect("current pool should remain");
                        let preserved_current: ContainerPool =
                            StateStoreEncoder::decode(&preserved_current)?;
                        assert_eq!(preserved_current.id, current_pool.id);
                        assert!(preserved_current.tombstoned);

                        let deleted_corrupt = db
                            .get(IndexifyObjectsColumns::SandboxPools.as_ref(), &corrupt_key)
                            .await?;
                        assert!(
                            deleted_corrupt.is_none(),
                            "unreadable pool row should be deleted"
                        );

                        Ok(())
                    })
                },
            )
            .await?;

        Ok(())
    }
}
