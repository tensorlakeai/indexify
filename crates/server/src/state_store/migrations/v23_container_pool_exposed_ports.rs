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

/// Legacy ContainerPool layout from v22 and earlier, before
/// `allow_unauthenticated_access` and `exposed_ports` were added.
#[derive(Debug, Deserialize, Serialize)]
struct V22ContainerPool {
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
    tombstoned: bool,
    #[serde(default)]
    created_at_clock: Option<u64>,
    #[serde(default)]
    updated_at_clock: Option<u64>,
    // NOTE: no allow_unauthenticated_access or exposed_ports fields
}

fn default_pool_type() -> ContainerPoolType {
    ContainerPoolType::Sandbox
}

fn repair_pool_from_legacy(legacy: V22ContainerPool) -> ContainerPool {
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
        .tombstoned(legacy.tombstoned)
        .created_at_clock(legacy.created_at_clock)
        .updated_at_clock(legacy.updated_at_clock)
        .allow_unauthenticated_access(false)
        .exposed_ports(None)
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

        match StateStoreEncoder::decode::<V22ContainerPool>(value) {
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
                    "V23 deleting unreadable container pool row"
                );
                ctx.txn.delete(cf.as_ref(), key).await?;
                deleted += 1;
            }
        }
    }

    Ok((total, recovered, deleted))
}

/// Migration to repair ContainerPool records written before
/// `allow_unauthenticated_access` and `exposed_ports` were added.
///
/// Postcard is positional; adding fields at the end is not
/// backward-compatible, so existing records must be rewritten.
#[derive(Clone)]
pub struct V23ContainerPoolExposedPorts;

#[async_trait]
impl Migration for V23ContainerPoolExposedPorts {
    fn version(&self) -> u64 {
        23
    }

    fn name(&self) -> &'static str {
        "Add allow_unauthenticated_access and exposed_ports fields to ContainerPool"
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
            "V23 container pool exposed_ports repair complete"
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
    async fn test_v23_repairs_legacy_container_pool_rows() -> Result<()> {
        let migration = V23ContainerPoolExposedPorts;

        let legacy_pool = V22ContainerPool {
            id: ContainerPoolId::new("sandbox_pool_legacy"),
            namespace: "ns".to_string(),
            pool_type: ContainerPoolType::Sandbox,
            image: "python:3.12".to_string(),
            resources: ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            },
            entrypoint: Some(vec!["python".to_string()]),
            network_policy: None,
            secret_names: vec!["secret_a".to_string()],
            timeout_secs: 300,
            min_containers: Some(1),
            max_containers: Some(4),
            buffer_containers: Some(1),
            created_at: 111,
            tombstoned: false,
            created_at_clock: Some(11),
            updated_at_clock: Some(12),
        };
        let legacy_bytes = StateStoreEncoder::encode(&legacy_pool)?;
        assert!(
            StateStoreEncoder::decode::<ContainerPool>(&legacy_bytes).is_err(),
            "legacy pool bytes should fail current decode before migration"
        );

        let current_pool = ContainerPoolBuilder::default()
            .id(ContainerPoolId::new("sandbox_pool_current"))
            .namespace("ns".to_string())
            .pool_type(ContainerPoolType::Sandbox)
            .image("python:3.12".to_string())
            .resources(ContainerResources {
                cpu_ms_per_sec: 500,
                memory_mb: 256,
                ephemeral_disk_mb: 512,
                gpu: None,
            })
            .timeout_secs(60)
            .allow_unauthenticated_access(true)
            .exposed_ports(Some(vec![8080, 9501]))
            .build()
            .expect("valid current pool");
        let current_bytes = StateStoreEncoder::encode(&current_pool)?;

        let legacy_key = ContainerPoolKey::new("ns", &legacy_pool.id)
            .to_string()
            .into_bytes();
        let current_key = ContainerPoolKey::new("ns", &current_pool.id)
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
                    let legacy_key = legacy_key.clone();
                    let current_key = current_key.clone();
                    let legacy_bytes = legacy_bytes.clone();
                    let current_bytes = current_bytes.clone();
                    Box::pin(async move {
                        db.put(
                            IndexifyObjectsColumns::SandboxPools.as_ref(),
                            &legacy_key,
                            &legacy_bytes,
                        )
                        .await?;
                        db.put(
                            IndexifyObjectsColumns::SandboxPools.as_ref(),
                            &current_key,
                            &current_bytes,
                        )
                        .await?;
                        Ok(())
                    })
                },
                |db| {
                    let legacy_key = legacy_key.clone();
                    let current_key = current_key.clone();
                    Box::pin(async move {
                        let repaired_legacy = db
                            .get(IndexifyObjectsColumns::SandboxPools.as_ref(), &legacy_key)
                            .await?
                            .expect("legacy row should exist after migration");
                        let repaired_legacy: ContainerPool =
                            StateStoreEncoder::decode(&repaired_legacy)?;
                        assert_eq!(repaired_legacy.id, legacy_pool.id);
                        assert!(!repaired_legacy.allow_unauthenticated_access);
                        assert!(repaired_legacy.exposed_ports.is_none());
                        assert!(!repaired_legacy.tombstoned);

                        let unchanged_current = db
                            .get(IndexifyObjectsColumns::SandboxPools.as_ref(), &current_key)
                            .await?
                            .expect("current row should exist after migration");
                        let unchanged_current: ContainerPool =
                            StateStoreEncoder::decode(&unchanged_current)?;
                        assert!(unchanged_current.allow_unauthenticated_access);
                        assert_eq!(unchanged_current.exposed_ports, Some(vec![8080, 9501]));

                        Ok(())
                    })
                },
            )
            .await?;

        Ok(())
    }
}
