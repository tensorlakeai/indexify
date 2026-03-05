use anyhow::{Result, anyhow};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::info;

use super::{contexts::MigrationContext, migration_trait::Migration};
use crate::{
    data_model::{
        ContainerId,
        ContainerPoolId,
        ContainerResources,
        ExecutorId,
        NetworkPolicy,
        Sandbox,
        SandboxBuilder,
        SandboxId,
        SandboxOutcome,
        SandboxStatus,
        SnapshotId,
    },
    state_store::{
        serializer::{StateStoreEncode, StateStoreEncoder},
        state_machine::IndexifyObjectsColumns,
    },
};

/// Legacy Sandbox layout from v21 and earlier, before `exposed_ports` was
/// added.
#[derive(Debug, Deserialize, Serialize)]
struct V21Sandbox {
    id: SandboxId,
    namespace: String,
    image: String,
    status: SandboxStatus,
    outcome: Option<SandboxOutcome>,
    creation_time_ns: u128,
    #[serde(default)]
    created_at_clock: Option<u64>,
    #[serde(default)]
    updated_at_clock: Option<u64>,
    resources: ContainerResources,
    secret_names: Vec<String>,
    timeout_secs: u64,
    #[serde(default)]
    allow_unauthenticated_access: bool,
    executor_id: Option<ExecutorId>,
    entrypoint: Option<Vec<String>>,
    #[serde(default)]
    network_policy: Option<NetworkPolicy>,
    #[serde(default)]
    pool_id: Option<ContainerPoolId>,
    #[serde(default)]
    container_id: Option<ContainerId>,
    #[serde(default)]
    snapshot_id: Option<SnapshotId>,
}

/// Migration to repair Sandbox records written before `exposed_ports` was
/// added.
///
/// Postcard is positional; adding fields at the end of a struct is not
/// backward-compatible, so existing records must be rewritten.
#[derive(Clone)]
pub struct V22SandboxExposedPorts;

#[async_trait]
impl Migration for V22SandboxExposedPorts {
    fn version(&self) -> u64 {
        22
    }

    fn name(&self) -> &'static str {
        "Add exposed_ports field to Sandbox"
    }

    async fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        ctx.iterate(&IndexifyObjectsColumns::Sandboxes, |key, value| {
            entries.push((key.to_vec(), value.to_vec()));
            Ok(())
        })
        .await?;

        let total = entries.len();
        let mut migrated: usize = 0;

        for (key, value) in &entries {
            if StateStoreEncoder::decode::<Sandbox>(value).is_ok() {
                continue;
            }

            let legacy: V21Sandbox = StateStoreEncoder::decode(value).map_err(|e| {
                anyhow!(
                    "V22 failed to decode sandbox key={} as current or legacy schema: {}",
                    String::from_utf8_lossy(key),
                    e
                )
            })?;

            let repaired = SandboxBuilder::default()
                .id(legacy.id)
                .namespace(legacy.namespace)
                .image(legacy.image)
                .status(legacy.status)
                .outcome(legacy.outcome)
                .creation_time_ns(legacy.creation_time_ns)
                .created_at_clock(legacy.created_at_clock)
                .updated_at_clock(legacy.updated_at_clock)
                .resources(legacy.resources)
                .secret_names(legacy.secret_names)
                .timeout_secs(legacy.timeout_secs)
                .allow_unauthenticated_access(legacy.allow_unauthenticated_access)
                .executor_id(legacy.executor_id)
                .entrypoint(legacy.entrypoint)
                .network_policy(legacy.network_policy)
                .pool_id(legacy.pool_id)
                .container_id(legacy.container_id)
                .snapshot_id(legacy.snapshot_id)
                .exposed_ports(None)
                .build()
                .expect("all fields provided");

            let encoded = StateStoreEncoder::encode(&repaired)?;
            ctx.txn
                .put(IndexifyObjectsColumns::Sandboxes.as_ref(), key, &encoded)
                .await?;
            migrated += 1;
        }

        info!(total, migrated, "V22 sandbox exposed_ports repair complete");
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
        data_model::{ContainerResources, SandboxKey},
        state_store::{
            driver::{Reader, Writer},
            migrations::testing::MigrationTestBuilder,
            state_machine::IndexifyObjectsColumns,
        },
    };

    #[tokio::test]
    async fn test_v22_repairs_legacy_sandbox_rows() -> Result<()> {
        let migration = V22SandboxExposedPorts;

        let legacy_sandbox = V21Sandbox {
            id: SandboxId::new("sandbox_legacy".to_string()),
            namespace: "ns".to_string(),
            image: "python:3.12".to_string(),
            status: SandboxStatus::Running,
            outcome: None,
            creation_time_ns: 111,
            created_at_clock: Some(11),
            updated_at_clock: Some(12),
            resources: ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            },
            secret_names: vec!["secret_a".to_string()],
            timeout_secs: 300,
            allow_unauthenticated_access: true,
            executor_id: Some(ExecutorId::new("exec_1".to_string())),
            entrypoint: Some(vec!["python".to_string()]),
            network_policy: None,
            pool_id: Some(ContainerPoolId::new("pool_1")),
            container_id: Some(ContainerId::new("container_1".to_string())),
            snapshot_id: Some(SnapshotId::new("snapshot_1".to_string())),
        };
        let legacy_bytes = StateStoreEncoder::encode(&legacy_sandbox)?;
        assert!(
            StateStoreEncoder::decode::<Sandbox>(&legacy_bytes).is_err(),
            "legacy sandbox bytes should fail current decode before migration"
        );

        let current_sandbox = SandboxBuilder::default()
            .id(SandboxId::new("sandbox_current".to_string()))
            .namespace("ns".to_string())
            .image("python:3.12".to_string())
            .status(SandboxStatus::Pending {
                reason: crate::data_model::SandboxPendingReason::Scheduling,
            })
            .resources(ContainerResources {
                cpu_ms_per_sec: 500,
                memory_mb: 256,
                ephemeral_disk_mb: 512,
                gpu: None,
            })
            .timeout_secs(60)
            .allow_unauthenticated_access(false)
            .exposed_ports(Some(vec![8080, 9501]))
            .build()
            .expect("valid current sandbox");
        let current_bytes = StateStoreEncoder::encode(&current_sandbox)?;

        let legacy_key = SandboxKey::new("ns", legacy_sandbox.id.get())
            .to_string()
            .into_bytes();
        let current_key = SandboxKey::new("ns", current_sandbox.id.get())
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
                            IndexifyObjectsColumns::Sandboxes.as_ref(),
                            &legacy_key,
                            &legacy_bytes,
                        )
                        .await?;
                        db.put(
                            IndexifyObjectsColumns::Sandboxes.as_ref(),
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
                            .get(IndexifyObjectsColumns::Sandboxes.as_ref(), &legacy_key)
                            .await?
                            .expect("legacy row should exist after migration");
                        let repaired_legacy: Sandbox =
                            StateStoreEncoder::decode(&repaired_legacy)?;
                        assert_eq!(repaired_legacy.id.get(), "sandbox_legacy");
                        assert!(repaired_legacy.allow_unauthenticated_access);
                        assert!(repaired_legacy.exposed_ports.is_none());
                        assert_eq!(repaired_legacy.executor_id.unwrap().get(), "exec_1");
                        assert_eq!(repaired_legacy.container_id.unwrap().get(), "container_1");
                        assert_eq!(repaired_legacy.snapshot_id.unwrap().get(), "snapshot_1");

                        let unchanged_current = db
                            .get(IndexifyObjectsColumns::Sandboxes.as_ref(), &current_key)
                            .await?
                            .expect("current row should exist after migration");
                        let unchanged_current: Sandbox =
                            StateStoreEncoder::decode(&unchanged_current)?;
                        assert_eq!(
                            unchanged_current.exposed_ports,
                            Some(vec![8080, 9501])
                        );
                        Ok(())
                    })
                },
            )
            .await?;

        Ok(())
    }
}
