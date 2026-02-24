use anyhow::Result;
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
        SandboxBuilder,
        SandboxId,
        SandboxOutcome,
        SandboxStatus,
    },
    state_store::{
        serializer::{StateStoreEncode, StateStoreEncoder},
        state_machine::IndexifyObjectsColumns,
    },
};

/// Legacy Sandbox layout matching the v16 postcard schema (no `snapshot_id`).
#[derive(Debug, Deserialize, Serialize)]
struct V16Sandbox {
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
    executor_id: Option<ExecutorId>,
    entrypoint: Option<Vec<String>>,
    #[serde(default)]
    network_policy: Option<NetworkPolicy>,
    #[serde(default)]
    pool_id: Option<ContainerPoolId>,
    #[serde(default)]
    container_id: Option<ContainerId>,
    // NOTE: no snapshot_id field — that's the whole point of this migration
}

/// Migration to add `snapshot_id: Option<SnapshotId>` to the Sandbox struct.
///
/// Old postcard-encoded records lack this trailing field. We decode with the
/// legacy struct (without the field), then re-encode using the current Sandbox
/// with `snapshot_id: None`.
#[derive(Clone)]
pub struct V17SandboxSnapshotField;

#[async_trait]
impl Migration for V17SandboxSnapshotField {
    fn version(&self) -> u64 {
        17
    }

    fn name(&self) -> &'static str {
        "Add snapshot_id to Sandbox"
    }

    async fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        ctx.iterate(&IndexifyObjectsColumns::Sandboxes, |key, value| {
            entries.push((key.to_vec(), value.to_vec()));
            Ok(())
        })
        .await?;

        let mut migrated: usize = 0;
        let total = entries.len();

        for (key_bytes, value_bytes) in &entries {
            let legacy: V16Sandbox = StateStoreEncoder::decode(value_bytes)?;

            let sandbox = SandboxBuilder::default()
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
                .executor_id(legacy.executor_id)
                .entrypoint(legacy.entrypoint)
                .network_policy(legacy.network_policy)
                .pool_id(legacy.pool_id)
                .container_id(legacy.container_id)
                .snapshot_id(None)
                .build()
                .expect("all fields provided");

            let encoded = StateStoreEncoder::encode(&sandbox)?;
            ctx.txn
                .put(
                    IndexifyObjectsColumns::Sandboxes.as_ref(),
                    key_bytes,
                    &encoded,
                )
                .await?;
            migrated += 1;
        }

        info!("V17 sandbox snapshot_id field: {migrated}/{total} sandboxes re-encoded");

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
        data_model::{ContainerResources, SandboxId, SandboxPendingReason, SandboxStatus},
        state_store::{
            driver::{Reader, Writer},
            migrations::testing::MigrationTestBuilder,
            state_machine::IndexifyObjectsColumns,
        },
    };

    #[tokio::test]
    async fn test_v17_adds_snapshot_id_field() -> Result<()> {
        let migration = V17SandboxSnapshotField;

        // Encode a pending sandbox using the V16 layout (no snapshot_id)
        let pending_sandbox = V16Sandbox {
            id: SandboxId::new("sandbox_1".to_string()),
            namespace: "test_ns".to_string(),
            image: "python:3.11".to_string(),
            status: SandboxStatus::Pending {
                reason: SandboxPendingReason::Scheduling,
            },
            outcome: None,
            creation_time_ns: 1000,
            created_at_clock: Some(1),
            updated_at_clock: Some(1),
            resources: ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            },
            secret_names: vec![],
            timeout_secs: 300,
            executor_id: None,
            entrypoint: None,
            network_policy: None,
            pool_id: None,
            container_id: None,
        };
        let pending_bytes = StateStoreEncoder::encode(&pending_sandbox)?;
        let pending_key = b"test_ns|sandbox_1";

        // Encode a running sandbox
        let running_sandbox = V16Sandbox {
            id: SandboxId::new("sandbox_2".to_string()),
            namespace: "test_ns".to_string(),
            image: "python:3.11".to_string(),
            status: SandboxStatus::Running,
            outcome: None,
            creation_time_ns: 2000,
            created_at_clock: Some(2),
            updated_at_clock: Some(2),
            resources: ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            },
            secret_names: vec![],
            timeout_secs: 300,
            executor_id: None,
            entrypoint: None,
            network_policy: None,
            pool_id: None,
            container_id: None,
        };
        let running_bytes = StateStoreEncoder::encode(&running_sandbox)?;
        let running_key = b"test_ns|sandbox_2";

        let mut builder = MigrationTestBuilder::new();
        for cf in IndexifyObjectsColumns::iter() {
            builder = builder.with_column_family(cf.as_ref());
        }

        builder
            .run_test(
                &migration,
                |db| {
                    Box::pin(async move {
                        db.put(
                            IndexifyObjectsColumns::Sandboxes.as_ref(),
                            pending_key,
                            &pending_bytes,
                        )
                        .await?;
                        db.put(
                            IndexifyObjectsColumns::Sandboxes.as_ref(),
                            running_key,
                            &running_bytes,
                        )
                        .await?;
                        Ok(())
                    })
                },
                |db| {
                    Box::pin(async move {
                        // Verify pending sandbox was migrated with snapshot_id: None
                        let pending_result = db
                            .get(IndexifyObjectsColumns::Sandboxes.as_ref(), pending_key)
                            .await?
                            .expect("pending sandbox should exist");
                        let pending: crate::data_model::Sandbox =
                            StateStoreEncoder::decode(&pending_result)?;
                        assert_eq!(pending.id.get(), "sandbox_1");
                        assert_eq!(
                            pending.status,
                            SandboxStatus::Pending {
                                reason: SandboxPendingReason::Scheduling,
                            }
                        );
                        assert_eq!(pending.snapshot_id, None);

                        // Verify running sandbox was migrated with snapshot_id: None
                        let running_result = db
                            .get(IndexifyObjectsColumns::Sandboxes.as_ref(), running_key)
                            .await?
                            .expect("running sandbox should exist");
                        let running: crate::data_model::Sandbox =
                            StateStoreEncoder::decode(&running_result)?;
                        assert_eq!(running.id.get(), "sandbox_2");
                        assert_eq!(running.status, SandboxStatus::Running);
                        assert_eq!(running.snapshot_id, None);

                        Ok(())
                    })
                },
            )
            .await?;

        Ok(())
    }
}
