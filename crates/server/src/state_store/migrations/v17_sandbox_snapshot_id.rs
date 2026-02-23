use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::info;

use super::{contexts::MigrationContext, migration_trait::Migration};
use crate::{
    data_model::{
        ContainerId, ContainerPoolId, ContainerResources, ExecutorId, NetworkPolicy, SandboxBuilder,
        SandboxId, SandboxOutcome, SandboxStatus,
    },
    state_store::{
        serializer::{StateStoreEncode, StateStoreEncoder},
        state_machine::IndexifyObjectsColumns,
    },
};

/// Legacy Sandbox layout matching the postcard schema before the snapshot_id
/// field was added. Identical to the current Sandbox except it lacks
/// `snapshot_id`.
#[derive(Debug, Deserialize, Serialize)]
struct LegacySandbox {
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
}

/// Migration to add `snapshot_id` field to persisted `Sandbox` records.
///
/// Before: Sandbox is serialized without the `snapshot_id` field.
/// After:  Sandbox includes `snapshot_id: Option<SnapshotId>` (set to `None`
///         for all existing records).
#[derive(Clone)]
pub struct V17SandboxSnapshotId;

#[async_trait]
impl Migration for V17SandboxSnapshotId {
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
            let legacy: LegacySandbox = StateStoreEncoder::decode(value_bytes)?;

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
                // snapshot_id defaults to None via #[builder(default)]
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

        info!("V17 sandbox snapshot_id: {migrated}/{total} sandboxes re-encoded");

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
        data_model::{ContainerResources, Sandbox, SandboxId, SandboxStatus},
        state_store::{
            driver::{Reader, Writer},
            migrations::testing::MigrationTestBuilder,
            state_machine::IndexifyObjectsColumns,
        },
    };

    #[tokio::test]
    async fn test_v17_adds_snapshot_id() -> Result<()> {
        let migration = V17SandboxSnapshotId;

        let legacy_sandbox = LegacySandbox {
            id: SandboxId::new("sandbox_1".to_string()),
            namespace: "test_ns".to_string(),
            image: "python:3.11".to_string(),
            status: SandboxStatus::Running,
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
        let legacy_bytes = StateStoreEncoder::encode(&legacy_sandbox)?;
        let key = b"test_ns|sandbox_1";

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
                            key,
                            &legacy_bytes,
                        )
                        .await?;
                        Ok(())
                    })
                },
                |db| {
                    Box::pin(async move {
                        let result = db
                            .get(IndexifyObjectsColumns::Sandboxes.as_ref(), key)
                            .await?
                            .expect("sandbox should exist");
                        let migrated: Sandbox = StateStoreEncoder::decode(&result)?;
                        assert_eq!(migrated.id.get(), "sandbox_1");
                        assert_eq!(migrated.status, SandboxStatus::Running);
                        assert_eq!(migrated.snapshot_id, None);
                        Ok(())
                    })
                },
            )
            .await?;

        Ok(())
    }
}
