use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::{contexts::MigrationContext, migration_trait::Migration};
use crate::{
    data_model::{ContainerResources, SandboxId, Snapshot, SnapshotId, SnapshotStatus},
    state_store::{
        serializer::{StateStoreEncode, StateStoreEncoder},
        state_machine::IndexifyObjectsColumns,
    },
};

/// Legacy Snapshot layout from before `upload_uri` was added.
#[derive(Debug, Deserialize, Serialize)]
struct LegacySnapshot {
    id: SnapshotId,
    namespace: String,
    sandbox_id: SandboxId,
    base_image: String,
    status: SnapshotStatus,
    #[serde(default)]
    snapshot_uri: Option<String>,
    #[serde(default)]
    size_bytes: Option<u64>,
    creation_time_ns: u128,
    resources: ContainerResources,
    #[serde(default)]
    entrypoint: Option<Vec<String>>,
    #[serde(default)]
    secret_names: Vec<String>,
    // NOTE: no upload_uri field
}

/// Migration to repair Snapshot records written before `upload_uri` was added.
///
/// Postcard is positional; adding a field is not backward-compatible.
/// We decode legacy rows and re-encode them with `upload_uri: None`.
#[derive(Clone)]
pub struct V19FixSnapshotUploadUriField;

#[async_trait]
impl Migration for V19FixSnapshotUploadUriField {
    fn version(&self) -> u64 {
        19
    }

    fn name(&self) -> &'static str {
        "Fix Snapshot postcard schema (add upload_uri default)"
    }

    async fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        ctx.iterate(&IndexifyObjectsColumns::Snapshots, |key, value| {
            entries.push((key.to_vec(), value.to_vec()));
            Ok(())
        })
        .await?;

        let total = entries.len();
        let mut recovered: usize = 0;
        let mut deleted: usize = 0;

        for (key, value) in &entries {
            if StateStoreEncoder::decode::<Snapshot>(value).is_ok() {
                continue;
            }

            match StateStoreEncoder::decode::<LegacySnapshot>(value) {
                Ok(legacy) => {
                    let repaired = Snapshot {
                        id: legacy.id,
                        namespace: legacy.namespace,
                        sandbox_id: legacy.sandbox_id,
                        base_image: legacy.base_image,
                        status: legacy.status,
                        snapshot_uri: legacy.snapshot_uri,
                        size_bytes: legacy.size_bytes,
                        creation_time_ns: legacy.creation_time_ns,
                        resources: legacy.resources,
                        entrypoint: legacy.entrypoint,
                        secret_names: legacy.secret_names,
                        upload_uri: None,
                    };

                    let encoded = StateStoreEncoder::encode(&repaired)?;
                    ctx.txn
                        .put(IndexifyObjectsColumns::Snapshots.as_ref(), key, &encoded)
                        .await?;
                    recovered += 1;
                }
                Err(err) => {
                    warn!(
                        key = %String::from_utf8_lossy(key),
                        error = ?err,
                        "V19 deleting unreadable snapshot row"
                    );
                    ctx.txn
                        .delete(IndexifyObjectsColumns::Snapshots.as_ref(), key)
                        .await?;
                    deleted += 1;
                }
            }
        }

        info!(total, recovered, deleted, "V19 snapshot repair complete");

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
        data_model::SnapshotKey,
        state_store::{
            driver::{Reader, Writer},
            migrations::testing::MigrationTestBuilder,
            state_machine::IndexifyObjectsColumns,
        },
    };

    #[tokio::test]
    async fn test_v19_repairs_legacy_snapshot_rows() -> Result<()> {
        let migration = V19FixSnapshotUploadUriField;

        let legacy_snapshot = LegacySnapshot {
            id: SnapshotId::new("snapshot_legacy".to_string()),
            namespace: "ns".to_string(),
            sandbox_id: SandboxId::new("sandbox_legacy".to_string()),
            base_image: "python:3.12".to_string(),
            status: SnapshotStatus::InProgress,
            snapshot_uri: None,
            size_bytes: None,
            creation_time_ns: 123,
            resources: crate::data_model::ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            },
            entrypoint: None,
            secret_names: vec![],
        };
        let legacy_bytes = StateStoreEncoder::encode(&legacy_snapshot)?;
        assert!(
            StateStoreEncoder::decode::<Snapshot>(&legacy_bytes).is_err(),
            "legacy snapshot bytes should fail current decode before migration"
        );

        let current_snapshot = Snapshot {
            id: SnapshotId::new("snapshot_current".to_string()),
            namespace: "ns".to_string(),
            sandbox_id: SandboxId::new("sandbox_current".to_string()),
            base_image: "python:3.12".to_string(),
            status: SnapshotStatus::Completed,
            snapshot_uri: Some("s3://bucket/path".to_string()),
            size_bytes: Some(42),
            creation_time_ns: 456,
            resources: crate::data_model::ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 1024,
                ephemeral_disk_mb: 2048,
                gpu: None,
            },
            entrypoint: Some(vec!["python".to_string()]),
            secret_names: vec!["secret_a".to_string()],
            upload_uri: Some("https://upload.example".to_string()),
        };
        let current_bytes = StateStoreEncoder::encode(&current_snapshot)?;

        let corrupt_bytes = vec![0x01, 0xAA, 0xBB];

        let legacy_key = SnapshotKey::new("ns", "snapshot_legacy")
            .to_string()
            .into_bytes();
        let current_key = SnapshotKey::new("ns", "snapshot_current")
            .to_string()
            .into_bytes();
        let corrupt_key = SnapshotKey::new("ns", "snapshot_corrupt")
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
                    let corrupt_key = corrupt_key.clone();
                    let legacy_bytes = legacy_bytes.clone();
                    let current_bytes = current_bytes.clone();
                    let corrupt_bytes = corrupt_bytes.clone();
                    Box::pin(async move {
                        db.put(
                            IndexifyObjectsColumns::Snapshots.as_ref(),
                            &legacy_key,
                            &legacy_bytes,
                        )
                        .await?;
                        db.put(
                            IndexifyObjectsColumns::Snapshots.as_ref(),
                            &current_key,
                            &current_bytes,
                        )
                        .await?;
                        db.put(
                            IndexifyObjectsColumns::Snapshots.as_ref(),
                            &corrupt_key,
                            &corrupt_bytes,
                        )
                        .await?;
                        Ok(())
                    })
                },
                |db| {
                    let legacy_key = legacy_key.clone();
                    let current_key = current_key.clone();
                    let corrupt_key = corrupt_key.clone();
                    Box::pin(async move {
                        let repaired_legacy = db
                            .get(IndexifyObjectsColumns::Snapshots.as_ref(), &legacy_key)
                            .await?
                            .expect("legacy snapshot should be present after repair");
                        let repaired_legacy: Snapshot =
                            StateStoreEncoder::decode(&repaired_legacy)?;
                        assert_eq!(repaired_legacy.id.get(), "snapshot_legacy");
                        assert_eq!(repaired_legacy.upload_uri, None);

                        let preserved_current = db
                            .get(IndexifyObjectsColumns::Snapshots.as_ref(), &current_key)
                            .await?
                            .expect("current snapshot should remain");
                        let preserved_current: Snapshot =
                            StateStoreEncoder::decode(&preserved_current)?;
                        assert_eq!(preserved_current.id.get(), "snapshot_current");
                        assert_eq!(
                            preserved_current.upload_uri,
                            Some("https://upload.example".to_string())
                        );

                        let deleted_corrupt = db
                            .get(IndexifyObjectsColumns::Snapshots.as_ref(), &corrupt_key)
                            .await?;
                        assert!(
                            deleted_corrupt.is_none(),
                            "unreadable snapshot row should be deleted"
                        );
                        Ok(())
                    })
                },
            )
            .await?;

        Ok(())
    }
}
