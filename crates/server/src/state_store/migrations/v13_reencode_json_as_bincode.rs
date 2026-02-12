use anyhow::Result;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use tracing::info;

use super::{contexts::MigrationContext, migration_trait::Migration};
use crate::{
    data_model::{
        Allocation,
        AllocationUsageEvent,
        Application,
        ApplicationVersion,
        ContainerPool,
        GcUrl,
        Namespace,
        RequestCtx,
        Sandbox,
        StateChange,
        StateMachineMetadata,
    },
    state_store::{
        request_events::PersistedRequestStateChangeEvent,
        serializer::{StateStoreEncode, StateStoreEncoder},
        state_machine::IndexifyObjectsColumns,
    },
};

/// Version byte prefix for binary-encoded values.
const BINARY_VERSION: u8 = 0x01;

/// Migration to re-encode any remaining JSON-encoded state store values as
/// bincode.
///
/// The state store serializer currently maintains a backward-compatibility
/// fallback: on decode, if the first byte is `0x01` it decodes as bincode,
/// otherwise it falls back to JSON. This migration converts all remaining
/// JSON entries to bincode so the fallback can be removed.
#[derive(Clone)]
pub struct V13ReencodeJsonAsBincode;

#[async_trait]
impl Migration for V13ReencodeJsonAsBincode {
    fn version(&self) -> u64 {
        13
    }

    fn name(&self) -> &'static str {
        "Re-encode JSON values as bincode"
    }

    async fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        let mut total_entries: usize = 0;
        let mut total_reencoded: usize = 0;

        macro_rules! reencode {
            ($cf:expr, $type:ty) => {{
                let stats = reencode_cf::<$type>(ctx, $cf).await?;
                total_entries += stats.total;
                total_reencoded += stats.reencoded;
            }};
        }

        reencode!(
            &IndexifyObjectsColumns::StateMachineMetadata,
            StateMachineMetadata
        );
        reencode!(&IndexifyObjectsColumns::Namespaces, Namespace);
        reencode!(&IndexifyObjectsColumns::Applications, Application);
        reencode!(
            &IndexifyObjectsColumns::ApplicationVersions,
            ApplicationVersion
        );
        reencode!(&IndexifyObjectsColumns::RequestCtx, RequestCtx);
        // RequestCtxSecondaryIndex stores empty values (keys-only index) — skip.
        reencode!(
            &IndexifyObjectsColumns::UnprocessedStateChanges,
            StateChange
        );
        reencode!(&IndexifyObjectsColumns::Allocations, Allocation);
        reencode!(
            &IndexifyObjectsColumns::AllocationUsage,
            AllocationUsageEvent
        );
        reencode!(&IndexifyObjectsColumns::GcUrls, GcUrl);
        // Stats CF is unused — skip.
        reencode!(&IndexifyObjectsColumns::ExecutorStateChanges, StateChange);
        reencode!(
            &IndexifyObjectsColumns::ApplicationStateChanges,
            StateChange
        );
        reencode!(
            &IndexifyObjectsColumns::RequestStateChangeEvents,
            PersistedRequestStateChangeEvent
        );
        reencode!(&IndexifyObjectsColumns::Sandboxes, Sandbox);
        reencode!(&IndexifyObjectsColumns::FunctionPools, ContainerPool);
        reencode!(&IndexifyObjectsColumns::SandboxPools, ContainerPool);
        // FunctionRuns and FunctionCalls CFs don't exist until V14 creates
        // them, and V14 writes them fresh in postcard format — skip here.

        info!(
            "Re-encode JSON->bincode migration: {total_reencoded} re-encoded out of {total_entries} total entries"
        );

        Ok(())
    }

    fn box_clone(&self) -> Box<dyn Migration> {
        Box::new(self.clone())
    }
}

struct ReencodeStats {
    total: usize,
    reencoded: usize,
}

/// Re-encode any JSON entries in a column family as bincode.
///
/// For each entry whose first byte is NOT `0x01` (the bincode version prefix),
/// decode the value from JSON into `T`, then re-encode it via
/// `StateStoreEncoder::encode` (which produces the bincode format with version
/// prefix).
fn reencode_cf<'a, T>(
    ctx: &'a MigrationContext,
    cf: &'a IndexifyObjectsColumns,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<ReencodeStats>> + Send + 'a>>
where
    T: DeserializeOwned + serde::Serialize + std::fmt::Debug + 'a,
{
    Box::pin(async move {
        let mut total: usize = 0;
        let mut entries_to_reencode: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        ctx.iterate(cf, |key, value| {
            total += 1;

            if value.is_empty() || value[0] == BINARY_VERSION {
                // Already bincode-encoded or empty — skip.
                return Ok(());
            }

            // Legacy JSON-encoded data: decode and re-encode as bincode.
            let decoded: T = serde_json::from_slice(value).map_err(|e| {
                anyhow::anyhow!(
                    "V13 migration: failed to decode JSON from CF {cf}, key {:?}: {}",
                    String::from_utf8_lossy(key),
                    e
                )
            })?;

            let reencoded = StateStoreEncoder::encode(&decoded)?;
            entries_to_reencode.push((key.to_vec(), reencoded));

            Ok(())
        })
        .await?;

        let reencoded = entries_to_reencode.len();
        for (key, value) in entries_to_reencode {
            ctx.txn.put(cf.as_ref(), &key, &value).await?;
        }

        if reencoded > 0 {
            info!("Re-encoded {reencoded} entries in CF {cf}");
        }

        Ok(ReencodeStats { total, reencoded })
    })
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator;

    use super::*;
    use crate::{
        data_model::NamespaceBuilder,
        state_store::{
            driver::{Reader, Writer},
            migrations::testing::MigrationTestBuilder,
            state_machine::IndexifyObjectsColumns,
        },
    };

    #[tokio::test]
    async fn test_v13_reencodes_json_to_bincode() -> Result<()> {
        let migration = V13ReencodeJsonAsBincode;

        // Create a Namespace value as JSON (legacy format, no 0x01 prefix)
        let ns = NamespaceBuilder::default()
            .name("test_ns".to_string())
            .created_at(1234567890)
            .blob_storage_bucket(Some("bucket".to_string()))
            .blob_storage_region(None)
            .build()?;
        let json_bytes = serde_json::to_vec(&ns)?;

        // Also create a bincode-encoded entry that should be left alone
        let ns2 = NamespaceBuilder::default()
            .name("already_bincode".to_string())
            .created_at(999)
            .blob_storage_bucket(None)
            .blob_storage_region(None)
            .build()?;
        let bincode_bytes = StateStoreEncoder::encode(&ns2)?;

        // V13 iterates all CFs so we must create them all
        let mut builder = MigrationTestBuilder::new();
        for cf in IndexifyObjectsColumns::iter() {
            builder = builder.with_column_family(cf.as_ref());
        }
        builder
            .run_test(
                &migration,
                |db| {
                    Box::pin(async move {
                        // Insert JSON-encoded entry
                        db.put(
                            IndexifyObjectsColumns::Namespaces.as_ref(),
                            b"test_ns",
                            &json_bytes,
                        )
                        .await?;
                        // Insert already-bincode entry
                        db.put(
                            IndexifyObjectsColumns::Namespaces.as_ref(),
                            b"already_bincode",
                            &bincode_bytes,
                        )
                        .await?;
                        Ok(())
                    })
                },
                |db| {
                    Box::pin(async move {
                        // The JSON entry should now be bincode-encoded
                        let result = db
                            .get(IndexifyObjectsColumns::Namespaces.as_ref(), b"test_ns")
                            .await?
                            .expect("entry should exist");
                        assert_eq!(
                            result[0], BINARY_VERSION,
                            "re-encoded entry should start with bincode version byte"
                        );

                        // Verify it round-trips correctly
                        let decoded: Namespace = StateStoreEncoder::decode(&result)?;
                        assert_eq!(decoded.name, "test_ns");
                        assert_eq!(decoded.blob_storage_bucket, Some("bucket".to_string()));

                        // The already-bincode entry should be unchanged
                        let result2 = db
                            .get(
                                IndexifyObjectsColumns::Namespaces.as_ref(),
                                b"already_bincode",
                            )
                            .await?
                            .expect("entry should exist");
                        assert_eq!(result2[0], BINARY_VERSION);
                        let decoded2: Namespace = StateStoreEncoder::decode(&result2)?;
                        assert_eq!(decoded2.name, "already_bincode");

                        Ok(())
                    })
                },
            )
            .await?;

        Ok(())
    }
}
