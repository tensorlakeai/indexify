use anyhow::Result;
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

/// Version byte prefix for bincode-encoded values.
const BINCODE_VERSION: u8 = 0x01;

/// Migration to re-encode any remaining JSON-encoded state store values as
/// bincode.
///
/// The state store serializer currently maintains a backward-compatibility
/// fallback: on decode, if the first byte is `0x01` it decodes as bincode,
/// otherwise it falls back to JSON. This migration converts all remaining
/// JSON entries to bincode so the fallback can be removed.
#[derive(Clone)]
pub struct V13ReencodeJsonAsBincode;

impl Migration for V13ReencodeJsonAsBincode {
    fn version(&self) -> u64 {
        13
    }

    fn name(&self) -> &'static str {
        "Re-encode JSON values as bincode"
    }

    fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        let mut total_entries: usize = 0;
        let mut total_reencoded: usize = 0;

        let cfs: Vec<(
            &IndexifyObjectsColumns,
            Box<dyn Fn(&MigrationContext, &IndexifyObjectsColumns) -> Result<ReencodeStats>>,
        )> = vec![
            (
                &IndexifyObjectsColumns::StateMachineMetadata,
                Box::new(reencode_cf::<StateMachineMetadata>),
            ),
            (
                &IndexifyObjectsColumns::Namespaces,
                Box::new(reencode_cf::<Namespace>),
            ),
            (
                &IndexifyObjectsColumns::Applications,
                Box::new(reencode_cf::<Application>),
            ),
            (
                &IndexifyObjectsColumns::ApplicationVersions,
                Box::new(reencode_cf::<ApplicationVersion>),
            ),
            (
                &IndexifyObjectsColumns::RequestCtx,
                Box::new(reencode_cf::<RequestCtx>),
            ),
            // RequestCtxSecondaryIndex stores empty values (keys-only index) — skip.
            (
                &IndexifyObjectsColumns::UnprocessedStateChanges,
                Box::new(reencode_cf::<StateChange>),
            ),
            (
                &IndexifyObjectsColumns::Allocations,
                Box::new(reencode_cf::<Allocation>),
            ),
            (
                &IndexifyObjectsColumns::AllocationUsage,
                Box::new(reencode_cf::<AllocationUsageEvent>),
            ),
            (
                &IndexifyObjectsColumns::GcUrls,
                Box::new(reencode_cf::<GcUrl>),
            ),
            // Stats CF is unused — skip.
            (
                &IndexifyObjectsColumns::ExecutorStateChanges,
                Box::new(reencode_cf::<StateChange>),
            ),
            (
                &IndexifyObjectsColumns::ApplicationStateChanges,
                Box::new(reencode_cf::<StateChange>),
            ),
            (
                &IndexifyObjectsColumns::RequestStateChangeEvents,
                Box::new(reencode_cf::<PersistedRequestStateChangeEvent>),
            ),
            (
                &IndexifyObjectsColumns::Sandboxes,
                Box::new(reencode_cf::<Sandbox>),
            ),
            (
                &IndexifyObjectsColumns::ContainerPools,
                Box::new(reencode_cf::<ContainerPool>),
            ),
        ];

        for (cf, reencode_fn) in &cfs {
            let stats = reencode_fn(ctx, cf)?;
            total_entries += stats.total;
            total_reencoded += stats.reencoded;
        }

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
fn reencode_cf<T>(ctx: &MigrationContext, cf: &IndexifyObjectsColumns) -> Result<ReencodeStats>
where
    T: DeserializeOwned + serde::Serialize + std::fmt::Debug,
{
    let mut total: usize = 0;
    let mut entries_to_reencode: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    ctx.iterate(cf, |key, value| {
        total += 1;

        if value.is_empty() || value[0] == BINCODE_VERSION {
            // Already bincode-encoded or empty — skip.
            return Ok(());
        }

        // Legacy JSON-encoded data: decode and re-encode as bincode.
        let decoded: T = serde_json::from_slice(value).map_err(|e| {
            anyhow::anyhow!(
                "V13 migration: failed to decode JSON from CF {}, key {:?}: {}",
                cf.to_string(),
                String::from_utf8_lossy(key),
                e
            )
        })?;

        let reencoded = StateStoreEncoder::encode(&decoded)?;
        entries_to_reencode.push((key.to_vec(), reencoded));

        Ok(())
    })?;

    let reencoded = entries_to_reencode.len();
    for (key, value) in entries_to_reencode {
        ctx.txn.put(cf.as_ref(), &key, &value)?;
    }

    if reencoded > 0 {
        info!("Re-encoded {reencoded} entries in CF {}", cf.to_string());
    }

    Ok(ReencodeStats { total, reencoded })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data_model::NamespaceBuilder,
        state_store::{
            driver::{Reader, Writer},
            migrations::testing::MigrationTestBuilder,
        },
    };

    #[test]
    fn test_v13_reencodes_json_to_bincode() -> Result<()> {
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

        MigrationTestBuilder::new()
            .with_column_family(IndexifyObjectsColumns::Namespaces.as_ref())
            .run_test(
                &migration,
                |db| {
                    // Insert JSON-encoded entry
                    db.put(
                        IndexifyObjectsColumns::Namespaces.as_ref(),
                        b"test_ns",
                        &json_bytes,
                    )?;
                    // Insert already-bincode entry
                    db.put(
                        IndexifyObjectsColumns::Namespaces.as_ref(),
                        b"already_bincode",
                        &bincode_bytes,
                    )?;
                    Ok(())
                },
                |db| {
                    // The JSON entry should now be bincode-encoded
                    let result = db
                        .get(IndexifyObjectsColumns::Namespaces.as_ref(), b"test_ns")?
                        .expect("entry should exist");
                    assert_eq!(
                        result[0], BINCODE_VERSION,
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
                        )?
                        .expect("entry should exist");
                    assert_eq!(result2[0], BINCODE_VERSION);
                    let decoded2: Namespace = StateStoreEncoder::decode(&result2)?;
                    assert_eq!(decoded2.name, "already_bincode");

                    Ok(())
                },
            )?;

        Ok(())
    }
}
