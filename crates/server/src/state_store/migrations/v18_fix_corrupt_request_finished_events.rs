use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::info;

use super::{contexts::MigrationContext, migration_trait::Migration};
use crate::{
    data_model::RequestOutcome,
    state_store::{
        request_events::{
            AllocationCompleted,
            AllocationCreated,
            FunctionRunCompleted,
            FunctionRunCreated,
            FunctionRunMatchedCache,
            PersistedRequestStateChangeEvent,
            RequestStartedEvent,
            RequestStateChangeEvent,
            RequestStateChangeEventId,
        },
        serializer::{StateStoreEncode, StateStoreEncoder},
        state_machine::IndexifyObjectsColumns,
    },
};

// ── Legacy types
// ──────────────────────────────────────────────────────────────
//
// Commit ef0147f1 re-added `#[serde(skip_serializing_if = "Option::is_none")]`
// to `RequestFinishedEvent::output`.  Postcard is a positional binary format:
// skipping a `None` field on write means the discriminant byte is absent, so
// any subsequent read of that record fails with "unexpected end of buffer".
//
// These mirror types reproduce the *old* on-disk layout so we can deserialize
// the corrupt bytes and re-encode them correctly.

#[derive(Debug, Deserialize, Serialize)] // Serialize needed to produce test fixtures
struct LegacyRequestFinishedEvent {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
    pub outcome: RequestOutcome,
    #[serde(default)]
    pub created_at: DateTime<Utc>,
    // `output` intentionally absent — mirrors skip_serializing_if on None
}

/// The enum must have the same variant *order* as `RequestStateChangeEvent` so
/// that postcard assigns the same integer discriminant to each variant.
#[derive(Debug, Deserialize, Serialize)] // Serialize needed to produce test fixtures
enum LegacyRequestStateChangeEvent {
    RequestStarted(RequestStartedEvent),
    FunctionRunCreated(FunctionRunCreated),
    FunctionRunCompleted(FunctionRunCompleted),
    FunctionRunMatchedCache(FunctionRunMatchedCache),
    AllocationCreated(AllocationCreated),
    AllocationCompleted(AllocationCompleted),
    RequestFinished(LegacyRequestFinishedEvent),
}

#[derive(Debug, Deserialize, Serialize)] // Serialize needed to produce test fixtures
struct LegacyPersistedEvent {
    pub id: RequestStateChangeEventId,
    pub event: LegacyRequestStateChangeEvent,
}

// ── Migration
// ─────────────────────────────────────────────────────────────────

/// Fix `RequestFinishedEvent` records whose `output` field was omitted on write
/// because `#[serde(skip_serializing_if = "Option::is_none")]` was active on a
/// postcard-encoded column family.
///
/// Strategy per record:
///  1. Try current decode → OK means the record is fine, skip.
///  2. Fail → try legacy decode (struct without `output`).
///     * Success → re-encode with `output: None` and overwrite.
///     * Also fail → the record is corrupt beyond recovery; delete it.
#[derive(Clone)]
pub struct V18FixCorruptRequestFinishedEvents;

#[async_trait]
impl Migration for V18FixCorruptRequestFinishedEvents {
    fn version(&self) -> u64 {
        18
    }

    fn name(&self) -> &'static str {
        "Fix corrupt RequestFinished events (postcard skip_serializing_if)"
    }

    async fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        // Collect raw key/value pairs first so we can mutate via the txn
        // while holding an immutable borrow of `ctx.db`.
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        ctx.iterate(
            &IndexifyObjectsColumns::RequestStateChangeEvents,
            |key, value| {
                entries.push((key.to_vec(), value.to_vec()));
                Ok(())
            },
        )
        .await?;

        let total = entries.len();
        let mut recovered: usize = 0;
        let mut deleted: usize = 0;

        for (key, value) in &entries {
            // Step 1: try decoding with the current schema.
            if StateStoreEncoder::decode::<PersistedRequestStateChangeEvent>(value).is_ok() {
                continue; // healthy record — nothing to do
            }

            // Step 2: try the legacy schema (RequestFinishedEvent without output).
            match StateStoreEncoder::decode::<LegacyPersistedEvent>(value) {
                Ok(legacy) => {
                    let event = match legacy.event {
                        LegacyRequestStateChangeEvent::RequestFinished(e) => {
                            RequestStateChangeEvent::RequestFinished(
                                crate::state_store::request_events::RequestFinishedEvent {
                                    namespace: e.namespace,
                                    application_name: e.application_name,
                                    application_version: e.application_version,
                                    request_id: e.request_id,
                                    outcome: e.outcome,
                                    created_at: e.created_at,
                                    output: None,
                                },
                            )
                        }
                        // Other variants are identical in both schemas; if they
                        // failed current decode but passed legacy decode that is
                        // unexpected — treat as unrecoverable and delete.
                        _ => {
                            ctx.txn
                                .delete(
                                    IndexifyObjectsColumns::RequestStateChangeEvents.as_ref(),
                                    key,
                                )
                                .await?;
                            deleted += 1;
                            continue;
                        }
                    };

                    let fixed = PersistedRequestStateChangeEvent::new(legacy.id, event);
                    let encoded = StateStoreEncoder::encode(&fixed)?;
                    ctx.txn
                        .put(
                            IndexifyObjectsColumns::RequestStateChangeEvents.as_ref(),
                            key,
                            &encoded,
                        )
                        .await?;
                    recovered += 1;
                }
                Err(_) => {
                    // Unrecoverable — delete so it cannot permanently stall the
                    // export drain.
                    ctx.txn
                        .delete(
                            IndexifyObjectsColumns::RequestStateChangeEvents.as_ref(),
                            key,
                        )
                        .await?;
                    deleted += 1;
                }
            }
        }

        info!(
            total,
            recovered, deleted, "V18: request state change event repair complete"
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
    use crate::state_store::{
        driver::{Reader, Writer},
        migrations::testing::MigrationTestBuilder,
        request_events::RequestFinishedEvent,
        state_machine::IndexifyObjectsColumns,
    };

    /// Build a raw key the same way `PersistedRequestStateChangeEvent::key`
    /// does.
    fn event_key(seq: u64) -> Vec<u8> {
        seq.to_be_bytes().to_vec()
    }

    #[tokio::test]
    async fn test_v18_recovers_corrupt_request_finished_event() -> Result<()> {
        let migration = V18FixCorruptRequestFinishedEvents;

        // Encode a corrupt event using the legacy struct (no output field),
        // simulating what ef0147f1 produced with skip_serializing_if.
        let legacy_event = LegacyPersistedEvent {
            id: RequestStateChangeEventId::new(1),
            event: LegacyRequestStateChangeEvent::RequestFinished(LegacyRequestFinishedEvent {
                namespace: "ns".to_string(),
                application_name: "app".to_string(),
                application_version: "1.0.0".to_string(),
                request_id: "req-corrupt".to_string(),
                outcome: RequestOutcome::Success,
                created_at: Utc::now(),
            }),
        };
        let corrupt_bytes = StateStoreEncoder::encode(&legacy_event)?;

        // Verify it cannot be decoded with the current schema (proves it's
        // genuinely corrupt before the migration runs).
        assert!(
            StateStoreEncoder::decode::<PersistedRequestStateChangeEvent>(&corrupt_bytes).is_err(),
            "corrupt bytes should not decode with current schema"
        );

        // Encode a healthy event (output = None, the only value produced by the
        // current codebase) written correctly with the current schema.  The
        // migration must leave this record untouched.
        let healthy_event = PersistedRequestStateChangeEvent::new(
            RequestStateChangeEventId::new(2),
            RequestStateChangeEvent::RequestFinished(RequestFinishedEvent {
                namespace: "ns".to_string(),
                application_name: "app".to_string(),
                application_version: "1.0.0".to_string(),
                request_id: "req-healthy".to_string(),
                outcome: RequestOutcome::Success,
                created_at: Utc::now(),
                output: None,
            }),
        );
        let healthy_bytes = StateStoreEncoder::encode(&healthy_event)?;

        let corrupt_key = event_key(1);
        let healthy_key = event_key(2);

        let mut builder = MigrationTestBuilder::new();
        for cf in IndexifyObjectsColumns::iter() {
            builder = builder.with_column_family(cf.as_ref());
        }

        builder
            .run_test(
                &migration,
                |db| {
                    let corrupt_bytes = corrupt_bytes.clone();
                    let healthy_bytes = healthy_bytes.clone();
                    let corrupt_key = corrupt_key.clone();
                    let healthy_key = healthy_key.clone();
                    Box::pin(async move {
                        db.put(
                            IndexifyObjectsColumns::RequestStateChangeEvents.as_ref(),
                            &corrupt_key,
                            &corrupt_bytes,
                        )
                        .await?;
                        db.put(
                            IndexifyObjectsColumns::RequestStateChangeEvents.as_ref(),
                            &healthy_key,
                            &healthy_bytes,
                        )
                        .await?;
                        Ok(())
                    })
                },
                |db| {
                    let corrupt_key = corrupt_key.clone();
                    let healthy_key = healthy_key.clone();
                    Box::pin(async move {
                        // Corrupt record must now decode successfully.
                        let raw = db
                            .get(
                                IndexifyObjectsColumns::RequestStateChangeEvents.as_ref(),
                                &corrupt_key,
                            )
                            .await?
                            .expect("recovered event should still exist");

                        let recovered: PersistedRequestStateChangeEvent =
                            StateStoreEncoder::decode(&raw)
                                .expect("recovered event should decode with current schema");

                        assert_eq!(recovered.id.value(), 1);
                        match recovered.event {
                            RequestStateChangeEvent::RequestFinished(e) => {
                                assert_eq!(e.request_id, "req-corrupt");
                                assert!(e.output.is_none(), "output should be None after recovery");
                            }
                            other => panic!("expected RequestFinished, got {:?}", other),
                        }

                        // Healthy record must be untouched.
                        let raw = db
                            .get(
                                IndexifyObjectsColumns::RequestStateChangeEvents.as_ref(),
                                &healthy_key,
                            )
                            .await?
                            .expect("healthy event should still exist");

                        let preserved: PersistedRequestStateChangeEvent =
                            StateStoreEncoder::decode(&raw)
                                .expect("healthy event should still decode");

                        assert_eq!(preserved.id.value(), 2);
                        match preserved.event {
                            RequestStateChangeEvent::RequestFinished(e) => {
                                assert_eq!(e.request_id, "req-healthy");
                                assert!(e.output.is_none(), "healthy record should be unchanged");
                            }
                            other => panic!("expected RequestFinished, got {:?}", other),
                        }

                        Ok(())
                    })
                },
            )
            .await?;

        Ok(())
    }
}
