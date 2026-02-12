use anyhow::Result;
use async_trait::async_trait;
use tracing::info;

use super::{
    contexts::{MigrationContext, PrepareContext},
    migration_trait::Migration,
};
use crate::{
    data_model::{FunctionCall, PersistedRequestCtx, RequestCtx},
    state_store::{
        driver::{Writer, rocksdb::RocksDBDriver},
        serializer::{StateStoreEncode, StateStoreEncoder},
        state_machine::IndexifyObjectsColumns,
    },
};

/// Migration to normalize FunctionRuns and FunctionCalls out of RequestCtx.
///
/// Before this migration, RequestCtx embeds `function_runs` and
/// `function_calls` HashMaps. Every state change serializes the ENTIRE
/// RequestCtx. For large map-reduce requests this causes O(N^2) write
/// amplification.
///
/// After this migration:
/// - RequestCtx CF stores `PersistedRequestCtx` (without embedded maps, with
///   summary counts instead)
/// - FunctionRuns CF stores individual `FunctionRun` entries
/// - FunctionCalls CF stores individual `FunctionCall` entries
#[derive(Clone)]
pub struct V14NormalizeRequestCtx;

#[async_trait]
impl Migration for V14NormalizeRequestCtx {
    fn version(&self) -> u64 {
        14
    }

    fn name(&self) -> &'static str {
        "Normalize FunctionRuns and FunctionCalls out of RequestCtx"
    }

    async fn prepare(&self, ctx: &PrepareContext) -> Result<RocksDBDriver> {
        // Create the new column families if they don't exist
        let existing_cfs = ctx.list_cfs().unwrap_or_default();
        ctx.reopen_with_cf_operations(|db| {
            Box::pin(async move {
                let fr_cf = IndexifyObjectsColumns::FunctionRuns.to_string();
                if !existing_cfs.contains(&fr_cf) {
                    db.create(&fr_cf, &Default::default()).await?;
                }

                let fc_cf = IndexifyObjectsColumns::FunctionCalls.to_string();
                if !existing_cfs.contains(&fc_cf) {
                    db.create(&fc_cf, &Default::default()).await?;
                }

                Ok(())
            })
        })
        .await
    }

    async fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        let mut total_requests: usize = 0;
        let mut total_function_runs: usize = 0;
        let mut total_function_calls: usize = 0;

        // Collect entries to migrate (can't mutate during iteration)
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        ctx.iterate(&IndexifyObjectsColumns::RequestCtx, |key, value| {
            entries.push((key.to_vec(), value.to_vec()));
            Ok(())
        })
        .await?;

        for (key, value) in entries {
            // Try to decode as the old RequestCtx format (with embedded maps)
            let old_ctx: RequestCtx = StateStoreEncoder::decode(&value)?;

            total_requests += 1;

            // Write each FunctionRun to the new CF
            for function_run in old_ctx.function_runs.values() {
                let fr_key = function_run.key();
                let encoded = StateStoreEncoder::encode(function_run)?;
                ctx.txn
                    .put(
                        IndexifyObjectsColumns::FunctionRuns.as_ref(),
                        fr_key.as_bytes(),
                        &encoded,
                    )
                    .await?;
                total_function_runs += 1;
            }

            // Write each FunctionCall to the new CF
            for function_call in old_ctx.function_calls.values() {
                let fc_key = FunctionCall::key_for_request(
                    &old_ctx.namespace,
                    &old_ctx.application_name,
                    &old_ctx.request_id,
                    &function_call.function_call_id,
                );
                let encoded = StateStoreEncoder::encode(function_call)?;
                ctx.txn
                    .put(
                        IndexifyObjectsColumns::FunctionCalls.as_ref(),
                        fc_key.as_bytes(),
                        &encoded,
                    )
                    .await?;
                total_function_calls += 1;
            }

            // Convert to PersistedRequestCtx and write back
            let persisted: PersistedRequestCtx = (&old_ctx).into();
            let encoded = StateStoreEncoder::encode(&persisted)?;
            ctx.txn
                .put(IndexifyObjectsColumns::RequestCtx.as_ref(), &key, &encoded)
                .await?;
        }

        info!(
            "V14 migration: normalized {total_requests} requests, \
             {total_function_runs} function runs, \
             {total_function_calls} function calls"
        );

        Ok(())
    }

    fn box_clone(&self) -> Box<dyn Migration> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::{
        data_model::{
            ComputeOp,
            DataPayloadBuilder,
            FunctionArgs,
            FunctionCallId,
            FunctionRun,
            FunctionRunBuilder,
            FunctionRunStatus,
            RequestCtxBuilder,
        },
        state_store::{
            driver::{Reader, Writer},
            migrations::testing::MigrationTestBuilder,
        },
    };

    #[tokio::test]
    async fn test_v14_normalizes_request_ctx() -> Result<()> {
        let migration = V14NormalizeRequestCtx;

        // Build a RequestCtx with 2 function_runs and 2 function_calls
        let fc_id1 = FunctionCallId::from("fc1");
        let fc_id2 = FunctionCallId::from("fc2");

        let fc1 = FunctionCall {
            function_call_id: fc_id1.clone(),
            inputs: vec![],
            fn_name: "fn1".to_string(),
            call_metadata: bytes::Bytes::new(),
            parent_function_call_id: None,
        };
        let fc2 = FunctionCall {
            function_call_id: fc_id2.clone(),
            inputs: vec![],
            fn_name: "fn2".to_string(),
            call_metadata: bytes::Bytes::new(),
            parent_function_call_id: Some(fc_id1.clone()),
        };

        let payload = DataPayloadBuilder::default()
            .path("test/path".to_string())
            .metadata_size(0)
            .offset(0)
            .size(100)
            .sha256_hash("abc123".to_string())
            .build()?;

        let fr1 = FunctionRunBuilder::default()
            .id(fc_id1.clone())
            .request_id("req1".to_string())
            .namespace("ns".to_string())
            .application("app".to_string())
            .version("v1".to_string())
            .name("fn1".to_string())
            .compute_op(ComputeOp::FunctionCall(fc1.clone()))
            .input_args(vec![])
            .status(FunctionRunStatus::Pending)
            .attempt_number(0)
            .call_metadata(bytes::Bytes::new())
            .build()?;

        let fr2 = FunctionRunBuilder::default()
            .id(fc_id2.clone())
            .request_id("req1".to_string())
            .namespace("ns".to_string())
            .application("app".to_string())
            .version("v1".to_string())
            .name("fn2".to_string())
            .compute_op(ComputeOp::FunctionCall(fc2.clone()))
            .input_args(vec![])
            .status(FunctionRunStatus::Pending)
            .attempt_number(0)
            .call_metadata(bytes::Bytes::new())
            .build()?;

        let mut function_runs = HashMap::new();
        function_runs.insert(fc_id1.clone(), fr1);
        function_runs.insert(fc_id2.clone(), fr2);

        let mut function_calls = HashMap::new();
        function_calls.insert(fc_id1.clone(), fc1);
        function_calls.insert(fc_id2.clone(), fc2);

        let ctx = RequestCtxBuilder::default()
            .namespace("ns".to_string())
            .application_name("app".to_string())
            .application_version("v1".to_string())
            .request_id("req1".to_string())
            .function_runs(function_runs)
            .function_calls(function_calls)
            .build()?;

        let old_encoded = StateStoreEncoder::encode(&ctx)?;
        let ctx_key = ctx.key();
        let ctx_key_clone = ctx_key.clone();

        MigrationTestBuilder::new()
            .with_column_family(IndexifyObjectsColumns::RequestCtx.as_ref())
            .run_test(
                &migration,
                |db| {
                    Box::pin(async move {
                        // Insert old-format RequestCtx
                        db.put(
                            IndexifyObjectsColumns::RequestCtx.as_ref(),
                            ctx_key.as_bytes(),
                            &old_encoded,
                        )
                        .await?;
                        Ok(())
                    })
                },
                |db| {
                    Box::pin(async move {
                        // Verify PersistedRequestCtx in RequestCtx CF
                        let result = db
                            .get(
                                IndexifyObjectsColumns::RequestCtx.as_ref(),
                                ctx_key_clone.as_bytes(),
                            )
                            .await?
                            .expect("persisted request ctx should exist");
                        let persisted: PersistedRequestCtx = StateStoreEncoder::decode(&result)?;
                        assert_eq!(persisted.namespace, "ns");
                        assert_eq!(persisted.application_name, "app");
                        assert_eq!(persisted.request_id, "req1");
                        assert_eq!(persisted.function_runs_count, 2);
                        assert_eq!(persisted.function_calls_count, 2);

                        // Verify FunctionRuns in FunctionRuns CF
                        let fr1_key = "ns|app|req1|fc1";
                        let fr1_result = db
                            .get(
                                IndexifyObjectsColumns::FunctionRuns.as_ref(),
                                fr1_key.as_bytes(),
                            )
                            .await?
                            .expect("function run 1 should exist");
                        let fr1: FunctionRun = StateStoreEncoder::decode(&fr1_result)?;
                        assert_eq!(fr1.name, "fn1");

                        let fr2_key = "ns|app|req1|fc2";
                        let fr2_result = db
                            .get(
                                IndexifyObjectsColumns::FunctionRuns.as_ref(),
                                fr2_key.as_bytes(),
                            )
                            .await?
                            .expect("function run 2 should exist");
                        let fr2: FunctionRun = StateStoreEncoder::decode(&fr2_result)?;
                        assert_eq!(fr2.name, "fn2");

                        // Verify FunctionCalls in FunctionCalls CF
                        let fc1_key = "ns|app|req1|fc1";
                        let fc1_result = db
                            .get(
                                IndexifyObjectsColumns::FunctionCalls.as_ref(),
                                fc1_key.as_bytes(),
                            )
                            .await?
                            .expect("function call 1 should exist");
                        let fc1: FunctionCall = StateStoreEncoder::decode(&fc1_result)?;
                        assert_eq!(fc1.fn_name, "fn1");

                        let fc2_key = "ns|app|req1|fc2";
                        let fc2_result = db
                            .get(
                                IndexifyObjectsColumns::FunctionCalls.as_ref(),
                                fc2_key.as_bytes(),
                            )
                            .await?
                            .expect("function call 2 should exist");
                        let fc2: FunctionCall = StateStoreEncoder::decode(&fc2_result)?;
                        assert_eq!(fc2.fn_name, "fn2");

                        Ok(())
                    })
                },
            )
            .await?;

        Ok(())
    }
}
