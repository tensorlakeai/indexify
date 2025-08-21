#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use anyhow::Result;
    use bytes::Bytes;
    use futures::stream;

    use crate::{
        data_model::{
            test_objects::tests::{test_graph_a, TEST_NAMESPACE},
            GraphInvocationCtx,
            InvocationPayload,
            NodeOutput,
        },
        service::Service,
        state_store::{
            requests::{
                CreateOrUpdateComputeGraphRequest,
                DeleteComputeGraphRequest,
                RequestPayload,
                StateMachineUpdateRequest,
            },
            serializer::{JsonEncode, JsonEncoder},
            state_machine::IndexifyObjectsColumns,
        },
        testing,
        utils::get_epoch_time_in_ms,
    };

    #[tokio::test]
    async fn test_gc() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service {
            indexify_state,
            blob_storage_registry,
            gc_executor,
            ..
        } = test_srv.service;

        // Create a compute graph
        let compute_graph = {
            let mut compute_graph = test_graph_a().clone();
            let data = "code";
            let path = compute_graph.code.path.to_string();

            let data_stream = Box::pin(stream::once(async { Ok(Bytes::from(data)) }));
            let res = blob_storage_registry
                .get_blob_store(TEST_NAMESPACE)
                .put(&path, data_stream)
                .await?;
            compute_graph.code.path = res.url;

            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::CreateOrUpdateComputeGraph(
                        CreateOrUpdateComputeGraphRequest {
                            namespace: TEST_NAMESPACE.to_string(),
                            compute_graph: compute_graph.clone(),
                            upgrade_tasks_to_current_version: false,
                        },
                    ),
                })
                .await?;

            compute_graph
        };

        let res = {
            let data = "invocation_payload";
            let path = "invocation_payload";
            let data_stream = Box::pin(stream::once(async { Ok(Bytes::from(data)) }));
            let res = blob_storage_registry
                .get_blob_store(TEST_NAMESPACE)
                .put(path, data_stream)
                .await?;

            // Create a graph invocation
            let invocation = InvocationPayload {
                id: "invocation_id".to_string(),
                namespace: TEST_NAMESPACE.to_string(),
                compute_graph_name: compute_graph.name.clone(),
                payload: crate::data_model::DataPayload {
                    path: res.url.clone(),
                    size: res.size_bytes,
                    sha256_hash: res.sha256_hash.clone(),
                    offset: 0, // All BLOB operations are not offset-aware
                },
                created_at: get_epoch_time_in_ms(),
                encoding: "application/octet-stream".to_string(),
            };

            indexify_state.db.put_cf(
                &IndexifyObjectsColumns::GraphInvocations.cf_db(&indexify_state.db),
                invocation.key().as_bytes(),
                &JsonEncoder::encode(&invocation)?,
            )?;

            indexify_state.db.put_cf(
                &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&indexify_state.db),
                invocation.key().as_bytes(),
                &JsonEncoder::encode(&GraphInvocationCtx {
                    invocation_id: invocation.id.clone(),
                    compute_graph_name: compute_graph.name.clone(),
                    namespace: TEST_NAMESPACE.to_string(),
                    graph_version: compute_graph.version.clone(),
                    completed: false,
                    outcome: crate::data_model::GraphInvocationOutcome::Failure(
                        crate::data_model::GraphInvocationFailureReason::InternalError,
                    ),
                    outstanding_tasks: 0,
                    outstanding_reducer_tasks: 0,
                    fn_task_analytics: HashMap::new(),
                    created_at: get_epoch_time_in_ms(),
                    invocation_error: None,
                })?,
            )?;

            let output = NodeOutput {
                id: "id".to_string(),
                namespace: TEST_NAMESPACE.to_string(),
                compute_fn_name: "fn_a".to_string(),
                compute_graph_name: compute_graph.name.clone(),
                invocation_id: invocation.id.clone(),
                payloads: vec![crate::data_model::DataPayload {
                    path: res.url.clone(),
                    size: res.size_bytes,
                    sha256_hash: res.sha256_hash.clone(),
                    offset: 0, // All BLOB operations are not offset-aware
                }],
                created_at: 5,
                reducer_output: false,
                allocation_id: "allocation_id".to_string(),
                next_functions: vec!["fn_b".to_string(), "fn_c".to_string()],
                encoding: "application/octet-stream".to_string(),
                invocation_error_payload: None,
            };
            let key = output.key();
            let serialized_output = JsonEncoder::encode(&output)?;
            indexify_state.db.put_cf(
                &IndexifyObjectsColumns::FnOutputs.cf_db(&indexify_state.db),
                key,
                &serialized_output,
            )?;

            blob_storage_registry
                .get_blob_store(TEST_NAMESPACE)
                .read_bytes(&res.url)
                .await?;

            res
        };

        let (urls, _) = indexify_state.reader().get_gc_urls(None)?;
        assert!(urls.is_empty(), "all gc urls are empty: {urls:?}");

        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::DeleteComputeGraphRequest((
                    DeleteComputeGraphRequest {
                        namespace: TEST_NAMESPACE.to_string(),
                        name: compute_graph.name.clone(),
                    },
                    vec![],
                )),
            })
            .await?;

        let (urls, _) = indexify_state.reader().get_gc_urls(None)?;
        assert!(
            !urls.is_empty(),
            "all gc urls should not be empty: {urls:?}"
        );

        gc_executor.lock().await.run().await?;

        let (urls, _) = indexify_state.reader().get_gc_urls(None)?;
        assert!(urls.is_empty(), "all gc urls are empty: {urls:?}");

        let read_res = blob_storage_registry
            .get_blob_store(TEST_NAMESPACE)
            .read_bytes(&res.url)
            .await;
        assert!(read_res.is_err(), "file is not deleted: {read_res:#?}");

        Ok(())
    }
}
