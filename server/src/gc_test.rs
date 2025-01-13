#[cfg(test)]
mod tests {
    use anyhow::Result;
    use bytes::Bytes;
    use data_model::{
        test_objects::tests::{mock_graph_a, TEST_NAMESPACE},
        NodeOutput,
    };
    use futures::stream;
    use state_store::{
        requests::{
            CreateOrUpdateComputeGraphRequest,
            DeleteComputeGraphRequest,
            RequestPayload,
            StateMachineUpdateRequest,
        },
        serializer::{JsonEncode, JsonEncoder},
        state_machine::IndexifyObjectsColumns,
    };

    use crate::{service::Service, testing};

    #[tokio::test]
    async fn test_gc() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service {
            indexify_state,
            blob_storage,
            gc_executor,
            ..
        } = test_srv.service;

        // Create a compute graph
        let compute_graph = mock_graph_a("image_hash".to_string());
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateComputeGraph(
                    CreateOrUpdateComputeGraphRequest {
                        namespace: TEST_NAMESPACE.to_string(),
                        compute_graph: compute_graph.clone(),
                    },
                ),
                processed_state_changes: vec![],
            })
            .await?;

        let data = "aaaa";
        let path = "qqqq";
        let data_stream = Box::pin(stream::once(async { Ok(Bytes::from(data)) }));
        let res = blob_storage.put(path, data_stream).await?;

        let output = NodeOutput {
            id: "id".to_string(),
            namespace: TEST_NAMESPACE.to_string(),
            compute_fn_name: "fn_a".to_string(),
            compute_graph_name: "graph_A".to_string(),
            invocation_id: "invocation_id".to_string(),
            payload: data_model::OutputPayload::Fn(vec![data_model::DataPayload {
                path: res.url.clone(),
                size: res.size_bytes,
                sha256_hash: res.sha256_hash,
            }]),
            errors: None,
            reduced_state: false,
            created_at: 5,
            encoding: "application/octet-stream".to_string(),
        };
        let key = output.key();
        let serialized_output = JsonEncoder::encode(&output)?;
        indexify_state.db.put_cf(
            &IndexifyObjectsColumns::FnOutputs.cf_db(&indexify_state.db),
            key,
            &serialized_output,
        )?;

        blob_storage.read_bytes(&res.url).await?;

        let request = RequestPayload::TombstoneComputeGraph(DeleteComputeGraphRequest {
            namespace: TEST_NAMESPACE.to_string(),
            name: compute_graph.name.clone(),
        });
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: request,
                processed_state_changes: vec![],
            })
            .await?;

        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::DeleteComputeGraphRequest(DeleteComputeGraphRequest {
                    namespace: TEST_NAMESPACE.to_string(),
                    name: compute_graph.name.clone(),
                }),
                processed_state_changes: vec![],
            })
            .await?;

        gc_executor.lock().await.run().await?;

        let urls = indexify_state.reader().get_gc_urls(None)?;
        assert!(urls.is_empty(), "all gc urls are empty");

        assert!(
            blob_storage.read_bytes(&res.url).await.is_err(),
            "file is deleted"
        );

        Ok(())
    }
}
