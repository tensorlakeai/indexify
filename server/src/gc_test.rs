#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use anyhow::Result;
    use bytes::Bytes;
    use futures::stream;

    use crate::{
        data_model::{
            test_objects::tests::{mock_application, mock_function_call, TEST_NAMESPACE},
            DataPayload, GraphInvocationCtxBuilder, InputArgs,
        },
        service::Service,
        state_store::{
            driver::Writer,
            requests::{
                CreateOrUpdateComputeGraphRequest, DeleteComputeGraphRequest, RequestPayload,
                StateMachineUpdateRequest,
            },
            serializer::{JsonEncode, JsonEncoder},
            state_machine::IndexifyObjectsColumns,
        },
        testing,
    };

    #[ignore]
    #[tokio::test()]
    async fn test_gc() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service {
            indexify_state,
            blob_storage_registry,
            gc_executor,
            ..
        } = test_srv.service;

        // Create an application
        let application = {
            let mut application = mock_application().clone();
            let data = "code";
            let path = application.code.path.to_string();

            let data_stream = Box::pin(stream::once(async { Ok(Bytes::from(data)) }));
            let res = blob_storage_registry
                .get_blob_store(TEST_NAMESPACE)
                .put(&path, data_stream)
                .await?;
            application.code.path = res.url;

            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::CreateOrUpdateComputeGraph(Box::new(
                        CreateOrUpdateComputeGraphRequest {
                            namespace: TEST_NAMESPACE.to_string(),
                            application: application.clone(),
                            upgrade_requests_to_current_version: false,
                        },
                    )),
                })
                .await?;

            application
        };

        let res = {
            let data = "invocation_payload";
            let path = "invocation_payload";
            let data_stream = Box::pin(stream::once(async { Ok(Bytes::from(data)) }));
            let res = blob_storage_registry
                .get_blob_store(TEST_NAMESPACE)
                .put(path, data_stream)
                .await?;

            let mock_function_call = mock_function_call();
            let mock_application = mock_application();
            let input_payload = DataPayload {
                id: "test".to_string(),
                path: res.url.clone(),
                metadata_size: 0,
                encoding: "application/octet-stream".to_string(),
                size: res.size_bytes,
                sha256_hash: res.sha256_hash.clone(),
                offset: 0,
            };
            let request_id = nanoid::nanoid!();
            let mock_function_run = mock_application.to_version().unwrap().create_function_run(
                &mock_function_call,
                vec![InputArgs {
                    function_call_id: None,
                    data_payload: input_payload,
                }],
                &request_id,
            )?;
            let graph_ctx = GraphInvocationCtxBuilder::default()
                .request_id(request_id)
                .application_name(application.name.clone())
                .namespace(TEST_NAMESPACE.to_string())
                .application_version(application.version.clone())
                .outcome(Some(crate::data_model::GraphInvocationOutcome::Failure(
                    crate::data_model::GraphInvocationFailureReason::InternalError,
                )))
                .function_runs(HashMap::from([(
                    mock_function_run.id.clone(),
                    mock_function_run,
                )]))
                .function_calls(HashMap::from([(
                    mock_function_call.function_call_id.clone(),
                    mock_function_call,
                )]))
                .build()?;

            indexify_state.db.put(
                &IndexifyObjectsColumns::GraphInvocationCtx.as_ref(),
                graph_ctx.key().as_bytes(),
                &JsonEncoder::encode(&graph_ctx)?,
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
                        name: application.name.clone(),
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
