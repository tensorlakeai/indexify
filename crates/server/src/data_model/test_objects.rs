#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use nanoid::nanoid;

    use super::super::{Application, Function};
    use crate::{
        data_model::{
            ApplicationBuilder,
            ApplicationEntryPoint,
            ApplicationState,
            ComputeOp,
            DataPayload,
            ExecutorId,
            ExecutorMetadata,
            ExecutorMetadataBuilder,
            FunctionArgs,
            FunctionCall,
            FunctionCallId,
            FunctionRetryPolicy,
            InputArgs,
            RequestCtx,
            RequestCtxBuilder,
        },
        state_store::requests::RequestUpdates,
        utils::get_epoch_time_in_ms,
    };

    pub const TEST_NAMESPACE: &str = "test_ns";
    pub const TEST_EXECUTOR_ID: &str = "test_executor_1";

    pub fn mock_blocking_function_call(
        fn_name: &str,
        source_function_call_id: &FunctionCallId,
    ) -> RequestUpdates {
        RequestUpdates {
            request_updates: vec![ComputeOp::FunctionCall(mock_function_call_with_name(
                fn_name,
                vec![FunctionArgs::DataPayload(mock_data_payload())],
                Some(source_function_call_id.clone()),
            ))],
            output_function_call_id: FunctionCallId(nanoid!()),
        }
    }

    pub fn mock_updates() -> RequestUpdates {
        let fn_b = mock_function_call_with_name(
            "fn_b",
            vec![FunctionArgs::DataPayload(mock_data_payload())],
            None,
        );
        let fn_c = mock_function_call_with_name(
            "fn_c",
            vec![FunctionArgs::DataPayload(mock_data_payload())],
            None,
        );
        let fn_d = mock_function_call_with_name(
            "fn_d",
            vec![
                FunctionArgs::FunctionRunOutput(fn_b.function_call_id.clone()),
                FunctionArgs::FunctionRunOutput(fn_c.function_call_id.clone()),
            ],
            Some(fn_c.function_call_id.clone()),
        );
        let updates = vec![
            ComputeOp::FunctionCall(fn_b),
            ComputeOp::FunctionCall(fn_c),
            ComputeOp::FunctionCall(fn_d.clone()),
        ];
        RequestUpdates {
            request_updates: updates,
            output_function_call_id: fn_d.function_call_id,
        }
    }

    pub fn test_function(name: &str, max_retries: u32) -> Function {
        Function {
            name: name.to_string(),
            description: format!("description {name}"),
            retry_policy: FunctionRetryPolicy {
                max_retries,
                ..Default::default()
            },
            max_concurrency: 1,
            ..Default::default()
        }
    }

    pub fn mock_data_payload() -> DataPayload {
        DataPayload {
            id: nanoid!(),
            metadata_size: 0,
            encoding: "application/octet-stream".to_string(),
            path: nanoid!(),
            size: 23,
            sha256_hash: nanoid!(),
            offset: 0,
        }
    }

    pub fn mock_request_ctx(namespace: &str, application: &Application) -> RequestCtx {
        let request_id = nanoid!();
        mock_request_ctx_with_id(namespace, application, &request_id)
    }

    pub fn mock_request_ctx_with_id(
        namespace: &str,
        application: &Application,
        request_id: &str,
    ) -> RequestCtx {
        let fn_call = mock_function_call();
        let input_args = vec![InputArgs {
            function_call_id: None,
            data_payload: mock_data_payload(),
        }];
        let fn_run = application
            .to_version()
            .unwrap()
            .create_function_run(&fn_call, input_args, request_id)
            .unwrap();
        RequestCtxBuilder::default()
            .namespace(namespace.to_string())
            .request_id(request_id.to_string())
            .application_name(application.name.clone())
            .application_version(application.version.clone())
            .function_runs(HashMap::from([(fn_run.id.clone(), fn_run)]))
            .function_calls(HashMap::from([(fn_call.function_call_id.clone(), fn_call)]))
            .created_at(get_epoch_time_in_ms())
            .build()
            .unwrap()
    }

    pub fn mock_app_with_retries(app_name: &str, version: &str, max_retries: u32) -> Application {
        let fn_a = test_function("fn_a", max_retries);
        let fn_b = test_function("fn_b", max_retries);
        let fn_c = test_function("fn_c", max_retries);
        let fn_d = test_function("fn_d", max_retries);

        ApplicationBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .state(ApplicationState::Active)
            .name(app_name.to_string())
            .tags(HashMap::from([
                ("tag1".to_string(), "val1".to_string()),
                ("tag2".to_string(), "val2".to_string()),
            ]))
            .tombstoned(false)
            .functions(HashMap::from([
                ("fn_b".to_string(), fn_b),
                ("fn_c".to_string(), fn_c),
                ("fn_a".to_string(), fn_a.clone()),
                ("fn_d".to_string(), fn_d),
            ]))
            .version(version.to_string())
            .description(format!("description {}", app_name))
            .code(Some(DataPayload {
                id: "code_id".to_string(),
                metadata_size: 0,
                offset: 0,
                encoding: "application/octet-stream".to_string(),
                path: "cg_path".to_string(),
                size: 23,
                sha256_hash: "hash123".to_string(),
            }))
            .created_at(5)
            .entrypoint(Some(ApplicationEntryPoint {
                function_name: "fn_a".to_string(),
                input_serializer: "json".to_string(),
                output_serializer: "json".to_string(),
                output_type_hints_base64: "".to_string(),
            }))
            .build()
            .unwrap()
    }

    pub fn mock_application() -> Application {
        mock_app_with_retries("graph_A", "1", 0)
    }

    pub fn mock_function_call_with_name(
        fn_name: &str,
        inputs: Vec<FunctionArgs>,
        parent_function_call_id: Option<FunctionCallId>,
    ) -> FunctionCall {
        FunctionCall {
            function_call_id: FunctionCallId(nanoid!()),
            inputs,
            fn_name: fn_name.to_string(),
            call_metadata: Bytes::new(),
            parent_function_call_id,
        }
    }

    pub fn mock_function_call() -> FunctionCall {
        mock_function_call_with_name(
            "fn_a",
            vec![FunctionArgs::DataPayload(mock_data_payload())],
            None,
        )
    }

    pub fn mock_executor_metadata(id: ExecutorId) -> ExecutorMetadata {
        mock_executor_metadata_with_version(id, "1.0.0")
    }

    pub fn mock_sandbox_executor_metadata(id: ExecutorId) -> ExecutorMetadata {
        mock_executor_metadata_with_version(id, "0.2.0")
    }

    pub fn mock_executor_metadata_with_version(id: ExecutorId, version: &str) -> ExecutorMetadata {
        ExecutorMetadataBuilder::default()
            .id(id)
            .executor_version(version.to_string())
            .function_allowlist(None)
            .addr("".to_string())
            .labels(Default::default())
            .host_resources(crate::data_model::HostResources {
                cpu_ms_per_sec: 8 * 1000, // 8 cores
                memory_bytes: 16 * 1024 * 1024 * 1024,
                disk_bytes: 100 * 1024 * 1024 * 1024,
                gpu: None,
            })
            .state(Default::default())
            .containers(Default::default())
            .tombstoned(false)
            .state_hash("state_hash".to_string())
            .clock(0)
            .build()
            .unwrap()
    }
}
