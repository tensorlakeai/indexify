#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use nanoid::nanoid;

    use super::super::{ComputeFn, ComputeGraph};
    use crate::{
        data_model::{
            ComputeGraphBuilder,
            ComputeGraphState,
            ComputeOp,
            DataPayload,
            ExecutorId,
            ExecutorMetadata,
            ExecutorMetadataBuilder,
            FunctionArgs,
            FunctionCall,
            FunctionCallId,
            FunctionRetryPolicy,
            GraphInvocationCtx,
            GraphInvocationCtxBuilder,
            InputArgs,
        },
        utils::get_epoch_time_in_ms,
    };

    pub const TEST_NAMESPACE: &str = "test_ns";
    pub const TEST_EXECUTOR_ID: &str = "test_executor_1";

    pub fn mock_updates() -> Vec<ComputeOp> {
        let fn_b = mock_function_call_with_name(
            "fn_b",
            vec![FunctionArgs::DataPayload(mock_data_payload())],
        );
        let fn_c = mock_function_call_with_name(
            "fn_c",
            vec![FunctionArgs::DataPayload(mock_data_payload())],
        );
        let fn_d = mock_function_call_with_name(
            "fn_d",
            vec![
                FunctionArgs::FunctionRunOutput(fn_b.function_call_id.clone()),
                FunctionArgs::FunctionRunOutput(fn_c.function_call_id.clone()),
            ],
        );
        let updates = vec![
            ComputeOp::FunctionCall(fn_b),
            ComputeOp::FunctionCall(fn_c),
            ComputeOp::FunctionCall(fn_d),
        ];
        updates
    }

    pub fn test_compute_fn(name: &str, max_retries: u32) -> ComputeFn {
        ComputeFn {
            name: name.to_string(),
            description: format!("description {name}"),
            fn_name: name.to_string(),
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

    pub fn mock_request_ctx(namespace: &str, compute_graph: &ComputeGraph) -> GraphInvocationCtx {
        let request_id = nanoid!();
        let fn_call = mock_function_call();
        let input_args = vec![InputArgs {
            function_call_id: None,
            data_payload: mock_data_payload(),
        }];
        let fn_run = compute_graph
            .to_version()
            .unwrap()
            .create_function_run(&fn_call, input_args, &request_id)
            .unwrap();
        GraphInvocationCtxBuilder::default()
            .namespace(namespace.to_string())
            .request_id(request_id)
            .compute_graph_name(compute_graph.name.clone())
            .graph_version(compute_graph.version.clone())
            .function_runs(HashMap::from([(fn_run.id.clone(), fn_run)]))
            .function_calls(HashMap::from([(fn_call.function_call_id.clone(), fn_call)]))
            .created_at(get_epoch_time_in_ms())
            .build()
            .unwrap()
    }

    pub fn mock_graph_with_retries(max_retries: u32) -> ComputeGraph {
        let fn_a = test_compute_fn("fn_a", max_retries);
        let fn_b = test_compute_fn("fn_b", max_retries);
        let fn_c = test_compute_fn("fn_c", max_retries);
        let fn_d = test_compute_fn("fn_d", max_retries);

        ComputeGraphBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .state(ComputeGraphState::Active)
            .name("graph_A".to_string())
            .tags(HashMap::from([
                ("tag1".to_string(), "val1".to_string()),
                ("tag2".to_string(), "val2".to_string()),
            ]))
            .tombstoned(false)
            .nodes(HashMap::from([
                ("fn_b".to_string(), fn_b),
                ("fn_c".to_string(), fn_c),
                ("fn_a".to_string(), fn_a.clone()),
                ("fn_d".to_string(), fn_d),
            ]))
            .version(crate::data_model::GraphVersion::from("1"))
            .description("description graph_A".to_string())
            .code(DataPayload {
                id: "code_id".to_string(),
                metadata_size: 0,
                offset: 0,
                encoding: "application/octet-stream".to_string(),
                path: "cg_path".to_string(),
                size: 23,
                sha256_hash: "hash123".to_string(),
            })
            .created_at(5)
            .start_fn(fn_a)
            .build()
            .unwrap()
    }

    pub fn mock_graph() -> ComputeGraph {
        mock_graph_with_retries(0)
    }

    pub fn mock_function_call_with_name(fn_name: &str, inputs: Vec<FunctionArgs>) -> FunctionCall {
        FunctionCall {
            function_call_id: FunctionCallId(nanoid!()),
            inputs,
            fn_name: fn_name.to_string(),
            call_metadata: Bytes::new(),
        }
    }

    pub fn mock_function_call() -> FunctionCall {
        mock_function_call_with_name("fn_a", vec![FunctionArgs::DataPayload(mock_data_payload())])
    }

    pub fn mock_executor_metadata(id: ExecutorId) -> ExecutorMetadata {
        ExecutorMetadataBuilder::default()
            .id(id)
            .executor_version("1.0.0".to_string())
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
            .function_executors(Default::default())
            .tombstoned(false)
            .state_hash("state_hash".to_string())
            .clock(0)
            .build()
            .unwrap()
    }
}
