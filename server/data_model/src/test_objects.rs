pub mod tests {
    use std::collections::HashMap;

    use rand::Rng;

    use super::super::{ComputeFn, ComputeGraph, ComputeGraphCode, NodeOutput, RuntimeInformation};
    use crate::{
        DataPayload,
        ExecutorId,
        ExecutorMetadata,
        FunctionRetryPolicy,
        GraphInvocationCtx,
        GraphInvocationCtxBuilder,
        GraphVersion,
        ImageInformation,
        InvocationPayload,
        InvocationPayloadBuilder,
        NodeOutputBuilder,
        Task,
        TaskBuilder,
    };

    pub const TEST_NAMESPACE: &str = "test_ns";
    pub const TEST_EXECUTOR_ID: &str = "test_executor_1";
    pub const TEST_EXECUTOR_IMAGE_NAME: &str = "test_image_name";

    pub fn create_mock_task(
        cg: &ComputeGraph,
        cg_fn: &str,
        payload: DataPayload,
        inv_id: &str,
    ) -> Task {
        TaskBuilder::default()
            .namespace(cg.namespace.to_string())
            .compute_fn_name(cg_fn.to_string())
            .compute_graph_name(cg.name.to_string())
            .input(payload)
            .invocation_id(inv_id.to_string())
            .graph_version(Default::default())
            .build()
            .unwrap()
    }

    pub fn test_compute_fn(name: &str, image_hash: String, max_retries: u32) -> ComputeFn {
        let image_information = ImageInformation {
            image_name: TEST_EXECUTOR_IMAGE_NAME.to_string(),
            image_hash,
            ..Default::default()
        };
        ComputeFn {
            name: name.to_string(),
            description: format!("description {}", name),
            fn_name: name.to_string(),
            image_information,
            retry_policy: FunctionRetryPolicy {
                max_retries,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    pub fn reducer_fn(name: &str) -> ComputeFn {
        let mut compute_fn = test_compute_fn(name, "image_hash".to_string(), 0);
        compute_fn.reducer = true;
        compute_fn
    }

    pub fn test_node_fn_output_fn_a(
        invocation_id: &str,
        graph: &str,
        reducer_fn: Option<String>,
        num_outputs: usize,
        allocation_id: String,
    ) -> NodeOutput {
        test_node_fn_output(
            invocation_id,
            graph,
            "fn_a",
            reducer_fn,
            num_outputs,
            allocation_id,
            vec!["fn_b".to_string(), "fn_c".to_string()],
        )
    }

    pub fn test_node_fn_output(
        invocation_id: &str,
        graph: &str,
        compute_fn_name: &str,
        reducer_fn: Option<String>,
        num_outputs: usize,
        allocation_id: String,
        next_functions: Vec<String>,
    ) -> NodeOutput {
        let mut path = rand::rng()
            .sample_iter(rand::distr::Alphanumeric)
            .take(7)
            .map(char::from)
            .collect::<String>(); // Generate a random string for the path
        if let Some(reducer_fn) = reducer_fn {
            // Simulating overriding the existing output for accumulators
            path = format!("{}-{}-{}", invocation_id, graph, reducer_fn);
        }
        NodeOutputBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .compute_fn_name(compute_fn_name.to_string())
            .compute_graph_name(graph.to_string())
            .invocation_id(invocation_id.to_string())
            .allocation_id(allocation_id)
            .payloads(
                (0..num_outputs)
                    .map(|_| DataPayload {
                        sha256_hash: "3433".to_string(),
                        path: path.clone(),
                        size: 12,
                    })
                    .collect(),
            )
            .next_functions(next_functions)
            .build()
            .unwrap()
    }

    pub fn test_invocation_payload_graph_a() -> InvocationPayload {
        InvocationPayloadBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .compute_graph_name("graph_A".to_string())
            .payload(DataPayload {
                path: "test".to_string(),
                size: 23,
                sha256_hash: "hash1232".to_string(),
            })
            .encoding("application/octet-stream".to_string())
            .build()
            .unwrap()
    }

    pub fn test_invocation_ctx(
        namespace: &str,
        compute_graph: &ComputeGraph,
        invocation_payload: &InvocationPayload,
    ) -> GraphInvocationCtx {
        GraphInvocationCtxBuilder::default()
            .namespace(namespace.to_string())
            .compute_graph_name(compute_graph.name.clone())
            .graph_version(GraphVersion::default())
            .invocation_id(invocation_payload.id.clone())
            .fn_task_analytics(HashMap::new())
            .created_at(invocation_payload.created_at)
            .build(compute_graph.clone())
            .unwrap()
    }

    pub fn test_invocation_payload_graph_b() -> InvocationPayload {
        InvocationPayloadBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .compute_graph_name("graph_B".to_string())
            .created_at(5)
            .payload(DataPayload {
                path: "test".to_string(),
                size: 23,
                sha256_hash: "hash1232".to_string(),
            })
            .encoding("application/octet-stream".to_string())
            .build()
            .unwrap()
    }

    pub fn test_graph_a_retry(image_hash: String, max_retries: u32) -> ComputeGraph {
        let fn_a = test_compute_fn("fn_a", image_hash.clone(), max_retries);
        let fn_b = test_compute_fn("fn_b", image_hash.clone(), max_retries);
        let fn_c = test_compute_fn("fn_c", image_hash.clone(), max_retries);

        ComputeGraph {
            namespace: TEST_NAMESPACE.to_string(),
            name: "graph_A".to_string(),
            tags: HashMap::from([
                ("tag1".to_string(), "val1".to_string()),
                ("tag2".to_string(), "val2".to_string()),
            ]),
            tombstoned: false,
            nodes: HashMap::from([
                ("fn_b".to_string(), fn_b),
                ("fn_c".to_string(), fn_c),
                ("fn_a".to_string(), fn_a.clone()),
            ]),
            version: crate::GraphVersion::from("1"),
            edges: HashMap::from([(
                "fn_a".to_string(),
                vec!["fn_b".to_string(), "fn_c".to_string()],
            )]),
            description: "description graph_A".to_string(),
            code: ComputeGraphCode {
                path: "cg_path".to_string(),
                size: 23,
                sha256_hash: "hash123".to_string(),
            },
            created_at: 5,
            start_fn: fn_a,
            runtime_information: RuntimeInformation {
                major_version: 3,
                minor_version: 10,
                sdk_version: "1.2.3".to_string(),
            },
            replaying: false,
        }
    }

    pub fn test_graph_a(image_hash: String) -> ComputeGraph {
        test_graph_a_retry(image_hash, 0)
    }

    pub fn test_graph_b() -> ComputeGraph {
        let fn_a = test_compute_fn("fn_a", "image_hash".to_string(), 0);
        let router_x = ComputeFn {
            name: "router_x".to_string(),
            description: "description router_x".to_string(),
            input_encoder: "cloudpickle".to_string(),
            output_encoder: "cloudpickle".to_string(),
            image_information: ImageInformation {
                image_name: TEST_EXECUTOR_IMAGE_NAME.to_string(),
                tag: "tag-1".to_string(),
                base_image: "base-image".to_string(),
                run_strs: vec![
                    "run 1".to_string(),
                    "run 2".to_string(),
                    "run 3".to_string(),
                ],
                image_hash: "".to_string(),
                image_uri: Some("1234567890.dkr.ecr.us-east-1.amazonaws.com/test".to_string()),
                sdk_version: Some("1.2.3".to_string()),
            },
            ..Default::default()
        };
        let fn_b = test_compute_fn("fn_b", "image_hash".to_string(), 0);
        let fn_c = test_compute_fn("fn_c", "image_hash".to_string(), 0);
        ComputeGraph {
            namespace: TEST_NAMESPACE.to_string(),
            name: "graph_B".to_string(),
            tags: HashMap::from([
                ("tag1".to_string(), "val1".to_string()),
                ("tag2".to_string(), "val2".to_string()),
            ]),
            tombstoned: false,
            nodes: HashMap::from([
                ("fn_b".to_string(), fn_b),
                ("fn_c".to_string(), fn_c),
                ("router_x".to_string(), router_x),
                ("fn_a".to_string(), fn_a.clone()),
            ]),
            version: crate::GraphVersion::from("1"),
            edges: HashMap::from([("fn_a".to_string(), vec!["router_x".to_string()])]),
            description: "description graph_B".to_string(),
            code: ComputeGraphCode {
                path: "cg_path".to_string(),
                size: 23,
                sha256_hash: "hash123".to_string(),
            },
            created_at: 5,
            start_fn: fn_a,
            runtime_information: RuntimeInformation {
                major_version: 3,
                minor_version: 10,
                sdk_version: "1.2.3".to_string(),
            },
            replaying: false,
        }
    }

    pub fn test_graph_with_reducer() -> ComputeGraph {
        let fn_a = test_compute_fn("fn_a", "image_hash".to_string(), 0);
        let fn_b = reducer_fn("fn_b");
        let fn_c = test_compute_fn("fn_c", "image_hash".to_string(), 0);
        ComputeGraph {
            namespace: TEST_NAMESPACE.to_string(),
            name: "graph_R".to_string(),
            tags: HashMap::from([
                ("tag1".to_string(), "val1".to_string()),
                ("tag2".to_string(), "val2".to_string()),
            ]),
            tombstoned: false,
            nodes: HashMap::from([
                ("fn_a".to_string(), fn_a.clone()),
                ("fn_b".to_string(), fn_b),
                ("fn_c".to_string(), fn_c),
            ]),
            edges: HashMap::from([
                ("fn_a".to_string(), vec!["fn_b".to_string()]),
                ("fn_b".to_string(), vec!["fn_c".to_string()]),
            ]),
            description: "description graph_R".to_string(),
            code: ComputeGraphCode {
                path: "cg_path".to_string(),
                size: 23,
                sha256_hash: "hash123".to_string(),
            },
            version: crate::GraphVersion::from("1"),
            created_at: 5,
            start_fn: fn_a,
            runtime_information: RuntimeInformation {
                major_version: 3,
                minor_version: 10,
                sdk_version: "1.2.3".to_string(),
            },
            replaying: false,
        }
    }

    pub fn test_executor_metadata(id: ExecutorId) -> ExecutorMetadata {
        ExecutorMetadata {
            id,
            executor_version: "1.0.0".to_string(),
            function_allowlist: None,
            addr: "".to_string(),
            labels: Default::default(),
            // Executor must have resources to be schedulable.
            host_resources: crate::HostResources {
                cpu_ms_per_sec: 8 * 1000, // 8 cores
                memory_bytes: 16 * 1024 * 1024 * 1024,
                disk_bytes: 100 * 1024 * 1024 * 1024,
                gpu: None,
            },
            state: Default::default(),
            function_executors: Default::default(),
            tombstoned: false,
            state_hash: "state_hash".to_string(),
            clock: 0,
        }
    }
}
