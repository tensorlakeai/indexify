pub mod tests {
    use std::collections::HashMap;

    use rand::Rng;

    use super::super::{
        ComputeFn,
        ComputeGraph,
        ComputeGraphCode,
        Node,
        Node::Compute,
        NodeOutput,
        RuntimeInformation,
    };
    use crate::{
        DataPayload,
        DynamicEdgeRouter,
        ExecutorId,
        ExecutorMetadata,
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
        node_output_key: &str,
        inv_id: &str,
    ) -> Task {
        TaskBuilder::default()
            .namespace(cg.namespace.to_string())
            .compute_fn_name(cg_fn.to_string())
            .compute_graph_name(cg.name.to_string())
            .input_node_output_key(node_output_key.to_string())
            .invocation_id(inv_id.to_string())
            .reducer_output_id(None)
            .graph_version(Default::default())
            .secret_names(None)
            .build()
            .unwrap()
    }

    pub fn test_compute_fn(name: &str, image_hash: String) -> ComputeFn {
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
            ..Default::default()
        }
    }

    pub fn reducer_fn(name: &str) -> ComputeFn {
        let mut compute_fn = test_compute_fn(name, "image_hash".to_string());
        compute_fn.reducer = true;
        compute_fn
    }

    pub fn mock_node_fn_output_fn_a(
        invocation_id: &str,
        graph: &str,
        reducer_fn: Option<String>,
    ) -> NodeOutput {
        mock_node_fn_output(invocation_id, graph, "fn_a", reducer_fn)
    }

    pub fn mock_node_fn_output(
        invocation_id: &str,
        graph: &str,
        compute_fn_name: &str,
        reducer_fn: Option<String>,
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
            .payload(crate::OutputPayload::Fn(DataPayload {
                sha256_hash: "3433".to_string(),
                path,
                size: 12,
            }))
            .build()
            .unwrap()
    }

    pub fn mock_node_router_output_x(invocation_id: &str, graph: &str) -> NodeOutput {
        NodeOutputBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .compute_fn_name("router_x".to_string())
            .compute_graph_name(graph.to_string())
            .invocation_id(invocation_id.to_string())
            .payload(crate::OutputPayload::Router(crate::RouterOutput {
                edges: vec!["fn_c".to_string()],
            }))
            .build()
            .unwrap()
    }

    pub fn mock_invocation_payload() -> InvocationPayload {
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

    pub fn mock_invocation_ctx(
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

    pub fn mock_invocation_payload_graph_b() -> InvocationPayload {
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

    pub fn mock_graph_a(image_hash: String) -> ComputeGraph {
        let fn_a = test_compute_fn("fn_a", image_hash.clone());
        let fn_b = test_compute_fn("fn_b", image_hash.clone());
        let fn_c = test_compute_fn("fn_c", image_hash.clone());

        ComputeGraph {
            namespace: TEST_NAMESPACE.to_string(),
            name: "graph_A".to_string(),
            tags: HashMap::from([
                ("tag1".to_string(), "val1".to_string()),
                ("tag2".to_string(), "val2".to_string()),
            ]),
            tombstoned: false,
            nodes: HashMap::from([
                ("fn_b".to_string(), Node::Compute(fn_b)),
                ("fn_c".to_string(), Node::Compute(fn_c)),
                ("fn_a".to_string(), Node::Compute(fn_a.clone())),
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
            start_fn: Compute(fn_a),
            runtime_information: RuntimeInformation {
                major_version: 3,
                minor_version: 10,
                sdk_version: "1.2.3".to_string(),
            },
            replaying: false,
        }
    }

    pub fn mock_graph_b() -> ComputeGraph {
        let fn_a = test_compute_fn("fn_a", "image_hash".to_string());
        let router_x = DynamicEdgeRouter {
            name: "router_x".to_string(),
            description: "description router_x".to_string(),
            source_fn: "fn_a".to_string(),
            target_functions: vec!["fn_b".to_string(), "fn_c".to_string()],
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
        let fn_b = test_compute_fn("fn_b", "image_hash".to_string());
        let fn_c = test_compute_fn("fn_c", "image_hash".to_string());
        ComputeGraph {
            namespace: TEST_NAMESPACE.to_string(),
            name: "graph_B".to_string(),
            tags: HashMap::from([
                ("tag1".to_string(), "val1".to_string()),
                ("tag2".to_string(), "val2".to_string()),
            ]),
            tombstoned: false,
            nodes: HashMap::from([
                ("fn_b".to_string(), Node::Compute(fn_b)),
                ("fn_c".to_string(), Node::Compute(fn_c)),
                ("router_x".to_string(), Node::Router(router_x)),
                ("fn_a".to_string(), Node::Compute(fn_a.clone())),
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
            start_fn: Compute(fn_a),
            runtime_information: RuntimeInformation {
                major_version: 3,
                minor_version: 10,
                sdk_version: "1.2.3".to_string(),
            },
            replaying: false,
        }
    }

    pub fn mock_graph_with_reducer() -> ComputeGraph {
        let fn_a = test_compute_fn("fn_a", "image_hash".to_string());
        let fn_b = reducer_fn("fn_b");
        let fn_c = test_compute_fn("fn_c", "image_hash".to_string());
        ComputeGraph {
            namespace: TEST_NAMESPACE.to_string(),
            name: "graph_R".to_string(),
            tags: HashMap::from([
                ("tag1".to_string(), "val1".to_string()),
                ("tag2".to_string(), "val2".to_string()),
            ]),
            tombstoned: false,
            nodes: HashMap::from([
                ("fn_a".to_string(), Node::Compute(fn_a.clone())),
                ("fn_b".to_string(), Node::Compute(fn_b)),
                ("fn_c".to_string(), Node::Compute(fn_c)),
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
            start_fn: Compute(fn_a),
            runtime_information: RuntimeInformation {
                major_version: 3,
                minor_version: 10,
                sdk_version: "1.2.3".to_string(),
            },
            replaying: false,
        }
    }

    pub fn mock_executor_id() -> ExecutorId {
        ExecutorId::new(TEST_EXECUTOR_ID.to_string())
    }

    pub fn mock_executor() -> ExecutorMetadata {
        ExecutorMetadata {
            id: mock_executor_id(),
            executor_version: "1.0.0".to_string(),
            function_allowlist: None,
            addr: "".to_string(),
            labels: Default::default(),
            host_resources: Default::default(),
            state: Default::default(),
            function_executors: Default::default(),
            tombstoned: false,
            state_hash: "state_hash".to_string(),
        }
    }
}
