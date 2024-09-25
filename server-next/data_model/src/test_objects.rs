pub mod tests {
    use std::collections::HashMap;

    use rand::{distributions::Alphanumeric, Rng};

    use super::super::{
        ComputeFn,
        ComputeGraph,
        ComputeGraphCode,
        Node,
        Node::Compute,
        NodeOutput,
    };
    use crate::{
        DataPayload,
        DynamicEdgeRouter,
        ExecutorId,
        ExecutorMetadata,
        InvocationPayload,
        InvocationPayloadBuilder,
        NodeOutputBuilder,
    };

    pub const TEST_NAMESPACE: &str = "test_ns";
    pub const TEST_EXECUTOR_ID: &str = "test_executor_1";

    fn test_compute_fn(name: &str) -> ComputeFn {
        ComputeFn {
            name: name.to_string(),
            description: format!("description {}", name),
            fn_name: name.to_string(),
            ..Default::default()
        }
    }

    fn reducer_fn(name: &str, reduce: bool) -> ComputeFn {
        let mut compute_fn = test_compute_fn(name);
        compute_fn.reducer = reduce;
        compute_fn
    }

    pub fn mock_node_fn_output_fn_a(
        invocation_id: &str,
        graph: &str,
        reducer_fn: Option<String>,
    ) -> NodeOutput {
        let mut path: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();
        if let Some(reducer_fn) = reducer_fn {
            // Simulating overriding the existing output for accumulators
            path = format!("{}-{}-{}", invocation_id, graph, reducer_fn);
        }
        NodeOutputBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .compute_fn_name("fn_a".to_string())
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
            .build()
            .unwrap()
    }

    pub fn mock_invocation_payload_graph_b() -> InvocationPayload {
        InvocationPayloadBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .compute_graph_name("graph_B".to_string())
            .payload(DataPayload {
                path: "test".to_string(),
                size: 23,
                sha256_hash: "hash1232".to_string(),
            })
            .build()
            .unwrap()
    }

    pub fn mock_graph_a() -> ComputeGraph {
        let fn_a = test_compute_fn("fn_a");
        let fn_b = test_compute_fn("fn_b");
        let fn_c = test_compute_fn("fn_c");
        ComputeGraph {
            namespace: TEST_NAMESPACE.to_string(),
            name: "graph_A".to_string(),
            nodes: HashMap::from([
                ("fn_b".to_string(), Node::Compute(fn_b)),
                ("fn_c".to_string(), Node::Compute(fn_c)),
                ("fn_a".to_string(), Node::Compute(fn_a.clone())),
            ]),
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
            create_at: 5,
            tomb_stoned: false,
            start_fn: Compute(fn_a),
        }
    }

    pub fn mock_graph_b() -> ComputeGraph {
        let fn_a = test_compute_fn("fn_a");
        let router_x = DynamicEdgeRouter {
            name: "router_x".to_string(),
            description: "description router_x".to_string(),
            source_fn: "fn_a".to_string(),
            target_functions: vec!["fn_b".to_string(), "fn_c".to_string()],
        };
        let fn_b = test_compute_fn("fn_b");
        let fn_c = test_compute_fn("fn_c");
        ComputeGraph {
            namespace: TEST_NAMESPACE.to_string(),
            name: "graph_B".to_string(),
            nodes: HashMap::from([
                ("fn_b".to_string(), Node::Compute(fn_b)),
                ("fn_c".to_string(), Node::Compute(fn_c)),
                ("router_x".to_string(), Node::Router(router_x)),
                ("fn_a".to_string(), Node::Compute(fn_a.clone())),
            ]),
            edges: HashMap::from([("fn_a".to_string(), vec!["router_x".to_string()])]),
            description: "description graph_B".to_string(),
            code: ComputeGraphCode {
                path: "cg_path".to_string(),
                size: 23,
                sha256_hash: "hash123".to_string(),
            },
            create_at: 5,
            tomb_stoned: false,
            start_fn: Compute(fn_a),
        }
    }

    pub fn mock_graph_with_reducer() -> ComputeGraph {
        let fn_a = test_compute_fn("fn_a");
        let fn_b = reducer_fn("fn_b", true);
        let fn_c = test_compute_fn("fn_c");
        ComputeGraph {
            namespace: TEST_NAMESPACE.to_string(),
            name: "graph_R".to_string(),
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
            create_at: 5,
            tomb_stoned: false,
            start_fn: Compute(fn_a),
        }
    }

    pub fn mock_executor_id() -> ExecutorId {
        ExecutorId::new(TEST_EXECUTOR_ID.to_string())
    }

    pub fn mock_executor() -> ExecutorMetadata {
        ExecutorMetadata {
            id: mock_executor_id(),
            runner_name: "test_runner".to_string(),
            addr: "".to_string(),
            labels: Default::default(),
        }
    }
}
