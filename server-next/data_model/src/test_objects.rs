pub mod tests {
    use std::collections::HashMap;

    use super::super::{ComputeFn, ComputeGraph, ComputeGraphCode, Node, Node::Compute};
    use crate::{DataPayload, InvocationPayload, InvocationPayloadBuilder};

    pub const TEST_NAMESPACE: &str = "test_ns";

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

    pub fn mock_graph_a() -> ComputeGraph {
        let fn_a = ComputeFn {
            name: "fn_a".to_string(),
            description: "description fn_a".to_string(),
            fn_name: "fn_a".to_string(),
            placement_constraints: Default::default(),
        };
        let fn_b = ComputeFn {
            name: "fn_b".to_string(),
            description: "description fn_b".to_string(),
            fn_name: "fn_b".to_string(),
            placement_constraints: Default::default(),
        };
        let fn_c = ComputeFn {
            name: "fn_c".to_string(),
            description: "description fn_c".to_string(),
            fn_name: "fn_c".to_string(),
            placement_constraints: Default::default(),
        };
        ComputeGraph {
            namespace: TEST_NAMESPACE.to_string(),
            name: "graph_A".to_string(),
            nodes: HashMap::from([("fn_b".to_string(), Node::Compute(fn_b)), ("fn_c".to_string(), Node::Compute(fn_c)), ("fn_a".to_string(), Node::Compute(fn_a))]),
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
            start_fn: Compute(ComputeFn {
                name: "fn_a".to_string(),
                description: "description fn_a".to_string(),
                fn_name: "fn_a".to_string(),
                placement_constraints: Default::default(),
            }),
        }
    }
}
