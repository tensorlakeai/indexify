pub mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use data_model::{
        test_objects::tests::{
            self,
            mock_invocation_payload,
            mock_invocation_payload_graph_b,
            mock_node_fn_output_fn_a,
            mock_node_router_output_x,
            TEST_EXECUTOR_ID,
            TEST_NAMESPACE,
        },
        ExecutorId,
        TaskId,
        TaskOutcome,
    };
    use tempfile::TempDir;

    use crate::{
        requests::{
            CreateComputeGraphRequest,
            FinalizeTaskRequest,
            InvokeComputeGraphRequest,
            RequestPayload,
            StateMachineUpdateRequest,
        },
        IndexifyState,
    };

    pub struct TestStateStore {
        pub indexify_state: Arc<IndexifyState>,
    }

    impl TestStateStore {
        pub async fn new() -> Result<Self> {
            let temp_dir = TempDir::new()?;
            let indexify_state = IndexifyState::new(temp_dir.path().join("state"))
                .await
                .unwrap();
            Ok(Self { indexify_state })
        }

        pub async fn with_simple_graph(&self) -> String {
            let cg_request = CreateComputeGraphRequest {
                namespace: TEST_NAMESPACE.to_string(),
                compute_graph: tests::mock_graph_a(),
            };
            self.indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::CreateComputeGraph(cg_request),
                    state_changes_processed: vec![],
                })
                .await
                .unwrap();
            let invocation_payload = mock_invocation_payload();
            let request = InvokeComputeGraphRequest {
                namespace: TEST_NAMESPACE.to_string(),
                compute_graph_name: "graph_A".to_string(),
                invocation_payload: invocation_payload.clone(),
            };
            self.indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::InvokeComputeGraph(request),
                    state_changes_processed: vec![],
                })
                .await
                .unwrap();
            invocation_payload.id
        }

        pub async fn with_router_graph(&self) -> String {
            let cg_request = CreateComputeGraphRequest {
                namespace: TEST_NAMESPACE.to_string(),
                compute_graph: tests::mock_graph_b(),
            };
            self.indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::CreateComputeGraph(cg_request),
                    state_changes_processed: vec![],
                })
                .await
                .unwrap();

            let invocation_payload = mock_invocation_payload_graph_b();
            let request = InvokeComputeGraphRequest {
                namespace: TEST_NAMESPACE.to_string(),
                compute_graph_name: "graph_B".to_string(),
                invocation_payload: invocation_payload.clone(),
            };
            self.indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::InvokeComputeGraph(request),
                    state_changes_processed: vec![],
                })
                .await
                .unwrap();
            invocation_payload.id
        }

        pub async fn finalize_task(&self, invocation_id: &str, task_id: &TaskId, task_outcome: TaskOutcome) -> Result<()> {
            let request = FinalizeTaskRequest {
                namespace: TEST_NAMESPACE.to_string(),
                compute_graph: "graph_A".to_string(),
                compute_fn: "fn_a".to_string(),
                invocation_id: invocation_id.to_string(),
                task_id: task_id.clone(),
                node_outputs: vec![mock_node_fn_output_fn_a(&invocation_id, "graph_A")],
                task_outcome,
                executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
                diagnostics: None,
            };

            self.indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await
        }

        pub async fn finalize_task_graph_b(
            &self,
            invocation_id: &str,
            task_id: &TaskId,
        ) -> Result<()> {
            let request = FinalizeTaskRequest {
                namespace: TEST_NAMESPACE.to_string(),
                compute_graph: "graph_B".to_string(),
                compute_fn: "fn_a".to_string(),
                invocation_id: invocation_id.to_string(),
                task_id: task_id.clone(),
                node_outputs: vec![mock_node_fn_output_fn_a(&invocation_id, "graph_B")],
                task_outcome: TaskOutcome::Success,
                executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
                diagnostics: None,
            };
            self.indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await
        }

        pub async fn finalize_router_x(&self, invocation_id: &str, task_id: &TaskId) -> Result<()> {
            let request = FinalizeTaskRequest {
                namespace: TEST_NAMESPACE.to_string(),
                compute_graph: "graph_B".to_string(),
                compute_fn: "router_x".to_string(),
                invocation_id: invocation_id.to_string(),
                task_id: task_id.clone(),
                node_outputs: vec![mock_node_router_output_x(&invocation_id, "graph_B")],
                task_outcome: TaskOutcome::Success,
                executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
                diagnostics: None,
            };
            self.indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::FinalizeTask(request),
                    state_changes_processed: vec![],
                })
                .await
        }
    }
}
