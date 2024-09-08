pub mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use data_model::{
        test_objects::tests::{
            self,
            mock_invocation_payload,
            mock_node_fn_output_fn_a,
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
        pub invocation_payload_id: String,
    }

    impl TestStateStore {
        pub async fn new() -> Result<Self> {
            let temp_dir = TempDir::new()?;
            let indexify_state = Arc::new(IndexifyState::new(temp_dir.path().join("state"))?);
            let cg_request = CreateComputeGraphRequest {
                namespace: TEST_NAMESPACE.to_string(),
                compute_graph: tests::mock_graph_a(),
            };
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::CreateComputeGraph(cg_request),
                    state_changes_processed: vec![],
                })
                .await?;
            let invocation_payload = mock_invocation_payload();
            let request = InvokeComputeGraphRequest {
                namespace: TEST_NAMESPACE.to_string(),
                compute_graph_name: "graph_A".to_string(),
                invocation_payload: invocation_payload.clone(),
            };
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::InvokeComputeGraph(request),
                    state_changes_processed: vec![],
                })
                .await
                .unwrap();

            Ok(Self {
                indexify_state,
                invocation_payload_id: invocation_payload.id,
            })
        }

        pub async fn finalize_task(&self, invocation_id: &str, task_id: &TaskId) -> Result<()> {
            let request = FinalizeTaskRequest {
                namespace: TEST_NAMESPACE.to_string(),
                compute_graph: "graph_A".to_string(),
                compute_fn: "fn_a".to_string(),
                invocation_id: invocation_id.to_string(),
                task_id: task_id.clone(),
                node_outputs: vec![mock_node_fn_output_fn_a(&invocation_id, task_id)],
                task_outcome: TaskOutcome::Success,
                executor_id: ExecutorId::new(TEST_EXECUTOR_ID.to_string()),
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
