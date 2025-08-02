use std::sync::Arc;

use anyhow::Result;

use crate::{
    data_model::test_objects::tests::{
        self,
        test_invocation_ctx,
        test_invocation_payload_graph_a,
        TEST_NAMESPACE,
    },
    state_store::{
        requests::{
            CreateOrUpdateComputeGraphRequest,
            InvokeComputeGraphRequest,
            RequestPayload,
            StateMachineUpdateRequest,
        },
        IndexifyState,
    },
};

pub struct TestStateStore {
    pub indexify_state: Arc<IndexifyState>,
}

impl TestStateStore {
    pub async fn new() -> Result<TestStateStore> {
        let temp_dir = tempfile::tempdir()?;
        let indexify_state = IndexifyState::new(
            temp_dir.path().join("state"),
            crate::state_store::ExecutorLabelSet::default(),
        )
        .await?;
        Ok(TestStateStore { indexify_state })
    }
}

pub async fn with_simple_retry_graph(indexify_state: &IndexifyState, max_retries: u32) -> String {
    let cg_request = CreateOrUpdateComputeGraphRequest {
        namespace: TEST_NAMESPACE.to_string(),
        compute_graph: tests::test_graph_a_retry("image_hash".to_string(), max_retries),
        upgrade_tasks_to_current_version: true,
    };
    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
            processed_state_changes: vec![],
        })
        .await
        .unwrap();
    let invocation_payload = test_invocation_payload_graph_a();
    let ctx = test_invocation_ctx(
        TEST_NAMESPACE,
        &tests::test_graph_a_retry("image_hash".to_string(), max_retries),
        &invocation_payload,
    );
    let request = InvokeComputeGraphRequest {
        namespace: TEST_NAMESPACE.to_string(),
        compute_graph_name: "graph_A".to_string(),
        invocation_payload: invocation_payload.clone(),
        ctx,
    };
    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::InvokeComputeGraph(request),
            processed_state_changes: vec![],
        })
        .await
        .unwrap();
    invocation_payload.id
}

pub async fn with_simple_graph(indexify_state: &IndexifyState) -> String {
    with_simple_retry_graph(indexify_state, 0).await
}
