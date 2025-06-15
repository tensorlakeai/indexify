use std::sync::Arc;

use anyhow::Result;
use data_model::test_objects::tests::{
    self,
    mock_invocation_ctx,
    mock_invocation_payload,
    mock_invocation_payload_graph_b,
    TEST_NAMESPACE,
};

use crate::{
    requests::{
        CreateOrUpdateComputeGraphRequest,
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
    pub async fn new() -> Result<TestStateStore> {
        let temp_dir = tempfile::tempdir()?;
        let indexify_state = IndexifyState::new(temp_dir.path().join("state")).await?;
        Ok(TestStateStore { indexify_state })
    }

    pub async fn with_simple_graph(&self) -> String {
        with_simple_graph(&self.indexify_state).await
    }

    pub async fn with_router_graph(&self) -> String {
        with_router_graph(&self.indexify_state).await
    }

    pub async fn with_reducer_graph(&self) -> String {
        with_reducer_graph(&self.indexify_state).await
    }
}

pub async fn with_simple_graph(indexify_state: &IndexifyState) -> String {
    let cg_request = CreateOrUpdateComputeGraphRequest {
        namespace: TEST_NAMESPACE.to_string(),
        compute_graph: tests::mock_graph_a("image_hash".to_string()),
        upgrade_tasks_to_current_version: true,
    };
    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
            processed_state_changes: vec![],
        })
        .await
        .unwrap();
    let invocation_payload = mock_invocation_payload();
    let ctx = mock_invocation_ctx(
        TEST_NAMESPACE,
        &tests::mock_graph_a("image_hash".to_string()),
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

pub async fn with_router_graph(indexify_state: &IndexifyState) -> String {
    let cg_request = CreateOrUpdateComputeGraphRequest {
        namespace: TEST_NAMESPACE.to_string(),
        compute_graph: tests::mock_graph_b(),
        upgrade_tasks_to_current_version: false,
    };
    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
            processed_state_changes: vec![],
        })
        .await
        .unwrap();

    let invocation_payload = mock_invocation_payload_graph_b();
    let ctx = mock_invocation_ctx(TEST_NAMESPACE, &tests::mock_graph_b(), &invocation_payload);
    let request = InvokeComputeGraphRequest {
        namespace: TEST_NAMESPACE.to_string(),
        compute_graph_name: "graph_B".to_string(),
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

pub async fn with_reducer_graph(indexify_state: &IndexifyState) -> String {
    let cg_request = CreateOrUpdateComputeGraphRequest {
        namespace: TEST_NAMESPACE.to_string(),
        compute_graph: tests::mock_graph_with_reducer(),
        upgrade_tasks_to_current_version: false,
    };
    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::CreateOrUpdateComputeGraph(cg_request),
            processed_state_changes: vec![],
        })
        .await
        .unwrap();

    let invocation_payload = mock_invocation_payload_graph_b();
    let ctx = mock_invocation_ctx(
        TEST_NAMESPACE,
        &tests::mock_graph_with_reducer(),
        &invocation_payload,
    );
    let request = InvokeComputeGraphRequest {
        namespace: TEST_NAMESPACE.to_string(),
        compute_graph_name: "graph_R".to_string(),
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
