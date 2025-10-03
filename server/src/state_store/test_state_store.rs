use std::sync::Arc;

use anyhow::Result;

use crate::{
    data_model::test_objects::tests::{self, mock_request_ctx, TEST_NAMESPACE},
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
            crate::state_store::ExecutorCatalog::default(),
        )
        .await?;
        Ok(TestStateStore { indexify_state })
    }
}

pub async fn with_simple_retry_graph(indexify_state: &IndexifyState, max_retries: u32) -> String {
    let cg = tests::mock_graph_with_retries(max_retries);
    let cg_request = CreateOrUpdateComputeGraphRequest {
        namespace: TEST_NAMESPACE.to_string(),
        application: cg.clone(),
        upgrade_requests_to_current_version: true,
    };
    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::CreateOrUpdateComputeGraph(Box::new(cg_request)),
        })
        .await
        .unwrap();
    let ctx = mock_request_ctx(TEST_NAMESPACE, &cg);
    let request_id = ctx.request_id.clone();

    let request = InvokeComputeGraphRequest {
        namespace: TEST_NAMESPACE.to_string(),
        application_name: cg.name.clone(),
        ctx,
    };
    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::InvokeComputeGraph(request),
        })
        .await
        .unwrap();
    request_id
}

pub async fn with_simple_graph(indexify_state: &IndexifyState) -> String {
    with_simple_retry_graph(indexify_state, 0).await
}
