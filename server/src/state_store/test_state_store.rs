use std::sync::Arc;

use anyhow::Result;

use crate::{
    data_model::{
        test_objects::tests::{self, mock_request_ctx, TEST_NAMESPACE},
        ComputeGraph,
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
            crate::state_store::ExecutorCatalog::default(),
        )
        .await?;
        Ok(TestStateStore { indexify_state })
    }
}

pub async fn with_simple_retry_graph(indexify_state: &IndexifyState, max_retries: u32) -> String {
    let app = create_or_update_application(indexify_state, max_retries).await;
    invoke_application(indexify_state, &app).await.unwrap()
}

pub async fn with_simple_graph(indexify_state: &IndexifyState) -> String {
    with_simple_retry_graph(indexify_state, 0).await
}

pub async fn create_or_update_application(
    indexify_state: &IndexifyState,
    max_retries: u32,
) -> ComputeGraph {
    let app = tests::mock_graph_with_retries(max_retries);
    let request = CreateOrUpdateComputeGraphRequest {
        namespace: TEST_NAMESPACE.to_string(),
        compute_graph: app.clone(),
        upgrade_requests_to_current_version: true,
    };
    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::CreateOrUpdateComputeGraph(Box::new(request)),
        })
        .await
        .unwrap();

    app
}

pub async fn invoke_application(
    indexify_state: &IndexifyState,
    app: &ComputeGraph,
) -> Result<String> {
    let ctx = mock_request_ctx(&app.namespace, app);
    let request_id = ctx.request_id.clone();

    let request = InvokeComputeGraphRequest {
        namespace: app.namespace.clone(),
        compute_graph_name: app.name.clone(),
        ctx,
    };
    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::InvokeComputeGraph(request),
        })
        .await?;
    Ok(request_id)
}
