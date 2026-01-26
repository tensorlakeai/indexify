use std::sync::Arc;

use anyhow::Result;

use crate::{
    config::WorkloadPlacementConstraints,
    data_model::{
        Application,
        test_objects::tests::{self, TEST_NAMESPACE, mock_request_ctx},
    },
    state_store::{
        IndexifyState,
        driver::rocksdb::RocksDBConfig,
        requests::{
            CreateOrUpdateApplicationRequest,
            InvokeApplicationRequest,
            RequestPayload,
            StateMachineUpdateRequest,
        },
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
            RocksDBConfig::default(),
            crate::state_store::ExecutorCatalog::default(),
            WorkloadPlacementConstraints::default(),
        )
        .await?;
        Ok(TestStateStore { indexify_state })
    }
}

pub async fn with_simple_retry_application(
    indexify_state: &IndexifyState,
    max_retries: u32,
) -> String {
    let app = create_or_update_application(indexify_state, "graph_A", max_retries).await;
    invoke_application(indexify_state, &app).await.unwrap()
}

pub async fn with_simple_application(indexify_state: &IndexifyState) -> String {
    with_simple_retry_application(indexify_state, 0).await
}

pub async fn create_or_update_application(
    indexify_state: &IndexifyState,
    app_name: &str,
    max_retries: u32,
) -> Application {
    let app = tests::mock_app_with_retries(app_name, "1", max_retries);
    let request = CreateOrUpdateApplicationRequest {
        namespace: TEST_NAMESPACE.to_string(),
        application: app.clone(),
        upgrade_requests_to_current_version: true,
    };
    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::CreateOrUpdateApplication(Box::new(request)),
        })
        .await
        .unwrap();

    app
}

pub fn mock_application(app_name: &str, version: &str) -> Application {
    tests::mock_app_with_retries(app_name, version, 0)
}

pub async fn invoke_application(
    indexify_state: &IndexifyState,
    app: &Application,
) -> Result<String> {
    let ctx = mock_request_ctx(&app.namespace, app);
    let request_id = ctx.request_id.clone();
    let request = InvokeApplicationRequest {
        namespace: app.namespace.clone(),
        application_name: app.name.clone(),
        ctx,
    };
    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::InvokeApplication(request),
        })
        .await?;
    Ok(request_id)
}

pub async fn invoke_application_with_request_id(
    indexify_state: &IndexifyState,
    app: &Application,
    request_id: &str,
) -> Result<String> {
    let ctx = tests::mock_request_ctx_with_id(&app.namespace, app, request_id);
    let request = InvokeApplicationRequest {
        namespace: app.namespace.clone(),
        application_name: app.name.clone(),
        ctx,
    };
    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::InvokeApplication(request),
        })
        .await?;
    Ok(request_id.to_string())
}
