use tracing::info;

use super::routes_state::RouteState;
use crate::{
    data_model::Application,
    http_objects::IndexifyAPIError,
    state_store::requests::{
        CreateOrUpdateApplicationRequest,
        RequestPayload,
        StateMachineUpdateRequest,
    },
};

/// Shared validation and submission logic for creating or updating an
/// application.
///
/// This function validates that:
/// 1. The application is not disabled
/// 2. The application can be scheduled with the current executor catalog
///
/// Then it submits the creation/update request to the state store.
pub async fn validate_and_submit_application(
    state: &RouteState,
    namespace: String,
    application: Application,
    upgrade_requests_to_current_version: bool,
) -> Result<(), IndexifyAPIError> {
    let existing_application = state
        .indexify_state
        .reader()
        .get_application(&namespace, &application.name)
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    // Don't allow deploying disabled applications
    if let Some(reason) = existing_application.and_then(|a| a.state.as_disabled()) {
        return Err(IndexifyAPIError::bad_request(&format!(
            "Application is not enabled: {reason}",
        )));
    }

    let executor_catalog = state
        .indexify_state
        .in_memory_state
        .read()
        .await
        .executor_catalog
        .clone();
    application
        .can_be_scheduled(&executor_catalog)
        .map_err(|e| IndexifyAPIError::bad_request(&e.to_string()))?;
    let name = application.name.clone();

    info!(
        "creating application {}, upgrade existing function runs and requests: {}",
        name, upgrade_requests_to_current_version
    );
    let request =
        RequestPayload::CreateOrUpdateApplication(Box::new(CreateOrUpdateApplicationRequest {
            namespace,
            application,
            upgrade_requests_to_current_version,
        }));

    state
        .indexify_state
        .write(StateMachineUpdateRequest { payload: request })
        .await
        .map_err(IndexifyAPIError::internal_error)
}
