use axum::{
    Json,
    extract::{Path, State},
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    data_model::{Container, ContainerServerMetadata, ContainerState},
    http_objects::IndexifyAPIError,
    routes::routes_state::RouteState,
};

/// Container information returned by list operations
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ContainerInfo {
    /// Container ID
    pub id: String,
    /// Application name
    pub application_name: String,
    /// Application version
    pub version: String,
    /// Function name
    pub function_name: String,
    /// Desired state set by server
    pub desired_state: String,
    /// Current state reported by executor
    pub current_state: String,
    /// Executor ID where the container is running
    pub executor_id: String,
    /// Logical clock when container was created
    pub created_at_clock: Option<u64>,
}

impl ContainerInfo {
    fn from_container(container: &Container, metadata: &ContainerServerMetadata) -> Self {
        Self {
            id: container.id.to_string(),
            application_name: container.application_name.clone(),
            version: container.version.clone(),
            function_name: container.function_name.clone(),
            desired_state: state_to_string(&metadata.desired_state),
            current_state: state_to_string(&container.state),
            executor_id: metadata.executor_id.to_string(),
            created_at_clock: container.created_at_clock(),
        }
    }
}

/// Response containing list of containers
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListContainersResponse {
    pub containers: Vec<ContainerInfo>,
}

/// Convert ContainerState to a human-readable string
fn state_to_string(state: &ContainerState) -> String {
    match state {
        ContainerState::Unknown => "unknown".to_string(),
        ContainerState::Pending => "pending".to_string(),
        ContainerState::Running => "running".to_string(),
        ContainerState::Terminated { reason, .. } => format!("terminated({})", reason),
    }
}

/// List all containers for a given application
///
/// Returns information about all containers (function executors) associated
/// with an application, including their version, function name, desired state
/// (from server), current state (reported by executor), and created time.
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/applications/{application}/containers",
    tag = "indexify",
    params(
        ("namespace" = String, Path, description = "Namespace name"),
        ("application" = String, Path, description = "Application name"),
    ),
    responses(
        (status = 200, description = "List of containers", body = ListContainersResponse),
        (status = 500, description = "Internal server error", body = IndexifyAPIError),
    )
)]
pub async fn list_application_containers(
    Path((namespace, application)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<Json<ListContainersResponse>, IndexifyAPIError> {
    // Get container scheduler to access all containers
    let container_scheduler = state.indexify_state.container_scheduler.read().await;

    // Filter containers by namespace and application
    let containers: Vec<ContainerInfo> = container_scheduler
        .function_containers
        .iter()
        .filter(|(_, metadata)| {
            let container = &metadata.function_container;
            container.namespace == namespace && container.application_name == application
        })
        .map(|(_, metadata)| ContainerInfo::from_container(&metadata.function_container, metadata))
        .collect();

    Ok(Json(ListContainersResponse { containers }))
}
