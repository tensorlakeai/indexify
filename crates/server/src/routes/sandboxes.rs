use axum::{
    Json,
    extract::{Path, State},
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    data_model::{self, Sandbox, SandboxBuilder, SandboxId, SandboxStatus},
    http_objects::{ContainerResources, IndexifyAPIError},
    routes::routes_state::RouteState,
    state_store::requests::{
        CreateSandboxRequest as StateCreateSandboxRequest,
        RequestPayload,
        StateMachineUpdateRequest,
        TerminateSandboxRequest,
    },
    utils::get_epoch_time_in_ns,
};

/// Request to create a new sandbox
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateSandboxRequest {
    /// Docker image for the sandbox (optional, uses server default if not
    /// provided)
    #[serde(default)]
    pub image: Option<String>,
    /// Resource requirements (optional, has defaults)
    #[serde(default)]
    pub resources: ContainerResources,
    /// Secret names to inject (optional)
    #[serde(default)]
    pub secret_names: Vec<String>,
    /// Timeout in seconds, 0 = no timeout (optional, uses server default if not
    /// provided)
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Optional entrypoint command to run when sandbox starts.
    /// If not provided, sandbox waits for commands via HTTP API.
    #[serde(default)]
    pub entrypoint: Option<Vec<String>>,
}

/// Response after creating a sandbox
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateSandboxResponse {
    pub sandbox_id: String,
    pub status: String,
}

/// Resource info for sandbox response
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ContainerResourcesInfo {
    pub cpus: f64,
    pub memory_mb: u64,
    pub ephemeral_disk_mb: u64,
}

/// Sandbox information returned by list/get operations
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SandboxInfo {
    pub id: String,
    pub namespace: String,
    pub application: String,
    pub image: String,
    pub status: String,
    pub outcome: Option<String>,
    pub created_at: u64,
    pub container_id: Option<String>,
    pub executor_id: Option<String>,
    pub resources: ContainerResourcesInfo,
    pub timeout_secs: u64,
    /// HTTP address of the sandbox API (host:port).
    pub sandbox_http_address: Option<String>,
}

impl From<&Sandbox> for SandboxInfo {
    fn from(sandbox: &Sandbox) -> Self {
        Self {
            id: sandbox.id.get().to_string(),
            namespace: sandbox.namespace.clone(),
            application: sandbox.application.clone(),
            image: sandbox.image.clone(),
            status: sandbox.status.to_string(),
            outcome: sandbox.outcome.as_ref().map(|o| o.to_string()),
            created_at: (sandbox.creation_time_ns / 1_000_000) as u64, // Convert ns to ms
            container_id: Some(sandbox.id.get().to_string()),
            executor_id: sandbox.executor_id.as_ref().map(|e| e.get().to_string()),
            resources: ContainerResourcesInfo {
                cpus: sandbox.resources.cpu_ms_per_sec as f64 / 1000.0,
                memory_mb: sandbox.resources.memory_mb,
                ephemeral_disk_mb: sandbox.resources.ephemeral_disk_mb,
            },
            timeout_secs: sandbox.timeout_secs,
            sandbox_http_address: sandbox.sandbox_http_address.clone(),
        }
    }
}

/// List sandboxes response
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListSandboxesResponse {
    pub sandboxes: Vec<SandboxInfo>,
}

/// Create a new sandbox for an application
#[utoipa::path(
    post,
    path = "/v1/namespaces/{namespace}/applications/{application}/sandboxes",
    tag = "sandboxes",
    request_body = CreateSandboxRequest,
    responses(
        (status = 200, description = "Sandbox created", body = CreateSandboxResponse),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error")
    ),
)]
pub async fn create_sandbox(
    Path((namespace, application)): Path<(String, String)>,
    State(state): State<RouteState>,
    Json(request): Json<CreateSandboxRequest>,
) -> Result<Json<CreateSandboxResponse>, IndexifyAPIError> {
    // Apply config defaults for image and timeout
    let image = request
        .image
        .unwrap_or_else(|| state.config.default_sandbox_image.clone());
    let timeout_secs = request
        .timeout_secs
        .unwrap_or(state.config.default_sandbox_timeout_secs);

    let sandbox_id = SandboxId::default();
    let sandbox = SandboxBuilder::default()
        .id(sandbox_id.clone())
        .namespace(namespace.clone())
        .application(application.clone())
        .application_version("inline".to_string()) // No app version needed for inline spec
        .image(image)
        .status(SandboxStatus::Pending)
        .creation_time_ns(get_epoch_time_in_ns())
        .resources(data_model::ContainerResources {
            cpu_ms_per_sec: (request.resources.cpus * 1000.0).ceil() as u32,
            memory_mb: request.resources.memory_mb,
            ephemeral_disk_mb: request.resources.ephemeral_disk_mb,
            gpu: request
                .resources
                .gpu_configs
                .first()
                .map(|g| data_model::GPUResources {
                    count: g.count,
                    model: g.model.clone(),
                }),
        })
        .secret_names(request.secret_names.clone())
        .timeout_secs(timeout_secs)
        .entrypoint(request.entrypoint.clone())
        .build()
        .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()))?;

    // Write to state store via CreateSandboxRequest
    let state_request = StateMachineUpdateRequest {
        payload: RequestPayload::CreateSandbox(StateCreateSandboxRequest {
            sandbox: sandbox.clone(),
        }),
    };

    state
        .indexify_state
        .write(state_request)
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    Ok(Json(CreateSandboxResponse {
        sandbox_id: sandbox_id.get().to_string(),
        status: "Pending".to_string(),
    }))
}

/// List all sandboxes for an application
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/applications/{application}/sandboxes",
    tag = "sandboxes",
    responses(
        (status = 200, description = "List of sandboxes", body = ListSandboxesResponse),
        (status = 404, description = "Application not found"),
        (status = 500, description = "Internal server error")
    ),
)]
pub async fn list_sandboxes(
    Path((namespace, application)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<Json<ListSandboxesResponse>, IndexifyAPIError> {
    // Read sandboxes from database (includes terminated sandboxes)
    let reader = state.indexify_state.reader();
    let sandboxes = reader
        .list_sandboxes(&namespace, &application)
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    let sandbox_infos: Vec<SandboxInfo> = sandboxes.iter().map(SandboxInfo::from).collect();

    Ok(Json(ListSandboxesResponse {
        sandboxes: sandbox_infos,
    }))
}

/// Get a specific sandbox
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/applications/{application}/sandboxes/{sandbox_id}",
    tag = "sandboxes",
    responses(
        (status = 200, description = "Sandbox details", body = SandboxInfo),
        (status = 404, description = "Sandbox not found"),
        (status = 500, description = "Internal server error")
    ),
)]
pub async fn get_sandbox(
    Path((namespace, application, sandbox_id)): Path<(String, String, String)>,
    State(state): State<RouteState>,
) -> Result<Json<SandboxInfo>, IndexifyAPIError> {
    let reader = state.indexify_state.reader();
    let sandbox = reader
        .get_sandbox(&namespace, &application, &sandbox_id)
        .await
        .map_err(IndexifyAPIError::internal_error)?
        .ok_or_else(|| IndexifyAPIError::not_found("Sandbox not found"))?;

    Ok(Json(SandboxInfo::from(&sandbox)))
}

/// Delete (terminate) a sandbox
#[utoipa::path(
    delete,
    path = "/v1/namespaces/{namespace}/applications/{application}/sandboxes/{sandbox_id}",
    tag = "sandboxes",
    responses(
        (status = 200, description = "Sandbox terminated"),
        (status = 404, description = "Sandbox not found"),
        (status = 500, description = "Internal server error")
    ),
)]
pub async fn delete_sandbox(
    Path((namespace, application, sandbox_id)): Path<(String, String, String)>,
    State(state): State<RouteState>,
) -> Result<(), IndexifyAPIError> {
    // Check if sandbox exists and is not already terminated
    let reader = state.indexify_state.reader();
    let sandbox = reader
        .get_sandbox(&namespace, &application, &sandbox_id)
        .await
        .map_err(IndexifyAPIError::internal_error)?
        .ok_or_else(|| IndexifyAPIError::not_found("Sandbox not found"))?;

    // If already terminated, return success (idempotent)
    if sandbox.status == SandboxStatus::Terminated {
        return Ok(());
    }

    // Write TerminateSandboxRequest to state store
    let request = StateMachineUpdateRequest {
        payload: RequestPayload::TerminateSandbox(TerminateSandboxRequest {
            namespace: namespace.clone(),
            application: application.clone(),
            sandbox_id: SandboxId::new(sandbox_id),
        }),
    };

    state
        .indexify_state
        .write(request)
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    Ok(())
}
