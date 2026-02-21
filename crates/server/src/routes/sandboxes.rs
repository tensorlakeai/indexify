use axum::{
    Json,
    extract::{Path, State},
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    data_model::{
        self,
        Sandbox,
        SandboxBuilder,
        SandboxId,
        SandboxStatus,
        SnapshotId,
        SnapshotStatus,
    },
    http_objects::{ContainerResources, ContainerResourcesInfo, IndexifyAPIError},
    routes::routes_state::RouteState,
    state_store::requests::{
        CreateSandboxRequest as StateCreateSandboxRequest,
        RequestPayload,
        StateMachineUpdateRequest,
        TerminateSandboxRequest,
    },
    utils::get_epoch_time_in_ns,
};

/// Network access control settings for sandbox creation
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
pub struct NetworkAccessControl {
    /// If false, all outbound internet access is blocked by default.
    /// If true (default), outbound is allowed unless explicitly denied.
    #[serde(default = "default_true")]
    pub allow_internet_access: bool,
    /// List of allowed destination IPs/CIDRs (e.g., "8.8.8.8", "10.0.0.0/8").
    /// Allow rules take precedence over deny rules.
    #[serde(default)]
    pub allow_out: Vec<String>,
    /// List of denied destination IPs/CIDRs (e.g., "192.168.1.100").
    #[serde(default)]
    pub deny_out: Vec<String>,
}

fn default_true() -> bool {
    true
}

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
    /// Network access control settings (optional).
    #[serde(default)]
    pub network: Option<NetworkAccessControl>,
    /// Snapshot ID to restore from. When set, the sandbox will be created
    /// from the snapshot's filesystem state. Image, resources, entrypoint,
    /// and secrets are inherited from the snapshot unless explicitly
    /// overridden.
    #[serde(default)]
    pub snapshot_id: Option<String>,
}

/// Simplified sandbox status for the API.
///
/// The internal `SandboxStatus::Pending` variant carries a `reason` field
/// that is exposed separately as `pending_reason` in the response structs.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApiSandboxStatus {
    Pending,
    Running,
    Snapshotting,
    Terminated,
}

impl From<&data_model::SandboxStatus> for ApiSandboxStatus {
    fn from(status: &data_model::SandboxStatus) -> Self {
        match status {
            data_model::SandboxStatus::Pending { .. } => Self::Pending,
            data_model::SandboxStatus::Running => Self::Running,
            data_model::SandboxStatus::Snapshotting { .. } => Self::Snapshotting,
            data_model::SandboxStatus::Terminated => Self::Terminated,
        }
    }
}

/// Response after creating a sandbox
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateSandboxResponse {
    pub sandbox_id: String,
    pub status: ApiSandboxStatus,
    /// Reason why the sandbox is pending (only set when status is "pending").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_reason: Option<String>,
}

/// Sandbox information returned by list/get operations
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SandboxInfo {
    pub id: String,
    pub namespace: String,
    pub image: String,
    pub status: ApiSandboxStatus,
    /// Reason why the sandbox is pending (only set when status is "pending").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_reason: Option<String>,
    pub outcome: Option<String>,
    pub created_at: u64,
    pub container_id: Option<String>,
    pub executor_id: Option<String>,
    pub resources: ContainerResourcesInfo,
    pub timeout_secs: u64,
    /// Full URL to access the sandbox daemon API via sandbox-proxy.
    pub sandbox_url: Option<String>,
    /// Pool ID if this sandbox is associated with a container pool.
    pub pool_id: Option<String>,
}

/// Default sandbox-proxy port (for production with nip.io).
const SANDBOX_PROXY_PORT: u16 = 9443;

/// Check if a domain is local (127.0.0.1 or localhost).
fn is_local_domain(domain: &str) -> bool {
    domain.contains("127.0.0.1") || domain.contains("localhost")
}

impl SandboxInfo {
    pub fn from_sandbox(
        sandbox: &Sandbox,
        sandbox_proxy_domain: Option<&str>,
        scheme: &str,
        dataplane_api_address: Option<&str>,
    ) -> Self {
        let is_local = sandbox_proxy_domain.map(is_local_domain).unwrap_or(false);

        let sandbox_url = if is_local {
            // Local dev: use dataplane address directly (UI will add Tensorlake-Sandbox-Id
            // header)
            dataplane_api_address.map(|addr| format!("http://{}", addr))
        } else {
            // Production: use sandbox-proxy URL
            sandbox_proxy_domain.map(|domain| {
                if domain.ends_with(".nip.io") || domain.ends_with(".sslip.io") {
                    format!(
                        "{}://{}.{}:{}",
                        scheme,
                        sandbox.id.get(),
                        domain,
                        SANDBOX_PROXY_PORT
                    )
                } else {
                    format!("{}://{}.{}", scheme, sandbox.id.get(), domain)
                }
            })
        };

        let pending_reason = match &sandbox.status {
            data_model::SandboxStatus::Pending { reason } => Some(reason.to_string()),
            _ => None,
        };

        Self {
            id: sandbox.id.get().to_string(),
            namespace: sandbox.namespace.clone(),
            image: sandbox.image.clone(),
            status: ApiSandboxStatus::from(&sandbox.status),
            pending_reason,
            outcome: sandbox.outcome.as_ref().map(|o| o.to_string()),
            created_at: (sandbox.creation_time_ns / 1_000_000) as u64, // Convert ns to ms
            container_id: sandbox.container_id.as_ref().map(|c| c.get().to_string()),
            executor_id: sandbox.executor_id.as_ref().map(|e| e.get().to_string()),
            resources: ContainerResourcesInfo {
                cpus: sandbox.resources.cpu_ms_per_sec as f64 / 1000.0,
                memory_mb: sandbox.resources.memory_mb,
                ephemeral_disk_mb: sandbox.resources.ephemeral_disk_mb,
            },
            timeout_secs: sandbox.timeout_secs,
            sandbox_url,
            pool_id: sandbox.pool_id.as_ref().map(|p| p.get().to_string()),
        }
    }
}

/// List sandboxes response
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListSandboxesResponse {
    pub sandboxes: Vec<SandboxInfo>,
}

/// Create a new sandbox in a namespace
#[utoipa::path(
    post,
    path = "/v1/namespaces/{namespace}/sandboxes",
    tag = "sandboxes",
    request_body = CreateSandboxRequest,
    responses(
        (status = 200, description = "Sandbox created", body = CreateSandboxResponse),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error")
    ),
)]
pub async fn create_sandbox(
    Path(namespace): Path<String>,
    State(state): State<RouteState>,
    Json(request): Json<CreateSandboxRequest>,
) -> Result<Json<CreateSandboxResponse>, IndexifyAPIError> {
    // If snapshot_id is set, look up the snapshot and inherit defaults
    let snapshot = if let Some(ref snap_id) = request.snapshot_id {
        let reader = state.indexify_state.reader();
        let s = reader
            .get_snapshot(&namespace, snap_id)
            .await
            .map_err(IndexifyAPIError::internal_error)?
            .ok_or_else(|| IndexifyAPIError::not_found("Snapshot not found"))?;
        if s.status != SnapshotStatus::Completed {
            return Err(IndexifyAPIError::bad_request("Snapshot is not completed"));
        }
        Some(s)
    } else {
        None
    };

    // Apply config defaults for image and timeout, inheriting from snapshot
    let image = request.image.unwrap_or_else(|| {
        snapshot
            .as_ref()
            .map(|s| s.base_image.clone())
            .unwrap_or_else(|| state.config.default_sandbox_image.clone())
    });
    let timeout_secs = request
        .timeout_secs
        .unwrap_or(state.config.default_sandbox_timeout_secs);

    let sandbox_id = SandboxId::default();

    // Use snapshot resources as defaults if available
    let resources = if let Some(ref snap) = snapshot {
        data_model::ContainerResources {
            cpu_ms_per_sec: if request.resources.cpus > 0.0 {
                (request.resources.cpus * 1000.0).ceil() as u32
            } else {
                snap.resources.cpu_ms_per_sec
            },
            memory_mb: if request.resources.memory_mb > 0 {
                request.resources.memory_mb
            } else {
                snap.resources.memory_mb
            },
            ephemeral_disk_mb: if request.resources.ephemeral_disk_mb > 0 {
                request.resources.ephemeral_disk_mb
            } else {
                snap.resources.ephemeral_disk_mb
            },
            gpu: request
                .resources
                .gpu_configs
                .first()
                .map(|g| data_model::GPUResources {
                    count: g.count,
                    model: g.model.clone(),
                })
                .or(snap.resources.gpu.clone()),
        }
    } else {
        data_model::ContainerResources {
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
        }
    };

    let entrypoint = request
        .entrypoint
        .or_else(|| snapshot.as_ref().and_then(|s| s.entrypoint.clone()));

    let secret_names = if request.secret_names.is_empty() {
        snapshot
            .as_ref()
            .map(|s| s.secret_names.clone())
            .unwrap_or_default()
    } else {
        request.secret_names.clone()
    };

    let snapshot_id_field = request.snapshot_id.map(SnapshotId::from);
    let sandbox = SandboxBuilder::default()
        .id(sandbox_id.clone())
        .namespace(namespace.clone())
        .image(image)
        .status(SandboxStatus::Pending {
            reason: data_model::SandboxPendingReason::Scheduling,
        })
        .creation_time_ns(get_epoch_time_in_ns())
        .resources(resources)
        .secret_names(secret_names)
        .timeout_secs(timeout_secs)
        .entrypoint(entrypoint)
        .snapshot_id(snapshot_id_field)
        .network_policy(request.network.map(|n| data_model::NetworkPolicy {
            allow_internet_access: n.allow_internet_access,
            allow_out: n.allow_out,
            deny_out: n.deny_out,
        }))
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
        status: ApiSandboxStatus::Pending,
        pending_reason: Some("scheduling".to_string()),
    }))
}

/// List all sandboxes in a namespace
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/sandboxes",
    tag = "sandboxes",
    responses(
        (status = 200, description = "List of sandboxes", body = ListSandboxesResponse),
        (status = 500, description = "Internal server error")
    ),
)]
pub async fn list_sandboxes(
    Path(namespace): Path<String>,
    State(state): State<RouteState>,
) -> Result<Json<ListSandboxesResponse>, IndexifyAPIError> {
    // Read sandboxes from database (includes terminated sandboxes)
    let reader = state.indexify_state.reader();
    let sandboxes = reader
        .list_sandboxes(&namespace)
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    let sandbox_domain = state.config.sandbox_proxy_domain.as_deref();
    let scheme = &state.config.sandbox_proxy_scheme;

    // Get container scheduler to look up executor proxy addresses
    let container_scheduler = state.indexify_state.container_scheduler.read().await;

    let sandbox_infos: Vec<SandboxInfo> = sandboxes
        .iter()
        .map(|s| {
            // Look up dataplane proxy address from the executor
            let dataplane_api_address = s
                .executor_id
                .as_ref()
                .and_then(|eid| container_scheduler.executors.get(eid))
                .and_then(|executor| executor.proxy_address.as_deref());

            SandboxInfo::from_sandbox(s, sandbox_domain, scheme, dataplane_api_address)
        })
        .collect();

    Ok(Json(ListSandboxesResponse {
        sandboxes: sandbox_infos,
    }))
}

/// Get a specific sandbox
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/sandboxes/{sandbox_id}",
    tag = "sandboxes",
    responses(
        (status = 200, description = "Sandbox details", body = SandboxInfo),
        (status = 404, description = "Sandbox not found"),
        (status = 500, description = "Internal server error")
    ),
)]
pub async fn get_sandbox(
    Path((namespace, sandbox_id)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<Json<SandboxInfo>, IndexifyAPIError> {
    let reader = state.indexify_state.reader();
    let sandbox = reader
        .get_sandbox(&namespace, &sandbox_id)
        .await
        .map_err(IndexifyAPIError::internal_error)?
        .ok_or_else(|| IndexifyAPIError::not_found("Sandbox not found"))?;

    let sandbox_proxy_domain = state.config.sandbox_proxy_domain.as_deref();
    let scheme = &state.config.sandbox_proxy_scheme;

    // Look up dataplane proxy address from the executor
    let container_scheduler = state.indexify_state.container_scheduler.read().await;
    let dataplane_api_address = sandbox
        .executor_id
        .as_ref()
        .and_then(|eid| container_scheduler.executors.get(eid))
        .and_then(|executor| executor.proxy_address.as_deref());

    Ok(Json(SandboxInfo::from_sandbox(
        &sandbox,
        sandbox_proxy_domain,
        scheme,
        dataplane_api_address,
    )))
}

/// Delete (terminate) a sandbox
#[utoipa::path(
    delete,
    path = "/v1/namespaces/{namespace}/sandboxes/{sandbox_id}",
    tag = "sandboxes",
    responses(
        (status = 200, description = "Sandbox terminated"),
        (status = 404, description = "Sandbox not found"),
        (status = 500, description = "Internal server error")
    ),
)]
pub async fn delete_sandbox(
    Path((namespace, sandbox_id)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<(), IndexifyAPIError> {
    // Check if sandbox exists and is not already terminated
    let reader = state.indexify_state.reader();
    let sandbox = reader
        .get_sandbox(&namespace, &sandbox_id)
        .await
        .map_err(IndexifyAPIError::internal_error)?
        .ok_or_else(|| IndexifyAPIError::not_found("Sandbox not found"))?;

    // If already terminated, return success (idempotent)
    if sandbox.status == data_model::SandboxStatus::Terminated {
        return Ok(());
    }

    // Write TerminateSandboxRequest to state store
    let request = StateMachineUpdateRequest {
        payload: RequestPayload::TerminateSandbox(TerminateSandboxRequest {
            namespace: namespace.clone(),
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
