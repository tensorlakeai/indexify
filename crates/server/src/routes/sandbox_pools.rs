use axum::{
    Json,
    extract::{Path, State},
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    data_model::{self, ContainerPool, ContainerPoolBuilder, ContainerPoolId, ContainerPoolKey},
    http_objects::{ContainerResources, IndexifyAPIError},
    routes::routes_state::RouteState,
    state_store::requests::{
        CreateContainerPoolRequest as StateCreateContainerPoolRequest,
        DeleteContainerPoolRequest as StateDeleteContainerPoolRequest,
        RequestPayload,
        StateMachineUpdateRequest,
        UpdateContainerPoolRequest as StateUpdateContainerPoolRequest,
    },
};

/// Request to create a new sandbox pool
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateSandboxPoolRequest {
    /// Unique identifier for the pool within the namespace
    pub pool_id: String,
    /// Docker image for containers in this pool
    pub image: String,
    /// Resource requirements for each container
    #[serde(default)]
    pub resources: ContainerResources,
    /// Optional entrypoint command
    #[serde(default)]
    pub entrypoint: Option<Vec<String>>,
    /// Secret names to inject
    #[serde(default)]
    pub secret_names: Vec<String>,
    /// Timeout in seconds for sandboxes from this pool (0 = no timeout)
    #[serde(default)]
    pub timeout_secs: u64,
    /// Minimum containers to maintain (floor)
    #[serde(default)]
    pub min_containers: Option<u32>,
    /// Maximum containers allowed (ceiling)
    #[serde(default)]
    pub max_containers: Option<u32>,
    /// Number of warm (unclaimed) containers to maintain as buffer
    #[serde(default)]
    pub buffer_containers: Option<u32>,
}

/// Request to update an existing sandbox pool
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateSandboxPoolRequest {
    /// Docker image for containers in this pool
    pub image: String,
    /// Resource requirements for each container
    #[serde(default)]
    pub resources: ContainerResources,
    /// Optional entrypoint command
    #[serde(default)]
    pub entrypoint: Option<Vec<String>>,
    /// Secret names to inject
    #[serde(default)]
    pub secret_names: Vec<String>,
    /// Timeout in seconds for sandboxes from this pool (0 = no timeout)
    #[serde(default)]
    pub timeout_secs: u64,
    /// Minimum containers to maintain (floor)
    #[serde(default)]
    pub min_containers: Option<u32>,
    /// Maximum containers allowed (ceiling)
    #[serde(default)]
    pub max_containers: Option<u32>,
    /// Number of warm (unclaimed) containers to maintain as buffer
    #[serde(default)]
    pub buffer_containers: Option<u32>,
}

/// Response after creating a sandbox pool
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateSandboxPoolResponse {
    pub pool_id: String,
    pub namespace: String,
}

/// Sandbox pool information returned by list/get operations
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SandboxPoolInfo {
    pub pool_id: String,
    pub namespace: String,
    pub image: String,
    pub resources: SandboxPoolResourcesInfo,
    pub min_containers: Option<u32>,
    pub max_containers: Option<u32>,
    pub buffer_containers: Option<u32>,
    pub timeout_secs: u64,
    pub created_at: u64,
}

/// Resource info for sandbox pool response
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SandboxPoolResourcesInfo {
    pub cpus: f64,
    pub memory_mb: u64,
    pub ephemeral_disk_mb: u64,
}

impl SandboxPoolInfo {
    pub fn from_pool(pool: &ContainerPool) -> Self {
        Self {
            pool_id: pool.id.get().to_string(),
            namespace: pool.namespace.clone(),
            image: pool.image.clone(),
            resources: SandboxPoolResourcesInfo {
                cpus: pool.resources.cpu_ms_per_sec as f64 / 1000.0,
                memory_mb: pool.resources.memory_mb,
                ephemeral_disk_mb: pool.resources.ephemeral_disk_mb,
            },
            min_containers: pool.min_containers,
            max_containers: pool.max_containers,
            buffer_containers: pool.buffer_containers,
            timeout_secs: pool.timeout_secs,
            created_at: pool.created_at,
        }
    }
}

/// List sandbox pools response
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListSandboxPoolsResponse {
    pub pools: Vec<SandboxPoolInfo>,
}

/// Create a new sandbox pool in a namespace
#[utoipa::path(
    post,
    path = "/v1/namespaces/{namespace}/sandbox-pools",
    tag = "sandbox-pools",
    request_body = CreateSandboxPoolRequest,
    responses(
        (status = 200, description = "Sandbox pool created", body = CreateSandboxPoolResponse),
        (status = 400, description = "Bad request"),
        (status = 409, description = "Pool already exists"),
        (status = 500, description = "Internal server error")
    ),
)]
pub async fn create_sandbox_pool(
    Path(namespace): Path<String>,
    State(state): State<RouteState>,
    Json(request): Json<CreateSandboxPoolRequest>,
) -> Result<Json<CreateSandboxPoolResponse>, IndexifyAPIError> {
    let pool_id = ContainerPoolId::new(&request.pool_id);
    let pool_key = ContainerPoolKey::new(&namespace, &pool_id);

    // Check if pool already exists
    {
        let scheduler = state.indexify_state.container_scheduler.read().await;
        if scheduler.container_pools.contains_key(&pool_key) {
            return Err(IndexifyAPIError::conflict(&format!(
                "Sandbox pool '{}' already exists in namespace '{}'",
                request.pool_id, namespace
            )));
        }
    }
    let pool = ContainerPoolBuilder::default()
        .id(pool_id)
        .namespace(namespace.clone())
        .image(request.image)
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
        .entrypoint(request.entrypoint)
        .secret_names(request.secret_names)
        .timeout_secs(request.timeout_secs)
        .min_containers(request.min_containers)
        .max_containers(request.max_containers)
        .buffer_containers(request.buffer_containers)
        .build()
        .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()))?;

    // Validate pool configuration
    pool.validate()
        .map_err(|e| IndexifyAPIError::bad_request(&e.to_string()))?;

    // Write to state store
    let state_request = StateMachineUpdateRequest {
        payload: RequestPayload::CreateContainerPool(StateCreateContainerPoolRequest { pool }),
    };

    state
        .indexify_state
        .write(state_request)
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    Ok(Json(CreateSandboxPoolResponse {
        pool_id: request.pool_id,
        namespace,
    }))
}

/// List all sandbox pools in a namespace
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/sandbox-pools",
    tag = "sandbox-pools",
    responses(
        (status = 200, description = "List of sandbox pools", body = ListSandboxPoolsResponse),
        (status = 500, description = "Internal server error")
    ),
)]
pub async fn list_sandbox_pools(
    Path(namespace): Path<String>,
    State(state): State<RouteState>,
) -> Result<Json<ListSandboxPoolsResponse>, IndexifyAPIError> {
    let scheduler = state.indexify_state.container_scheduler.read().await;

    let pools: Vec<SandboxPoolInfo> = scheduler
        .container_pools
        .iter()
        .filter(|(key, _)| key.namespace == namespace)
        .map(|(_, pool)| SandboxPoolInfo::from_pool(pool))
        .collect();

    Ok(Json(ListSandboxPoolsResponse { pools }))
}

/// Get a specific sandbox pool
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/sandbox-pools/{pool_id}",
    tag = "sandbox-pools",
    responses(
        (status = 200, description = "Sandbox pool details", body = SandboxPoolInfo),
        (status = 404, description = "Pool not found"),
        (status = 500, description = "Internal server error")
    ),
)]
pub async fn get_sandbox_pool(
    Path((namespace, pool_id)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<Json<SandboxPoolInfo>, IndexifyAPIError> {
    let pool_key = ContainerPoolKey::new(&namespace, &ContainerPoolId::new(&pool_id));

    let scheduler = state.indexify_state.container_scheduler.read().await;
    let pool = scheduler
        .container_pools
        .get(&pool_key)
        .ok_or_else(|| IndexifyAPIError::not_found("Sandbox pool not found"))?;

    Ok(Json(SandboxPoolInfo::from_pool(pool)))
}

/// Update a sandbox pool
#[utoipa::path(
    put,
    path = "/v1/namespaces/{namespace}/sandbox-pools/{pool_id}",
    tag = "sandbox-pools",
    request_body = UpdateSandboxPoolRequest,
    responses(
        (status = 200, description = "Sandbox pool updated", body = SandboxPoolInfo),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Pool not found"),
        (status = 500, description = "Internal server error")
    ),
)]
pub async fn update_sandbox_pool(
    Path((namespace, pool_id)): Path<(String, String)>,
    State(state): State<RouteState>,
    Json(request): Json<UpdateSandboxPoolRequest>,
) -> Result<Json<SandboxPoolInfo>, IndexifyAPIError> {
    let pool_id_obj = ContainerPoolId::new(&pool_id);
    let pool_key = ContainerPoolKey::new(&namespace, &pool_id_obj);

    // Check if pool exists
    let scheduler = state.indexify_state.container_scheduler.read().await;
    let existing_pool = scheduler
        .container_pools
        .get(&pool_key)
        .ok_or_else(|| IndexifyAPIError::not_found("Sandbox pool not found"))?;
    let created_at = existing_pool.created_at;
    drop(scheduler);

    let pool = ContainerPoolBuilder::default()
        .id(pool_id_obj)
        .namespace(namespace.clone())
        .image(request.image)
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
        .entrypoint(request.entrypoint)
        .secret_names(request.secret_names)
        .timeout_secs(request.timeout_secs)
        .min_containers(request.min_containers)
        .max_containers(request.max_containers)
        .buffer_containers(request.buffer_containers)
        .created_at(created_at)
        .build()
        .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()))?;

    // Validate pool configuration
    pool.validate()
        .map_err(|e| IndexifyAPIError::bad_request(&e.to_string()))?;

    // Write to state store
    let state_request = StateMachineUpdateRequest {
        payload: RequestPayload::UpdateContainerPool(StateUpdateContainerPoolRequest {
            pool: pool.clone(),
        }),
    };

    state
        .indexify_state
        .write(state_request)
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    Ok(Json(SandboxPoolInfo::from_pool(&pool)))
}

/// Delete a sandbox pool
#[utoipa::path(
    delete,
    path = "/v1/namespaces/{namespace}/sandbox-pools/{pool_id}",
    tag = "sandbox-pools",
    responses(
        (status = 200, description = "Sandbox pool deleted"),
        (status = 404, description = "Pool not found"),
        (status = 409, description = "Pool has active sandboxes"),
        (status = 500, description = "Internal server error")
    ),
)]
pub async fn delete_sandbox_pool(
    Path((namespace, pool_id)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<(), IndexifyAPIError> {
    let pool_id_obj = ContainerPoolId::new(&pool_id);
    let pool_key = ContainerPoolKey::new(&namespace, &pool_id_obj);

    // Check if pool exists
    let scheduler = state.indexify_state.container_scheduler.read().await;
    if !scheduler.container_pools.contains_key(&pool_key) {
        return Err(IndexifyAPIError::not_found("Sandbox pool not found"));
    }
    drop(scheduler);

    // Check if any sandboxes are using this pool
    let in_memory = state.indexify_state.in_memory_state.read().await;
    let active_sandboxes: Vec<_> = in_memory
        .sandboxes
        .values()
        .filter(|s| {
            s.pool_id.as_ref() == Some(&pool_id_obj) &&
                s.namespace == namespace &&
                s.status != data_model::SandboxStatus::Terminated
        })
        .collect();

    if !active_sandboxes.is_empty() {
        return Err(IndexifyAPIError::conflict(&format!(
            "Cannot delete pool '{}': {} active sandbox(es) are using it",
            pool_id,
            active_sandboxes.len()
        )));
    }
    drop(in_memory);

    // Write TombstoneContainerPool to state store - this emits a state change
    // which will terminate containers, then issue the actual delete
    let request = StateMachineUpdateRequest {
        payload: RequestPayload::TombstoneContainerPool(StateDeleteContainerPoolRequest {
            namespace: namespace.clone(),
            pool_id: pool_id_obj,
        }),
    };

    state
        .indexify_state
        .write(request)
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    Ok(())
}
