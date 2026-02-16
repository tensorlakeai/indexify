use axum::{
    Json,
    extract::{Path, State},
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    data_model::{
        self,
        ContainerPool,
        ContainerPoolBuilder,
        ContainerPoolId,
        ContainerPoolKey,
        SandboxBuilder,
        SandboxId,
        SandboxPendingReason,
        SandboxStatus,
    },
    http_objects::{ContainerResources, ContainerResourcesInfo, IndexifyAPIError},
    routes::routes_state::RouteState,
    state_store::requests::{
        CreateContainerPoolRequest as StateCreateContainerPoolRequest,
        CreateSandboxRequest as StateCreateSandboxRequest,
        DeleteContainerPoolRequest as StateDeleteContainerPoolRequest,
        RequestPayload,
        StateMachineUpdateRequest,
        UpdateContainerPoolRequest as StateUpdateContainerPoolRequest,
    },
    utils::get_epoch_time_in_ns,
};

/// Request to create a new sandbox pool
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateSandboxPoolRequest {
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
    /// Maximum containers allowed
    #[serde(default)]
    pub max_containers: Option<u32>,
    /// Number of warm containers to maintain
    #[serde(default)]
    pub warm_containers: Option<u32>,
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
    /// Maximum containers allowed
    #[serde(default)]
    pub max_containers: Option<u32>,
    /// Number of warm containers to maintain
    #[serde(default)]
    pub warm_containers: Option<u32>,
}

/// Response after creating a sandbox pool
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateSandboxPoolResponse {
    pub pool_id: String,
    pub namespace: String,
}

/// Response after creating a sandbox under a pool
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreatePoolSandboxResponse {
    pub sandbox_id: String,
    pub status: super::sandboxes::SandboxStatusInfo,
}

/// Sandbox pool information returned by list/get operations
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SandboxPoolInfo {
    pub pool_id: String,
    pub namespace: String,
    pub image: String,
    pub resources: ContainerResourcesInfo,
    pub min_containers: Option<u32>,
    pub max_containers: Option<u32>,
    pub warm_containers: Option<u32>,
    pub timeout_secs: u64,
    pub created_at: u64,
}

impl SandboxPoolInfo {
    pub fn from_pool(pool: &ContainerPool) -> Self {
        Self {
            pool_id: pool.id.get().to_string(),
            namespace: pool.namespace.clone(),
            image: pool.image.clone(),
            resources: ContainerResourcesInfo {
                cpus: pool.resources.cpu_ms_per_sec as f64 / 1000.0,
                memory_mb: pool.resources.memory_mb,
                ephemeral_disk_mb: pool.resources.ephemeral_disk_mb,
            },
            min_containers: pool.min_containers,
            max_containers: pool.max_containers,
            warm_containers: pool.buffer_containers,
            timeout_secs: pool.timeout_secs,
            created_at: pool.created_at,
        }
    }
}

/// Container information returned in sandbox pool details
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ContainerInfo {
    pub id: String,
    pub state: String,
    pub sandbox_id: Option<String>,
    pub executor_id: String,
}

/// Detailed sandbox pool information returned by the single-pool GET
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SandboxPoolDetail {
    #[serde(flatten)]
    pub pool: SandboxPoolInfo,
    pub containers: Vec<ContainerInfo>,
}

/// List sandbox pools response
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListSandboxPoolsResponse {
    pub pools: Vec<SandboxPoolInfo>,
}

/// Build a ContainerPool from request parameters
#[allow(clippy::too_many_arguments)]
fn build_pool(
    pool_id: ContainerPoolId,
    namespace: String,
    image: String,
    resources: &ContainerResources,
    entrypoint: Option<Vec<String>>,
    secret_names: Vec<String>,
    timeout_secs: u64,
    max_containers: Option<u32>,
    buffer_containers: Option<u32>,
    created_at: u64,
) -> Result<ContainerPool, IndexifyAPIError> {
    ContainerPoolBuilder::default()
        .id(pool_id)
        .namespace(namespace)
        .image(image)
        .resources(data_model::ContainerResources {
            cpu_ms_per_sec: (resources.cpus * 1000.0).ceil() as u32,
            memory_mb: resources.memory_mb,
            ephemeral_disk_mb: resources.ephemeral_disk_mb,
            gpu: resources
                .gpu_configs
                .first()
                .map(|g| data_model::GPUResources {
                    count: g.count,
                    model: g.model.clone(),
                }),
        })
        .entrypoint(entrypoint)
        .secret_names(secret_names)
        .timeout_secs(timeout_secs)
        .max_containers(max_containers)
        .buffer_containers(buffer_containers)
        .created_at(created_at)
        .build()
        .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()))
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
    let pool_id = ContainerPoolId::for_pool();

    let pool = build_pool(
        pool_id.clone(),
        namespace.clone(),
        request.image,
        &request.resources,
        request.entrypoint,
        request.secret_names,
        request.timeout_secs,
        request.max_containers,
        request.warm_containers,
        0, // created_at will be set by state machine
    )?;

    pool.validate()
        .map_err(|e| IndexifyAPIError::bad_request(&e.to_string()))?;

    let state_request = StateMachineUpdateRequest {
        payload: RequestPayload::CreateContainerPool(StateCreateContainerPoolRequest { pool }),
    };

    state
        .indexify_state
        .write(state_request)
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    Ok(Json(CreateSandboxPoolResponse {
        pool_id: pool_id.get().to_string(),
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
        .sandbox_pools
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
        (status = 200, description = "Sandbox pool details", body = SandboxPoolDetail),
        (status = 404, description = "Pool not found"),
        (status = 500, description = "Internal server error")
    ),
)]
pub async fn get_sandbox_pool(
    Path((namespace, pool_id)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<Json<SandboxPoolDetail>, IndexifyAPIError> {
    let pool_key = ContainerPoolKey::new(&namespace, &ContainerPoolId::new(&pool_id));

    let scheduler = state.indexify_state.container_scheduler.read().await;
    let pool = scheduler
        .sandbox_pools
        .get(&pool_key)
        .ok_or_else(|| IndexifyAPIError::not_found("Sandbox pool not found"))?;

    let pool_info = SandboxPoolInfo::from_pool(pool);

    let containers: Vec<ContainerInfo> = scheduler
        .containers_by_pool
        .get(&pool_key)
        .map(|container_ids| {
            container_ids
                .iter()
                .filter_map(|cid| {
                    let meta = scheduler.function_containers.get(cid)?;
                    let c = &meta.function_container;
                    Some(ContainerInfo {
                        id: c.id.get().to_string(),
                        state: c.state.to_string(),
                        sandbox_id: c.sandbox_id.as_ref().map(|s| s.get().to_string()),
                        executor_id: meta.executor_id.get().to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(Json(SandboxPoolDetail {
        pool: pool_info,
        containers,
    }))
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

    // Check if pool exists and get created_at timestamp
    let created_at = {
        let scheduler = state.indexify_state.container_scheduler.read().await;
        scheduler
            .sandbox_pools
            .get(&pool_key)
            .ok_or_else(|| IndexifyAPIError::not_found("Sandbox pool not found"))?
            .created_at
    };

    let pool = build_pool(
        pool_id_obj,
        namespace.clone(),
        request.image,
        &request.resources,
        request.entrypoint,
        request.secret_names,
        request.timeout_secs,
        request.max_containers,
        request.warm_containers,
        created_at,
    )?;

    pool.validate()
        .map_err(|e| IndexifyAPIError::bad_request(&e.to_string()))?;

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

/// Create a sandbox under a pool, using the pool's configuration
#[utoipa::path(
    post,
    path = "/v1/namespaces/{namespace}/sandbox-pools/{pool_id}/sandboxes",
    tag = "sandbox-pools",
    responses(
        (status = 200, description = "Sandbox created from pool", body = CreatePoolSandboxResponse),
        (status = 404, description = "Pool not found"),
        (status = 500, description = "Internal server error")
    ),
)]
pub async fn create_pool_sandbox(
    Path((namespace, pool_id)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<Json<CreatePoolSandboxResponse>, IndexifyAPIError> {
    let pool_id_obj = ContainerPoolId::new(&pool_id);
    let pool_key = ContainerPoolKey::new(&namespace, &pool_id_obj);

    // Look up the pool to get its configuration
    let scheduler = state.indexify_state.container_scheduler.read().await;
    let pool = scheduler
        .sandbox_pools
        .get(&pool_key)
        .ok_or_else(|| IndexifyAPIError::not_found("Sandbox pool not found"))?;

    // Build a sandbox using the pool's configuration
    let sandbox_id = SandboxId::default();
    let sandbox = SandboxBuilder::default()
        .id(sandbox_id.clone())
        .namespace(namespace.clone())
        .image(pool.image.clone())
        .status(SandboxStatus::Pending {
            reason: SandboxPendingReason::Scheduling,
        })
        .creation_time_ns(get_epoch_time_in_ns())
        .resources(pool.resources.clone())
        .secret_names(pool.secret_names.clone())
        .timeout_secs(pool.timeout_secs)
        .entrypoint(pool.entrypoint.clone())
        .network_policy(pool.network_policy.clone())
        .pool_id(Some(pool_id_obj))
        .build()
        .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()))?;

    // Drop the scheduler lock before writing
    drop(scheduler);

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

    Ok(Json(CreatePoolSandboxResponse {
        sandbox_id: sandbox_id.get().to_string(),
        status: super::sandboxes::SandboxStatusInfo::Pending {
            reason: "scheduling".to_string(),
        },
    }))
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
    {
        let scheduler = state.indexify_state.container_scheduler.read().await;
        if !scheduler.sandbox_pools.contains_key(&pool_key) {
            return Err(IndexifyAPIError::not_found("Sandbox pool not found"));
        }
    }

    // Check if any sandboxes are using this pool
    let active_sandboxes = {
        let in_memory = state.indexify_state.in_memory_state.read().await;
        in_memory.sandboxes.values().any(|s| {
            s.pool_id.as_ref() == Some(&pool_id_obj) &&
                s.namespace == namespace &&
                s.status != data_model::SandboxStatus::Terminated
        })
    };
    if active_sandboxes {
        return Err(IndexifyAPIError::conflict(&format!(
            "Cannot delete pool '{}': active sandbox(es) are using it",
            pool_id
        )));
    }

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
