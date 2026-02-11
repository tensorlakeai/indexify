use std::collections::HashMap;

use anyhow::Result;
use axum::{
    Json,
    Router,
    body::Body,
    extract::{Path, State},
    http::Response,
    response::{Html, IntoResponse},
    routing::{get, post, put},
};
use hyper::StatusCode;
use serde::{Deserialize, Serialize};
use tracing::{error, info};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    data_model::{
        Allocation as DataModelAllocation,
        AllocationId,
        ContainerId,
        ExecutorId,
        SandboxKey,
    },
    http_objects::{
        Allocation,
        ApplicationVersion,
        CacheKey,
        CreateNamespace,
        ExecutorCatalog,
        ExecutorMetadata,
        ExecutorsAllocationsResponse,
        FunctionRunOutcome,
        HealthzChecks,
        HealthzResponse,
        HealthzStatus,
        IndexifyAPIError,
        Namespace,
        NamespaceList,
        PendingResourcesResponse,
        ResourceProfile,
        ResourceProfileEntry,
        ResourceProfileHistogram,
        StateChangesResponse,
        UnallocatedFunctionRuns,
        from_data_model_executor_metadata,
    },
    http_objects_v1::{self, Application, ApplicationMetadata, ApplicationState, CodeDigest},
    indexify_ui::Assets as UiAssets,
    routes::{common::validate_and_submit_application, routes_state::RouteState},
    state_store::requests::{
        CreateOrUpdateApplicationRequest,
        NamespaceRequest,
        RequestPayload,
        StateMachineUpdateRequest,
    },
};

#[derive(OpenApi)]
#[openapi(
        paths(
            create_namespace,
            namespaces,
            list_executors,
            list_allocations,
            list_unallocated_function_runs,
            list_unprocessed_state_changes,
            list_executor_catalog,
            get_application_by_version,
            create_or_update_application_with_metadata,
            healthz_handler,
            get_pending_resources,
        ),
        components(
            schemas(
                CreateNamespace,
                NamespaceList,
                IndexifyAPIError,
                Namespace,
		        CacheKey,
                ExecutorMetadata,
                FunctionRunOutcome,
                Allocation,
                ExecutorsAllocationsResponse,
                UnallocatedFunctionRuns,
                StateChangesResponse,
                ExecutorCatalog,
                HealthzChecks,
                HealthzResponse,
                Application,
                ApplicationMetadata,
                CodeDigest,
                ResourceProfile,
                ResourceProfileEntry,
                ResourceProfileHistogram,
                PendingResourcesResponse,
            )
        ),
        tags(
            (name = "indexify", description = "Indexify API")
        )
    )]
pub struct ApiDoc;

pub fn configure_internal_routes(route_state: RouteState) -> Router {
    Router::new()
        .route(
            "/namespaces",
            get(namespaces).with_state(route_state.clone()),
        )
        .route(
            "/namespaces",
            post(create_namespace).with_state(route_state.clone()),
        )
        .route(
            "/internal/executors",
            get(list_executors).with_state(route_state.clone()),
        )
        .route(
            "/internal/allocations",
            get(list_allocations).with_state(route_state.clone()),
        )
        .route(
            "/internal/unallocated_function_runs",
            get(list_unallocated_function_runs).with_state(route_state.clone()),
        )
        .route(
            "/internal/unprocessed_state_changes",
            get(list_unprocessed_state_changes).with_state(route_state.clone()),
        )
        .route(
            "/internal/executor_catalog",
            get(list_executor_catalog).with_state(route_state.clone()),
        )
        .route(
            "/internal/v1/namespaces/{namespace}/applications",
            post(create_or_update_application_with_metadata).with_state(route_state.clone()),
        )
        .route(
            "/internal/namespaces/{namespace}/applications/{application}/state",
            put(change_application_state).with_state(route_state.clone()),
        )
        .route(
            "/internal/namespaces/{namespace}/applications/{application}/versions/{version}",
            get(get_application_by_version).with_state(route_state.clone()),
        )
        .route(
            "/internal/v1/namespaces/{namespace}/sandboxes/{sandbox_id}",
            get(get_sandbox_by_id).with_state(route_state.clone()),
        )
        .route(
            "/internal/pending_resources",
            get(get_pending_resources).with_state(route_state.clone()),
        )
}

pub fn configure_helper_router(route_state: RouteState) -> Router {
    Router::new()
        .merge(
            SwaggerUi::new("/docs/internal/swagger")
                .url("/docs/internal/openapi.json", ApiDoc::openapi()),
        )
        .route("/", get(index))
        .route(
            "/healthz",
            get(healthz_handler).with_state(route_state.clone()),
        )
        .route("/ui", get(ui_index_handler))
        .route("/ui/{*rest}", get(ui_handler))
}

async fn index() -> impl IntoResponse {
    Html(include_str!("./index.html"))
}

#[axum::debug_handler]
async fn ui_index_handler() -> impl IntoResponse {
    match UiAssets::get("index.html") {
        Some(content) => (
            StatusCode::OK,
            [(hyper::header::CONTENT_TYPE, content.metadata.mimetype())],
            content.data,
        )
            .into_response(),
        None => {
            error!("UI assets not found: index.html - UI may not be built");
            (
                StatusCode::NOT_FOUND,
                "UI assets not found. Please build the UI first.",
            )
                .into_response()
        }
    }
}

#[axum::debug_handler]
async fn ui_handler(Path(url): Path<String>) -> impl IntoResponse {
    let path = url.trim_start_matches('/');
    let content = UiAssets::get(path).or_else(|| UiAssets::get("index.html"));
    match content {
        Some(content) => (
            StatusCode::OK,
            [(hyper::header::CONTENT_TYPE, content.metadata.mimetype())],
            content.data,
        )
            .into_response(),
        None => {
            error!(path = %path, "UI asset not found");
            (
                StatusCode::NOT_FOUND,
                "UI assets not found. Please build the UI first.",
            )
                .into_response()
        }
    }
}

/// Create a new namespace
#[utoipa::path(
    post,
    path = "/namespaces",
    request_body = CreateNamespace,
    tag = "operations",
    responses(
        (status = 200, description = "Namespace created successfully"),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to create namespace")
    ),
)]
async fn create_namespace(
    State(state): State<RouteState>,
    Json(namespace): Json<CreateNamespace>,
) -> Result<(), IndexifyAPIError> {
    if let Some(blob_storage_bucket) = &namespace.blob_storage_bucket {
        let Some(_blob_storage_region) = &namespace.blob_storage_region else {
            return Err(IndexifyAPIError::bad_request(
                "blob storage region is required",
            ));
        };
        if let Err(e) = state.blob_storage.create_new_blob_store(
            &namespace.name,
            blob_storage_bucket,
            namespace.blob_storage_region.clone(),
        ) {
            error!("failed to create blob storage bucket: {:?}", e);
            return Err(IndexifyAPIError::internal_error(e));
        }
    }
    let req = StateMachineUpdateRequest {
        payload: RequestPayload::CreateNameSpace(NamespaceRequest {
            name: namespace.name.clone(),
            blob_storage_bucket: namespace.blob_storage_bucket,
            blob_storage_region: namespace.blob_storage_region,
        }),
    };
    state
        .indexify_state
        .write(req)
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    info!("namespace created: {:?}", namespace.name);
    Ok(())
}

/// List all namespaces
#[utoipa::path(
    get,
    path = "/namespaces",
    tag = "operations",
    responses(
        (status = 200, description = "List all namespaces", body = NamespaceList),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to list namespace")
    ),
)]
async fn namespaces(
    State(state): State<RouteState>,
) -> Result<Json<NamespaceList>, IndexifyAPIError> {
    let reader = state.indexify_state.reader();
    let namespaces = reader
        .get_all_namespaces()
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    let namespaces: Vec<Namespace> = namespaces.into_iter().map(|n| n.into()).collect();
    Ok(Json(NamespaceList { namespaces }))
}

/// Determines if an executor is ready for teardown.
/// An executor is ready when it has no running allocations and no sandboxes.
///
/// Note: allocations_by_executor only contains non-terminal allocations.
/// Terminal allocations are removed when allocation outputs are ingested.
fn is_executor_ready_for_teardown(
    executor_id: &ExecutorId,
    allocations_by_executor: &imbl::HashMap<
        ExecutorId,
        HashMap<ContainerId, HashMap<AllocationId, Box<DataModelAllocation>>>,
    >,
    sandboxes_by_executor: &imbl::HashMap<ExecutorId, imbl::HashSet<SandboxKey>>,
) -> bool {
    // Check for running allocations (allocations_by_executor only contains
    // non-terminal ones)
    let has_running_allocations = allocations_by_executor
        .get(executor_id)
        .is_some_and(|containers| !containers.is_empty());

    // Check for sandboxes
    let has_sandboxes = sandboxes_by_executor
        .get(executor_id)
        .is_some_and(|sandboxes| !sandboxes.is_empty());

    !has_running_allocations && !has_sandboxes
}

/// List executors
#[utoipa::path(
    get,
    path = "/internal/executors",
    tag = "operations",
    responses(
        (status = 200, description = "List all executors", body = Vec<ExecutorMetadata>),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
pub(crate) async fn list_executors(
    State(state): State<RouteState>,
) -> Result<Json<Vec<ExecutorMetadata>>, IndexifyAPIError> {
    let executors = state
        .executor_manager
        .list_executors()
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    let container_sched = state.indexify_state.container_scheduler.read().await;
    let in_memory_state = state.indexify_state.in_memory_state.read().await;

    let mut http_executors = vec![];
    for executor in executors {
        if let Some(fe_server_metadata) = container_sched.executor_states.get(&executor.id) {
            let mut function_container_server_meta = HashMap::new();
            for container_id in &fe_server_metadata.function_container_ids {
                let Some(fe_metadata) = container_sched.function_containers.get(container_id)
                else {
                    continue;
                };
                function_container_server_meta.insert(container_id.clone(), fe_metadata.clone());
            }

            // Compute readiness for teardown
            let ready_for_teardown = is_executor_ready_for_teardown(
                &executor.id,
                &in_memory_state.allocations_by_executor,
                &in_memory_state.sandboxes_by_executor,
            );

            http_executors.push(from_data_model_executor_metadata(
                executor,
                fe_server_metadata.free_resources.clone(),
                function_container_server_meta,
                ready_for_teardown,
            ));
        }
    }

    Ok(Json(http_executors))
}

/// List Allocations
#[utoipa::path(
    get,
    path = "/internal/allocations",
    tag = "operations",
    responses(
        (status = 200, description = "List all allocations", body = HashMap<String, HashMap<String, Vec<Allocation>>>),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
async fn list_allocations(
    State(state): State<RouteState>,
) -> Result<Json<ExecutorsAllocationsResponse>, IndexifyAPIError> {
    let list_allocation_resp = state.executor_manager.api_list_allocations().await;
    Ok(Json(list_allocation_resp))
}

#[utoipa::path(
    get,
    path = "/internal/unprocessed_state_changes",
    tag = "operations",
    responses(
        (status = 200, description = "List all unprocessed state changes", body = StateChangesResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
async fn list_unprocessed_state_changes(
    State(state): State<RouteState>,
) -> Result<Json<StateChangesResponse>, IndexifyAPIError> {
    let state_changes = state
        .indexify_state
        .reader()
        .all_unprocessed_state_changes()
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    Ok(Json(StateChangesResponse {
        count: state_changes.len(),
        state_changes: state_changes.into_iter().map(Into::into).collect(),
    }))
}

#[utoipa::path(
    get,
    path = "/internal/unallocated_function_runs",
    tag = "operations",
    responses(
        (status = 200, description = "List all unallocated function runs", body = UnallocatedFunctionRuns),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
async fn list_unallocated_function_runs(
    State(state): State<RouteState>,
) -> Result<Json<UnallocatedFunctionRuns>, IndexifyAPIError> {
    let state = state.indexify_state.in_memory_state.read().await;
    let unallocated_function_runs: Vec<http_objects_v1::FunctionRun> = state
        .unallocated_function_runs
        .clone()
        .iter()
        .filter_map(|unallocated_function_run_id| {
            state.function_runs.get(unallocated_function_run_id)
        })
        .map(|t| {
            http_objects_v1::FunctionRun::from_data_model_function_run(*t.clone(), vec![], vec![])
        })
        .collect();

    Ok(Json(UnallocatedFunctionRuns {
        count: unallocated_function_runs.len(),
        function_runs: unallocated_function_runs,
    }))
}

/// Get structured executor catalog
#[utoipa::path(
    get,
    path = "/internal/executor_catalog",
    tag = "operations",
    responses(
        (status = 200, description = "Get the structured executor catalog", body = ExecutorCatalog),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
async fn list_executor_catalog(
    State(state): State<RouteState>,
) -> Result<Json<ExecutorCatalog>, IndexifyAPIError> {
    let catalog = &state
        .indexify_state
        .in_memory_state
        .read()
        .await
        .executor_catalog;
    Ok(Json(ExecutorCatalog::from(catalog)))
}

/// Get pending resource profiles for capacity planning
///
/// Returns a histogram of resource profiles for pending function runs and
/// sandboxes. Each profile represents a unique combination of resource
/// requirements with a count of how many pending items need those resources.
/// This can be used by external autoscalers to determine what types of machines
/// are needed.
#[utoipa::path(
    get,
    path = "/internal/pending_resources",
    tag = "operations",
    responses(
        (status = 200, description = "Get pending resource profiles", body = PendingResourcesResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
async fn get_pending_resources(
    State(state): State<RouteState>,
) -> Result<Json<PendingResourcesResponse>, IndexifyAPIError> {
    let pending = state
        .indexify_state
        .in_memory_state
        .read()
        .await
        .get_pending_resources()
        .clone();
    Ok(Json(pending.into()))
}

/// Update the application state
#[utoipa::path(
    get,
    path = "/internal/namespaces/{namespace}/applications/{application}/state",
    tag = "operations",
    responses(
        (status = 204, description = "Update the application state", body = ExecutorCatalog),
        (status = 404, description = "Application not found"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
#[axum::debug_handler]
async fn change_application_state(
    State(state): State<RouteState>,
    Path((namespace, application)): Path<(String, String)>,
    Json(app_state): Json<ApplicationState>,
) -> Result<Response<Body>, IndexifyAPIError> {
    let mut app = state
        .indexify_state
        .reader()
        .get_application(&namespace, &application)
        .await
        .map_err(IndexifyAPIError::internal_error)?
        .ok_or(IndexifyAPIError::not_found("Application not found"))?;

    app.state = app_state.into();

    let request = CreateOrUpdateApplicationRequest {
        namespace,
        application: app,
        upgrade_requests_to_current_version: true,
        container_pools: vec![],
    };

    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::CreateOrUpdateApplication(Box::new(request)),
        })
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap())
}

/// Get application definition for a specific version
#[utoipa::path(
    get,
    path = "/internal/namespaces/{namespace}/applications/{application}/versions/{version}",
    tag = "operations",
    responses(
        (status = 200, description = "Get application definition for a specific version", body = Application),
        (status = 404, description = "Application version not found"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
async fn get_application_by_version(
    State(state): State<RouteState>,
    Path((namespace, application, version)): Path<(String, String, String)>,
) -> Result<Json<ApplicationVersion>, IndexifyAPIError> {
    let application_version = state
        .indexify_state
        .reader()
        .get_application_version(&namespace, &application, &version)
        .await
        .map_err(IndexifyAPIError::internal_error)?
        .ok_or(IndexifyAPIError::not_found("Application version not found"))?;

    Ok(Json(application_version.into()))
}

/// Health check endpoint that returns the service status and version
/// Performs comprehensive health checks on critical components
#[utoipa::path(
    get,
    path = "/healthz",
    responses(
        (status = 200, description = "Service is healthy", body = HealthzResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Service is unhealthy or degraded")
    ),
    tag = "health"
)]
pub async fn healthz_handler(
    State(state): State<RouteState>,
) -> Result<Json<HealthzResponse>, StatusCode> {
    let mut overall_status = HealthzStatus::Ok;
    let mut checks = HealthzChecks {
        database: HealthzStatus::Ok,
        executor_manager: HealthzStatus::Ok,
    };

    // Check database/state store health
    match state.indexify_state.reader().get_all_namespaces().await {
        Ok(_) => checks.database = HealthzStatus::Ok,
        Err(e) => {
            error!("Database health check failed: {:?}", e);
            checks.database = HealthzStatus::Error;
            overall_status = HealthzStatus::Degraded;
        }
    }

    // Check executor manager health
    match state.executor_manager.list_executors().await {
        Ok(_) => checks.executor_manager = HealthzStatus::Ok,
        Err(e) => {
            error!("Executor manager health check failed: {:?}", e);
            checks.executor_manager = HealthzStatus::Error;
            overall_status = HealthzStatus::Degraded;
        }
    }

    let response = HealthzResponse {
        status: overall_status.clone(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        checks,
    };

    // Return 503 Service Unavailable if any critical component is down
    if overall_status == HealthzStatus::Degraded {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    Ok(Json(response))
}

/// Create or update an application from frontend proxy metadata
#[utoipa::path(
    post,
    path = "/internal/v1/namespaces/{namespace}/applications",
    tag = "operations",
    request_body = ApplicationMetadata,
    responses(
        (status = 200, description = "create or update an application"),
        (status = 400, description = "bad request"),
        (status = INTERNAL_SERVER_ERROR, description = "unable to create or update application")
    ),
)]
pub async fn create_or_update_application_with_metadata(
    Path(namespace): Path<String>,
    State(state): State<RouteState>,
    Json(payload): Json<ApplicationMetadata>,
) -> Result<(), IndexifyAPIError> {
    let application = payload.manifest.into_data_model(Some((
        &payload.code_digest.url,
        &payload.code_digest.sha256_hash,
        payload.code_digest.size_bytes,
    )))?;

    validate_and_submit_application(&state, namespace, application, payload.upgrade_requests).await
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxLookupResponse {
    pub id: String,
    pub status: String,
    pub dataplane_api_address: Option<String>,
}

async fn get_sandbox_by_id(
    State(state): State<RouteState>,
    Path((namespace, sandbox_id)): Path<(String, String)>,
) -> Result<Json<SandboxLookupResponse>, IndexifyAPIError> {
    let in_memory_state = state.indexify_state.in_memory_state.read().await;

    let sandbox_key = crate::data_model::SandboxKey::new(&namespace, &sandbox_id);
    let sandbox = in_memory_state
        .sandboxes
        .get(&sandbox_key)
        .ok_or_else(|| IndexifyAPIError::not_found("Sandbox not found"))?;

    let container_id = sandbox
        .container_id
        .as_ref()
        .ok_or_else(|| IndexifyAPIError::not_found("Sandbox has no container assigned"))?;

    let container_scheduler = state.indexify_state.container_scheduler.read().await;

    let container_meta = container_scheduler
        .function_containers
        .get(container_id)
        .ok_or_else(|| IndexifyAPIError::not_found("Container not found"))?;

    let status = match &container_meta.function_container.state {
        crate::data_model::ContainerState::Pending => "Pending",
        crate::data_model::ContainerState::Running => "Running",
        crate::data_model::ContainerState::Terminated { .. } => "Terminated",
        crate::data_model::ContainerState::Unknown => "Unknown",
    };

    let dataplane_api_address = container_scheduler
        .executors
        .get(&container_meta.executor_id)
        .and_then(|executor| executor.proxy_address.clone());

    Ok(Json(SandboxLookupResponse {
        id: sandbox_id,
        status: status.to_string(),
        dataplane_api_address,
    }))
}
