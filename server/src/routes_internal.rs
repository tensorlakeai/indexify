use anyhow::{Result, anyhow};
use axum::{
    Json,
    Router,
    body::Body,
    extract::{Multipart, Path, State},
    http::Response,
    response::{Html, IntoResponse},
    routing::{get, post, put},
};
use hyper::StatusCode;
use tracing::{error, info};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    http_objects::{
        Allocation,
        CacheKey,
        CreateNamespace,
        ExecutorCatalog,
        ExecutorMetadata,
        ExecutorsAllocationsResponse,
        Function,
        FunctionRunOutcome,
        HealthzChecks,
        HealthzResponse,
        HealthzStatus,
        IndexifyAPIError,
        Namespace,
        NamespaceList,
        StateChangesResponse,
        UnallocatedFunctionRuns,
        from_data_model_executor_metadata,
    },
    http_objects_v1::{self, ApplicationState},
    indexify_ui::Assets as UiAssets,
    routes::routes_state::RouteState,
    state_store::{
        kv::{ReadContextData, WriteContextData},
        requests::{
            CreateOrUpdateApplicationRequest,
            NamespaceRequest,
            RequestPayload,
            StateMachineUpdateRequest,
        },
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
            healthz_handler,
        ),
        components(
            schemas(
                CreateNamespace,
                NamespaceList,
                IndexifyAPIError,
                Namespace,
		        CacheKey,
                Function,
                ExecutorMetadata,
                FunctionRunOutcome,
                Allocation,
                ExecutorsAllocationsResponse,
                UnallocatedFunctionRuns,
                StateChangesResponse,
                ExecutorCatalog,
                HealthzChecks,
                HealthzResponse,
            )
        ),
        tags(
            (name = "indexify", description = "Indexify API")
        )
    )]
pub struct ApiDoc;

pub fn configure_internal_routes(route_state: RouteState) -> Router {
    Router::new()
        .merge(SwaggerUi::new("/docs/internal/swagger").url("/docs/internal/openapi.json", ApiDoc::openapi()))
        .route("/", get(index))
        .route(
            "/namespaces",
            get(namespaces).with_state(route_state.clone()),
        )
        .route(
            "/namespaces",
            post(create_namespace).with_state(route_state.clone()),
        )
        .route(
            "/internal/namespaces/{namespace}/compute_graphs/{compute_graph}/code",
            get(get_unversioned_code).with_state(route_state.clone()),
        )
        .route(
            "/internal/namespaces/{namespace}/compute_graphs/{compute_graph}/versions/{version}/code",
            get(get_versioned_code).with_state(route_state.clone()),
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
            "/internal/namespaces/{namespace}/applications/{compute_graph}/requests/{request_id}/ctx/{name}",
            post(set_ctx_state_key).with_state(route_state.clone()),
        )
        .route(
            "/internal/namespaces/{namespace}/applications/{compute_graph}/requests/{request_id}/ctx/{name}",
            get(get_ctx_state_key).with_state(route_state.clone()),
        )
        .route(
            "/internal/namespaces/{namespace}/applications/{application}/state",
            put(change_application_state).with_state(route_state.clone()),
        )
        .route("/healthz", get(healthz_handler).with_state(route_state.clone()))
        .route("/ui", get(ui_index_handler))
        .route("/ui/{*rest}", get(ui_handler))
}

async fn index() -> impl IntoResponse {
    Html(include_str!("./index.html"))
}

#[axum::debug_handler]
async fn ui_index_handler() -> impl IntoResponse {
    let content = UiAssets::get("index.html").unwrap();
    (
        [(hyper::header::CONTENT_TYPE, content.metadata.mimetype())],
        content.data,
    )
        .into_response()
}

#[axum::debug_handler]
async fn ui_handler(Path(url): Path<String>) -> impl IntoResponse {
    let content = UiAssets::get(url.trim_start_matches('/'))
        .unwrap_or_else(|| UiAssets::get("index.html").unwrap());
    (
        [(hyper::header::CONTENT_TYPE, content.metadata.mimetype())],
        content.data,
    )
        .into_response()
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
        .map_err(IndexifyAPIError::internal_error)?;
    let namespaces: Vec<Namespace> = namespaces.into_iter().map(|n| n.into()).collect();
    Ok(Json(NamespaceList { namespaces }))
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
async fn list_executors(
    State(state): State<RouteState>,
) -> Result<Json<Vec<ExecutorMetadata>>, IndexifyAPIError> {
    let executors = state
        .executor_manager
        .list_executors()
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    let executor_server_metadata = state
        .indexify_state
        .in_memory_state
        .read()
        .await
        .executor_states
        .clone();

    let mut http_executors = vec![];
    for executor in executors {
        if let Some(fe_server_metadata) = executor_server_metadata.get(&executor.id) {
            http_executors.push(from_data_model_executor_metadata(
                executor,
                fe_server_metadata.free_resources.clone(),
                fe_server_metadata
                    .function_executors
                    .clone()
                    .into_iter()
                    .map(|(k, v)| (k, v.clone()))
                    .collect(),
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
        .map(|t| http_objects_v1::FunctionRun::from_data_model_function_run(*t.clone(), vec![]))
        .collect();

    Ok(Json(UnallocatedFunctionRuns {
        count: unallocated_function_runs.len(),
        function_runs: unallocated_function_runs,
    }))
}

async fn get_unversioned_code(
    Path((namespace, application)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    get_versioned_code(Path((namespace, application, None)), State(state)).await
}

async fn get_versioned_code(
    Path((namespace, application, version)): Path<(String, String, Option<String>)>,
    State(state): State<RouteState>,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    if let Some(version) = version {
        info!(
            "getting code for application {} version {}",
            application, version
        );
        let application_version = state
            .indexify_state
            .reader()
            .get_application_version(&namespace, &application, &version)
            .map_err(IndexifyAPIError::internal_error)?;

        let application_version = application_version
            .ok_or(IndexifyAPIError::not_found("application version not found"))?;

        let storage_reader = state
            .blob_storage
            .get_blob_store(&namespace)
            .get(&application_version.code.path, None)
            .await
            .map_err(|e| {
                IndexifyAPIError::internal_error(anyhow!("unable to read from blob storage {e:?}",))
            })?;

        return Ok(Response::builder()
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", application_version.code.size.to_string())
            .body(Body::from_stream(storage_reader))
            .map_err(|e| {
                IndexifyAPIError::internal_error(anyhow!(
                    "unable to stream from blob storage {e:?}",
                ))
            }));
    }

    // Getting code without the application version is deprecated.
    // TODO: Remove this block after all clients are updated.

    let application = state
        .indexify_state
        .reader()
        .get_application(&namespace, &application)
        .map_err(IndexifyAPIError::internal_error)?;
    let application = application.ok_or(IndexifyAPIError::not_found("Application not found"))?;
    let storage_reader = state
        .blob_storage
        .get_blob_store(&namespace)
        .get(&application.code.path, None)
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("unable to read from blob storage {e}"))
        })?;

    Ok(Response::builder()
        .header("Content-Type", "application/octet-stream")
        .header("Content-Length", application.code.clone().size.to_string())
        .body(Body::from_stream(storage_reader))
        .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string())))
}

async fn set_ctx_state_key(
    Path((namespace, application, request_id, key)): Path<(String, String, String, String)>,
    State(state): State<RouteState>,
    mut values: Multipart,
) -> Result<(), IndexifyAPIError> {
    let mut request: WriteContextData = WriteContextData {
        namespace,
        application,
        request_id,
        key,
        value: vec![],
    };

    while let Some(field) = values.next_field().await.unwrap() {
        if let Some(name) = field.name() {
            if name == "value" {
                let content_type: &str = field.content_type().ok_or_else(|| {
                    IndexifyAPIError::bad_request("content-type of the value is required")
                })?;
                if content_type != "application/octet-stream" {
                    // Server doesn't support flexible client controlled content-type yet because
                    // we don't yet store content-type in the kv store.
                    return Err(IndexifyAPIError::bad_request(
                        "only 'application/octet-stream' content-type is currently supported",
                    ));
                }
                request.value = field
                    .bytes()
                    .await
                    .map_err(|e| {
                        IndexifyAPIError::internal_error(anyhow!("failed reading the value: {e}"))
                    })?
                    .to_vec();
            } else {
                return Err(IndexifyAPIError::bad_request(&format!(
                    "unexpected field: {name}"
                )));
            }
        }
    }

    state
        .kvs
        .put_ctx_state(request)
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    Ok(())
}

async fn get_ctx_state_key(
    Path((namespace, application, request_id, key)): Path<(String, String, String, String)>,
    State(state): State<RouteState>,
) -> Result<Response<Body>, IndexifyAPIError> {
    let value = state
        .kvs
        .get_ctx_state_key(ReadContextData {
            namespace,
            application,
            request_id,
            key,
        })
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    match value {
        Some(value) => Response::builder()
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", value.len().to_string())
            .body(Body::from(value))
            .map_err(|e| {
                tracing::error!("failed streaming get ctx response: {e:?}");
                IndexifyAPIError::internal_error_str("failed streaming the response")
            }),
        None => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap()),
    }
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
        .map_err(IndexifyAPIError::internal_error)?
        .ok_or(IndexifyAPIError::not_found("Application not found"))?;

    app.state = app_state.into();

    let request = CreateOrUpdateApplicationRequest {
        namespace,
        application: app,
        upgrade_requests_to_current_version: true,
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
    match state.indexify_state.reader().get_all_namespaces() {
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
        status: overall_status.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        checks,
    };

    // Return 503 Service Unavailable if any critical component is down
    if overall_status == HealthzStatus::Degraded {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    Ok(Json(response))
}
