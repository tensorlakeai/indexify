use std::{collections::HashMap, time::Duration};

use anyhow::{anyhow, Result};
use axum::{
    body::Body,
    extract::{Multipart, Path, Query, RawPathParams, Request, State},
    http::Response,
    middleware::{self, Next},
    response::{sse::Event, Html, IntoResponse},
    routing::{delete, get, post},
    Json,
    Router,
};
use base64::prelude::*;
use download::{download_fn_output_payload, download_invocation_error};
use futures::StreamExt;
use hyper::StatusCode;
use invoke::{invoke_with_object, progress_stream};
use logs::download_allocation_logs;
use nanoid::nanoid;
use tracing::{error, info};
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    blob_store::PutResult,
    data_model::ComputeGraphError,
    http_objects::{
        from_data_model_executor_metadata,
        Allocation,
        CacheKey,
        ComputeFn,
        ComputeGraph,
        ComputeGraphsList,
        CreateNamespace,
        CursorDirection,
        ExecutorMetadata,
        ExecutorsAllocationsResponse,
        FnOutput,
        FnOutputs,
        GraphInvocations,
        GraphVersion,
        ImageInformation,
        IndexifyAPIError,
        Invocation,
        ListParams,
        Namespace,
        NamespaceList,
        RuntimeInformation,
        StateChangesResponse,
        Task,
        TaskOutcome,
        Tasks,
        UnallocatedTasks,
    },
    indexify_ui::Assets as UiAssets,
    routes::{
        download,
        invoke,
        logs,
        logs::download_function_executor_startup_logs,
        routes_state::RouteState,
    },
    state_store::{
        self,
        kv::{ReadContextData, WriteContextData},
        requests::{
            CreateOrUpdateComputeGraphRequest,
            DeleteComputeGraphRequest,
            DeleteInvocationRequest,
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
            logs::download_allocation_logs,
            logs::download_function_executor_startup_logs,
            list_executors,
            list_allocations,
            list_unallocated_tasks,
            list_unprocessed_state_changes,
        ),
        components(
            schemas(
                CreateNamespace,
                NamespaceList,
                IndexifyAPIError,
                Namespace,
                ComputeGraph,
		        CacheKey,
                ComputeFn,
                ListParams,
                ComputeGraphCreateType,
                ComputeGraphsList,
                ImageInformation,
                ExecutorMetadata,
                RuntimeInformation,
                Task,
                TaskOutcome,
                Tasks,
                GraphInvocations,
                GraphVersion,
                Allocation,
                ExecutorsAllocationsResponse,
                UnallocatedTasks,
                StateChangesResponse,
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
        .nest(
            "/namespaces/{namespace}",
            namespace_routes(route_state.clone()),
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
            "/internal/unallocated_tasks",
            get(list_unallocated_tasks).with_state(route_state.clone()),
        )
        .route(
            "/internal/unprocessed_state_changes",
            get(list_unprocessed_state_changes).with_state(route_state.clone()),
        )
        .route(
            "/internal/namespaces/{namespace}/compute_graphs/{compute_graph}/invocations/{invocation_id}/ctx/{name}",
            post(set_ctx_state_key).with_state(route_state.clone()),
        )
        .route(
            "/internal/namespaces/{namespace}/compute_graphs/{compute_graph}/invocations/{invocation_id}/ctx/{name}",
            get(get_ctx_state_key).with_state(route_state.clone()),
        )
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

/// Namespace router with namespace specific layers.
pub fn namespace_routes(route_state: RouteState) -> Router {
    Router::new()
        .route(
            "/compute_graphs",
            post(create_or_update_compute_graph).with_state(route_state.clone()),
        )
        .route(
            "/compute_graphs",
            get(list_compute_graphs).with_state(route_state.clone()),
        )
        .route(
            "/compute_graphs/{compute_graph}",
            delete(delete_compute_graph).with_state(route_state.clone()),
        )
        .route(
            "/compute_graphs/{compute_graph}",
            get(get_compute_graph).with_state(route_state.clone()),
        )
        .route(
            "/compute_graphs/{compute_graph}/invocations/{invocation_id}/tasks",
            get(list_tasks).with_state(route_state.clone()),
        )
        .route(
            "/compute_graphs/{compute_graph}/invocations/{invocation_id}/outputs",
            get(list_outputs).with_state(route_state.clone()),
        )
        .route(
            "/compute_graphs/{compute_graph}/invocations/{invocation_id}/context",
            get(get_context).with_state(route_state.clone()),
        )
        .route(
            "/compute_graphs/{compute_graph}/invocations",
            get(graph_invocations).with_state(route_state.clone()),
        )
        .route(
            "/compute_graphs/{compute_graph}/invoke_object",
            post(invoke_with_object).with_state(route_state.clone()),
        )
        .route("/compute_graphs/{compute_graph}/invocations/{invocation_id}", get(find_invocation).with_state(route_state.clone()))
        .route(
            "/compute_graphs/{compute_graph}/invocations/{invocation_id}",
            delete(delete_invocation).with_state(route_state.clone()),
        )
        .route(
            "/compute_graphs/{compute_graph}/invocations/{invocation_id}/wait",
            get(progress_stream).with_state(route_state.clone()),
        )
        .route(
            "/compute_graphs/{compute_graph}/notify",
            get(notify_on_change).with_state(route_state.clone()),
        )
        .route(
            "/compute_graphs/{compute_graph}/invocations/{invocation_id}/fn/{fn_name}/output/{id}",
            get(download_fn_output_payload).with_state(route_state.clone()),
        )
        .route("/compute_graphs/{compute_graph}/invocations/{invocation_id}/allocations/{allocation_id}/logs/{file}", get(download_allocation_logs).with_state(route_state.clone()))
        .route("/compute_graphs/{compute_graph}/compute_functions/{compute_function}/versions/{version}/function_executors/{function_executor_id}/startup_logs/{file}", get(download_function_executor_startup_logs).with_state(route_state.clone()))
        .layer(middleware::from_fn(move |rpp, r, n| namespace_middleware(route_state.clone(), rpp, r, n)))
}

/// Middleware to check if the namespace exists.
async fn namespace_middleware(
    route_state: RouteState,
    params: RawPathParams,
    request: Request,
    next: Next,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    // get the namespace path variable from the path
    let namespace_param = params.iter().find(|(key, _)| *key == "namespace");

    // if the namespace path variable is found, check if the namespace exists
    if let Some((_, namespace)) = namespace_param {
        let reader = route_state.indexify_state.reader();
        let ns = reader
            .get_namespace(namespace)
            .map_err(IndexifyAPIError::internal_error)?;

        if ns.is_none() {
            route_state
                .indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::CreateNameSpace(NamespaceRequest {
                        name: namespace.to_string(),
                        blob_storage_bucket: None,
                        blob_storage_region: None,
                    }),
                    processed_state_changes: vec![],
                })
                .await
                .map_err(IndexifyAPIError::internal_error)?;

            info!("namespace created: {:?}", namespace);
        }
    }

    Ok(next.run(request).await)
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
        let Some(blob_storage_region) = &namespace.blob_storage_region else {
            return Err(IndexifyAPIError::bad_request(
                "blob storage region is required",
            ));
        };
        if let Err(e) = state.blob_storage.create_new_blob_store(
            &namespace.name,
            blob_storage_bucket,
            &blob_storage_region,
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
        processed_state_changes: vec![],
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

#[allow(dead_code)]
#[derive(ToSchema)]
struct ComputeGraphCreateType {
    compute_graph: ComputeGraph,
    #[schema(format = "binary")]
    code: String,
}

/// Create compute graph
#[utoipa::path(
    post,
    path = "/namespaces/{namespace}/compute_graphs",
    tag = "operations",
    request_body(content_type = "multipart/form-data", content = inline(ComputeGraphCreateType)),
    responses(
        (status = 200, description = "Create or update a Compute Graph"),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to create compute graphs")
    ),
)]
async fn create_or_update_compute_graph(
    Path(namespace): Path<String>,
    State(state): State<RouteState>,
    mut compute_graph_code: Multipart,
) -> Result<(), IndexifyAPIError> {
    let mut compute_graph_definition: Option<ComputeGraph> = Option::None;
    let mut put_result: Option<PutResult> = None;
    let mut upgrade_tasks_to_current_version: Option<bool> = None;
    while let Some(field) = compute_graph_code
        .next_field()
        .await
        .map_err(|err| IndexifyAPIError::internal_error(anyhow!(err)))?
    {
        let name = field.name();
        if let Some(name) = name {
            if name == "code" {
                let stream = field.map(|res| res.map_err(|err| anyhow!(err)));
                let file_name = format!("{}_{}", namespace, nanoid!());
                let result = state
                    .blob_storage
                    .get_blob_store(&namespace)
                    .put(&file_name, stream)
                    .await
                    .map_err(IndexifyAPIError::internal_error)?;
                put_result = Some(result);
            } else if name == "compute_graph" {
                let text = field
                    .text()
                    .await
                    .map_err(|e| IndexifyAPIError::bad_request(&e.to_string()))?;
                let mut json_value: serde_json::Value = serde_json::from_str(&text)?;
                json_value["namespace"] = serde_json::Value::String(namespace.clone());
                compute_graph_definition = Some(serde_json::from_value(json_value)?);
            } else if name == "upgrade_tasks_to_latest_version" {
                let text = field
                    .text()
                    .await
                    .map_err(|e| IndexifyAPIError::bad_request(&e.to_string()))?;
                upgrade_tasks_to_current_version = Some(serde_json::from_str::<bool>(&text)?);
            } else if name == "code_content_type" {
                let code_content_type = field
                    .text()
                    .await
                    .map_err(|e| IndexifyAPIError::bad_request(&e.to_string()))?;
                if code_content_type != "application/zip" {
                    return Err(IndexifyAPIError::bad_request(
                        "Code content type must be application/zip",
                    ));
                }
            }
        }
    }

    let compute_graph_definition = compute_graph_definition.ok_or(
        IndexifyAPIError::bad_request("Compute graph definition is required"),
    )?;

    let put_result = put_result.ok_or(IndexifyAPIError::bad_request("Code is required"))?;

    let compute_graph = compute_graph_definition.into_data_model(
        &put_result.url,
        &put_result.sha256_hash,
        put_result.size_bytes,
        &state.config.executor,
    )?;
    let name = compute_graph.name.clone();
    info!(
        "creating compute graph {}, upgrade existing tasks and invocations: {}",
        name,
        upgrade_tasks_to_current_version.unwrap_or(false)
    );
    let request = RequestPayload::CreateOrUpdateComputeGraph(CreateOrUpdateComputeGraphRequest {
        namespace,
        compute_graph,
        upgrade_tasks_to_current_version: upgrade_tasks_to_current_version.unwrap_or(false),
    });
    let result = state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: request,
            processed_state_changes: vec![],
        })
        .await;
    if let Err(err) = result {
        return match err.root_cause().downcast_ref::<ComputeGraphError>() {
            Some(ComputeGraphError::VersionExists) => Err(IndexifyAPIError::bad_request(
                "This graph version already exists, please update the graph version",
            )),
            _ => Err(IndexifyAPIError::internal_error(err)),
        };
    }

    info!("compute graph created: {}", name);
    Ok(())
}

/// Delete compute graph
#[utoipa::path(
    delete,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}",
    tag = "operations",
    responses(
        (status = 200, description = "Extraction graph deleted successfully"),
        (status = BAD_REQUEST, description = "Unable to delete extraction graph")
    ),
)]
async fn delete_compute_graph(
    Path((namespace, compute_graph)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<(), IndexifyAPIError> {
    let request = RequestPayload::TombstoneComputeGraph(DeleteComputeGraphRequest {
        namespace,
        name: compute_graph.clone(),
    });
    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: request,
            processed_state_changes: vec![],
        })
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    info!("compute graph deleted: {}", compute_graph);
    Ok(())
}

/// List compute graphs
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/compute_graphs",
    tag = "operations",
    params(
        ListParams
    ),
    responses(
        (status = 200, description = "Lists Compute Graph", body = ComputeGraphsList),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
async fn list_compute_graphs(
    Path(namespace): Path<String>,
    Query(params): Query<ListParams>,
    State(state): State<RouteState>,
) -> Result<Json<ComputeGraphsList>, IndexifyAPIError> {
    let cursor = params
        .cursor
        .map(|c| BASE64_STANDARD.decode(c).unwrap_or_default());
    let (compute_graphs, cursor) = state
        .indexify_state
        .reader()
        .list_compute_graphs(&namespace, cursor.as_deref(), params.limit)
        .map_err(IndexifyAPIError::internal_error)?;
    let cursor = cursor.map(|c| BASE64_STANDARD.encode(c));
    Ok(Json(ComputeGraphsList {
        compute_graphs: compute_graphs.into_iter().map(|c| c.into()).collect(),
        cursor,
    }))
}

/// Get a compute graph definition
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}",
    tag = "operations",
    responses(
        (status = 200, description = "Compute Graph Definition", body = ComputeGraph),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
async fn get_compute_graph(
    Path((namespace, name)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<Json<ComputeGraph>, IndexifyAPIError> {
    let compute_graph = state
        .indexify_state
        .reader()
        .get_compute_graph(&namespace, &name)
        .map_err(IndexifyAPIError::internal_error)?;
    if let Some(compute_graph) = compute_graph {
        return Ok(Json(compute_graph.into()));
    }
    Err(IndexifyAPIError::not_found("Compute Graph not found"))
}

/// List Graph invocations
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}/invocations",
    tag = "ingestion",
    params(
        ListParams
    ),
    responses(
        (status = 200, description = "List Graph Invocations", body = GraphInvocations),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
async fn graph_invocations(
    Path((namespace, compute_graph)): Path<(String, String)>,
    Query(params): Query<ListParams>,
    State(state): State<RouteState>,
) -> Result<Json<GraphInvocations>, IndexifyAPIError> {
    let cursor = params
        .cursor
        .map(|c| BASE64_STANDARD.decode(c).unwrap_or_default());
    let direction = match params.direction {
        Some(CursorDirection::Forward) => Some(state_store::scanner::CursorDirection::Forward),
        Some(CursorDirection::Backward) => Some(state_store::scanner::CursorDirection::Backward),
        None => None,
    };
    let (invocation_ctxs, prev_cursor, next_cursor) = state
        .indexify_state
        .reader()
        .list_invocations(
            &namespace,
            &compute_graph,
            cursor.as_deref(),
            params.limit.unwrap_or(100),
            direction,
        )
        .map_err(IndexifyAPIError::internal_error)?;
    let mut invocations = vec![];
    for invocation_ctx in invocation_ctxs {
        let mut invocation: Invocation = invocation_ctx.clone().into();
        invocation.invocation_error = download_invocation_error(
            invocation_ctx.invocation_error.clone(),
            &state.blob_storage.get_blob_store(&namespace),
        )
        .await?;
        invocations.push(invocation);
    }
    let prev_cursor = prev_cursor.map(|c| BASE64_STANDARD.encode(c));
    let next_cursor = next_cursor.map(|c| BASE64_STANDARD.encode(c));

    Ok(Json(GraphInvocations {
        invocations,
        prev_cursor,
        next_cursor,
    }))
}

async fn notify_on_change(
    Path((_namespace, _compute_graph)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    let mut rx = state.indexify_state.task_event_stream();
    let invocation_event_stream = async_stream::stream! {
        loop {
            if let Ok(ev)  =  rx.recv().await {
                    yield Event::default().json_data(ev.clone());
                    return;
            }
        }
    };

    Ok(
        axum::response::Sse::new(invocation_event_stream).keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(Duration::from_secs(1))
                .text("keep-alive-text"),
        ),
    )
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
    path = "/internal/unallocated_tasks",
    tag = "operations",
    responses(
        (status = 200, description = "List all unallocated tasks", body = UnallocatedTasks),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
async fn list_unallocated_tasks(
    State(state): State<RouteState>,
) -> Result<Json<UnallocatedTasks>, IndexifyAPIError> {
    let state = state.indexify_state.in_memory_state.read().await;
    let unallocated_tasks: Vec<Task> = state
        .unallocated_tasks
        .clone()
        .iter()
        .filter_map(|unallocated_task_id| state.tasks.get(&unallocated_task_id.task_key))
        .map(|t| Task::from_data_model_task(*t.clone(), vec![]))
        .collect();

    Ok(Json(UnallocatedTasks {
        count: unallocated_tasks.len(),
        tasks: unallocated_tasks,
    }))
}

/// List tasks for an invocation
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}/invocations/{invocation_id}/tasks",
    tag = "operations",
    params(
        ListParams
    ),
    responses(
        (status = 200, description = "List tasks for a given invocation id", body = Tasks),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
#[axum::debug_handler]
async fn list_tasks(
    Path((namespace, compute_graph, invocation_id)): Path<(String, String, String)>,
    Query(params): Query<ListParams>,
    State(state): State<RouteState>,
) -> Result<Json<Tasks>, IndexifyAPIError> {
    let cursor = params
        .cursor
        .map(|c| BASE64_STANDARD.decode(c).unwrap_or_default());
    let (tasks, cursor) = state
        .indexify_state
        .reader()
        .list_tasks_by_compute_graph(
            &namespace,
            &compute_graph,
            &invocation_id,
            cursor.as_deref(),
            params.limit,
        )
        .map_err(IndexifyAPIError::internal_error)?;
    let allocations = state
        .indexify_state
        .reader()
        .get_allocations_by_invocation(&namespace, &compute_graph, &invocation_id)
        .map_err(IndexifyAPIError::internal_error)?;
    let mut allocations_by_task_id: HashMap<String, Vec<Allocation>> = HashMap::new();
    for allocation in allocations {
        allocations_by_task_id
            .entry(allocation.task_id.to_string())
            .or_default()
            .push(allocation.into());
    }
    let mut http_tasks = vec![];
    for task in tasks {
        let allocations = allocations_by_task_id
            .get(task.id.get())
            .cloned()
            .clone()
            .unwrap_or_default();
        http_tasks.push(Task::from_data_model_task(task, allocations));
    }
    let cursor = cursor.map(|c| BASE64_STANDARD.encode(c));
    Ok(Json(Tasks {
        tasks: http_tasks,
        cursor,
    }))
}

/// Get accounting information for a compute graph invocation
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}/invocations/{invocation_id}/context",
    tag = "operations",
    responses(
        (status = 200, description = "Accounting information for an invocation id", body = Tasks),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
async fn get_context(
    Path((namespace, compute_graph, invocation_id)): Path<(String, String, String)>,
    State(state): State<RouteState>,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    let context = state
        .indexify_state
        .reader()
        .invocation_ctx(&namespace, &compute_graph, &invocation_id)
        .map_err(IndexifyAPIError::internal_error)?;
    Ok(Json(context))
}

/// Get outputs of a function
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}/invocations/{invocation_id}/outputs",
    tag = "retrieve",
    responses(
        (status = 200, description = "List outputs for a given invocation id", body = Tasks),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
#[axum::debug_handler]
async fn list_outputs(
    Path((namespace, compute_graph, invocation_id)): Path<(String, String, String)>,
    Query(params): Query<ListParams>,
    State(state): State<RouteState>,
) -> Result<Json<FnOutputs>, IndexifyAPIError> {
    let cursor = params
        .cursor
        .map(|c| BASE64_STANDARD.decode(c).unwrap_or_default());
    let invocation_ctx = state
        .indexify_state
        .reader()
        .invocation_ctx(&namespace, &compute_graph, &invocation_id)
        .map_err(IndexifyAPIError::internal_error)?
        .ok_or(IndexifyAPIError::not_found("invocation not found"))?;

    let (outputs, cursor) = state
        .indexify_state
        .reader()
        .list_outputs_by_compute_graph(
            &namespace,
            &compute_graph,
            &invocation_id,
            cursor.as_deref(),
            params.limit,
        )
        .map_err(IndexifyAPIError::internal_error)?;
    let mut http_outputs = vec![];
    for output in outputs {
        for (idx, _payload) in output.payloads.iter().enumerate() {
            http_outputs.push(FnOutput {
                id: format!("{}|{}", output.id, idx),
                compute_fn: output.compute_fn_name.clone(),
                created_at: output.created_at,
            });
        }
    }

    let mut invocation: Invocation = invocation_ctx.clone().into();
    invocation.invocation_error = download_invocation_error(
        invocation_ctx.invocation_error.clone(),
        &state.blob_storage.get_blob_store(&namespace),
    )
    .await?;

    let cursor = cursor.map(|c| BASE64_STANDARD.encode(c));

    // We return the outputs of finalized and pending invocations to allow getting
    // partial results.
    Ok(Json(FnOutputs {
        invocation: invocation.clone(),
        status: invocation.status,
        outcome: invocation.outcome,
        outputs: http_outputs,
        cursor,
    }))
}

#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}/invocations/{invocation_id}",
    tag = "retrieve",
    responses(
        (status = 200, description = "Details about a given invocation", body = Invocation),
        (status = NOT_FOUND, description = "Invocation not found"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
async fn find_invocation(
    Path((namespace, compute_graph, invocation_id)): Path<(String, String, String)>,
    State(state): State<RouteState>,
) -> Result<Json<Invocation>, IndexifyAPIError> {
    let invocation_ctx = state
        .indexify_state
        .reader()
        .invocation_ctx(&namespace, &compute_graph, &invocation_id)
        .map_err(IndexifyAPIError::internal_error)?
        .ok_or(IndexifyAPIError::not_found("invocation not found"))?;

    let mut invocation: Invocation = invocation_ctx.clone().into();
    invocation.invocation_error = download_invocation_error(
        invocation_ctx.invocation_error.clone(),
        &state.blob_storage.get_blob_store(&namespace),
    )
    .await?;
    Ok(Json(invocation))
}

/// Delete a specific invocation
#[utoipa::path(
    delete,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}/invocations/{invocation_id}",
    tag = "operations",
    responses(
        (status = 200, description = "Invocation has been deleted"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
#[axum::debug_handler]
async fn delete_invocation(
    Path((namespace, compute_graph, invocation_id)): Path<(String, String, String)>,
    State(state): State<RouteState>,
) -> Result<(), IndexifyAPIError> {
    let request = RequestPayload::TombstoneInvocation(DeleteInvocationRequest {
        namespace,
        compute_graph,
        invocation_id,
    });
    let req = StateMachineUpdateRequest {
        payload: request,
        processed_state_changes: vec![],
    };

    state
        .indexify_state
        .write(req)
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    Ok(())
}

async fn get_unversioned_code(
    Path((namespace, compute_graph)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    get_versioned_code(Path((namespace, compute_graph, None)), State(state)).await
}

async fn get_versioned_code(
    Path((namespace, compute_graph, version)): Path<(String, String, Option<GraphVersion>)>,
    State(state): State<RouteState>,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    if let Some(version) = version {
        info!(
            "getting code for compute graph {} version {}",
            compute_graph, version.0
        );
        let compute_graph_version = state
            .indexify_state
            .reader()
            .get_compute_graph_version(&namespace, &compute_graph, &version.into())
            .map_err(IndexifyAPIError::internal_error)?;

        let compute_graph_version = compute_graph_version.ok_or(IndexifyAPIError::not_found(
            "compute graph version not found",
        ))?;

        let storage_reader = state
            .blob_storage
            .get_blob_store(&namespace)
            .get(&compute_graph_version.code.path)
            .await
            .map_err(|e| {
                IndexifyAPIError::internal_error(anyhow!(
                    "unable to read from blob storage {:?}",
                    e
                ))
            })?;

        return Ok(Response::builder()
            .header("Content-Type", "application/octet-stream")
            .header(
                "Content-Length",
                compute_graph_version.code.size.to_string(),
            )
            .body(Body::from_stream(storage_reader))
            .map_err(|e| {
                IndexifyAPIError::internal_error(anyhow!(
                    "unable to stream from blob storage {:?}",
                    e
                ))
            }));
    }

    // Getting code without the compute graph version is deprecated.
    // TODO: Remove this block after all clients are updated.

    let compute_graph = state
        .indexify_state
        .reader()
        .get_compute_graph(&namespace, &compute_graph)
        .map_err(IndexifyAPIError::internal_error)?;
    let compute_graph =
        compute_graph.ok_or(IndexifyAPIError::not_found("Compute Graph not found"))?;
    let storage_reader = state
        .blob_storage
        .get_blob_store(&namespace)
        .get(&compute_graph.code.path)
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("unable to read from blob storage {}", e))
        })?;

    Ok(Response::builder()
        .header("Content-Type", "application/octet-stream")
        .header(
            "Content-Length",
            compute_graph.code.clone().size.to_string(),
        )
        .body(Body::from_stream(storage_reader))
        .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string())))
}

async fn set_ctx_state_key(
    Path((namespace, compute_graph, invocation_id, key)): Path<(String, String, String, String)>,
    State(state): State<RouteState>,
    mut values: Multipart,
) -> Result<(), IndexifyAPIError> {
    let mut request: WriteContextData = WriteContextData {
        namespace,
        compute_graph,
        invocation_id,
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
                        IndexifyAPIError::internal_error(anyhow!("failed reading the value: {}", e))
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
    Path((namespace, compute_graph, invocation_id, key)): Path<(String, String, String, String)>,
    State(state): State<RouteState>,
) -> Result<Response<Body>, IndexifyAPIError> {
    let value = state
        .kvs
        .get_ctx_state_key(ReadContextData {
            namespace,
            compute_graph,
            invocation_id,
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
                tracing::error!("failed streaming get ctx response: {:?}", e);
                IndexifyAPIError::internal_error_str("failed streaming the response")
            }),
        None => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap()),
    }
}
