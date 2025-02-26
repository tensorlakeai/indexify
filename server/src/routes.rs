use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Multipart, Path, Query, RawPathParams, Request, State},
    http::{Method, Response},
    middleware::{self, Next},
    response::{sse::Event, Html, IntoResponse},
    routing::{delete, get, post},
    Json,
    Router,
};
use axum_tracing_opentelemetry::{
    self,
    middleware::{OtelAxumLayer, OtelInResponseLayer},
};
use blob_store::PutResult;
use data_model::{ComputeGraphError, ExecutorId};
use futures::StreamExt;
use hyper::StatusCode;
use indexify_ui::Assets as UiAssets;
use indexify_utils::GuardStreamExt;
use metrics::api_io_stats;
use nanoid::nanoid;
use prometheus::Encoder;
use state_store::{
    kv::{ReadContextData, WriteContextData, KVS},
    requests::{
        CreateOrUpdateComputeGraphRequest,
        DeleteComputeGraphRequest,
        DeleteInvocationRequest,
        NamespaceRequest,
        RequestPayload,
        StateMachineUpdateRequest,
    },
    IndexifyState,
};
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info};
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    executors::{self, EXECUTOR_TIMEOUT},
    http_objects::{
        ExecutorAllocations,
        Invocation,
        InvocationStatus,
        StateChangesResponse,
        UnallocatedTasks,
    },
};

mod download;
mod internal_ingest;
mod invoke;
mod logs;
use download::{
    download_fn_output_by_key,
    download_fn_output_payload,
    download_invocation_payload,
};
use internal_ingest::ingest_files_from_executor;
use invoke::{invoke_with_file, invoke_with_object, wait_until_invocation_completed};
use logs::download_task_logs;

use crate::{
    executors::ExecutorManager,
    http_objects::{
        Allocation,
        ComputeFn,
        ComputeGraph,
        ComputeGraphsList,
        CreateNamespace,
        DynamicRouter,
        ExecutorMetadata,
        ExecutorsAllocations,
        FnOutputs,
        GraphInvocations,
        GraphVersion,
        ImageInformation,
        IndexifyAPIError,
        ListParams,
        Namespace,
        NamespaceList,
        Node,
        RuntimeInformation,
        Task,
        TaskOutcome,
        Tasks,
    },
};

#[derive(OpenApi)]
#[openapi(
        paths(
            create_namespace,
            namespaces,
            invoke::invoke_with_object,
            graph_invocations,
            find_invocation,
            create_or_update_compute_graph,
            list_compute_graphs,
            get_compute_graph,
            delete_compute_graph,
            list_tasks,
            list_outputs,
            delete_invocation,
            logs::download_task_logs,
            list_executors,
            list_allocations,
            list_unallocated_tasks,
            list_unprocessed_state_changes,
            download::download_fn_output_payload,
        ),
        components(
            schemas(
                CreateNamespace,
                NamespaceList,
                IndexifyAPIError,
                Namespace,
                ComputeGraph,
                Node,
                DynamicRouter,
                ComputeFn,
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
                ExecutorsAllocations,
                UnallocatedTasks,
                StateChangesResponse,
            )
        ),
        tags(
            (name = "indexify", description = "Indexify API")
        )
    )]

struct ApiDoc;

#[derive(Clone)]
pub struct RouteState {
    pub indexify_state: Arc<IndexifyState>,
    pub blob_storage: Arc<blob_store::BlobStorage>,
    pub kvs: Arc<KVS>,
    pub executor_manager: Arc<ExecutorManager>,
    pub registry: Arc<prometheus::Registry>,
    pub metrics: Arc<api_io_stats::Metrics>,
}

pub fn create_routes(route_state: RouteState) -> Router {
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::DELETE])
        .allow_origin(Any)
        .allow_headers(Any);

    Router::new()
        .merge(SwaggerUi::new("/docs/swagger").url("/docs/openapi.json", ApiDoc::openapi()))
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
            "/internal/ingest_files",
            post(ingest_files_from_executor).with_state(route_state.clone()),
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
            "/internal/executors/{id}/tasks",
            post(executor_tasks).with_state(route_state.clone()),
        )
        .route(
            "/internal/fn_outputs/{input_key}",
            get(download_fn_output_by_key).with_state(route_state.clone()),
        )
        .route(
            "/internal/namespaces/{namespace}/compute_graphs/{compute_graph}/invocations/{invocation_id}/ctx/{name}",
            post(set_ctx_state_key).with_state(route_state.clone()),
        )
        .route(
            "/internal/namespaces/{namespace}/compute_graphs/{compute_graph}/invocations/{invocation_id}/ctx/{name}",
            get(get_ctx_state_key).with_state(route_state.clone()),
        )
        .layer(OtelInResponseLayer)
        .layer(OtelAxumLayer::default())
        // No tracing starting here.
        .route("/ui", get(ui_index_handler))
        .route("/ui/{*rest}", get(ui_handler))
        .route("/metrics/service",get(service_metrics).with_state(route_state.clone()))
        .layer(cors)
        .layer(DefaultBodyLimit::disable())
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
            "/compute_graphs/{compute_graph}/invoke_file",
            post(invoke_with_file).with_state(route_state.clone()),
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
            get(wait_until_invocation_completed).with_state(route_state.clone()),
        )
        .route(
            "/compute_graphs/{compute_graph}/notify",
            get(notify_on_change).with_state(route_state.clone()),
        )
        .route(
            "/compute_graphs/{compute_graph}/invocations/{invocation_id}/payload",
            get(download_invocation_payload).with_state(route_state.clone()),
        )
        .route(
            "/compute_graphs/{compute_graph}/invocations/{invocation_id}/fn/{fn_name}/output/{id}",
            get(download_fn_output_payload).with_state(route_state.clone()),
        )
        .route("/compute_graphs/{compute_graph}/invocations/{invocation_id}/fn/{fn_name}/tasks/{task_id}/logs/{file}", get(download_task_logs).with_state(route_state.clone()))
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
    let req = StateMachineUpdateRequest {
        payload: RequestPayload::CreateNameSpace(NamespaceRequest {
            name: namespace.name.clone(),
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
    let (compute_graphs, cursor) = state
        .indexify_state
        .reader()
        .list_compute_graphs(&namespace, params.cursor.as_deref(), params.limit)
        .map_err(IndexifyAPIError::internal_error)?;
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
    responses(
        (status = 200, description = "Compute Graph Definition", body = GraphInvocations),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
async fn graph_invocations(
    Path((namespace, compute_graph)): Path<(String, String)>,
    Query(params): Query<ListParams>,
    State(state): State<RouteState>,
) -> Result<Json<GraphInvocations>, IndexifyAPIError> {
    let (invocation_ctxs, cursor) = state
        .indexify_state
        .reader()
        .list_invocations(
            &namespace,
            &compute_graph,
            params.cursor.as_deref(),
            params.limit,
        )
        .map_err(IndexifyAPIError::internal_error)?;
    let mut invocations = vec![];
    for invocation_ctx in invocation_ctxs {
        invocations.push(invocation_ctx.into());
    }
    Ok(Json(GraphInvocations {
        invocations,
        cursor,
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
    let http_executors = executors.into_iter().map(|e| e.into()).collect();
    Ok(Json(http_executors))
}

/// List Allocations
#[utoipa::path(
    get,
    path = "/internal/allocations",
    tag = "operations",
    responses(
        (status = 200, description = "List all allocations", body = HashMap<String, Vec<Allocation>>),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
async fn list_allocations(
    State(state): State<RouteState>,
) -> Result<Json<ExecutorsAllocations>, IndexifyAPIError> {
    let allocations = state.executor_manager.list_allocations().await;
    Ok(Json(ExecutorsAllocations {
        allocations: allocations
            .into_iter()
            .map(|(executor_id, allocations)| {
                let executor_id = executor_id.clone();
                let mut allocs = vec![];
                for allocation in allocations {
                    allocs.push(allocation.clone().into());
                }
                (
                    executor_id,
                    ExecutorAllocations {
                        count: allocs.len(),
                        allocations: allocs,
                    },
                )
            })
            .collect(),
    }))
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
        .filter_map(|(task_id, _)| state.tasks.get(task_id))
        .map(|t| (**t).clone().into())
        .collect();

    Ok(Json(UnallocatedTasks {
        count: unallocated_tasks.len(),
        tasks: unallocated_tasks,
    }))
}

async fn executor_tasks(
    Path(executor_id): Path<ExecutorId>,
    State(state): State<RouteState>,
    Json(payload): Json<ExecutorMetadata>,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    const TASK_LIMIT: usize = 10;
    let function_allowlist = payload.function_allowlist.map(|function_uris| {
        function_uris
            .iter()
            .map(|f| data_model::FunctionURI {
                namespace: f.namespace.clone(),
                compute_graph_name: f.compute_graph.clone(),
                compute_fn_name: f.compute_fn.clone(),
                version: f.version.clone().map(|v| v.into()),
            })
            .collect()
    });
    let err = state
        .executor_manager
        .register_executor(data_model::ExecutorMetadata {
            id: executor_id.clone(),
            executor_version: payload.executor_version.clone(),
            addr: payload.addr.clone(),
            function_allowlist,
            labels: payload.labels.clone(),
        })
        .await;
    if let Err(e) = err {
        error!("failed to register executor {}: {:?}", executor_id, e);
        return Err(IndexifyAPIError::internal_error_str(&e.to_string()));
    }
    let stream = state_store::task_stream(state.indexify_state, executor_id.clone(), TASK_LIMIT);
    let executor_manager = state.executor_manager.clone();
    let stream = stream
        .map(|item| match item {
            Ok(item) => {
                let item: Vec<Task> = item.into_iter().map(Into::into).collect();
                axum::response::sse::Event::default().json_data(item)
            }
            Err(e) => {
                error!("error in task stream: {:?}", e);
                Err(axum::Error::new(e))
            }
        })
        .guard(|| executors::schedule_deregister(executor_manager, executor_id, EXECUTOR_TIMEOUT));
    Ok(axum::response::Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(1))
            .text("keep-alive-text"),
    ))
}

/// List tasks for an invocation
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}/invocations/{invocation_id}/tasks",
    tag = "operations",
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
    let (tasks, cursor) = state
        .indexify_state
        .reader()
        .list_tasks_by_compute_graph(
            &namespace,
            &compute_graph,
            &invocation_id,
            params.cursor.as_deref(),
            params.limit,
        )
        .map_err(IndexifyAPIError::internal_error)?;
    let tasks = tasks.into_iter().map(Into::into).collect();
    Ok(Json(Tasks { tasks, cursor }))
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
            params.cursor.as_deref(),
            params.limit,
        )
        .map_err(IndexifyAPIError::internal_error)?;
    let outputs = outputs.into_iter().map(Into::into).collect();

    let status = if invocation_ctx.completed {
        InvocationStatus::Finalized
    } else if invocation_ctx.outstanding_tasks > 0 {
        InvocationStatus::Running
    } else {
        InvocationStatus::Pending
    };

    // We return the outputs of finalized and pending invocations to allow getting
    // partial results.
    Ok(Json(FnOutputs {
        status,
        outcome: invocation_ctx.outcome.into(),
        outputs,
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

    Ok(Json(invocation_ctx.into()))
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
                    "unexpected field: {}",
                    name
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

#[axum::debug_handler]
async fn service_metrics(
    State(state): State<RouteState>,
) -> Result<Response<Body>, IndexifyAPIError> {
    let metric_families = state.registry.gather();
    let mut buffer = vec![];
    let encoder = prometheus::TextEncoder::new();
    encoder.encode(&metric_families, &mut buffer).map_err(|e| {
        tracing::error!("failed to encode metrics: {:?}", e);
        IndexifyAPIError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to encode metrics",
        )
    })?;

    Ok(Response::new(Body::from(buffer)))
}
