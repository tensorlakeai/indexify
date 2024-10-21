use std::{sync::Arc, time::Duration};

use anyhow::Result;
use axum::{
    body::Body,
    extract::{DefaultBodyLimit, MatchedPath, Multipart, Path, Query, Request, State},
    http::{Method, Response},
    response::{sse::Event, IntoResponse},
    routing::{delete, get, post},
    Json,
    Router,
};
use blob_store::PutResult;
use data_model::ExecutorId;
use futures::StreamExt;
use indexify_ui::Assets as UiAssets;
use indexify_utils::GuardStreamExt;
use nanoid::nanoid;
use state_store::{
    requests::{
        CreateComputeGraphRequest,
        DeleteComputeGraphRequest,
        DeleteInvocationRequest,
        NamespaceRequest,
        RequestPayload,
        StateMachineUpdateRequest,
    },
    IndexifyState,
};
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing::info;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use crate::executors::{self, EXECUTOR_TIMEOUT};

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
use invoke::{invoke_with_file, invoke_with_object, rerun_compute_graph};
use logs::download_logs;

use crate::{
    executors::ExecutorManager,
    http_objects::{
        ComputeFn,
        ComputeGraph,
        ComputeGraphsList,
        CreateNamespace,
        DataObject,
        DynamicRouter,
        ExecutorMetadata,
        FnOutputs,
        GraphInvocations,
        GraphVersion,
        ImageInformation,
        IndexifyAPIError,
        InvocationResult,
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
            create_compute_graph,
            list_compute_graphs,
            get_compute_graph,
            delete_compute_graph,
            list_tasks,
            list_outputs,
            delete_invocation,
            logs::download_logs,
            list_executors,
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
                InvocationResult,
                ExecutorMetadata,
                RuntimeInformation,
                Task,
                TaskOutcome,
                Tasks,
                GraphInvocations,
                GraphVersion,
                DataObject,
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
    pub executor_manager: Arc<ExecutorManager>,
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
        .route(
            "/namespaces/:namespace/compute_graphs",
            post(create_compute_graph).with_state(route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs",
            get(list_compute_graphs).with_state(route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph",
            delete(delete_compute_graph).with_state(route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph",
            get(get_compute_graph).with_state(route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph/invocations/:invocation_id/tasks",
            get(list_tasks).with_state(route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph/invocations/:invocation_id/outputs",
            get(list_outputs).with_state(route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph/invocations/:invocation_id/context",
            get(get_context).with_state(route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph/invocations",
            get(graph_invocations).with_state(route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph/invoke_file",
            post(invoke_with_file).with_state(route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph/invoke_object",
            post(invoke_with_object).with_state(route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph/rerun",
            post(rerun_compute_graph).with_state(route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph/invocations/:invocation_id",
            delete(delete_invocation).with_state(route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph/notify",
            get(notify_on_change).with_state(route_state.clone()),
        )
        .route(
            "/internal/namespaces/:namespace/compute_graphs/:compute_graph/code",
            get(get_code).with_state(route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph/invocations/:invocation_id/payload",
            get(download_invocation_payload).with_state(route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph/invocations/:invocation_id/fn/:fn_name/output/:id",
            get(download_fn_output_payload).with_state(route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph/invocations/:invocation_id/fn/:fn_name/logs/:file",
            get(download_logs).with_state(route_state.clone()),
        )
        .route(
            "/internal/ingest_files",
            post(ingest_files_from_executor).with_state(route_state.clone()),
        )
        .route("/internal/executors", get(list_executors).with_state(route_state.clone()))
        .route(
            "/internal/executors/:id/tasks",
            post(executor_tasks).with_state(route_state.clone()),
        )
        .route(
            "/internal/fn_outputs/:input_key",
            get(download_fn_output_by_key).with_state(route_state.clone()),
        )
        .route("/ui", get(ui_index_handler))
        .route("/ui/*rest", get(ui_handler))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|req: &Request| {
                    let method = req.method();
                    let uri = req.uri();

                    let matched_path = req
                        .extensions()
                        .get::<MatchedPath>()
                        .map(|matched_path| matched_path.as_str());

                    tracing::debug_span!("request", %method, %uri, matched_path)
                })
                .on_failure(()),
        )
        .layer(cors)
        .layer(DefaultBodyLimit::max(usize::MAX))
}

async fn index() -> &'static str {
    "Indexify Server"
}

#[axum::debug_handler]
#[tracing::instrument(skip_all)]
async fn ui_index_handler() -> impl IntoResponse {
    let content = UiAssets::get("index.html").unwrap();
    (
        [(hyper::header::CONTENT_TYPE, content.metadata.mimetype())],
        content.data,
    )
        .into_response()
}

#[axum::debug_handler]
#[tracing::instrument(skip_all)]
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
    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::CreateNameSpace(NamespaceRequest {
                name: namespace.name,
            }),
            state_changes_processed: vec![],
        })
        .await
        .map_err(IndexifyAPIError::internal_error)?;
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
        (status = 200, description = "Create a Compute Graph"),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to create compute graphs")
    ),
)]
async fn create_compute_graph(
    Path(namespace): Path<String>,
    State(state): State<RouteState>,
    mut compute_graph_code: Multipart,
) -> Result<(), IndexifyAPIError> {
    let mut compute_graph_definition: Option<ComputeGraph> = Option::None;
    let mut put_result: Option<PutResult> = None;
    while let Some(field) = compute_graph_code.next_field().await.unwrap() {
        let name = field.name();
        if let Some(name) = name {
            if name == "code" {
                let stream = field.map(|res| res.map_err(|err| anyhow::anyhow!(err)));
                let file_name = format!("{}_{}", namespace, nanoid!());
                let result = state
                    .blob_storage
                    .put(&file_name, stream)
                    .await
                    .map_err(|e| IndexifyAPIError::internal_error(e))?;
                put_result = Some(result);
            } else if name == "compute_graph" {
                let text = field
                    .text()
                    .await
                    .map_err(|e| IndexifyAPIError::bad_request(&e.to_string()))?;
                let mut json_value: serde_json::Value = serde_json::from_str(&text)?;
                json_value["namespace"] = serde_json::Value::String(namespace.clone());
                compute_graph_definition = Some(serde_json::from_value(json_value)?);
            }
        }
    }

    if compute_graph_definition.is_none() {
        return Err(IndexifyAPIError::bad_request(
            "Compute graph definition is required",
        ));
    }

    if put_result.is_none() {
        return Err(IndexifyAPIError::bad_request("Code is required"));
    }
    let put_result = put_result.unwrap();
    let compute_graph_definition = compute_graph_definition.unwrap();
    let compute_graph = compute_graph_definition.into_data_model(
        &put_result.url,
        &put_result.sha256_hash,
        put_result.size_bytes,
    )?;
    let name = compute_graph.name.clone();
    let request = RequestPayload::CreateComputeGraph(CreateComputeGraphRequest {
        namespace,
        compute_graph,
    });
    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: request,
            state_changes_processed: vec![],
        })
        .await
        .map_err(IndexifyAPIError::internal_error)?;
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
    let request = RequestPayload::DeleteComputeGraph(DeleteComputeGraphRequest {
        namespace,
        name: compute_graph,
    });
    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: request,
            state_changes_processed: vec![],
        })
        .await
        .map_err(IndexifyAPIError::internal_error)?;
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
    let (data_objects, cursor) = state
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
    for data_object in data_objects {
        invocations.push(DataObject {
            id: data_object.id,
            payload_size: data_object.payload.size,
            payload_sha_256: data_object.payload.sha256_hash,
        });
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

async fn executor_tasks(
    Path(executor_id): Path<ExecutorId>,
    State(state): State<RouteState>,
    Json(payload): Json<ExecutorMetadata>,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    const TASK_LIMIT: usize = 10;
    let err = state
        .executor_manager
        .register_executor(data_model::ExecutorMetadata {
            id: executor_id.clone(),
            image_name: payload.image_name.clone(),
            addr: payload.addr.clone(),
            labels: payload.labels.clone(),
        })
        .await;
    if let Err(e) = err {
        tracing::error!("failed to register executor {}: {:?}", executor_id, e);
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
                tracing::error!("error in task stream: {}", e);
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
    Ok(Json(FnOutputs { outputs, cursor }))
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
    let request = RequestPayload::DeleteInvocation(DeleteInvocationRequest {
        namespace,
        compute_graph,
        invocation_id,
    });
    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: request,
            state_changes_processed: vec![],
        })
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    Ok(())
}

async fn get_code(
    Path((namespace, compute_graph)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    let compute_graph = state
        .indexify_state
        .reader()
        .get_compute_graph(&namespace, &compute_graph)
        .map_err(IndexifyAPIError::internal_error)?;
    if compute_graph.is_none() {
        return Err(IndexifyAPIError::not_found("Compute Graph not found"));
    }
    let compute_graph = compute_graph.unwrap();
    let storage_reader = state.blob_storage.get(&compute_graph.code.path);
    let code_stream = storage_reader
        .get()
        .await
        .map_err(|e| IndexifyAPIError::internal_error(e))?;

    Response::builder()
        .header("Content-Type", "application/octet-stream")
        .header(
            "Content-Length",
            compute_graph.code.clone().size.to_string(),
        )
        .body(Body::from_stream(code_stream))
        .map_err(|e| IndexifyAPIError::internal_error_str(&e.to_string()))
}
