use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};
use axum::{
    body::Body,
    extract::{Multipart, Path, State},
    http::Response,
    response::IntoResponse,
    routing::{delete, get, post},
    Json,
    Router,
};
use blob_store::PutResult;
use data_model::DataObjectBuilder;
use futures::{stream, StreamExt};
use nanoid::nanoid;
use state_store::{
    requests::{
        CreateComputeGraphRequest,
        DeleteComputeGraphRequest,
        InvokeComputeGraphRequest,
        NamespaceRequest,
        RequestType,
    },
    IndexifyState,
};
use tracing::info;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;

use crate::http_objects::{
    ComputeFn,
    ComputeGraph,
    ComputeGraphsList,
    CreateNamespace,
    DataObject,
    DynamicRouter,
    GraphInputFile,
    GraphInvocations,
    IndexifyAPIError,
    InvocationResult,
    Namespace,
    NamespaceList,
    Node,
    Task,
    TaskOutcome,
    Tasks,
};

#[derive(OpenApi)]
#[openapi(
        paths(
            create_namespace,
            namespaces,
            upload_data,
            create_compute_graph,
            list_compute_graphs,
            get_compute_graph,
            delete_compute_graph,
            get_outputs,
            list_tasks,
            delete_invocation,
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
                InvocationResult,
                Task,
                TaskOutcome,
                Tasks,
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
    pub blob_storage: blob_store::BlobStorage,
}

pub fn create_routes(route_state: RouteState) -> Router {
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
            "/namespaces/:namespace/compute_graphs",
            delete(delete_compute_graph).with_state(route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph",
            get(get_compute_graph).with_state(route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph/tasks",
            get(list_tasks).with_state(route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph/invocations",
            get(graph_invocations).with_state(route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph/invocations",
            post(upload_data).with_state(route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph/invocations/:invocation_id",
            get(get_outputs).with_state(route_state.clone()),
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
}

async fn index() -> &'static str {
    "Indexify Server"
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
        .write(RequestType::CreateNameSpace(NamespaceRequest {
            name: namespace.name,
        }))
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
        .get_all_namespaces(None)
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
    let request = RequestType::CreateComputeGraph(CreateComputeGraphRequest {
        namespace,
        compute_graph,
    });
    state
        .indexify_state
        .write(request)
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    info!("compute graph created: {}", name);
    Ok(())
}

/// Delete compute graph
#[utoipa::path(
    delete,
    path = "/namespaces/{namespace}/compute_graphs/{name}",
    tag = "operations",
    responses(
        (status = 200, description = "Extraction graph deleted successfully"),
        (status = BAD_REQUEST, description = "Unable to delete extraction graph")
    ),
)]
async fn delete_compute_graph(
    Path((namespace, name)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<(), IndexifyAPIError> {
    let request = RequestType::DeleteComputeGraph(DeleteComputeGraphRequest { namespace, name });
    state
        .indexify_state
        .write(request)
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
    State(state): State<RouteState>,
) -> Result<Json<ComputeGraphsList>, IndexifyAPIError> {
    let (compute_graphs, cursor) = state
        .indexify_state
        .reader()
        .list_compute_graphs(&namespace, None)
        .map_err(IndexifyAPIError::internal_error)?;
    Ok(Json(ComputeGraphsList {
        compute_graphs: compute_graphs.into_iter().map(|c| c.into()).collect(),
        cursor: cursor.map(|c| String::from_utf8(c).unwrap()),
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
    path = "/namespaces/{namespace}/compute_graphs/{name}",
    tag = "ingestion",
    responses(
        (status = 200, description = "Compute Graph Definition", body = GraphInvocations),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
async fn graph_invocations(
    Path((namespace, compute_graph)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<Json<GraphInvocations>, IndexifyAPIError> {
    let data_objects = state
        .indexify_state
        .reader()
        .list_invocations(&namespace, &compute_graph, None)
        .map_err(IndexifyAPIError::internal_error)?;
    let mut api_data_objects = vec![];
    for data_object in data_objects {
        let payload = state
            .blob_storage
            .read_bytes(&data_object.payload_url)
            .await
            .map_err(IndexifyAPIError::internal_error)?;
        let payload = serde_json::from_slice(&payload)?;
        api_data_objects.push(DataObject {
            id: data_object.id,
            payload,
            hash: data_object.payload_hash,
        });
    }
    Ok(Json(GraphInvocations {
        invocations: api_data_objects,
        cursor: None,
    }))
}

#[allow(dead_code)]
#[derive(ToSchema)]
struct InvokeWithFile {
    /// Extra metadata for file
    metadata: Option<HashMap<String, serde_json::Value>>,
    #[schema(format = "binary")]
    /// File to upload
    file: Option<String>,

    /// JSON encoded payload for graph input. You should either provide a file
    /// or a payload. Both file and payload are not accepted.
    payload: Option<serde_json::Value>,
}
/// Upload data to a compute graph
#[utoipa::path(
    post,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}/invocations",
    request_body(content_type = "multipart/form-data", content = inline(InvokeWithFile)),
    tag = "ingestion",
    responses(
        (status = 200, description = "upload successful"),
        (status = 400, description = "bad request"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
async fn upload_data(
    Path((namespace, compute_graph)): Path<(String, String)>,
    State(state): State<RouteState>,
    mut files: Multipart,
) -> Result<(), IndexifyAPIError> {
    let mut metadata: Option<serde_json::Value> = None;
    let mut url: Option<String> = None;
    let mut hash: Option<String> = None;

    while let Some(field) = files.next_field().await.unwrap() {
        if let Some(name) = field.name() {
            if name == "file" {
                let name = Uuid::new_v4().to_string();
                info!("writing to blob store, file name = {:?}", name);
                let stream = field.map(|res| res.map_err(|err| anyhow::anyhow!(err)));
                let res = state.blob_storage.put(&name, stream).await.map_err(|e| {
                    IndexifyAPIError::internal_error(anyhow!(
                        "failed to write to blob store: {}",
                        e
                    ))
                })?;
                url = Some(res.url);
                hash = Some(res.sha256_hash);
            } else if name == "metadata" {
                let text = field
                    .text()
                    .await
                    .map_err(|e| IndexifyAPIError::bad_request(&e.to_string()))?;
                let file_metadata = serde_json::from_str(&text)?;
                metadata = Some(file_metadata);
            }
        }
    }
    if url.is_none() {
        return Err(IndexifyAPIError::bad_request("file is required"));
    }
    let payload = GraphInputFile {
        metadata: metadata.unwrap_or_default(),
        url: url.unwrap(),
        sha_256: hash.unwrap(),
    };
    let payload_json = serde_json::to_string(&payload)?;
    let payload_key = Uuid::new_v4().to_string();
    let payload_stream = stream::once(async move {
        let payload_json = payload_json.as_bytes().to_vec().clone();
        Ok(payload_json.into())
    });
    let put_result = state
        .blob_storage
        .put(&payload_key, Box::pin(payload_stream))
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;
    let data_object = DataObjectBuilder::default()
        .namespace(namespace.clone())
        .compute_graph_name(compute_graph.clone())
        .compute_fn_name("".to_string())
        .payload_url(put_result.url)
        .payload_hash(put_result.sha256_hash)
        .build()
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;

    state
        .indexify_state
        .write(RequestType::InvokeComputeGraph(InvokeComputeGraphRequest {
            namespace: namespace.clone(),
            compute_graph_name: compute_graph.clone(),
            data_object,
        }))
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
        })?;
    Ok(())
}

/// Get output of a compute graph invocation
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}/invocations/{invocation_id}",
    tag = "retrieval",
    responses(
        (status = 200, description = "Get outputs of Graph for a given invocation id", body = InvocationResult),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
#[axum::debug_handler]
async fn get_outputs(
    Path((_namespace, _compute_graph, _object_id)): Path<(String, String, String)>,
    State(_state): State<RouteState>,
) -> Result<Json<InvocationResult>, IndexifyAPIError> {
    todo!()
}

async fn notify_on_change(
    Path((_namespace, _compute_graph)): Path<(String, String)>,
    State(_state): State<RouteState>,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    Ok(())
}

/// List tasks for a compute graph invocation
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/compute_graphs/{compute_graph}/tasks",
    tag = "operations",
    responses(
        (status = 200, description = "List tasks for a given invocation id", body = Tasks),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
#[axum::debug_handler]
async fn list_tasks(
    Path((_namespace, _compute_graph, _object_id)): Path<(String, String, String)>,
    State(_state): State<RouteState>,
) -> Result<Json<Tasks>, IndexifyAPIError> {
    todo!()
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
    Path((_namespace, _compute_graph, _object_id)): Path<(String, String, String)>,
    State(_state): State<RouteState>,
) -> Result<Json<Tasks>, IndexifyAPIError> {
    todo!()
}

async fn get_code(
    Path((_namespace, _compute_graph)): Path<(String, String)>,
    State(_state): State<RouteState>,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    let compute_graph = _state
        .indexify_state
        .reader()
        .get_compute_graph(&_namespace, &_compute_graph)
        .map_err(|e| IndexifyAPIError::internal_error(e))?;
    if compute_graph.is_none() {
        return Err(IndexifyAPIError::not_found("Compute Graph not found"));
    }
    let compute_graph = compute_graph.unwrap();
    let storage_reader = _state.blob_storage.get(&compute_graph.code.path);
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
