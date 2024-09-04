use anyhow::{anyhow, Result};
use data_model::DataObjectBuilder;
use uuid::Uuid;
use sha2::Sha256;
use sha2::Digest;
use axum::{
    extract::{Multipart, Path, State},
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use futures::StreamExt;
use nanoid::nanoid;
use tracing::info;
use std::sync::Arc;

use state_store::{
    requests::{
        CreateComputeGraphRequest, DeleteComputeGraphRequest, InvokeComputeGraphRequest, NamespaceRequest, RequestType
    },
    IndexifyState,
};
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use crate::http_objects::{
    ComputeGraph, ComputeGraphsList, CreateNamespace, DataObject, IndexifyAPIError, Namespace,
    NamespaceList, Node, DynamicRouter, ComputeFn
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

pub fn create_routes(_route_state: RouteState) -> Router {
    let app = Router::new()
        .merge(SwaggerUi::new("/docs/swagger").url("/docs/openapi.json", ApiDoc::openapi()))
        .route("/", get(index))
        .route(
            "/namespaces",
            get(namespaces).with_state(_route_state.clone()),
        )
        .route(
            "/namespaces",
            post(create_namespace).with_state(_route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs",
            post(create_compute_graph).with_state(_route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs",
            get(list_compute_graphs).with_state(_route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs",
            delete(delete_compute_graph).with_state(_route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph",
            get(get_compute_graph).with_state(_route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph/inputs",
            get(ingested_data).with_state(_route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph/inputs",
            post(upload_data).with_state(_route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph/invocations/:object_id/outputs/:object_id",
            get(get_output).with_state(_route_state.clone()),
        )
        .route(
            "/namespaces/:namespace/compute_graphs/:compute_graph/notify",
            get(notify_on_change).with_state(_route_state.clone()),
        );

    app
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
        .map_err(|e| IndexifyAPIError::internal_error(e))?;
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
        .map_err(|e| IndexifyAPIError::internal_error(e))?;
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
    let mut code_url: Option<String> = None;
    while let Some(field) = compute_graph_code.next_field().await.unwrap() {
        let name = field.name().clone();
        if let Some(name) = name {
            if name == "code" {
                let stream = field.map(|res| res.map_err(|err| anyhow::anyhow!(err)));

                let file_name=format!("{}_{}", namespace, nanoid!());

                let put_result = state.blob_storage.put(&file_name, stream).await.map_err(|e| IndexifyAPIError::internal_error(e))?;
                code_url = Some(put_result.url);
            } else if name == "compute_graph" {
                let text = field.text().await.map_err(|e| IndexifyAPIError::bad_request(&e.to_string()))?;
                compute_graph_definition = Some(serde_json::from_str(&text)?);
            }
        }
    }

    if compute_graph_definition.is_none() {
        return Err(IndexifyAPIError::bad_request("Compute graph definition is required"));
    }

    if code_url.is_none() {
        return Err(IndexifyAPIError::bad_request("Code is required"));
    }
    let compute_graph_definition = compute_graph_definition.unwrap();
    let code_url = code_url.unwrap();

    let compute_graph = compute_graph_definition.into_data_model(&code_url)?;
    let name = compute_graph.name.clone();
    let request = RequestType::CreateComputeGraph(CreateComputeGraphRequest {
        namespace,
        compute_graph,
    });
    state
        .indexify_state
        .write(request)
        .await
        .map_err(|e| IndexifyAPIError::internal_error(e))?;
    info!("compute graph created: {}", name);
    Ok(())
}

/// Delete compute graph
#[utoipa::path(
    delete,
    path = "/namespaces/{namespace}/compute_graphs/{name}",
    tag = "ingestion",
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
        .map_err(|e| IndexifyAPIError::internal_error(e))?;
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
        .map_err(|e| IndexifyAPIError::internal_error(e))?;
    Ok(Json(ComputeGraphsList {
        compute_graphs: compute_graphs.into_iter().map(|c| c.into()).collect(),
        cursor: cursor.map(|c| String::from_utf8(c).unwrap()),
    }))
}

/// Get a compute graph definition
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/compute_graphs/{name}",
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
        .map_err(|e| IndexifyAPIError::internal_error(e))?;
    if let Some(compute_graph) = compute_graph {
        return Ok(Json(compute_graph.into()));
    }
    Err(IndexifyAPIError::not_found("Compute Graph not found"))
}

async fn ingested_data(
    Path((_namespace, _compute_graph)): Path<(String, String)>,
    State(_state): State<RouteState>,
) -> Result<Json<Vec<DataObject>>, IndexifyAPIError> {
    Ok(Json(vec![]))
}

#[utoipa::path(
    post,
    path = "/{namespace}/compute_graphs/{name}/inputs",
    tag = "operations",
    responses(
        (status = 200, description = "upload successful"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
async fn upload_data(
    Path((namespace, compute_graph)): Path<(String, String)>,
    State(state): State<RouteState>,
    mut files: Multipart,
) -> Result<(), IndexifyAPIError> {
    while let Some(field) = files.next_field().await.unwrap() {
        if let Some(_) = field.file_name() {
            let name = Uuid::new_v4().to_string();
            info!("writing to blob store, file name = {:?}", name);
            let stream = field.map(|res| res.map_err(|err| anyhow::anyhow!(err)));
            let mut hasher = Sha256::new();
            let hashed_stream = stream.map(|item| {
                item.map(|bytes| {
                    hasher.update(&bytes);
                    bytes
                })
            });
            let res = state.blob_storage.put(&name, hashed_stream).await.map_err(|e| {
                IndexifyAPIError::internal_error(anyhow!("failed to write to blob store: {}", e))
            })?;
            let hash = hasher.finalize();

            let data_object = DataObjectBuilder::default()
                .namespace(namespace.clone())
                .compute_graph_name(compute_graph.clone())
                .compute_fn_name("".to_string())
                .payload_url(res.url)
                .payload_hash(hash.into())
                .build()
            .map_err(|e| {
                IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
            })?;

            state.indexify_state.write(RequestType::InvokeComputeGraph(InvokeComputeGraphRequest{
                namespace: namespace.clone(),
                compute_graph_name: compute_graph.clone(),
                data_object
            })).await.map_err(|e| {
                IndexifyAPIError::internal_error(anyhow!("failed to upload content: {}", e))
            })?;
        }
    }
    Ok(())
}

async fn get_output(
    Path((_namespace, _compute_graph, _object_id)): Path<(String, String, String)>,
    State(_state): State<RouteState>,
) -> Result<Json<DataObject>, IndexifyAPIError> {
    Ok(Json(DataObject {
        id: "test".to_string(),
        data: serde_json::json!({}),
    }))
}

async fn notify_on_change(
    Path((_namespace, _compute_graph)): Path<(String, String)>,
    State(_state): State<RouteState>,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    Ok(())
}
