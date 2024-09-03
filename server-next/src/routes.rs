use anyhow::Result;
use axum::{
    extract::{Multipart, Path, State},
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};

use state_store::{IndexifyState, requests::{RequestType, NamespaceRequest}};

use crate::http_objects::{
    ComputeGraph, ComputeGraphsList, CreateNamespace, DataObject, IndexifyAPIError, NamespaceList,
};

#[derive(Clone)]
pub struct RouteState {
    pub indexify_state: IndexifyState,
}

pub fn create_routes(_route_state: RouteState) -> Router {
    let app = Router::new()
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
            "/:namespace/compute_graphs",
            post(create_compute_graph).with_state(_route_state.clone()),
        )
        .route(
            "/:namespace/compute_graphs",
            get(list_compute_graphs).with_state(_route_state.clone()),
        )
        .route(
            "/:namespace/compute_graphs",
            delete(delete_compute_graph).with_state(_route_state.clone()),
        )
        .route(
            "/:namespace/compute_graphs/{:compute_graph}/",
            get(get_compute_graph).with_state(_route_state.clone()),
        )
        .route(
            "/:namespace/compute_graphs/:compute_graph/inputs",
            get(ingested_data).with_state(_route_state.clone()),
        )
        .route(
            "/:namespace/compute_graphs/:compute_graph/inputs",
            post(upload_data).with_state(_route_state.clone()),
        )
        .route(
            "/{:namespace}/compute_graphs/{:compute_graph}/inputs/{object_id}/outputs/{object_id}",
            get(get_output).with_state(_route_state.clone()),
        )
        .route(
            "/{:namespace}/compute_graphs/{:compute_graph}/notify",
            get(notify_on_change).with_state(_route_state.clone()),
        );

    app
}

async fn index() -> &'static str {
    "Indexify Server"
}

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

async fn namespaces(
    State(state): State<RouteState>,
) -> Result<Json<NamespaceList>, IndexifyAPIError> {
    Ok(Json(NamespaceList { namespaces: vec![] }))
}

#[axum::debug_handler]
async fn create_compute_graph(
    Path(namespace): Path<String>,
    State(state): State<RouteState>,
    Json(compute_graph): Json<ComputeGraph>,
) -> Result<(), IndexifyAPIError> {
    Ok(())
}

async fn delete_compute_graph(
    Path((namespace, name)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<(), IndexifyAPIError> {
    Ok(())
}

async fn list_compute_graphs(
    Path(namespace): Path<String>,
    State(state): State<RouteState>,
) -> Result<Json<ComputeGraphsList>, IndexifyAPIError> {
    Ok(Json(ComputeGraphsList {
        compute_graphs: vec![],
    }))
}

async fn get_compute_graph(
    Path((namespace, compute_graph)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<Json<ComputeGraph>, IndexifyAPIError> {
    Ok(Json(ComputeGraph {
        name: "test".to_string(),
        namespace: "test".to_string(),
        description: "test".to_string(),
        fns: vec![],
        edges: Default::default(),
        created_at: 0,
    }))
}

async fn ingested_data(
    Path((namespace, compute_graph)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<Json<Vec<DataObject>>, IndexifyAPIError> {
    Ok(Json(vec![]))
}

async fn upload_data(
    Path((namespace, compute_graph)): Path<(String, String)>,
    State(state): State<RouteState>,
    files: Multipart,
) -> Result<(), IndexifyAPIError> {
    Ok(())
}

async fn get_output(
    Path((namespace, compute_graph, object_id)): Path<(String, String, String)>,
    State(state): State<RouteState>,
) -> Result<Json<DataObject>, IndexifyAPIError> {
    Ok(Json(DataObject {
        id: "test".to_string(),
        data: serde_json::json!({}),
    }))
}

async fn notify_on_change(
    Path((namespace, compute_graph)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    Ok(())
}
