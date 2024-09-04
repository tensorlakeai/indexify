use anyhow::Result;
use axum::{
    extract::{Multipart, Path, State},
    response::IntoResponse,
    routing::{delete, get, post},
    Json,
    Router,
};
use data_model::Namespace;
use state_store::{
    requests::{NamespaceRequest, RequestType},
    state_machine::IndexifyObjectsColumns,
    IndexifyState,
};

use crate::http_objects::{
    make_compute_graph,
    ComputeFn,
    ComputeGraph,
    ComputeGraphRequest,
    ComputeGraphsList,
    CreateNamespace,
    DataObject,
    IndexifyAPIError,
    NamespaceList,
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
    let res = state
        .indexify_state
        .reader()
        .get_all_rows_from_cf::<Namespace>(IndexifyObjectsColumns::Namespaces)
        .map_err(|e| IndexifyAPIError::internal_error(e))?;
    Ok(Json(NamespaceList {
        namespaces: res.into_iter().map(|(_, v)| v.into()).collect(),
    }))
}

#[axum::debug_handler]
async fn create_compute_graph(
    Path(namespace): Path<String>,
    State(state): State<RouteState>,
    Json(compute_graph): Json<ComputeGraphRequest>,
) -> Result<(), IndexifyAPIError> {
    state
        .indexify_state
        .write(RequestType::CreateComputeGraph(make_compute_graph(
            namespace,
            compute_graph,
        )))
        .await
        .map_err(|e| IndexifyAPIError::internal_error(e))?;
    Ok(())
}

async fn delete_compute_graph(
    Path((_namespace, _name)): Path<(String, String)>,
    State(_state): State<RouteState>,
) -> Result<(), IndexifyAPIError> {
    Ok(())
}

async fn list_compute_graphs(
    Path(namespace): Path<String>,
    State(state): State<RouteState>,
) -> Result<Json<ComputeGraphsList>, IndexifyAPIError> {
    let res = state
        .indexify_state
        .reader()
        .filter_cf::<data_model::ComputeGraph, _>(
            IndexifyObjectsColumns::ComputeGraphs,
            |_| true,
            data_model::ComputeGraph::namespace_prefix(&namespace).as_bytes(),
            None,
            None,
        )
        .map_err(|e| IndexifyAPIError::internal_error(e))?;
    Ok(Json(ComputeGraphsList {
        compute_graphs: res.items.into_iter().map(|v| v.into()).collect(),
    }))
}

async fn get_compute_graph(
    Path((_namespace, _compute_graph)): Path<(String, String)>,
    State(_state): State<RouteState>,
) -> Result<Json<ComputeGraph>, IndexifyAPIError> {
    Ok(Json(ComputeGraph {
        name: "test".to_string(),
        namespace: "test".to_string(),
        description: "test".to_string(),
        code_path: "test".to_string(),
        start_fn: ComputeFn {
            name: "test".to_string(),
            fn_name: "test".to_string(),
            description: "test".to_string(),
            placement_contstraints: Default::default(),
        },
        edges: Default::default(),
        created_at: Default::default(),
    }))
}

async fn ingested_data(
    Path((_namespace, _compute_graph)): Path<(String, String)>,
    State(_state): State<RouteState>,
) -> Result<Json<Vec<DataObject>>, IndexifyAPIError> {
    Ok(Json(vec![]))
}

async fn upload_data(
    Path((_namespace, _compute_graph)): Path<(String, String)>,
    State(_state): State<RouteState>,
    _files: Multipart,
) -> Result<(), IndexifyAPIError> {
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
