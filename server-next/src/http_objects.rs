use std::{collections::HashMap, time::SystemTime};

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use data_model::filter::LabelsFilter;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct IndexifyAPIError {
    status_code: StatusCode,
    message: String,
}

impl IndexifyAPIError {
    pub fn new(status_code: StatusCode, message: &str) -> Self {
        Self {
            status_code,
            message: message.to_string(),
        }
    }

    pub fn internal_error(e: anyhow::Error) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string().as_str())
    }

    pub fn _not_found(message: &str) -> Self {
        Self::new(StatusCode::NOT_FOUND, message)
    }
}

impl IntoResponse for IndexifyAPIError {
    fn into_response(self) -> Response {
        tracing::error!("API Error: {} - {}", self.status_code, self.message);
        (self.status_code, self.message).into_response()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Namespace {
    name: String,
    created_at: u64,
}

impl From<data_model::Namespace> for Namespace {
    fn from(namespace: data_model::Namespace) -> Self {
        Namespace {
            name: namespace.name,
            created_at: namespace.created_at,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NamespaceList {
    pub namespaces: Vec<Namespace>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ComputeFn {
    pub name: String,
    pub fn_name: String,
    pub description: String,
    pub placement_contstraints: LabelsFilter,
}

impl From<ComputeFn> for data_model::ComputeFn {
    fn from(compute_fn: ComputeFn) -> Self {
        data_model::ComputeFn {
            name: compute_fn.name,
            fn_name: compute_fn.fn_name,
            description: compute_fn.description,
            placement_constraints: compute_fn.placement_contstraints,
        }
    }
}

impl From<data_model::ComputeFn> for ComputeFn {
    fn from(compute_fn: data_model::ComputeFn) -> Self {
        ComputeFn {
            name: compute_fn.name,
            fn_name: compute_fn.fn_name,
            description: compute_fn.description,
            placement_contstraints: compute_fn.placement_constraints,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DynamicRouter {
    pub name: String,
    pub description: String,
    pub source_fn: String,
    pub target_fns: Vec<String>,
}

impl From<DynamicRouter> for data_model::DynamicRouter {
    fn from(router: DynamicRouter) -> Self {
        data_model::DynamicRouter {
            name: router.name,
            description: router.description,
            source_fn: router.source_fn,
            target_fns: router.target_fns,
        }
    }
}

impl From<data_model::DynamicRouter> for DynamicRouter {
    fn from(router: data_model::DynamicRouter) -> Self {
        DynamicRouter {
            name: router.name,
            description: router.description,
            source_fn: router.source_fn,
            target_fns: router.target_fns,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Edge {
    Router(DynamicRouter),
    Compute(ComputeFn),
}

impl From<Edge> for data_model::Edge {
    fn from(edge: Edge) -> Self {
        match edge {
            Edge::Router(v) => data_model::Edge::Router(v.into()),
            Edge::Compute(v) => data_model::Edge::Compute(v.into()),
        }
    }
}

impl From<data_model::Edge> for Edge {
    fn from(edge: data_model::Edge) -> Self {
        match edge {
            data_model::Edge::Router(v) => Edge::Router(v.into()),
            data_model::Edge::Compute(v) => Edge::Compute(v.into()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ComputeGraphRequest {
    pub name: String,
    pub code_path: String,
    pub description: String,
    pub start_fn: ComputeFn,
    pub edges: HashMap<String, Vec<Edge>>,
}

pub fn make_compute_graph(
    namespace: String,
    request: ComputeGraphRequest,
) -> data_model::ComputeGraph {
    data_model::ComputeGraph {
        name: request.name,
        namespace,
        code_path: request.code_path,
        description: request.description,
        start_fn: request.start_fn.into(),
        edges: request
            .edges
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().map(Into::into).collect()))
            .collect(),
        created_at: SystemTime::now(),
        tombstoned: false,
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ComputeGraph {
    pub name: String,
    pub namespace: String,
    pub code_path: String,
    pub description: String,
    pub start_fn: ComputeFn,
    pub edges: HashMap<String, Vec<Edge>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl From<data_model::ComputeGraph> for ComputeGraph {
    fn from(compute_graph: data_model::ComputeGraph) -> Self {
        ComputeGraph {
            name: compute_graph.name,
            namespace: compute_graph.namespace,
            code_path: compute_graph.code_path,
            description: compute_graph.description,
            start_fn: compute_graph.start_fn.into(),
            edges: compute_graph
                .edges
                .into_iter()
                .map(|(k, v)| (k, v.into_iter().map(Into::into).collect()))
                .collect(),
            created_at: compute_graph.created_at.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateNamespace {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ComputeGraphsList {
    pub compute_graphs: Vec<ComputeGraph>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DataObject {
    pub id: String,
    pub data: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryParams {
    pub input_id: Option<String>,
    pub on_graph_end: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GraphOutputNotification {
    pub output_id: String,
    pub compute_graph: String,
    pub fn_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateNamespaceResponse {
    pub name: Namespace,
}
