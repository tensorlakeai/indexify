use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use data_model::{self, filter::LabelsFilter};
use serde::{Deserialize, Serialize};
use serde_json::value;
use std::collections::HashMap;
use utoipa::ToSchema;

#[derive(Debug, ToSchema)]
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

    pub fn not_found(message: &str) -> Self {
        Self::new(StatusCode::NOT_FOUND, message)
    }
}

impl IntoResponse for IndexifyAPIError {
    fn into_response(self) -> Response {
        tracing::error!("API Error: {} - {}", self.status_code, self.message);
        (self.status_code, self.message).into_response()
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Namespace {
    name: String,
    created_at: u64,
}

impl From<data_model::Namespace> for Namespace {
    fn from(namespace: data_model::Namespace) -> Self {
        Self {
            name: namespace.name,
            created_at: namespace.created_at,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct NamespaceList {
    pub namespaces: Vec<Namespace>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ComputeFn {
    pub name: String,
    pub fn_name: String,
    pub description: String,
}

impl Into<data_model::ComputeFn> for &ComputeFn {
    fn into(self) -> data_model::ComputeFn {
        data_model::ComputeFn {
            name: self.name.clone(),
            fn_name: self.fn_name.clone(),
            description: self.description.clone(),
            placement_constraints: Default::default(),
        }
    }
}

impl Into<data_model::ComputeFn> for ComputeFn {
    fn into(self) -> data_model::ComputeFn {
        data_model::ComputeFn {
            name: self.name.clone(),
            fn_name: self.fn_name.clone(),
            description: self.description.clone(),
            placement_constraints: Default::default(),
        }
    }
}

impl From<data_model::ComputeFn> for ComputeFn {
    fn from(c: data_model::ComputeFn) -> Self {
        Self {
            name: c.name,
            fn_name: c.fn_name,
            description: c.description,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DynamicRouter {
    pub name: String,
    pub source_fn: String,
    pub description: String,
    pub target_fns: Vec<String>,
}

impl Into<data_model::DynamicEdgeRouter> for DynamicRouter {
    fn into(self) -> data_model::DynamicEdgeRouter {
        data_model::DynamicEdgeRouter {
            name: self.name.clone(),
            source_fn: self.source_fn.clone(),
            description: self.description.clone(),
            target_functions: self.target_fns.clone(),
        }
    }
}

impl From<data_model::DynamicEdgeRouter> for DynamicRouter {
    fn from(d: data_model::DynamicEdgeRouter) -> Self {
        Self {
            name: d.name,
            source_fn: d.source_fn,
            description: d.description,
            target_fns: d.target_functions,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub enum Node {
    DynamicRouter(DynamicRouter),
    ComputeFn(ComputeFn),
}

impl Into<data_model::Node> for Node {
    fn into(self) -> data_model::Node {
        match self {
            Node::DynamicRouter(d) => data_model::Node::Router(d.into()),
            Node::ComputeFn(c) => data_model::Node::Compute(c.into()),
        }
    }
}

impl From<data_model::Node> for Node {
    fn from(node: data_model::Node) -> Self {
        match node {
            data_model::Node::Router(d) => Node::DynamicRouter(d.into()),
            data_model::Node::Compute(c) => Node::ComputeFn(c.into()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ComputeGraph {
    pub name: String,
    pub namespace: String,
    pub description: String,
    pub start_fn: Node,
    pub edges: HashMap<String, Vec<Node>>,
    pub created_at: u64,
}

impl ComputeGraph {
    pub fn into_data_model(
        self,
        code_path: &str,
    ) -> Result<data_model::ComputeGraph, IndexifyAPIError> {
        let mut edges = HashMap::new();
        for (k, v) in self.edges.into_iter() {
            let v: Vec<data_model::Node> = v.into_iter().map(|e| e.into()).collect();
            edges.insert(k, v);
        }
        let start_fn: data_model::Node = self.start_fn.into();
        let compute_graph = data_model::ComputeGraph {
            name: self.name,
            namespace: self.namespace,
            description: self.description,
            start_fn,
            code_path: code_path.to_string(),
            edges,
            create_at: 0,
            tomb_stoned: false,
        };
        Ok(compute_graph)
    }
}

impl From<data_model::ComputeGraph> for ComputeGraph {
    fn from(compute_graph: data_model::ComputeGraph) -> Self {
        let start_fn = match compute_graph.start_fn {
            data_model::Node::Router(d) => Node::DynamicRouter(d.into()),
            data_model::Node::Compute(c) => Node::ComputeFn(c.into()),
        };
        let mut edges = HashMap::new();
        for (k, v) in compute_graph.edges.into_iter() {
            let v: Vec<Node> = v.into_iter().map(|e| e.into()).collect();
            edges.insert(k, v);
        }
        Self {
            name: compute_graph.name,
            namespace: compute_graph.namespace,
            description: compute_graph.description,
            start_fn,
            edges,
            created_at: compute_graph.create_at,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateNamespace {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ComputeGraphsList {
    pub compute_graphs: Vec<ComputeGraph>,
    pub cursor: Option<String>,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct FileUpload {
    // file:///s3://bucket/key
    // file:///data/path/to/file
    pub payload: String,
    pub labels: HashMap<String, String>,
}
