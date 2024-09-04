use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use data_model;
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Serialize, Deserialize)]
pub struct ComputeFn {
    pub name: String,
    pub fn_name: String,
    pub description: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DynamicRouter {
    pub name: String,
    pub source_fn: String,
    pub target_fns: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Edge {
    DynamicRouter(DynamicRouter),
    ComputeFn(ComputeFn),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ComputeGraph {
    pub name: String,
    pub namespace: String,
    pub description: String,
    pub fns: Vec<ComputeFn>,
    pub edges: HashMap<String, Edge>,
    pub created_at: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
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
