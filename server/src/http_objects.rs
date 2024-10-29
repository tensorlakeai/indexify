use std::{collections::HashMap, fmt};

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use data_model::ComputeGraphCode;
use indexify_utils::get_epoch_time_in_ms;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, ToSchema, Serialize, Deserialize)]
pub struct IndexifyAPIError {
    #[serde(skip)]
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

    pub fn _bad_request(e: &str) -> Self {
        Self::new(StatusCode::BAD_REQUEST, e)
    }

    pub fn internal_error(e: anyhow::Error) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string().as_str())
    }

    pub fn internal_error_str(e: &str) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, e)
    }

    pub fn not_found(message: &str) -> Self {
        Self::new(StatusCode::NOT_FOUND, message)
    }

    pub fn bad_request(message: &str) -> Self {
        Self::new(StatusCode::BAD_REQUEST, message)
    }
}

impl IntoResponse for IndexifyAPIError {
    fn into_response(self) -> Response {
        tracing::error!("API Error: {} - {}", self.status_code, self.message);
        (self.status_code, self.message).into_response()
    }
}

impl From<serde_json::Error> for IndexifyAPIError {
    fn from(e: serde_json::Error) -> Self {
        Self::bad_request(&e.to_string())
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ListParams {
    pub limit: Option<usize>,
    pub cursor: Option<Vec<u8>>,
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

#[derive(Clone, Serialize, Deserialize, ToSchema)]
pub struct ImageInformation {
    pub image_name: String,
    pub tag: String,
    pub base_image: String,
    pub run_strs: Vec<String>,
}

impl fmt::Debug for ImageInformation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ImageInformation")
            .field("image_name", &self.image_name)
            .field("tag", &self.tag)
            .field("base_image", &self.base_image)
            .field("run_strs", &self.run_strs)
            .finish()
    }
}

impl From<ImageInformation> for data_model::ImageInformation {
    fn from(value: ImageInformation) -> Self {
        data_model::ImageInformation::new(
            value.image_name,
            value.tag,
            value.base_image,
            value.run_strs,
        )
    }
}

impl From<data_model::ImageInformation> for ImageInformation {
    fn from(value: data_model::ImageInformation) -> ImageInformation {
        ImageInformation {
            image_name: value.image_name,
            tag: value.tag,
            base_image: value.base_image,
            run_strs: value.run_strs,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct ComputeFn {
    pub name: String,
    pub fn_name: String,
    pub description: String,
    pub reducer: bool,
    pub payload_encoder: String,
    pub image_name: String,
    pub image_information: ImageInformation,
}

impl From<ComputeFn> for data_model::ComputeFn {
    fn from(val: ComputeFn) -> Self {
        data_model::ComputeFn {
            name: val.name.clone(),
            fn_name: val.fn_name.clone(),
            description: val.description.clone(),
            placement_constraints: Default::default(),
            reducer: val.reducer,
            payload_encoder: val.payload_encoder.clone(),
            image_name: val.image_name.clone(),
            image_information: val.image_information.into(),
        }
    }
}

impl From<data_model::ComputeFn> for ComputeFn {
    fn from(c: data_model::ComputeFn) -> Self {
        Self {
            name: c.name,
            fn_name: c.fn_name,
            description: c.description,
            reducer: c.reducer,
            payload_encoder: c.payload_encoder,
            image_name: c.image_name,
            image_information: c.image_information.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct DynamicRouter {
    pub name: String,
    pub source_fn: String,
    pub description: String,
    pub target_fns: Vec<String>,
    pub payload_encoder: String,
    pub image_name: String,
    pub image_information: ImageInformation,
}

impl From<DynamicRouter> for data_model::DynamicEdgeRouter {
    fn from(val: DynamicRouter) -> Self {
        data_model::DynamicEdgeRouter {
            name: val.name.clone(),
            source_fn: val.source_fn.clone(),
            description: val.description.clone(),
            target_functions: val.target_fns.clone(),
            payload_encoder: val.payload_encoder.clone(),
            image_name: val.image_name.clone(),
            image_information: val.image_information.clone().into(),
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
            payload_encoder: d.payload_encoder,
            image_name: d.image_name,
            image_information: d.image_information.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub enum Node {
    #[serde(rename = "dynamic_router")]
    DynamicRouter(DynamicRouter),
    #[serde(rename = "compute_fn")]
    ComputeFn(ComputeFn),
}

impl Node {
    pub fn name(&self) -> String {
        match self {
            Node::DynamicRouter(d) => d.name.clone(),
            Node::ComputeFn(c) => c.name.clone(),
        }
    }
}

impl From<Node> for data_model::Node {
    fn from(val: Node) -> Self {
        match val {
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
pub struct RuntimeInformation {
    pub major_version: u8,
    pub minor_version: u8,
}

impl From<RuntimeInformation> for data_model::RuntimeInformation {
    fn from(value: RuntimeInformation) -> Self {
        data_model::RuntimeInformation {
            major_version: value.major_version,
            minor_version: value.minor_version,
        }
    }
}

impl From<data_model::RuntimeInformation> for RuntimeInformation {
    fn from(value: data_model::RuntimeInformation) -> Self {
        Self {
            major_version: value.major_version,
            minor_version: value.minor_version,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ComputeGraph {
    pub name: String,
    pub namespace: String,
    pub description: String,
    pub start_node: Node,
    pub nodes: HashMap<String, Node>,
    pub edges: HashMap<String, Vec<String>>,
    #[serde(default = "get_epoch_time_in_ms")]
    pub created_at: u64,
    pub runtime_information: RuntimeInformation,
}

impl ComputeGraph {
    pub fn into_data_model(
        self,
        code_path: &str,
        sha256_hash: &str,
        size: u64,
    ) -> Result<data_model::ComputeGraph, IndexifyAPIError> {
        let mut nodes = HashMap::new();
        for (name, node) in self.nodes {
            nodes.insert(name, node.into());
        }
        let start_fn: data_model::Node = self.start_node.into();

        let compute_graph = data_model::ComputeGraph {
            name: self.name,
            namespace: self.namespace,
            description: self.description,
            start_fn,
            version: Default::default(),
            code: ComputeGraphCode {
                sha256_hash: sha256_hash.to_string(),
                size,
                path: code_path.to_string(),
            },
            nodes,
            edges: self.edges.clone(),
            created_at: 0,
            runtime_information: self.runtime_information.into(),
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
        let mut nodes = HashMap::new();
        for (k, v) in compute_graph.nodes.into_iter() {
            nodes.insert(k, v.into());
        }
        Self {
            name: compute_graph.name,
            namespace: compute_graph.namespace,
            description: compute_graph.description,
            start_node: start_fn,
            nodes,
            edges: compute_graph.edges,
            created_at: compute_graph.created_at,
            runtime_information: compute_graph.runtime_information.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateNamespace {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ComputeGraphsList {
    pub compute_graphs: Vec<ComputeGraph>,
    pub cursor: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DataObject {
    pub id: String,
    pub payload_size: u64,
    pub payload_sha_256: String,
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

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateNamespaceResponse {
    pub name: Namespace,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct GraphInvocations {
    pub invocations: Vec<DataObject>,
    pub cursor: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct GraphInputJson {
    pub payload: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct GraphInputFile {
    // file:///s3://bucket/key
    // file:///data/path/to/file
    pub url: String,
    pub metadata: serde_json::Value,
    pub sha_256: String,
    pub size: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct InvocationResult {
    pub outputs: HashMap<String, Vec<DataObject>>,
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub enum TaskOutcome {
    Unknown,
    Success,
    Failure,
}

impl From<data_model::TaskOutcome> for TaskOutcome {
    fn from(outcome: data_model::TaskOutcome) -> Self {
        match outcome {
            data_model::TaskOutcome::Unknown => TaskOutcome::Unknown,
            data_model::TaskOutcome::Success => TaskOutcome::Success,
            data_model::TaskOutcome::Failure => TaskOutcome::Failure,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct GraphVersion(pub u32);

impl From<data_model::GraphVersion> for GraphVersion {
    fn from(version: data_model::GraphVersion) -> Self {
        Self(version.0)
    }
}

impl From<GraphVersion> for data_model::GraphVersion {
    fn from(version: GraphVersion) -> Self {
        Self(version.0)
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Task {
    pub id: String,
    pub namespace: String,
    pub compute_fn: String,
    pub compute_graph: String,
    pub invocation_id: String,
    pub input_key: String,
    pub outcome: TaskOutcome,
    pub reducer_output_id: Option<String>,
    pub graph_version: GraphVersion,
}

impl From<data_model::Task> for Task {
    fn from(task: data_model::Task) -> Self {
        Self {
            id: task.id.to_string(),
            namespace: task.namespace,
            compute_fn: task.compute_fn_name,
            compute_graph: task.compute_graph_name,
            invocation_id: task.invocation_id,
            input_key: task.input_node_output_key,
            outcome: task.outcome.into(),
            reducer_output_id: task.reducer_output_id,
            graph_version: task.graph_version.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Tasks {
    pub tasks: Vec<Task>,
    pub cursor: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct FnOutput {
    pub compute_fn: String,
    pub id: String,
}

impl From<data_model::NodeOutput> for FnOutput {
    fn from(output: data_model::NodeOutput) -> Self {
        Self {
            compute_fn: output.compute_fn_name,
            id: output.id.to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct FnOutputs {
    pub outputs: Vec<FnOutput>,
    pub cursor: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct InvocationId {
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ExecutorMetadata {
    pub id: String,
    pub executor_version: String,
    pub addr: String,
    pub image_name: String,
    pub image_version: u32,
    pub labels: HashMap<String, serde_json::Value>,
}

impl From<data_model::ExecutorMetadata> for ExecutorMetadata {
    fn from(executor: data_model::ExecutorMetadata) -> Self {
        Self {
            id: executor.id.to_string(),
            executor_version: executor.executor_version,
            addr: executor.addr,
            image_name: executor.image_name,
            image_version: executor.image_version,
            labels: executor.labels,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct InvocationQueryParams {
    pub block_until_finish: Option<bool>,
}

#[cfg(test)]
mod tests {
    use crate::http_objects::{ComputeFn, DynamicRouter};

    #[test]
    fn test_compute_graph_deserialization() {
        // Don't delete this. It makes it easier
        // to test the deserialization of the ComputeGraph struct
        // from the python side
        let json = r#"{"name":"test","description":"test","start_node":{"compute_fn":{"name":"extractor_a","fn_name":"extractor_a","description":"Random description of extractor_a", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"]}, "payload_encoder":"cloudpickle", "image_name": "default_image"}},"nodes":{"extractor_a":{"compute_fn":{"name":"extractor_a","fn_name":"extractor_a","description":"Random description of extractor_a", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"]}, "payload_encoder":"cloudpickle", "image_name": "default_image"}},"extractor_b":{"compute_fn":{"name":"extractor_b","fn_name":"extractor_b","description":"", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"]}, "payload_encoder":"cloudpickle", "image_name": "default_image"}},"extractor_c":{"compute_fn":{"name":"extractor_c","fn_name":"extractor_c","description":"", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"]}, "payload_encoder":"cloudpickle", "image_name": "default_image"}}},"edges":{"extractor_a":["extractor_b"],"extractor_b":["extractor_c"]},"runtime_information": {"major_version": 3, "minor_version": 10}}"#;
        let mut json_value: serde_json::Value = serde_json::from_str(json).unwrap();
        json_value["namespace"] = serde_json::Value::String("test".to_string());
        let _: super::ComputeGraph = serde_json::from_value(json_value).unwrap();
    }

    #[test]
    fn test_compute_graph_with_router_deserialization() {
        let json = r#"{"name":"graph_a_router","description":"description of graph_a","start_node":{"compute_fn":{"name":"extractor_a","fn_name":"extractor_a","description":"Random description of extractor_a", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"]}, "payload_encoder":"cloudpickle", "image_name": "default_image"}},"nodes":{"extractor_a":{"compute_fn":{"name":"extractor_a","fn_name":"extractor_a","description":"Random description of extractor_a", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"]}, "payload_encoder":"cloudpickle", "image_name": "default_image"}},"router_x":{"dynamic_router":{"name":"router_x","description":"","source_fn":"router_x","target_fns":["extractor_y","extractor_z"], "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"]}, "payload_encoder":"cloudpickle", "image_name": "default_image"}},"extractor_y":{"compute_fn":{"name":"extractor_y","fn_name":"extractor_y","description":"", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"]}, "payload_encoder":"cloudpickle", "image_name": "default_image"}},"extractor_z":{"compute_fn":{"name":"extractor_z","fn_name":"extractor_z","description":"", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"]}, "payload_encoder":"cloudpickle", "image_name": "default_image"}},"extractor_c":{"compute_fn":{"name":"extractor_c","fn_name":"extractor_c","description":"", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"]}, "payload_encoder":"cloudpickle", "image_name": "default_image"}}},"edges":{"extractor_a":["router_x"],"extractor_y":["extractor_c"],"extractor_z":["extractor_c"]},"runtime_information": {"major_version": 3, "minor_version": 10}}"#;
        let mut json_value: serde_json::Value = serde_json::from_str(json).unwrap();
        json_value["namespace"] = serde_json::Value::String("test".to_string());
        let _: super::ComputeGraph = serde_json::from_value(json_value).unwrap();
    }

    #[test]
    fn test_compute_fn_deserialization() {
        let json = r#"{"name": "one", "fn_name": "two", "description": "desc", "reducer": true, "image_name": "im1", "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"]}, "payload_encoder": "clouds"}"#;
        let compute_fn: ComputeFn = serde_json::from_str(json).unwrap();
        println!("{:?}", compute_fn);
    }

    #[test]
    fn test_router_deserialization() {
        let json = r#"{"name": "one", "source_fn": "two", "description": "desc", "target_fns": ["one", "two", "three"], "image_name": "im1", "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"]}, "payload_encoder": "clouds"}"#;
        let dynamic_router: DynamicRouter = serde_json::from_str(json).unwrap();
        println!("{:?}", dynamic_router);
    }
}
