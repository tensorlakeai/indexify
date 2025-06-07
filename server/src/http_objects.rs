use std::{collections::HashMap, fmt};

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use data_model::{
    ComputeGraphCode,
    FunctionExecutorId,
    FunctionExecutorServerMetadata,
    FunctionExecutorState,
    GraphInvocationCtx,
    GraphInvocationOutcome,
};
use indexify_utils::get_epoch_time_in_ms;
use serde::{Deserialize, Serialize};
use tracing::error;
use utoipa::{IntoParams, ToSchema};

use crate::config::ExecutorConfig;

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
        error!("API Error: {} - {}", self.status_code, self.message);
        (self.status_code, self.message).into_response()
    }
}

impl From<serde_json::Error> for IndexifyAPIError {
    fn from(e: serde_json::Error) -> Self {
        Self::bad_request(&e.to_string())
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub enum CursorDirection {
    #[serde(rename = "forward")]
    Forward,
    #[serde(rename = "backward")]
    Backward,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, IntoParams)]
pub struct ListParams {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub direction: Option<CursorDirection>,
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
    #[serde(default)]
    pub image_hash: String,
    pub tag: String,           // Deprecated
    pub base_image: String,    // Deprecated
    pub run_strs: Vec<String>, // Deprecated
    pub image_uri: Option<String>,
    pub sdk_version: Option<String>,
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
            value.image_hash,
            value.image_uri,
            value.tag,
            value.base_image,
            value.run_strs,
            value.sdk_version,
        )
    }
}

impl From<data_model::ImageInformation> for ImageInformation {
    fn from(value: data_model::ImageInformation) -> ImageInformation {
        ImageInformation {
            image_name: value.image_name,
            image_hash: value.image_hash,
            tag: value.tag,
            base_image: value.base_image,
            run_strs: value.run_strs,
            image_uri: value.image_uri,
            sdk_version: value.sdk_version,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NodeTimeoutSeconds(pub u32);

impl NodeTimeoutSeconds {
    fn validate(&self) -> Result<(), IndexifyAPIError> {
        if self.0 == 0 {
            return Err(IndexifyAPIError::bad_request(
                "Node timeout must be greater than 0",
            ));
        }
        if self.0 > 24 * 60 * 60 {
            return Err(IndexifyAPIError::bad_request(
                "Node timeout must be less than or equal 24 hours",
            ));
        }
        Ok(())
    }
}

impl From<NodeTimeoutSeconds> for data_model::NodeTimeoutMS {
    fn from(value: NodeTimeoutSeconds) -> Self {
        data_model::NodeTimeoutMS(value.0 * 1000)
    }
}

impl From<data_model::NodeTimeoutMS> for NodeTimeoutSeconds {
    fn from(value: data_model::NodeTimeoutMS) -> NodeTimeoutSeconds {
        NodeTimeoutSeconds(value.0 / 1000)
    }
}

impl Default for NodeTimeoutSeconds {
    fn default() -> Self {
        data_model::NodeTimeoutMS::default().into()
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NodeGPUConfig {
    pub count: u32,
    pub model: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NodeResources {
    pub cpus: f64,
    pub memory_mb: u64,
    pub ephemeral_disk_mb: u64,
    #[serde(default, rename = "gpus")]
    pub gpu_configs: Vec<NodeGPUConfig>,
}

impl NodeResources {
    fn validate(&self, executor_config: &ExecutorConfig) -> Result<(), IndexifyAPIError> {
        if self.cpus < 1.0 {
            return Err(IndexifyAPIError::bad_request(
                "CPU shares must be greater or equal to 1.0",
            ));
        }
        if self.cpus > executor_config.max_cpus_per_function as f64 {
            return Err(IndexifyAPIError::bad_request(&format!(
                "CPU shares must be less than or equal to {}",
                executor_config.max_cpus_per_function
            )));
        }
        if self.memory_mb < 1024 {
            return Err(IndexifyAPIError::bad_request(
                "Memory must be greater than or equal to 1 GB",
            ));
        }
        if self.memory_mb > (executor_config.max_memory_gb_per_function * 1024) as u64 {
            return Err(IndexifyAPIError::bad_request(&format!(
                "Memory must be less than or equal to {} GB",
                executor_config.max_memory_gb_per_function
            )));
        }
        if self.ephemeral_disk_mb > (executor_config.max_disk_gb_per_function * 1024) as u64 {
            return Err(IndexifyAPIError::bad_request(&format!(
                "Ephemeral disk must be less than or equal to {} GB",
                executor_config.max_disk_gb_per_function
            )));
        }
        for gpu_config in self.gpu_configs.iter() {
            if gpu_config.count == 0 {
                return Err(IndexifyAPIError::bad_request(
                    "GPU count must be greater than 0",
                ));
            }
            if gpu_config.count > executor_config.max_gpus_per_function {
                return Err(IndexifyAPIError::bad_request(&format!(
                    "GPU count must be less than or equal to {}",
                    executor_config.max_gpus_per_function
                )));
            }
            if !executor_config
                .allowed_gpu_models
                .contains(&gpu_config.model)
            {
                return Err(IndexifyAPIError::bad_request(&format!(
                    "GPU model must be one of '{}'",
                    executor_config.allowed_gpu_models.join(", ")
                )));
            }
        }
        Ok(())
    }
}

impl From<NodeResources> for data_model::NodeResources {
    fn from(value: NodeResources) -> Self {
        data_model::NodeResources {
            cpu_ms_per_sec: (value.cpus * 1000.0).ceil() as u32,
            memory_mb: value.memory_mb,
            ephemeral_disk_mb: value.ephemeral_disk_mb,
            gpu_configs: value
                .gpu_configs
                .into_iter()
                .map(|gpu| data_model::NodeGPUConfig {
                    count: gpu.count,
                    model: gpu.model,
                })
                .collect(),
        }
    }
}

impl From<data_model::NodeResources> for NodeResources {
    fn from(value: data_model::NodeResources) -> NodeResources {
        NodeResources {
            cpus: value.cpu_ms_per_sec as f64 / 1000.0,
            memory_mb: value.memory_mb,
            ephemeral_disk_mb: value.ephemeral_disk_mb,
            gpu_configs: value
                .gpu_configs
                .into_iter()
                .map(|gpu| NodeGPUConfig {
                    count: gpu.count,
                    model: gpu.model,
                })
                .collect(),
        }
    }
}

impl Default for NodeResources {
    fn default() -> Self {
        data_model::NodeResources::default().into()
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NodeRetryPolicy {
    pub max_retries: u32,
    pub initial_delay_sec: f64,
    pub max_delay_sec: f64,
    pub delay_multiplier: f64,
}

impl NodeRetryPolicy {
    fn validate(&self) -> Result<(), IndexifyAPIError> {
        if self.max_retries > 10 {
            return Err(IndexifyAPIError::bad_request(
                "Max retries must be less than or equal to 10",
            ));
        }
        if self.initial_delay_sec > 60.0 * 60.0 {
            // 1 hour max delay
            return Err(IndexifyAPIError::bad_request(
                "Initial delay must be less than 1 hour",
            ));
        }
        if self.max_delay_sec > 60.0 * 60.0 {
            // 1 hour max delay
            return Err(IndexifyAPIError::bad_request(
                "Max delay must be less than 1 hour",
            ));
        }
        if self.delay_multiplier < 0.1 {
            return Err(IndexifyAPIError::bad_request(
                "Delay multiplier must be greater than or equal to 0.1",
            ));
        }
        Ok(())
    }
}

impl From<NodeRetryPolicy> for data_model::NodeRetryPolicy {
    fn from(value: NodeRetryPolicy) -> Self {
        data_model::NodeRetryPolicy {
            max_retries: value.max_retries,
            initial_delay_ms: (value.initial_delay_sec * 1000.0).ceil() as u32,
            max_delay_ms: (value.max_delay_sec * 1000.0).ceil() as u32,
            delay_multiplier: (value.delay_multiplier * 1000.0).ceil() as u32,
        }
    }
}

impl From<data_model::NodeRetryPolicy> for NodeRetryPolicy {
    fn from(value: data_model::NodeRetryPolicy) -> Self {
        NodeRetryPolicy {
            max_retries: value.max_retries,
            initial_delay_sec: value.initial_delay_ms as f64 / 1000.0,
            max_delay_sec: value.max_delay_ms as f64 / 1000.0,
            delay_multiplier: value.delay_multiplier as f64 / 1000.0,
        }
    }
}

impl Default for NodeRetryPolicy {
    fn default() -> Self {
        data_model::NodeRetryPolicy::default().into()
    }
}

fn default_encoder() -> String {
    "cloudpickle".to_string()
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct CacheKey(String);

impl CacheKey {
    pub fn get(&self) -> &str {
        &self.0
    }
}

impl From<CacheKey> for data_model::CacheKey {
    fn from(val: CacheKey) -> Self {
        data_model::CacheKey::from(val.get())
    }
}

impl From<&str> for CacheKey {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<data_model::CacheKey> for CacheKey {
    fn from(val: data_model::CacheKey) -> Self {
        CacheKey::from(val.get())
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct ComputeFn {
    pub name: String,
    pub fn_name: String,
    pub description: String,
    pub reducer: bool,
    #[serde(default = "default_encoder")]
    pub input_encoder: String,
    #[serde(default = "default_encoder")]
    pub output_encoder: String,
    pub image_information: ImageInformation,
    #[serde(default)]
    pub secret_names: Vec<String>,
    #[serde(default, rename = "timeout_sec")]
    pub timeout: NodeTimeoutSeconds,
    #[serde(default)]
    pub resources: NodeResources,
    #[serde(default)]
    pub retry_policy: NodeRetryPolicy,
    #[serde(rename = "cache_key")]
    pub cache_key: Option<CacheKey>,
}

impl From<ComputeFn> for data_model::ComputeFn {
    fn from(val: ComputeFn) -> Self {
        data_model::ComputeFn {
            name: val.name.clone(),
            fn_name: val.fn_name.clone(),
            description: val.description.clone(),
            placement_constraints: Default::default(),
            reducer: val.reducer,
            input_encoder: val.input_encoder.clone(),
            output_encoder: val.output_encoder.clone(),
            image_information: val.image_information.into(),
            secret_names: Some(val.secret_names),
            timeout: val.timeout.into(),
            resources: val.resources.into(),
            retry_policy: val.retry_policy.into(),
            cache_key: val.cache_key.and_then(|v| Some(v.into())),
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
            input_encoder: c.input_encoder,
            output_encoder: c.output_encoder,
            image_information: c.image_information.into(),
            secret_names: c.secret_names.unwrap_or(vec![]),
            timeout: c.timeout.into(),
            resources: c.resources.into(),
            retry_policy: c.retry_policy.into(),
            cache_key: c.cache_key.and_then(|v| Some(v.into())),
        }
    }
}

impl ComputeFn {
    pub fn validate(&self, executor_config: &ExecutorConfig) -> Result<(), IndexifyAPIError> {
        if self.name.is_empty() {
            return Err(IndexifyAPIError::bad_request(
                "ComputeFn name cannot be empty",
            ));
        }
        if self.fn_name.is_empty() {
            return Err(IndexifyAPIError::bad_request(
                "ComputeFn fn_name cannot be empty",
            ));
        }
        self.timeout.validate()?;
        self.resources.validate(executor_config)?;
        self.retry_policy.validate()?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct DynamicRouter {
    pub name: String,
    pub source_fn: String,
    pub description: String,
    pub target_fns: Vec<String>,
    #[serde(default = "default_encoder")]
    pub input_encoder: String,
    #[serde(default = "default_encoder")]
    pub output_encoder: String,
    pub image_information: ImageInformation,
    #[serde(default)]
    pub secret_names: Vec<String>,
    #[serde(default, rename = "timeout_sec")]
    pub timeout: NodeTimeoutSeconds,
    #[serde(default)]
    pub resources: NodeResources,
    #[serde(default)]
    pub retry_policy: NodeRetryPolicy,
}

impl DynamicRouter {
    pub fn validate(&self, executor_config: &ExecutorConfig) -> Result<(), IndexifyAPIError> {
        if self.name.is_empty() {
            return Err(IndexifyAPIError::bad_request(
                "DynamicRouter name cannot be empty",
            ));
        }
        if self.source_fn.is_empty() {
            return Err(IndexifyAPIError::bad_request(
                "DynamicRouter source_fn cannot be empty",
            ));
        }
        if self.target_fns.is_empty() {
            return Err(IndexifyAPIError::bad_request(
                "DynamicRouter must have at least one target_fn",
            ));
        }
        self.timeout.validate()?;
        self.resources.validate(executor_config)?;
        self.retry_policy.validate()?;
        Ok(())
    }
}

impl From<DynamicRouter> for data_model::DynamicEdgeRouter {
    fn from(val: DynamicRouter) -> Self {
        data_model::DynamicEdgeRouter {
            name: val.name.clone(),
            source_fn: val.source_fn.clone(),
            description: val.description.clone(),
            target_functions: val.target_fns.clone(),
            input_encoder: val.input_encoder.clone(),
            output_encoder: val.output_encoder.clone(),
            image_information: val.image_information.clone().into(),
            secret_names: Some(val.secret_names),
            timeout: val.timeout.into(),
            resources: val.resources.into(),
            retry_policy: val.retry_policy.into(),
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
            input_encoder: d.input_encoder,
            output_encoder: d.output_encoder,
            image_information: d.image_information.into(),
            secret_names: d.secret_names.unwrap_or(vec![]),
            timeout: d.timeout.into(),
            resources: d.resources.into(),
            retry_policy: d.retry_policy.into(),
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

    pub fn validate(&self, executor_config: &ExecutorConfig) -> Result<(), IndexifyAPIError> {
        match self {
            Node::DynamicRouter(d) => d.validate(executor_config),
            Node::ComputeFn(c) => c.validate(executor_config),
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
    #[serde(default)]
    pub sdk_version: String,
}

impl From<RuntimeInformation> for data_model::RuntimeInformation {
    fn from(value: RuntimeInformation) -> Self {
        data_model::RuntimeInformation {
            major_version: value.major_version,
            minor_version: value.minor_version,
            sdk_version: value.sdk_version,
        }
    }
}

impl From<data_model::RuntimeInformation> for RuntimeInformation {
    fn from(value: data_model::RuntimeInformation) -> Self {
        Self {
            major_version: value.major_version,
            minor_version: value.minor_version,
            sdk_version: value.sdk_version,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ComputeGraph {
    pub name: String,
    pub namespace: String,
    pub description: String,
    #[serde(default)]
    pub tombstoned: bool,
    pub start_node: Node,
    pub version: GraphVersion,
    #[serde(default)]
    pub tags: Option<HashMap<String, String>>,
    pub nodes: HashMap<String, Node>,
    pub edges: HashMap<String, Vec<String>>,
    #[serde(default = "get_epoch_time_in_ms")]
    pub created_at: u64,
    pub runtime_information: RuntimeInformation,
    #[serde(skip_deserializing)]
    pub replaying: bool,
}

impl ComputeGraph {
    pub fn into_data_model(
        self,
        code_path: &str,
        sha256_hash: &str,
        size: u64,
        executor_config: &ExecutorConfig,
    ) -> Result<data_model::ComputeGraph, IndexifyAPIError> {
        let mut nodes = HashMap::new();
        for (name, node) in self.nodes {
            node.validate(executor_config)?;
            nodes.insert(name, node.into());
        }
        let start_fn: data_model::Node = self.start_node.into();

        let compute_graph = data_model::ComputeGraph {
            name: self.name,
            namespace: self.namespace,
            description: self.description,
            start_fn,
            tags: self.tags.unwrap_or_default(),
            version: self.version.into(),
            code: ComputeGraphCode {
                sha256_hash: sha256_hash.to_string(),
                size,
                path: code_path.to_string(),
            },
            nodes,
            edges: self.edges.clone(),
            created_at: 0,
            runtime_information: self.runtime_information.into(),
            replaying: false,
            tombstoned: self.tombstoned,
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
            tags: Some(compute_graph.tags),
            version: compute_graph.version.into(),
            nodes,
            edges: compute_graph.edges,
            created_at: compute_graph.created_at,
            runtime_information: compute_graph.runtime_information.into(),
            replaying: compute_graph.replaying,
            tombstoned: compute_graph.tombstoned,
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
    pub cursor: Option<String>,
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
    pub invocations: Vec<Invocation>,
    pub prev_cursor: Option<String>,
    pub next_cursor: Option<String>,
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
pub enum TaskOutcome {
    Undefined,
    Success,
    Failure,
}

impl From<data_model::TaskOutcome> for TaskOutcome {
    fn from(outcome: data_model::TaskOutcome) -> Self {
        match outcome {
            data_model::TaskOutcome::Unknown => TaskOutcome::Undefined,
            data_model::TaskOutcome::Success => TaskOutcome::Success,
            data_model::TaskOutcome::Failure => TaskOutcome::Failure,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct GraphVersion(pub String);

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

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
}

impl From<data_model::TaskStatus> for TaskStatus {
    fn from(status: data_model::TaskStatus) -> Self {
        match status {
            data_model::TaskStatus::Pending => TaskStatus::Pending,
            data_model::TaskStatus::Running => TaskStatus::Running,
            data_model::TaskStatus::Completed => TaskStatus::Completed,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct DataPayload {
    pub path: String,
    pub size: u64,
    pub sha256_hash: String,
}

impl From<data_model::DataPayload> for DataPayload {
    fn from(payload: data_model::DataPayload) -> Self {
        Self {
            path: payload.path,
            size: payload.size,
            sha256_hash: payload.sha256_hash,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Task {
    pub id: String,
    pub namespace: String,
    pub compute_fn: String,
    pub compute_graph: String,
    pub invocation_id: String,
    pub input: DataPayload,
    pub acc_input: Option<DataPayload>,
    pub status: TaskStatus,
    pub outcome: TaskOutcome,
    pub graph_version: GraphVersion,
    pub image_uri: Option<String>,
    pub secret_names: Vec<String>,
    pub graph_payload: Option<DataPayload>,
    pub input_payload: Option<DataPayload>,
    pub reducer_input_payload: Option<DataPayload>,
    pub output_payload_uri_prefix: Option<String>,
    pub timeout: NodeTimeoutSeconds,
    pub resources: NodeResources,
    pub retry_policy: NodeRetryPolicy,
}

impl From<data_model::Task> for Task {
    fn from(task: data_model::Task) -> Self {
        Self {
            id: task.id.to_string(),
            namespace: task.namespace,
            compute_fn: task.compute_fn_name,
            compute_graph: task.compute_graph_name,
            invocation_id: task.invocation_id,
            input: task.input.into(),
            acc_input: task.acc_input.map(|input| input.into()),
            outcome: task.outcome.into(),
            status: task.status.into(),
            graph_version: task.graph_version.into(),
            image_uri: None,
            secret_names: Default::default(),
            timeout: Default::default(),
            resources: Default::default(),
            retry_policy: Default::default(),
            graph_payload: None,
            input_payload: None,
            reducer_input_payload: None,
            output_payload_uri_prefix: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Tasks {
    pub tasks: Vec<Task>,
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct FnOutput {
    pub id: String,
    pub compute_fn: String,
    pub created_at: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub enum InvocationStatus {
    Pending,
    Running,
    Finalized,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub enum InvocationOutcome {
    Undefined,
    Success,
    Failure,
}

impl From<GraphInvocationOutcome> for InvocationOutcome {
    fn from(outcome: GraphInvocationOutcome) -> Self {
        match outcome {
            GraphInvocationOutcome::Undefined => InvocationOutcome::Undefined,
            GraphInvocationOutcome::Success => InvocationOutcome::Success,
            GraphInvocationOutcome::Failure => InvocationOutcome::Failure,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct FnOutputs {
    pub status: InvocationStatus,
    pub outcome: InvocationOutcome,
    pub outputs: Vec<FnOutput>,
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct InvocationId {
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Invocation {
    pub id: String,
    pub completed: bool,
    pub status: InvocationStatus,
    pub outcome: InvocationOutcome,
    pub outstanding_tasks: u64,
    pub task_analytics: HashMap<String, TaskAnalytics>,
    pub graph_version: String,
    pub created_at: u64,
}

impl From<GraphInvocationCtx> for Invocation {
    fn from(value: GraphInvocationCtx) -> Self {
        let mut task_analytics = HashMap::new();
        for (k, v) in value.fn_task_analytics {
            task_analytics.insert(
                k,
                TaskAnalytics {
                    pending_tasks: v.pending_tasks,
                    successful_tasks: v.successful_tasks,
                    failed_tasks: v.failed_tasks,
                },
            );
        }
        let status = if value.completed {
            InvocationStatus::Finalized
        } else if value.outstanding_tasks > 0 {
            InvocationStatus::Running
        } else {
            InvocationStatus::Pending
        };
        Self {
            id: value.invocation_id.to_string(),
            completed: value.completed,
            outcome: value.outcome.into(),
            status,
            outstanding_tasks: value.outstanding_tasks,
            task_analytics,
            graph_version: value.graph_version.0,
            created_at: value.created_at,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct TaskAnalytics {
    pub pending_tasks: u64,
    pub successful_tasks: u64,
    pub failed_tasks: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct FunctionAllowlist {
    pub namespace: Option<String>,
    pub compute_graph: Option<String>,
    pub compute_fn: Option<String>,

    // Temporary fix to enable internal migration
    // to new executor version, we will bring this back
    // when the scheduler can turn off containers of older
    // versions after all the invocations into them have been
    // completed, and turn on new versions of the executor.
    pub version: Option<GraphVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct GpuResources {
    pub count: u32,
    pub model: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct HostResources {
    pub cpu_count: u32,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    // Not all Executors have GPUs.
    pub gpu: Option<GpuResources>,
}

impl From<data_model::HostResources> for HostResources {
    fn from(host_resources: data_model::HostResources) -> Self {
        Self {
            cpu_count: host_resources.cpu_count,
            memory_bytes: host_resources.memory_bytes,
            disk_bytes: host_resources.disk_bytes,
            gpu: host_resources.gpu.map(|gpu| GpuResources {
                count: gpu.count,
                model: gpu.model,
            }),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct FunctionExecutorMetadata {
    pub id: String,
    pub namespace: String,
    pub compute_graph_name: String,
    pub compute_fn_name: String,
    pub version: String,
    pub state: String,
    pub desired_state: String,
}

pub fn from_data_model_function_executor(
    fe: data_model::FunctionExecutor,
    desired_state: FunctionExecutorState,
) -> FunctionExecutorMetadata {
    FunctionExecutorMetadata {
        id: fe.id.get().to_string(),
        namespace: fe.namespace,
        compute_graph_name: fe.compute_graph_name,
        compute_fn_name: fe.compute_fn_name,
        version: fe.version.to_string(),
        state: fe.state.to_string(),
        desired_state: desired_state.to_string(),
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ExecutorMetadata {
    pub id: String,
    pub executor_version: String,
    pub function_allowlist: Option<Vec<FunctionAllowlist>>,
    pub addr: String,
    pub labels: HashMap<String, serde_json::Value>,
    pub function_executors: Vec<FunctionExecutorMetadata>,
    pub host_resources: HostResources,
    pub free_resources: HostResources,
    pub state: String,
    pub tombstoned: bool,
    pub state_hash: String,
    pub clock: u64,
}

pub fn from_data_model_executor_metadata(
    executor: data_model::ExecutorMetadata,
    free_resources: data_model::HostResources,
    function_executor_server_metadata: HashMap<
        FunctionExecutorId,
        Box<FunctionExecutorServerMetadata>,
    >,
) -> ExecutorMetadata {
    let function_allowlist = executor.function_allowlist.map(|allowlist| {
        allowlist
            .iter()
            .map(|fn_uri| FunctionAllowlist {
                namespace: fn_uri.namespace.clone(),
                compute_graph: fn_uri.compute_graph_name.clone(),
                compute_fn: fn_uri.compute_fn_name.clone(),
                version: fn_uri.version.clone().map(|v| v.into()),
            })
            .collect()
    });
    let mut function_executors = Vec::new();
    for (fe_id, fe) in executor.function_executors.iter() {
        if let Some(fe_server_metadata) = function_executor_server_metadata.get(fe_id) {
            let desired_state = fe_server_metadata.desired_state.clone();
            function_executors.push(from_data_model_function_executor(fe.clone(), desired_state));
        } else {
            function_executors.push(from_data_model_function_executor(
                fe.clone(),
                FunctionExecutorState::Unknown,
            ));
        }
    }
    ExecutorMetadata {
        id: executor.id.to_string(),
        executor_version: executor.executor_version,
        addr: executor.addr,
        function_allowlist,
        labels: executor.labels,
        function_executors,
        host_resources: executor.host_resources.into(),
        free_resources: free_resources.into(),
        state: executor.state.as_ref().to_string(),
        tombstoned: executor.tombstoned,
        state_hash: executor.state_hash,
        clock: executor.clock,
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Allocation {
    pub namespace: String,
    pub compute_graph: String,
    pub compute_fn: String,
    pub executor_id: String,
    pub task_id: String,
    pub invocation_id: String,
    pub created_at: u128,
}

impl From<data_model::Allocation> for Allocation {
    fn from(allocation: data_model::Allocation) -> Self {
        Self {
            namespace: allocation.namespace,
            compute_graph: allocation.compute_graph,
            compute_fn: allocation.compute_fn,
            executor_id: allocation.executor_id.to_string(),
            task_id: allocation.task_id.to_string(),
            invocation_id: allocation.invocation_id.to_string(),
            created_at: allocation.created_at,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct StateChange {
    pub id: String,
    pub object_id: String,
    pub change_type: String,
    pub created_at: u64,
    pub namespace: Option<String>,
    pub compute_graph: Option<String>,
    pub invocation: Option<String>,
}
impl From<data_model::StateChange> for StateChange {
    fn from(item: data_model::StateChange) -> Self {
        StateChange {
            id: item.id.to_string(),
            object_id: item.object_id.to_string(),
            change_type: item.change_type.to_string(),
            created_at: item.created_at,
            namespace: item.namespace,
            compute_graph: item.compute_graph,
            invocation: item.invocation,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct StateChangesResponse {
    pub count: usize,
    pub state_changes: Vec<StateChange>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UnallocatedTasks {
    pub count: usize,
    pub tasks: Vec<Task>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct FnExecutor {
    pub count: usize,
    pub function_executor_id: String,
    pub fn_uri: String,
    pub state: String,
    pub desired_state: String,
    pub allocations: Vec<Allocation>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ExecutorAllocations {
    pub count: usize,
    pub executor_id: String,
    pub function_executors: Vec<FnExecutor>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ExecutorsAllocationsResponse {
    pub executors: Vec<ExecutorAllocations>,
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
        let json = r#"{"name":"test","description":"test","start_node":{"compute_fn":{"name":"extractor_a","fn_name":"extractor_a","description":"Random description of extractor_a", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder":"cloudpickle", "output_encoder":"cloudpickle", "image_name": "default_image"}},"nodes":{"extractor_a":{"compute_fn":{"name":"extractor_a","fn_name":"extractor_a","description":"Random description of extractor_a", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder":"cloudpickle", "output_encoder":"cloudpickle","image_name": "default_image"}},"extractor_b":{"compute_fn":{"name":"extractor_b","fn_name":"extractor_b","description":"", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder":"cloudpickle", "output_encoder":"cloudpickle", "image_name": "default_image"}},"extractor_c":{"compute_fn":{"name":"extractor_c","fn_name":"extractor_c","description":"", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder":"cloudpickle", "output_encoder":"cloudpickle", "image_name": "default_image"}}},"edges":{"extractor_a":["extractor_b"],"extractor_b":["extractor_c"]},"runtime_information": {"major_version": 3, "minor_version": 10, "sdk_version": "1.2.3"}, "version": "1.2.3"}"#;
        let mut json_value: serde_json::Value = serde_json::from_str(json).unwrap();
        json_value["namespace"] = serde_json::Value::String("test".to_string());
        let _: super::ComputeGraph = serde_json::from_value(json_value).unwrap();
    }

    #[test]
    fn test_compute_graph_with_router_deserialization() {
        let json = r#"{"name":"graph_a_router","description":"description of graph_a","start_node":{"compute_fn":{"name":"extractor_a","fn_name":"extractor_a","description":"Random description of extractor_a", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder":"cloudpickle", "output_encoder":"cloudpickle", "image_name": "default_image"}},"nodes":{"extractor_a":{"compute_fn":{"name":"extractor_a","fn_name":"extractor_a","description":"Random description of extractor_a", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder":"cloudpickle", "output_encoder":"cloudpickle", "image_name": "default_image"}},"router_x":{"dynamic_router":{"name":"router_x","description":"","source_fn":"router_x","target_fns":["extractor_y","extractor_z"], "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder":"cloudpickle", "output_encoder":"cloudpickle", "image_name": "default_image"}},"extractor_y":{"compute_fn":{"name":"extractor_y","fn_name":"extractor_y","description":"", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder":"cloudpickle", "output_encoder":"cloudpickle", "image_name": "default_image"}},"extractor_z":{"compute_fn":{"name":"extractor_z","fn_name":"extractor_z","description":"", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder":"cloudpickle", "output_encoder":"cloudpickle", "image_name": "default_image"}},"extractor_c":{"compute_fn":{"name":"extractor_c","fn_name":"extractor_c","description":"", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder":"cloudpickle", "output_encoder":"cloudpickle", "image_name": "default_image"}}},"edges":{"extractor_a":["router_x"],"extractor_y":["extractor_c"],"extractor_z":["extractor_c"]},"runtime_information": {"major_version": 3, "minor_version": 10, "sdk_version": "1.2.3"}, "version": "1.2.3"}"#;
        let mut json_value: serde_json::Value = serde_json::from_str(json).unwrap();
        json_value["namespace"] = serde_json::Value::String("test".to_string());
        let _: super::ComputeGraph = serde_json::from_value(json_value).unwrap();
    }

    #[test]
    fn test_compute_fn_deserialization() {
        let json = r#"{"name": "one", "fn_name": "two", "description": "desc", "reducer": true, "image_name": "im1", "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder": "cloudpickle", "output_encoder":"cloudpickle"}"#;
        let compute_fn: ComputeFn = serde_json::from_str(json).unwrap();
        println!("{:?}", compute_fn);
    }

    #[test]
    fn test_router_deserialization() {
        let json = r#"{"name": "one", "source_fn": "two", "description": "desc", "target_fns": ["one", "two", "three"], "image_name": "im1", "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "encoder": "clouds"}"#;
        let dynamic_router: DynamicRouter = serde_json::from_str(json).unwrap();
        println!("{:?}", dynamic_router);
    }
}
