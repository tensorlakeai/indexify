use std::{collections::HashMap, fmt};

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use tracing::error;
use utoipa::{IntoParams, ToSchema};

use crate::{
    data_model::{
        self,
        ComputeGraphCode,
        FunctionExecutorId,
        FunctionExecutorServerMetadata,
        FunctionExecutorState,
        GraphInvocationCtx,
        GraphInvocationFailureReason,
        GraphInvocationOutcome,
    },
    utils::get_epoch_time_in_ms,
};

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

    pub fn conflict(message: &str) -> Self {
        Self::new(StatusCode::CONFLICT, message)
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
    blob_storage_bucket: Option<String>,
}

impl From<data_model::Namespace> for Namespace {
    fn from(namespace: data_model::Namespace) -> Self {
        Self {
            name: namespace.name,
            created_at: namespace.created_at,
            blob_storage_bucket: namespace.blob_storage_bucket,
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
pub struct TimeoutSeconds(pub u32);

impl TimeoutSeconds {
    fn validate(&self) -> Result<(), IndexifyAPIError> {
        if self.0 == 0 {
            return Err(IndexifyAPIError::bad_request(
                "timeout must be greater than 0",
            ));
        }
        if self.0 > 24 * 60 * 60 {
            return Err(IndexifyAPIError::bad_request(
                "timeout must be less than or equal 24 hours",
            ));
        }
        Ok(())
    }
}

impl From<TimeoutSeconds> for data_model::NodeTimeoutMS {
    fn from(value: TimeoutSeconds) -> Self {
        data_model::NodeTimeoutMS(value.0 * 1000)
    }
}

impl From<data_model::NodeTimeoutMS> for TimeoutSeconds {
    fn from(value: data_model::NodeTimeoutMS) -> TimeoutSeconds {
        TimeoutSeconds(value.0 / 1000)
    }
}

impl Default for TimeoutSeconds {
    fn default() -> Self {
        data_model::NodeTimeoutMS::default().into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct GPUResources {
    pub count: u32,
    pub model: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct NodeResources {
    pub cpus: f64,
    pub memory_mb: u64,
    pub ephemeral_disk_mb: u64,
    #[serde(default, rename = "gpus")]
    pub gpu_configs: Vec<GPUResources>,
}

impl From<NodeResources> for data_model::FunctionResources {
    fn from(value: NodeResources) -> Self {
        data_model::FunctionResources {
            cpu_ms_per_sec: (value.cpus * 1000.0).ceil() as u32,
            memory_mb: value.memory_mb,
            ephemeral_disk_mb: value.ephemeral_disk_mb,
            gpu_configs: value
                .gpu_configs
                .into_iter()
                .map(|gpu| data_model::GPUResources {
                    count: gpu.count,
                    model: gpu.model,
                })
                .collect(),
        }
    }
}

impl From<data_model::FunctionResources> for NodeResources {
    fn from(value: data_model::FunctionResources) -> NodeResources {
        NodeResources {
            cpus: value.cpu_ms_per_sec as f64 / 1000.0,
            memory_mb: value.memory_mb,
            ephemeral_disk_mb: value.ephemeral_disk_mb,
            gpu_configs: value
                .gpu_configs
                .into_iter()
                .map(|gpu| GPUResources {
                    count: gpu.count,
                    model: gpu.model,
                })
                .collect(),
        }
    }
}

impl Default for NodeResources {
    fn default() -> Self {
        data_model::FunctionResources::default().into()
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

impl From<NodeRetryPolicy> for data_model::FunctionRetryPolicy {
    fn from(value: NodeRetryPolicy) -> Self {
        data_model::FunctionRetryPolicy {
            max_retries: value.max_retries,
            initial_delay_ms: (value.initial_delay_sec * 1000.0).ceil() as u32,
            max_delay_ms: (value.max_delay_sec * 1000.0).ceil() as u32,
            delay_multiplier: (value.delay_multiplier * 1000.0).ceil() as u32,
        }
    }
}

impl From<data_model::FunctionRetryPolicy> for NodeRetryPolicy {
    fn from(value: data_model::FunctionRetryPolicy) -> Self {
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
        data_model::FunctionRetryPolicy::default().into()
    }
}

fn default_encoder() -> String {
    "cloudpickle".to_string()
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone, Default)]
pub struct PlacementConstraints {
    /// List of label filter expressions in the format "key=value",
    /// "key!=value", etc.
    #[serde(default)]
    pub filter_expressions: Vec<String>,
}

impl TryFrom<PlacementConstraints> for data_model::filter::LabelsFilter {
    type Error = anyhow::Error;

    fn try_from(value: PlacementConstraints) -> Result<Self, Self::Error> {
        let mut expressions = Vec::new();
        for expr_str in value.filter_expressions {
            let expression = data_model::filter::Expression::from_str(&expr_str)
                .map_err(|e| anyhow::anyhow!("Failed to parse placement constraints: {}", e))?;
            expressions.push(expression);
        }
        Ok(data_model::filter::LabelsFilter(expressions))
    }
}

impl From<data_model::filter::LabelsFilter> for PlacementConstraints {
    fn from(value: data_model::filter::LabelsFilter) -> Self {
        Self {
            filter_expressions: value.0.into_iter().map(|expr| expr.to_string()).collect(),
        }
    }
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
    pub timeout: TimeoutSeconds,
    #[serde(default)]
    pub resources: NodeResources,
    #[serde(default)]
    pub retry_policy: NodeRetryPolicy,
    #[serde(rename = "cache_key")]
    pub cache_key: Option<CacheKey>,
    #[serde(default)]
    pub parameters: Vec<ParameterMetadata>,
    #[serde(default)]
    pub return_type: Option<serde_json::Value>,
    #[serde(default)]
    pub placement_constraints: PlacementConstraints,
}

impl TryFrom<ComputeFn> for data_model::ComputeFn {
    type Error = anyhow::Error;

    fn try_from(val: ComputeFn) -> Result<Self, Self::Error> {
        Ok(data_model::ComputeFn {
            name: val.name.clone(),
            fn_name: val.fn_name.clone(),
            description: val.description.clone(),
            placement_constraints: val.placement_constraints.try_into()?,
            reducer: val.reducer,
            input_encoder: val.input_encoder.clone(),
            output_encoder: val.output_encoder.clone(),
            image_information: val.image_information.into(),
            secret_names: Some(val.secret_names),
            timeout: val.timeout.into(),
            resources: val.resources.into(),
            retry_policy: val.retry_policy.into(),
            cache_key: val.cache_key.map(|v| v.into()),
            parameters: val.parameters.into_iter().map(|p| p.into()).collect(),
            return_type: val.return_type,
        })
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
            secret_names: c.secret_names.unwrap_or_default(),
            timeout: c.timeout.into(),
            resources: c.resources.into(),
            retry_policy: c.retry_policy.into(),
            cache_key: c.cache_key.map(|v| v.into()),
            parameters: c.parameters.into_iter().map(|p| p.into()).collect(),
            return_type: c.return_type,
            placement_constraints: c.placement_constraints.into(),
        }
    }
}

impl ComputeFn {
    pub fn validate(&self) -> Result<(), IndexifyAPIError> {
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
        self.retry_policy.validate()?;
        Ok(())
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

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct ParameterMetadata {
    pub name: String,
    pub description: Option<String>,
    pub required: bool,
    pub data_type: serde_json::Value,
}

impl From<data_model::ParameterMetadata> for ParameterMetadata {
    fn from(parameter: data_model::ParameterMetadata) -> Self {
        Self {
            name: parameter.name,
            description: parameter.description,
            required: parameter.required,
            data_type: parameter.data_type,
        }
    }
}

impl From<ParameterMetadata> for data_model::ParameterMetadata {
    fn from(parameter: ParameterMetadata) -> Self {
        Self {
            name: parameter.name,
            description: parameter.description,
            required: parameter.required,
            data_type: parameter.data_type,
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
    pub start_node: ComputeFn,
    pub version: GraphVersion,
    #[serde(default)]
    pub tags: Option<HashMap<String, String>>,
    pub nodes: HashMap<String, ComputeFn>,
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
            node.validate()?;
            let converted_node: data_model::ComputeFn = node.try_into().map_err(|e| {
                IndexifyAPIError::bad_request(&format!(
                    "Invalid placement constraints in node '{}': {}",
                    name, e
                ))
            })?;
            nodes.insert(name, converted_node);
        }
        let start_fn: data_model::ComputeFn = self.start_node.try_into().map_err(|e| {
            IndexifyAPIError::bad_request(&format!(
                "Invalid placement constraints in start node: {}",
                e
            ))
        })?;

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
            tombstoned: self.tombstoned,
            state: data_model::ComputeGraphState::Active,
        };
        Ok(compute_graph)
    }
}

impl From<data_model::ComputeGraph> for ComputeGraph {
    fn from(compute_graph: data_model::ComputeGraph) -> Self {
        let start_fn = compute_graph.start_fn.into();
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
            tombstoned: compute_graph.tombstoned,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateNamespace {
    pub name: String,
    pub blob_storage_bucket: Option<String>,
    pub blob_storage_region: Option<String>,
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

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
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
            data_model::TaskOutcome::Failure(_) => TaskOutcome::Failure,
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
            data_model::TaskStatus::Running(_) => TaskStatus::Running,
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
    pub timeout: TimeoutSeconds,
    pub resources: NodeResources,
    pub retry_policy: NodeRetryPolicy,
    pub allocations: Vec<Allocation>,
    pub creation_time_ns: u128,
}

impl Task {
    pub fn from_data_model_task(task: data_model::Task, allocations: Vec<Allocation>) -> Self {
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
            allocations,
            creation_time_ns: task.creation_time_ns,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Tasks {
    pub tasks: Vec<Task>,
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct FnOutput {
    pub id: String,
    pub compute_fn: String,
    pub created_at: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub enum InvocationStatus {
    Pending,
    Running,
    Finalized,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub enum InvocationOutcome {
    Undefined,
    Success,
    Failure,
}

impl From<GraphInvocationOutcome> for InvocationOutcome {
    fn from(outcome: GraphInvocationOutcome) -> Self {
        match outcome {
            GraphInvocationOutcome::Unknown => InvocationOutcome::Undefined,
            GraphInvocationOutcome::Success => InvocationOutcome::Success,
            GraphInvocationOutcome::Failure(_) => InvocationOutcome::Failure,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub enum InvocationFailureReason {
    Unknown,
    InternalError,
    FunctionError,
    InvocationError,
    NextFunctionNotFound,
    ConstraintUnsatisfiable,
}

impl From<GraphInvocationFailureReason> for InvocationFailureReason {
    fn from(failure_reason: GraphInvocationFailureReason) -> Self {
        match failure_reason {
            GraphInvocationFailureReason::Unknown => InvocationFailureReason::Unknown,
            GraphInvocationFailureReason::InternalError => InvocationFailureReason::InternalError,
            GraphInvocationFailureReason::FunctionError => InvocationFailureReason::FunctionError,
            GraphInvocationFailureReason::InvocationError => {
                InvocationFailureReason::InvocationError
            }
            GraphInvocationFailureReason::NextFunctionNotFound => {
                InvocationFailureReason::NextFunctionNotFound
            }
            GraphInvocationFailureReason::ConstraintUnsatisfiable => {
                InvocationFailureReason::ConstraintUnsatisfiable
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct RequestError {
    pub function_name: String,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct FnOutputs {
    pub invocation: Invocation,
    // Deprecated, duplicates Invocation.status
    pub status: InvocationStatus,
    // Deprecated, duplicates Invocation.outcome
    pub outcome: InvocationOutcome,
    pub outputs: Vec<FnOutput>,
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct RequestId {
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct Invocation {
    pub id: String,
    pub completed: bool,
    pub status: InvocationStatus,
    pub outcome: InvocationOutcome,
    pub failure_reason: InvocationFailureReason,
    pub outstanding_tasks: u64,
    pub task_analytics: HashMap<String, TaskAnalytics>,
    pub graph_version: String,
    pub created_at: u64,
    pub invocation_error: Option<RequestError>,
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
            outcome: value.outcome.clone().into(),
            failure_reason: match &value.outcome {
                GraphInvocationOutcome::Failure(reason) => reason.clone().into(),
                _ => GraphInvocationFailureReason::Unknown.into(),
            },
            status,
            outstanding_tasks: value.outstanding_tasks,
            task_analytics,
            graph_version: value.graph_version.0,
            created_at: value.created_at,
            invocation_error: None, // Set by API handlers if needed
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
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
pub struct HostResources {
    pub cpu_count: u32,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    // Not all Executors have GPUs.
    pub gpu: Option<GPUResources>,
}

impl From<data_model::HostResources> for HostResources {
    fn from(host_resources: data_model::HostResources) -> Self {
        Self {
            // int division is okay because cpu_ms_per_sec derived from host CPU cores is always a
            // multiple of 1000
            cpu_count: host_resources.cpu_ms_per_sec / 1000,
            memory_bytes: host_resources.memory_bytes,
            disk_bytes: host_resources.disk_bytes,
            gpu: host_resources.gpu.map(|gpu| GPUResources {
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
    pub labels: HashMap<String, String>,
    pub function_executors: Vec<FunctionExecutorMetadata>,
    pub server_only_function_executors: Vec<FunctionExecutorMetadata>,
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
    let server_only_function_executors = function_executor_server_metadata
        .iter()
        .filter(|(fe_id, _fe)| !executor.function_executors.contains_key(fe_id))
        .map(|(_fe_id, fe)| {
            from_data_model_function_executor(
                fe.function_executor.clone(),
                fe.desired_state.clone(),
            )
        })
        .collect();
    ExecutorMetadata {
        id: executor.id.to_string(),
        executor_version: executor.executor_version,
        addr: executor.addr,
        function_allowlist,
        labels: executor.labels,
        function_executors,
        server_only_function_executors,
        host_resources: executor.host_resources.into(),
        free_resources: free_resources.into(),
        state: executor.state.as_ref().to_string(),
        tombstoned: executor.tombstoned,
        state_hash: executor.state_hash,
        clock: executor.clock,
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct ExecutorCatalogEntry {
    pub name: String,
    pub regions: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct ExecutorCatalog {
    pub entries: Vec<ExecutorCatalogEntry>,
    pub remark: Option<String>,
}

impl From<&crate::state_store::ExecutorCatalog> for ExecutorCatalog {
    fn from(catalog: &crate::state_store::ExecutorCatalog) -> Self {
        let remark = if catalog.allows_any_labels() {
            Some("Executor catalog is empty - all executor labels are allowed".to_string())
        } else {
            None
        };

        ExecutorCatalog {
            entries: catalog
                .entries
                .iter()
                .map(|entry| ExecutorCatalogEntry {
                    name: entry.name.clone(),
                    regions: entry.regions.clone(),
                })
                .collect(),
            remark,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct Allocation {
    pub id: String,
    pub namespace: String,
    pub compute_graph: String,
    pub compute_fn: String,
    pub executor_id: String,
    pub function_executor_id: String,
    pub task_id: String,
    pub invocation_id: String,
    pub created_at: u128,
    pub outcome: TaskOutcome,
    pub attempt_number: u32,
    pub execution_duration_ms: Option<u64>,
}

impl From<data_model::Allocation> for Allocation {
    fn from(allocation: data_model::Allocation) -> Self {
        Self {
            id: allocation.id.to_string(),
            namespace: allocation.namespace,
            compute_graph: allocation.compute_graph,
            compute_fn: allocation.compute_fn,
            executor_id: allocation.target.executor_id.to_string(),
            function_executor_id: allocation.target.function_executor_id.get().to_string(),
            task_id: allocation.task_id.to_string(),
            invocation_id: allocation.invocation_id.to_string(),
            created_at: allocation.created_at,
            outcome: allocation.outcome.into(),
            attempt_number: allocation.attempt_number,
            execution_duration_ms: allocation.execution_duration_ms,
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

#[cfg(test)]
mod tests {
    use crate::http_objects::{ComputeFn, PlacementConstraints};

    #[test]
    fn test_compute_graph_deserialization() {
        // Don't delete this. It makes it easier
        // to test the deserialization of the ComputeGraph struct
        // from the python side
        let json = r#"{"name":"test","description":"test","start_node":{"name":"extractor_a","fn_name":"extractor_a","description":"Random description of extractor_a", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder":"cloudpickle", "output_encoder":"cloudpickle", "image_name": "default_image"},"nodes":{"extractor_a":{"name":"extractor_a","fn_name":"extractor_a","description":"Random description of extractor_a", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder":"cloudpickle", "output_encoder":"cloudpickle","image_name": "default_image"},"extractor_b":{"name":"extractor_b","fn_name":"extractor_b","description":"", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder":"cloudpickle", "output_encoder":"cloudpickle", "image_name": "default_image"},"extractor_c":{"name":"extractor_c","fn_name":"extractor_c","description":"", "reducer": false,  "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder":"cloudpickle", "output_encoder":"cloudpickle", "image_name": "default_image"}},"edges":{"extractor_a":["extractor_b"],"extractor_b":["extractor_c"]},"runtime_information": {"major_version": 3, "minor_version": 10, "sdk_version": "1.2.3"}, "version": "1.2.3"}"#;
        let mut json_value: serde_json::Value = serde_json::from_str(json).unwrap();
        json_value["namespace"] = serde_json::Value::String("test".to_string());
        let _: super::ComputeGraph = serde_json::from_value(json_value).unwrap();
    }

    #[test]
    fn test_compute_fn_deserialization() {
        let json = r#"{"name": "one", "fn_name": "two", "description": "desc", "reducer": true, "image_name": "im1", "image_information": {"image_name": "name1", "tag": "tag1", "base_image": "base1", "run_strs": ["tuff", "life", "running", "docker"], "sdk_version":"1.2.3"}, "input_encoder": "cloudpickle", "output_encoder":"cloudpickle"}"#;
        let compute_fn: ComputeFn = serde_json::from_str(json).unwrap();
        println!("{compute_fn:?}");
    }

    #[test]
    fn test_labels_filter_conversion() {
        // Test HTTP LabelsFilter to data_model conversion
        let http_filter = PlacementConstraints {
            filter_expressions: vec![
                "environment==production".to_string(),
                "gpu_type==nvidia".to_string(),
                "region!=us-east".to_string(),
            ],
        };

        let data_model_filter: crate::data_model::filter::LabelsFilter =
            http_filter.clone().try_into().unwrap();
        println!("{:?}", data_model_filter);
        assert_eq!(data_model_filter.0.len(), 3);

        // Test data_model LabelsFilter to HTTP conversion
        let converted_back: PlacementConstraints = data_model_filter.into();
        assert_eq!(converted_back.filter_expressions.len(), 3);

        // Test round-tripping
        let expected_expressions = vec![
            "environment==production".to_string(),
            "gpu_type==nvidia".to_string(),
            "region!=us-east".to_string(),
        ];

        // Should contain the normalized expressions
        for expr in &expected_expressions {
            assert!(
                converted_back.filter_expressions.contains(expr),
                "Expression '{}' not found in converted back: {:?}",
                expr,
                converted_back.filter_expressions
            );
        }
    }

    #[test]
    fn test_compute_fn_with_placement_constraints() {
        let json = r#"{"name": "test_fn", "fn_name": "test_fn", "description": "Test function", "reducer": false, "image_information": {"image_name": "test", "tag": "latest", "base_image": "python", "run_strs": [], "sdk_version":"1.0.0"}, "input_encoder": "cloudpickle", "output_encoder":"cloudpickle", "placement_constraints": {"filter_expressions": ["environment==production", "gpu_type==nvidia"]}}"#;

        let compute_fn: ComputeFn = serde_json::from_str(json).unwrap();
        assert_eq!(compute_fn.placement_constraints.filter_expressions.len(), 2);

        // Test conversion to data model
        let data_model_fn: crate::data_model::ComputeFn = compute_fn.try_into().unwrap();
        assert_eq!(data_model_fn.placement_constraints.0.len(), 2);
    }

    #[test]
    fn test_compute_fn_with_unparseable_placement_constraints() {
        let json = r#"{"name": "test_fn", "fn_name": "test_fn", "description": "Test function", "reducer": false, "image_information": {"image_name": "test", "tag": "latest", "base_image": "python", "run_strs": [], "sdk_version":"1.0.0"}, "input_encoder": "cloudpickle", "output_encoder":"cloudpickle", "placement_constraints": {"filter_expressions": ["environment=production", "gpu_type=nvidia"]}}"#;

        let compute_fn: ComputeFn = serde_json::from_str(json).unwrap();
        assert_eq!(compute_fn.placement_constraints.filter_expressions.len(), 2);

        // Test failed conversion to data model
        assert!(
            <ComputeFn as TryInto<crate::data_model::ComputeFn>>::try_into(compute_fn).is_err()
        );
    }
}
