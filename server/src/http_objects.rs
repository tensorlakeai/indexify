use std::collections::HashMap;

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use tracing::error;
use utoipa::{IntoParams, ToSchema};

use crate::{
    data_model::{self, FunctionExecutorId, FunctionExecutorServerMetadata, FunctionExecutorState},
    http_objects_v1::FunctionRun,
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
pub struct FunctionResources {
    pub cpus: f64,
    pub memory_mb: u64,
    pub ephemeral_disk_mb: u64,
    #[serde(default, rename = "gpus")]
    pub gpu_configs: Vec<GPUResources>,
}

impl From<FunctionResources> for data_model::FunctionResources {
    fn from(value: FunctionResources) -> Self {
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

impl From<data_model::FunctionResources> for FunctionResources {
    fn from(value: data_model::FunctionResources) -> FunctionResources {
        FunctionResources {
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

impl Default for FunctionResources {
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
                .map_err(|e| anyhow::anyhow!("Failed to parse placement constraints: {e}"))?;
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

fn default_max_concurrency() -> u32 {
    1
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
pub struct ApplicationFunction {
    pub name: String,
    pub description: String,
    pub secret_names: Vec<String>,
    #[serde(default)]
    pub initialization_timeout_sec: TimeoutSeconds,
    pub timeout_sec: TimeoutSeconds,
    pub resources: FunctionResources,
    pub retry_policy: NodeRetryPolicy,
    pub cache_key: Option<CacheKey>,
    #[serde(default)]
    pub parameters: Vec<ParameterMetadata>,
    #[serde(default)]
    pub return_type: Option<serde_json::Value>,
    pub placement_constraints: PlacementConstraints,
    pub max_concurrency: u32,
}

impl TryFrom<ApplicationFunction> for data_model::Function {
    type Error = anyhow::Error;

    fn try_from(val: ApplicationFunction) -> Result<Self, Self::Error> {
        Ok(data_model::Function {
            name: val.name.clone(),
            fn_name: val.name.clone(),
            description: val.description.clone(),
            placement_constraints: val.placement_constraints.try_into()?,
            input_encoder: "not-needed".to_string(),
            output_encoder: "".to_string(),
            secret_names: Some(val.secret_names),
            initialization_timeout: val.initialization_timeout_sec.into(),
            timeout: val.timeout_sec.into(),
            resources: val.resources.into(),
            retry_policy: val.retry_policy.into(),
            cache_key: val.cache_key.map(|v| v.into()),
            parameters: val.parameters.into_iter().map(|p| p.into()).collect(),
            return_type: val.return_type,
            max_concurrency: val.max_concurrency,
        })
    }
}

impl From<data_model::Function> for ApplicationFunction {
    fn from(c: data_model::Function) -> Self {
        Self {
            name: c.name,
            description: c.description,
            secret_names: c.secret_names.unwrap_or_default(),
            initialization_timeout_sec: c.initialization_timeout.into(),
            timeout_sec: c.timeout.into(),
            resources: c.resources.into(),
            retry_policy: c.retry_policy.into(),
            cache_key: c.cache_key.map(|v| v.into()),
            parameters: c.parameters.into_iter().map(|p| p.into()).collect(),
            return_type: c.return_type,
            placement_constraints: c.placement_constraints.into(),
            max_concurrency: c.max_concurrency,
        }
    }
}

impl ApplicationFunction {
    pub fn validate(&self) -> Result<(), IndexifyAPIError> {
        if self.name.is_empty() {
            return Err(IndexifyAPIError::bad_request(
                "ComputeFn name cannot be empty",
            ));
        }
        self.timeout_sec.validate()?;
        self.retry_policy.validate()?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct Function {
    pub name: String,
    pub fn_name: String,
    pub description: String,
    #[serde(default = "default_encoder")]
    pub input_encoder: String,
    #[serde(default = "default_encoder")]
    pub output_encoder: String,
    #[serde(default)]
    pub secret_names: Vec<String>,
    #[serde(default, rename = "initialization_timeout_sec")]
    pub initialization_timeout: TimeoutSeconds,
    #[serde(default, rename = "timeout_sec")]
    pub timeout: TimeoutSeconds,
    #[serde(default)]
    pub resources: FunctionResources,
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
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: u32,
}

impl TryFrom<Function> for data_model::Function {
    type Error = anyhow::Error;

    fn try_from(val: Function) -> Result<Self, Self::Error> {
        Ok(data_model::Function {
            name: val.name.clone(),
            fn_name: val.fn_name.clone(),
            description: val.description.clone(),
            placement_constraints: val.placement_constraints.try_into()?,
            input_encoder: val.input_encoder.clone(),
            output_encoder: val.output_encoder.clone(),
            secret_names: Some(val.secret_names),
            initialization_timeout: val.initialization_timeout.into(),
            timeout: val.timeout.into(),
            resources: val.resources.into(),
            retry_policy: val.retry_policy.into(),
            cache_key: val.cache_key.map(|v| v.into()),
            parameters: val.parameters.into_iter().map(|p| p.into()).collect(),
            return_type: val.return_type,
            max_concurrency: val.max_concurrency,
        })
    }
}

impl From<data_model::Function> for Function {
    fn from(c: data_model::Function) -> Self {
        Self {
            name: c.name,
            fn_name: c.fn_name,
            description: c.description,
            input_encoder: c.input_encoder,
            output_encoder: c.output_encoder,
            secret_names: c.secret_names.unwrap_or_default(),
            initialization_timeout: c.initialization_timeout.into(),
            timeout: c.timeout.into(),
            resources: c.resources.into(),
            retry_policy: c.retry_policy.into(),
            cache_key: c.cache_key.map(|v| v.into()),
            parameters: c.parameters.into_iter().map(|p| p.into()).collect(),
            return_type: c.return_type,
            placement_constraints: c.placement_constraints.into(),
            max_concurrency: c.max_concurrency,
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
pub struct CreateNamespace {
    pub name: String,
    pub blob_storage_bucket: Option<String>,
    pub blob_storage_region: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub enum FunctionRunOutcome {
    Undefined,
    Success,
    Failure,
}

impl From<data_model::FunctionRunOutcome> for FunctionRunOutcome {
    fn from(outcome: data_model::FunctionRunOutcome) -> Self {
        match outcome {
            data_model::FunctionRunOutcome::Unknown => FunctionRunOutcome::Undefined,
            data_model::FunctionRunOutcome::Success => FunctionRunOutcome::Success,
            data_model::FunctionRunOutcome::Failure(_) => FunctionRunOutcome::Failure,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub enum FunctionRunStatus {
    Pending,
    Running,
    Completed,
}

impl From<data_model::FunctionRunStatus> for FunctionRunStatus {
    fn from(status: data_model::FunctionRunStatus) -> Self {
        match status {
            data_model::FunctionRunStatus::Pending => FunctionRunStatus::Pending,
            data_model::FunctionRunStatus::Running(_) => FunctionRunStatus::Running,
            data_model::FunctionRunStatus::Completed => FunctionRunStatus::Completed,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema, Clone)]
pub struct RequestError {
    pub function_name: String,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct FunctionAllowlist {
    pub namespace: Option<String>,
    pub application: Option<String>,
    pub function: Option<String>,
    pub version: Option<String>,
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
    pub application_name: String,
    pub function_name: String,
    pub version: String,
    pub max_concurrency: u32,
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
        application_name: fe.application_name,
        function_name: fe.function_name,
        version: fe.version.to_string(),
        max_concurrency: fe.max_concurrency,
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
                application: fn_uri.application.clone(),
                function: fn_uri.function.clone(),
                version: fn_uri.version.clone(),
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
    pub cpu_cores: u32,
    pub memory_gb: u64,
    pub disk_gb: u64,
    #[serde(default)]
    pub gpu_models: Vec<String>,
    #[serde(default)]
    pub labels: std::collections::HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct ExecutorCatalog {
    pub entries: Vec<ExecutorCatalogEntry>,
    pub remark: Option<String>,
}

impl From<&crate::state_store::ExecutorCatalog> for ExecutorCatalog {
    fn from(catalog: &crate::state_store::ExecutorCatalog) -> Self {
        let remark = if catalog.empty() {
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
                    cpu_cores: entry.cpu_cores,
                    memory_gb: entry.memory_gb,
                    disk_gb: entry.disk_gb,
                    gpu_models: entry.gpu_models.clone(),
                    labels: entry.labels.clone(),
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
    pub application: String,
    pub function: String,
    pub executor_id: String,
    pub function_executor_id: String,
    pub function_call_id: String,
    pub request_id: String,
    pub created_at: u128,
    pub outcome: FunctionRunOutcome,
    pub attempt_number: u32,
    pub execution_duration_ms: Option<u64>,
}

impl From<data_model::Allocation> for Allocation {
    fn from(allocation: data_model::Allocation) -> Self {
        Self {
            id: allocation.id.to_string(),
            namespace: allocation.namespace,
            application: allocation.application,
            function: allocation.function,
            executor_id: allocation.target.executor_id.to_string(),
            function_executor_id: allocation.target.function_executor_id.get().to_string(),
            function_call_id: allocation.function_call_id.to_string(),
            request_id: allocation.request_id.to_string(),
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
    pub application: Option<String>,
    pub request: Option<String>,
}

impl From<data_model::StateChange> for StateChange {
    fn from(item: data_model::StateChange) -> Self {
        StateChange {
            id: item.id.to_string(),
            object_id: item.object_id.to_string(),
            change_type: item.change_type.to_string(),
            created_at: item.created_at,
            namespace: item.namespace,
            application: item.application,
            request: item.request,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct StateChangesResponse {
    pub count: usize,
    pub state_changes: Vec<StateChange>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UnallocatedFunctionRuns {
    pub count: usize,
    pub function_runs: Vec<FunctionRun>,
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
    use crate::http_objects::{Function, PlacementConstraints};

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
        println!("{data_model_filter:?}");
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
    fn test_function_with_placement_constraints() {
        let json = r#"{"name": "test_fn", "fn_name": "test_fn", "description": "Test function", "is_api": false, "image_information": {"image_name": "test", "tag": "latest", "base_image": "python", "run_strs": [], "sdk_version":"1.0.0"}, "input_encoder": "cloudpickle", "output_encoder":"cloudpickle", "placement_constraints": {"filter_expressions": ["environment==production", "gpu_type==nvidia"]}}"#;

        let function: Function = serde_json::from_str(json).unwrap();
        assert_eq!(function.placement_constraints.filter_expressions.len(), 2);

        // Test conversion to data model
        let data_model_fn: crate::data_model::Function = function.try_into().unwrap();
        assert_eq!(data_model_fn.placement_constraints.0.len(), 2);
    }

    #[test]
    fn test_function_with_unparseable_placement_constraints() {
        let json = r#"{"name": "test_fn", "fn_name": "test_fn", "description": "Test function", "is_api": false, "image_information": {"image_name": "test", "tag": "latest", "base_image": "python", "run_strs": [], "sdk_version":"1.0.0"}, "input_encoder": "cloudpickle", "output_encoder":"cloudpickle", "placement_constraints": {"filter_expressions": ["environment=production", "gpu_type=nvidia"]}}"#;

        let function: Function = serde_json::from_str(json).unwrap();
        assert_eq!(function.placement_constraints.filter_expressions.len(), 2);

        // Test failed conversion to data model
        assert!(<Function as TryInto<crate::data_model::Function>>::try_into(function).is_err());
    }
}
