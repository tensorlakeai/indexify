pub mod clocks;
pub mod filter;
pub mod test_objects;

use std::{
    collections::HashMap,
    fmt::{self, Display},
    hash::Hash,
    ops::Deref,
    str,
    vec,
};

use anyhow::{Result, anyhow};
use derive_builder::Builder;
use filter::LabelsFilter;
use nanoid::nanoid;
use serde::{Deserialize, Serialize};
use strum::Display;
use tracing::info;

use crate::{
    data_model::clocks::{Linearizable, VectorClock},
    utils::{get_epoch_time_in_ms, get_epoch_time_in_ns},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineMetadata {
    pub db_version: u64,
    pub last_change_idx: u64,
    #[serde(default)]
    pub last_usage_idx: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct ExecutorId(String);

impl Display for ExecutorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ExecutorId {
    pub fn new(id: String) -> Self {
        Self(id)
    }

    pub fn get(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ExecutorId {
    fn from(value: &str) -> Self {
        Self::new(value.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct AllocationTarget {
    pub executor_id: ExecutorId,
    pub function_executor_id: FunctionExecutorId,
}

impl AllocationTarget {
    pub fn new(executor_id: ExecutorId, function_executor_id: FunctionExecutorId) -> Self {
        Self {
            executor_id,
            function_executor_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct AllocationId(String);

impl Default for AllocationId {
    fn default() -> Self {
        Self(nanoid!())
    }
}

impl fmt::Display for AllocationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for AllocationId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for AllocationId {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputArgs {
    // ID of the function call that produced this payload
    // or consumed it from its child functionc call.
    pub function_call_id: Option<FunctionCallId>,
    pub data_payload: DataPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct Allocation {
    #[builder(default)]
    pub id: AllocationId,
    pub target: AllocationTarget,
    pub function_call_id: FunctionCallId,
    pub namespace: String,
    pub application: String,
    pub function: String,
    pub request_id: String,
    #[builder(default = "self.default_created_at()")]
    pub created_at: u128,
    pub outcome: FunctionRunOutcome,
    #[builder(setter(strip_option), default)]
    pub attempt_number: u32,
    #[builder(setter(into, strip_option), default)]
    pub execution_duration_ms: Option<u64>,
    pub input_args: Vec<InputArgs>,
    #[builder(default)]
    vector_clock: VectorClock,
    pub call_metadata: bytes::Bytes,
}

impl AllocationBuilder {
    fn default_created_at(&self) -> u128 {
        get_epoch_time_in_ms() as u128
    }
}

impl Linearizable for Allocation {
    fn vector_clock(&self) -> VectorClock {
        self.vector_clock.clone()
    }
}

impl Allocation {
    pub fn key(&self) -> String {
        Allocation::key_from(
            &self.namespace,
            &self.application,
            &self.request_id,
            &self.id,
        )
    }

    pub fn key_from(namespace: &str, application: &str, request_id: &str, id: &str) -> String {
        format!("{namespace}|{application}|{request_id}|{id}",)
    }

    pub fn key_prefix_from_request(namespace: &str, application: &str, request_id: &str) -> String {
        format!("{namespace}|{application}|{request_id}|")
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.outcome,
            FunctionRunOutcome::Success | FunctionRunOutcome::Failure(_)
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FunctionCallId(pub String);

impl Display for FunctionCallId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for FunctionCallId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionCall {
    pub function_call_id: FunctionCallId,
    pub inputs: Vec<FunctionArgs>,
    pub fn_name: String,
    pub call_metadata: bytes::Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FunctionArgs {
    FunctionRunOutput(FunctionCallId),
    DataPayload(DataPayload),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReduceOperation {
    pub function_call_id: FunctionCallId,
    pub collection: Vec<FunctionArgs>,
    pub fn_name: String,
    pub call_metadata: bytes::Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComputeOp {
    Reduce(ReduceOperation),
    FunctionCall(FunctionCall),
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct FunctionRun {
    pub id: FunctionCallId,
    pub request_id: String,
    pub namespace: String,
    pub application: String,
    pub name: String,
    pub version: String,
    pub compute_op: ComputeOp,
    pub input_args: Vec<InputArgs>,
    // Function call which output will be used as output of this function run
    // once the child function call finishes.
    #[builder(default)]
    pub child_function_call: Option<FunctionCallId>,
    // Some once this function run finishes with a value output
    // or its child call tree finishes and assigns its value output to this run.
    #[builder(default)]
    pub output: Option<DataPayload>,
    #[builder(default)]
    pub status: FunctionRunStatus,
    #[builder(default)]
    pub cache_hit: bool,
    #[builder(default)]
    pub outcome: Option<FunctionRunOutcome>,
    pub attempt_number: u32,
    #[builder(default = "self.default_creation_time_ns()")]
    pub creation_time_ns: u128,
    #[builder(default)]
    vector_clock: VectorClock,
    pub call_metadata: bytes::Bytes,
}

impl FunctionRunBuilder {
    fn default_creation_time_ns(&self) -> u128 {
        get_epoch_time_in_ns()
    }
}

impl FunctionRun {
    pub fn key_application_version(&self, application_name: &str) -> String {
        format!("{}|{}|{}", self.namespace, application_name, self.version,)
    }

    pub fn key(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            self.namespace, self.application, self.request_id, self.id
        )
    }

    pub fn key_prefix_for_request(namespace: &str, application: &str, request_id: &str) -> String {
        format!("{namespace}|{application}|{request_id}|")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeTimeoutMS(pub u32);

impl Default for NodeTimeoutMS {
    fn default() -> Self {
        NodeTimeoutMS(5 * 60 * 1000) // 5 minutes
    }
}

impl From<u32> for NodeTimeoutMS {
    fn from(val: u32) -> Self {
        NodeTimeoutMS(val)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GPUResources {
    pub count: u32,
    pub model: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FunctionResources {
    // 1000 CPU ms per sec is one full CPU core.
    // 2000 CPU ms per sec is two full CPU cores.
    pub cpu_ms_per_sec: u32,
    pub memory_mb: u64,
    pub ephemeral_disk_mb: u64,
    // The list is ordered from most to least preferred GPU configuration.
    pub gpu_configs: Vec<GPUResources>,
}

impl Default for FunctionResources {
    fn default() -> Self {
        FunctionResources {
            cpu_ms_per_sec: 1000,    // 1 full CPU core
            memory_mb: 1024,         // 1 GB
            ephemeral_disk_mb: 1024, // 1 GB
            gpu_configs: vec![],     // No GPUs by default
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FunctionRetryPolicy {
    pub max_retries: u32,
    pub initial_delay_ms: u32,
    pub max_delay_ms: u32,
    // The multiplier value is 1000x of the actual value to avoid working with floating point.
    pub delay_multiplier: u32,
}

impl Default for FunctionRetryPolicy {
    fn default() -> Self {
        FunctionRetryPolicy {
            max_retries: 0, // No retries by default
            initial_delay_ms: 1000,
            max_delay_ms: 1000,
            delay_multiplier: 1000,
        }
    }
}

fn default_data_encoder() -> String {
    "cloudpickle".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Hash, Eq)]
pub struct CacheKey(String);

impl CacheKey {
    pub fn get(&self) -> &str {
        &self.0
    }
}

impl From<&str> for CacheKey {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl fmt::Display for CacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

fn default_max_concurrency() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct Function {
    pub name: String,
    pub description: String,
    pub placement_constraints: LabelsFilter,
    pub fn_name: String,
    #[serde(default = "default_data_encoder")]
    pub input_encoder: String,
    #[serde(default = "default_data_encoder")]
    pub output_encoder: String,
    pub secret_names: Option<Vec<String>>,
    #[serde(default)]
    pub initialization_timeout: NodeTimeoutMS,
    #[serde(default)]
    pub timeout: NodeTimeoutMS,
    #[serde(default)]
    pub resources: FunctionResources,
    #[serde(default)]
    pub retry_policy: FunctionRetryPolicy,
    pub cache_key: Option<CacheKey>,
    #[serde(default)]
    pub parameters: Vec<ParameterMetadata>,
    #[serde(default)]
    pub return_type: Option<serde_json::Value>,
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: u32,
}

impl Function {
    pub fn create_function_call(
        &self,
        function_call_id: FunctionCallId,
        inputs: Vec<DataPayload>,
        call_metadata: bytes::Bytes,
    ) -> FunctionCall {
        FunctionCall {
            function_call_id,
            inputs: inputs.into_iter().map(FunctionArgs::DataPayload).collect(),
            fn_name: self.name.clone(),
            call_metadata,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ParameterMetadata {
    pub name: String,
    pub description: Option<String>,
    pub required: bool,
    pub data_type: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub enum ApplicationState {
    #[default]
    Active,
    Disabled {
        reason: String,
    },
}

impl ApplicationState {
    pub fn as_disabled(&self) -> Option<String> {
        match self {
            ApplicationState::Active => None,
            ApplicationState::Disabled { reason } => Some(reason.clone()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ApplicationEntryPoint {
    pub function_name: String,
    pub input_serializer: String,
    pub output_serializer: String,
    pub output_type_hints_base64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Builder)]
pub struct Application {
    pub namespace: String,
    pub name: String,
    pub tombstoned: bool,
    pub description: String,
    #[serde(default)]
    #[builder(default)]
    pub tags: HashMap<String, String>,
    #[builder(default = "self.default_created_at()")]
    pub created_at: u64,
    // Fields below are versioned. The version field is currently managed manually by users
    pub version: String,
    pub code: DataPayload,
    pub functions: HashMap<String, Function>,
    #[serde(default)]
    #[builder(default)]
    pub state: ApplicationState,
    #[builder(default)]
    vector_clock: VectorClock,
    pub entrypoint: ApplicationEntryPoint,
}

impl ApplicationBuilder {
    fn default_created_at(&self) -> u64 {
        get_epoch_time_in_ms()
    }
}

impl Linearizable for Application {
    fn vector_clock(&self) -> VectorClock {
        self.vector_clock.clone()
    }
}

impl Application {
    pub fn key(&self) -> String {
        Application::key_from(&self.namespace, &self.name)
    }

    pub fn key_from(namespace: &str, name: &str) -> String {
        format!("{namespace}|{name}")
    }

    /// Update the application from all the supplied Application fields.
    ///
    /// Assumes validated update values.
    pub fn update(&mut self, update: Application) {
        // immutable fields
        // self.namespace = other.namespace;
        // self.name = other.name;
        // self.created_at = other.created_at;
        // self.replaying = other.replaying;

        self.version = update.version;
        self.code = update.code;
        self.functions = update.functions.clone();
        self.description = update.description;
        self.tags = update.tags;
        self.entrypoint = update.entrypoint;
        self.state = update.state;
    }

    pub fn to_version(&self) -> Result<ApplicationVersion> {
        ApplicationVersionBuilder::default()
            .namespace(self.namespace.clone())
            .name(self.name.clone())
            .created_at(self.created_at)
            .version(self.version.clone())
            .code(self.code.clone())
            .entrypoint(self.entrypoint.clone())
            .functions(self.functions.clone())
            .state(self.state.clone())
            .build()
            .map_err(Into::into)
    }

    pub fn can_be_scheduled(
        &self,
        executor_catalog: &crate::state_store::ExecutorCatalog,
    ) -> Result<()> {
        for node in self.functions.values() {
            let mut has_cpu = false;
            let mut has_mem = false;
            let mut has_disk = false;
            let mut has_gpu_models = false;
            let mut met_placement_constraints = false;
            // If the executor catalog is empty, it implies there
            // are no resource limits or constraints indexify needs to check
            if executor_catalog.empty() {
                return Ok(());
            }
            for entry in executor_catalog.entries.iter() {
                if node.placement_constraints.matches(&entry.labels) {
                    met_placement_constraints = true;
                }

                has_cpu = (node.resources.cpu_ms_per_sec / 1000) <= entry.cpu_cores;
                has_mem = node.resources.memory_mb <= entry.memory_gb * 1024;
                has_disk = node.resources.ephemeral_disk_mb <= entry.disk_gb * 1024;
                has_gpu_models = node
                    .resources
                    .gpu_configs
                    .iter()
                    .all(|gpu| entry.gpu_models.contains(&gpu.model));

                if met_placement_constraints && has_cpu && has_mem && has_disk && has_gpu_models {
                    info!(
                        "function {} can be scheduled on executor catalog entry {}",
                        node.name, entry.name
                    );
                    break;
                }
            }
            if !met_placement_constraints {
                return Err(anyhow!(
                    "function {} is asking for labels {:?}, but no executor catalog entry matches, current catalog: {}",
                    node.name,
                    node.placement_constraints.0,
                    executor_catalog
                        .entries
                        .iter()
                        .map(|entry| entry.to_string())
                        .collect::<Vec<String>>()
                        .join(", "),
                ));
            }
            if !has_cpu {
                return Err(anyhow!(
                    "function {} is asking for CPU {}. Not available in any executor catalog entry, current catalog: {}",
                    node.name,
                    node.resources.cpu_ms_per_sec / 1000,
                    executor_catalog
                        .entries
                        .iter()
                        .map(|entry| entry.to_string())
                        .collect::<Vec<String>>()
                        .join(", "),
                ));
            }
            if !has_mem {
                return Err(anyhow!(
                    "function {} is asking for memory {}. Not available in any executor catalog entry, current catalog: {}",
                    node.name,
                    node.resources.memory_mb,
                    executor_catalog
                        .entries
                        .iter()
                        .map(|entry| entry.to_string())
                        .collect::<Vec<String>>()
                        .join(", "),
                ));
            }
            if !has_disk {
                return Err(anyhow!(
                    "function {} is asking for disk {}. Not available in any executor catalog entry, current catalog: {}",
                    node.name,
                    node.resources.ephemeral_disk_mb,
                    executor_catalog
                        .entries
                        .iter()
                        .map(|entry| entry.to_string())
                        .collect::<Vec<String>>()
                        .join(", "),
                ));
            }
            if !has_gpu_models {
                return Err(anyhow!(
                    "function {} is asking for GPU models {}. Not available in any executor catalog entry, current catalog: {}",
                    node.name,
                    node.resources
                        .gpu_configs
                        .iter()
                        .map(|gpu| gpu.model.clone())
                        .collect::<Vec<String>>()
                        .join(", "),
                    executor_catalog
                        .entries
                        .iter()
                        .map(|entry| entry.to_string())
                        .collect::<Vec<String>>()
                        .join(", "),
                ));
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Builder)]
pub struct ApplicationVersion {
    pub namespace: String,
    pub name: String,
    pub created_at: u64,
    pub version: String,
    pub code: DataPayload,
    pub entrypoint: ApplicationEntryPoint,
    #[builder(default)]
    pub functions: HashMap<String, Function>,
    #[builder(default)]
    pub edges: HashMap<String, Vec<String>>,
    #[serde(default)]
    #[builder(default)]
    pub state: ApplicationState,
    #[builder(default)]
    vector_clock: VectorClock,
}

impl Linearizable for ApplicationVersion {
    fn vector_clock(&self) -> VectorClock {
        self.vector_clock.clone()
    }
}

impl ApplicationVersion {
    pub fn create_function_run(
        &self,
        fn_call: &FunctionCall,
        input_args: Vec<InputArgs>,
        request_id: &str,
    ) -> Result<FunctionRun> {
        FunctionRunBuilder::default()
            .id(fn_call.function_call_id.clone())
            .namespace(self.namespace.clone())
            .application(self.name.clone())
            .version(self.version.clone())
            .compute_op(ComputeOp::FunctionCall(FunctionCall {
                function_call_id: fn_call.function_call_id.clone(),
                inputs: fn_call.inputs.clone(),
                fn_name: fn_call.fn_name.clone(),
                call_metadata: fn_call.call_metadata.clone(),
            }))
            .input_args(input_args)
            .name(fn_call.fn_name.clone())
            .request_id(request_id.to_string())
            .status(FunctionRunStatus::Pending)
            .attempt_number(0)
            .call_metadata(fn_call.call_metadata.clone())
            .creation_time_ns(get_epoch_time_in_ns())
            .vector_clock(VectorClock::default())
            .build()
            .map_err(|e| anyhow!("failed to create function run: {e}"))
    }

    pub fn key(&self) -> String {
        ApplicationVersion::key_from(&self.namespace, &self.name, &self.version)
    }

    pub fn key_from(namespace: &str, application_name: &str, version: &str) -> String {
        format!("{namespace}|{application_name}|{version}")
    }

    pub fn key_prefix_from(namespace: &str, name: &str) -> String {
        format!("{namespace}|{name}|")
    }

    pub fn function_run_max_retries(&self, fn_run: &FunctionRun) -> Option<u32> {
        self.functions
            .get(&fn_run.name)
            .map(|func| func.retry_policy.max_retries)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Builder)]
pub struct DataPayload {
    #[builder(default = "self.default_id()")]
    pub id: String,
    pub path: String,
    pub metadata_size: u64,
    pub offset: u64,
    pub size: u64,
    pub sha256_hash: String,
    #[builder(default = "self.default_encoding()")]
    pub encoding: String,
}

impl DataPayloadBuilder {
    fn default_encoding(&self) -> String {
        "application/octet-stream".to_string()
    }

    fn default_id(&self) -> String {
        nanoid::nanoid!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RequestOutcome {
    Unknown,
    Success,
    Failure(RequestFailureReason),
}

impl Default for RequestOutcome {
    fn default() -> Self {
        Self::Unknown
    }
}

impl From<FunctionRunOutcome> for RequestOutcome {
    fn from(outcome: FunctionRunOutcome) -> Self {
        match outcome {
            FunctionRunOutcome::Success => RequestOutcome::Success,
            FunctionRunOutcome::Failure(failure_reason) => {
                RequestOutcome::Failure(failure_reason.into())
            }
            FunctionRunOutcome::Unknown => RequestOutcome::Unknown,
        }
    }
}

impl Display for RequestOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestOutcome::Unknown => write!(f, "Unknown"),
            RequestOutcome::Success => write!(f, "Success"),
            RequestOutcome::Failure(reason) => {
                write!(f, "Failure (")?;
                reason.fmt(f)?;
                write!(f, ")")
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RequestFailureReason {
    // Used when request didn't finish yet and when request finished successfully
    Unknown,
    // Internal error on Executor aka platform error.
    InternalError,
    // Clear function code failure typically by raising an exception from the function code.
    FunctionError,
    // Function code raised RequestError to mark the request as permanently failed.
    RequestError,
    // A graph function cannot be scheduled given the specified constraints.
    ConstraintUnsatisfiable,
    // Cancelled.
    Cancelled,
}

impl Display for RequestFailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str_val = match self {
            RequestFailureReason::Unknown => "Unknown",
            RequestFailureReason::InternalError => "InternalError",
            RequestFailureReason::FunctionError => "FunctionError",
            RequestFailureReason::RequestError => "RequestError",
            RequestFailureReason::ConstraintUnsatisfiable => "ConstraintUnsatisfiable",
            RequestFailureReason::Cancelled => "Cancelled",
        };
        write!(f, "{str_val}")
    }
}

impl Default for RequestFailureReason {
    fn default() -> Self {
        Self::Unknown
    }
}

impl From<FunctionRunFailureReason> for RequestFailureReason {
    fn from(failure_reason: FunctionRunFailureReason) -> Self {
        match failure_reason {
            FunctionRunFailureReason::Unknown => RequestFailureReason::Unknown,
            FunctionRunFailureReason::InternalError => RequestFailureReason::InternalError,
            FunctionRunFailureReason::FunctionError => RequestFailureReason::FunctionError,
            FunctionRunFailureReason::FunctionTimeout => RequestFailureReason::FunctionError,
            FunctionRunFailureReason::RequestError => RequestFailureReason::RequestError,
            FunctionRunFailureReason::FunctionRunCancelled => RequestFailureReason::Cancelled,
            FunctionRunFailureReason::FunctionExecutorTerminated |
            FunctionRunFailureReason::ExecutorRemoved => RequestFailureReason::InternalError,
            FunctionRunFailureReason::ConstraintUnsatisfiable => {
                RequestFailureReason::ConstraintUnsatisfiable
            }
            FunctionRunFailureReason::OutOfMemory => RequestFailureReason::FunctionError,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct RequestError {
    pub function_name: String,
    pub payload: DataPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct RequestCtx {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
    #[serde(default)]
    #[builder(default)]
    pub outcome: Option<RequestOutcome>,
    #[builder(default = "self.default_created_at()")]
    pub created_at: u64,
    #[builder(setter(strip_option), default)]
    pub request_error: Option<RequestError>,
    #[builder(default)]
    vector_clock: VectorClock,

    #[builder(default)]
    pub function_runs: HashMap<FunctionCallId, FunctionRun>,
    pub function_calls: HashMap<FunctionCallId, FunctionCall>,

    #[builder(default)]
    pub child_function_calls: HashMap<String, RequestCtx>, /* Child Request ID ->
                                                            * Child
                                                            * RequestCtx */
}

impl RequestCtxBuilder {
    fn default_created_at(&self) -> u64 {
        get_epoch_time_in_ms()
    }
}

impl Linearizable for RequestCtx {
    fn vector_clock(&self) -> VectorClock {
        self.vector_clock.clone()
    }
}

impl RequestCtx {
    pub fn key(&self) -> String {
        format!(
            "{}|{}|{}",
            self.namespace, self.application_name, self.request_id
        )
    }

    pub fn secondary_index_key(&self) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(self.namespace.as_bytes());
        key.push(b'|');
        key.extend_from_slice(self.application_name.as_bytes());
        key.push(b'|');
        key.extend_from_slice(&self.created_at.to_be_bytes());
        key.push(b'|');
        key.extend_from_slice(self.request_id.as_bytes());
        key
    }

    pub fn get_request_id_from_secondary_index_key(key: &[u8]) -> Option<String> {
        key.split(|&b| b == b'|')
            .nth(3)
            .map(|s| String::from_utf8_lossy(s).into_owned())
    }

    pub fn key_from(ns: &str, cg: &str, id: &str) -> String {
        format!("{ns}|{cg}|{id}")
    }

    pub fn key_prefix_for_application(namespace: &str, application: &str) -> String {
        format!("{namespace}|{application}|")
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum FunctionRunOutcome {
    #[default]
    Unknown,
    Success,
    Failure(FunctionRunFailureReason),
}

impl Display for FunctionRunOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionRunOutcome::Unknown => write!(f, "Unknown"),
            FunctionRunOutcome::Success => write!(f, "Success"),
            FunctionRunOutcome::Failure(reason) => {
                write!(f, "Failure (")?;
                reason.fmt(f)?;
                write!(f, ")")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum FunctionRunFailureReason {
    Unknown,
    // Internal error on Executor aka platform error.
    InternalError,
    // Clear function code failure typically by raising an exception from the function code
    // and grey failures when we can't determine the exact cause.
    FunctionError,
    // Function code run time exceeded its configured timeout.
    FunctionTimeout,
    // Function code raised RequestError to mark the request as permanently failed.
    RequestError,
    // Server removed the task allocation from Executor desired state.
    // The task allocation didn't finish before the removal.
    FunctionRunCancelled,
    // Function Executor terminated - can't run the task allocation on it anymore.
    FunctionExecutorTerminated,
    // Function run cannot be scheduled given its constraints.
    ConstraintUnsatisfiable,
    // Executor was removed
    ExecutorRemoved,

    OutOfMemory,
}

impl Display for FunctionRunFailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str_val = match self {
            FunctionRunFailureReason::Unknown => "Unknown",
            FunctionRunFailureReason::InternalError => "InternalError",
            FunctionRunFailureReason::FunctionError => "FunctionError",
            FunctionRunFailureReason::FunctionTimeout => "FunctionTimeout",
            FunctionRunFailureReason::RequestError => "RequestError",
            FunctionRunFailureReason::FunctionRunCancelled => "FunctionRunCancelled",
            FunctionRunFailureReason::FunctionExecutorTerminated => "FunctionExecutorTerminated",
            FunctionRunFailureReason::ConstraintUnsatisfiable => "ConstraintUnsatisfiable",
            FunctionRunFailureReason::ExecutorRemoved => "ExecutorRemoved",
            FunctionRunFailureReason::OutOfMemory => "OOM",
        };
        write!(f, "{str_val}")
    }
}

impl Default for FunctionRunFailureReason {
    fn default() -> Self {
        Self::Unknown
    }
}

impl FunctionRunFailureReason {
    pub fn is_retriable(&self) -> bool {
        // RequestError and RetryLimitExceeded are not retriable;
        // they fail the request permanently.
        matches!(
            self,
            FunctionRunFailureReason::InternalError |
                FunctionRunFailureReason::FunctionError |
                FunctionRunFailureReason::FunctionTimeout |
                FunctionRunFailureReason::FunctionExecutorTerminated |
                FunctionRunFailureReason::ExecutorRemoved |
                FunctionRunFailureReason::OutOfMemory
        )
    }

    pub fn should_count_against_function_run_retry_attempts(self) -> bool {
        // Explicit platform decisions and provable infrastructure
        // failures don't count against retry attempts; everything
        // else counts against retry attempts.
        //
        // Includes InternalError right now to prevent infinite retries
        // with long lasting internal problems.
        matches!(
            self,
            FunctionRunFailureReason::InternalError |
                FunctionRunFailureReason::FunctionError |
                FunctionRunFailureReason::FunctionTimeout |
                FunctionRunFailureReason::OutOfMemory
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct RunningFunctionRunStatus {
    pub allocation_id: AllocationId,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum FunctionRunStatus {
    /// Function run is waiting for execution
    Pending,
    /// Function run is running
    Running(RunningFunctionRunStatus),
    /// Function run is completed
    Completed,
}

impl Default for FunctionRunStatus {
    fn default() -> Self {
        Self::Completed
    }
}

impl Display for FunctionRunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str_val = match self {
            FunctionRunStatus::Pending => "Pending".to_string(),
            FunctionRunStatus::Running(status) => format!("Running({status:?})"),
            FunctionRunStatus::Completed => "Completed".to_string(),
        };
        write!(f, "{str_val}")
    }
}

// FIXME Remove in next release
fn default_executor_ver() -> String {
    "0.2.17".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FunctionURI {
    pub namespace: String,
    pub application: String,
    pub function: String,
    pub version: String,
}

impl From<FunctionExecutorServerMetadata> for FunctionURI {
    fn from(fe_meta: FunctionExecutorServerMetadata) -> Self {
        FunctionURI {
            namespace: fe_meta.function_executor.namespace.clone(),
            application: fe_meta.function_executor.application_name.clone(),
            function: fe_meta.function_executor.function_name.clone(),
            version: fe_meta.function_executor.version.clone(),
        }
    }
}

impl From<Box<FunctionExecutorServerMetadata>> for FunctionURI {
    fn from(fe_meta: Box<FunctionExecutorServerMetadata>) -> Self {
        FunctionURI::from(*fe_meta)
    }
}

impl From<&FunctionExecutor> for FunctionURI {
    fn from(fe: &FunctionExecutor) -> Self {
        FunctionURI {
            namespace: fe.namespace.clone(),
            application: fe.application_name.clone(),
            function: fe.function_name.clone(),
            version: fe.version.clone(),
        }
    }
}

impl From<&FunctionRun> for FunctionURI {
    fn from(function_run: &FunctionRun) -> Self {
        FunctionURI {
            namespace: function_run.namespace.clone(),
            application: function_run.application.clone(),
            function: function_run.name.clone(),
            version: function_run.version.clone(),
        }
    }
}

impl Display for FunctionURI {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}",
            self.namespace, self.application, self.function, self.version
        )
    }
}

impl GPUResources {
    pub fn can_handle(&self, requested_resources: &GPUResources) -> bool {
        self.count >= requested_resources.count && self.model == requested_resources.model
    }
}

// Supported GPU models.
pub const GPU_MODEL_NVIDIA_H100_80GB: &str = "H100";
pub const GPU_MODEL_NVIDIA_A100_40GB: &str = "A100-40GB";
pub const GPU_MODEL_NVIDIA_A100_80GB: &str = "A100-80GB";
pub const GPU_MODEL_NVIDIA_TESLA_T4: &str = "T4";
pub const GPU_MODEL_NVIDIA_A6000: &str = "A6000";
pub const GPU_MODEL_NVIDIA_A10: &str = "A10";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct HostResources {
    // 1000 CPU ms per sec is one full CPU core.
    // 2000 CPU ms per sec is two full CPU cores.
    pub cpu_ms_per_sec: u32,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    // Not all Executors have GPUs.
    pub gpu: Option<GPUResources>,
}

impl HostResources {
    // If can't handle, returns error that describes the reason why.
    fn can_handle_gpu(&self, request: &Option<GPUResources>) -> Result<()> {
        match request {
            Some(requested_gpu) => match &self.gpu {
                Some(available_gpu) => {
                    if available_gpu.can_handle(requested_gpu) {
                        Ok(())
                    } else {
                        Err(anyhow!(
                            "Not enough GPU resources, available: {available_gpu:?}, requested: {requested_gpu:?}",
                        ))
                    }
                }
                None => Err(anyhow!(
                    "No GPU is available but a GPU is requested: {requested_gpu:?}",
                )),
            },
            None => Ok(()),
        }
    }

    // If can't handle, returns error that describes the reason why.
    pub fn can_handle_fe_resources(&self, request: &FunctionExecutorResources) -> Result<()> {
        let requested_memory_bytes = request.memory_mb * 1024 * 1024;
        let requested_disk_bytes = request.ephemeral_disk_mb * 1024 * 1024;

        if self.cpu_ms_per_sec < request.cpu_ms_per_sec {
            return Err(anyhow!(
                "Not enough CPU resources, {} < {}",
                self.cpu_ms_per_sec,
                request.cpu_ms_per_sec
            ));
        }

        if self.memory_bytes < requested_memory_bytes {
            return Err(anyhow!(
                "Not enough memory resources, {} < {}",
                self.memory_bytes,
                requested_memory_bytes
            ));
        }

        if self.disk_bytes < requested_disk_bytes {
            return Err(anyhow!(
                "Not enough disk resources, {} < {}",
                self.disk_bytes,
                requested_disk_bytes
            ));
        }

        self.can_handle_gpu(&request.gpu)
    }

    // If can't handle, returns error that describes the reason why.
    pub fn can_handle_function_resources(&self, request: &FunctionResources) -> Result<()> {
        let fe_resources_no_gpu = FunctionExecutorResources {
            cpu_ms_per_sec: request.cpu_ms_per_sec,
            memory_mb: request.memory_mb,
            ephemeral_disk_mb: request.ephemeral_disk_mb,
            gpu: None,
        };

        let mut result = self.can_handle_fe_resources(&fe_resources_no_gpu);

        for gpu in &request.gpu_configs {
            let mut fe_resources_gpu = fe_resources_no_gpu.clone();
            fe_resources_gpu.gpu = Some(gpu.clone());
            result = self.can_handle_fe_resources(&fe_resources_gpu);
            if result.is_ok() {
                return Ok(());
            }
        }

        result
    }

    pub fn consume_fe_resources(&mut self, request: &FunctionExecutorResources) -> Result<()> {
        self.can_handle_fe_resources(request)?;

        // Allocate the resources only after all the checks passed to not leak anything
        // on error. Callers rely on this behavior.
        self.cpu_ms_per_sec -= request.cpu_ms_per_sec;
        self.memory_bytes -= request.memory_mb * 1024 * 1024;
        self.disk_bytes -= request.ephemeral_disk_mb * 1024 * 1024;
        if let Some(requested_gpu) = &request.gpu &&
            let Some(available_gpu) = &mut self.gpu
        {
            available_gpu.count -= requested_gpu.count;
        }

        Ok(())
    }

    pub fn consume_function_resources(
        &mut self,
        request: &FunctionResources,
    ) -> Result<FunctionExecutorResources> {
        let fe_resources_no_gpu = FunctionExecutorResources {
            cpu_ms_per_sec: request.cpu_ms_per_sec,
            memory_mb: request.memory_mb,
            ephemeral_disk_mb: request.ephemeral_disk_mb,
            gpu: None,
        };

        if request.gpu_configs.is_empty() {
            self.consume_fe_resources(&fe_resources_no_gpu)?;
            return Ok(fe_resources_no_gpu);
        }

        for gpu in &request.gpu_configs {
            let mut fe_resources_gpu = fe_resources_no_gpu.clone();
            fe_resources_gpu.gpu = Some(gpu.clone());
            if self.consume_fe_resources(&fe_resources_gpu).is_ok() {
                return Ok(fe_resources_gpu);
            }
        }

        Err(anyhow!(
            "Function asked for GPUs {:?} but executor only has {:?}",
            request.gpu_configs,
            self.gpu
        ))
    }

    pub fn free(&mut self, allocated_resources: &FunctionExecutorResources) -> Result<()> {
        self.cpu_ms_per_sec += allocated_resources.cpu_ms_per_sec;
        self.memory_bytes += allocated_resources.memory_mb * 1024 * 1024;
        self.disk_bytes += allocated_resources.ephemeral_disk_mb * 1024 * 1024;

        if let Some(allocated_gpu) = &allocated_resources.gpu {
            if let Some(available_gpu) = &mut self.gpu {
                if available_gpu.model == allocated_gpu.model {
                    available_gpu.count += allocated_gpu.count;
                } else {
                    return Err(anyhow!(
                        "Can't free GPU resources, GPU model mismatch: {} != {}",
                        available_gpu.model,
                        allocated_gpu.model
                    ));
                }
            } else {
                return Err(anyhow!(
                    "Can't free GPU resources, no GPU available on the Executor"
                ));
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, strum::AsRefStr)]
pub enum ExecutorState {
    #[default]
    Unknown,
    StartingUp,
    Running,
    Drained,
    Stopped,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FunctionExecutorId(String);
impl FunctionExecutorId {
    pub fn new(id: String) -> Self {
        Self(id)
    }

    pub fn get(&self) -> &str {
        &self.0
    }
}
impl Default for FunctionExecutorId {
    fn default() -> Self {
        Self::new(nanoid::nanoid!())
    }
}

impl From<&str> for FunctionExecutorId {
    fn from(s: &str) -> Self {
        Self::new(s.to_string())
    }
}

#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Default, strum::AsRefStr, Display, Eq, Hash,
)]
pub enum FunctionExecutorState {
    #[default]
    Unknown,
    // Function Executor is being created.
    Pending,
    // Function Executor is running and ready to accept tasks.
    Running,
    // Function Executor is terminated, all resources are freed.
    Terminated {
        reason: FunctionExecutorTerminationReason,
        failed_alloc_ids: Vec<String>,
    },
}

#[derive(
    Debug,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    PartialEq,
    Default,
    strum::AsRefStr,
    Display,
    Eq,
    Hash,
)]
pub enum FunctionExecutorTerminationReason {
    #[default]
    Unknown,
    StartupFailedInternalError,
    StartupFailedFunctionError,
    StartupFailedFunctionTimeout,
    Unhealthy,
    InternalError,
    FunctionError,
    FunctionTimeout,
    FunctionCancelled,
    DesiredStateRemoved,
    ExecutorRemoved,
    Oom,
}

impl From<FunctionExecutorTerminationReason> for FunctionRunFailureReason {
    fn from(reason: FunctionExecutorTerminationReason) -> Self {
        match reason {
            FunctionExecutorTerminationReason::Unknown => {
                FunctionRunFailureReason::FunctionExecutorTerminated
            }
            FunctionExecutorTerminationReason::StartupFailedInternalError => {
                FunctionRunFailureReason::InternalError
            }
            FunctionExecutorTerminationReason::StartupFailedFunctionError => {
                FunctionRunFailureReason::FunctionError
            }
            FunctionExecutorTerminationReason::StartupFailedFunctionTimeout => {
                FunctionRunFailureReason::FunctionTimeout
            }
            FunctionExecutorTerminationReason::Unhealthy => {
                FunctionRunFailureReason::FunctionTimeout
            }
            FunctionExecutorTerminationReason::InternalError => {
                FunctionRunFailureReason::InternalError
            }
            FunctionExecutorTerminationReason::FunctionError => {
                FunctionRunFailureReason::FunctionError
            }
            FunctionExecutorTerminationReason::FunctionTimeout => {
                FunctionRunFailureReason::FunctionTimeout
            }
            FunctionExecutorTerminationReason::FunctionCancelled => {
                FunctionRunFailureReason::FunctionRunCancelled
            }
            FunctionExecutorTerminationReason::DesiredStateRemoved => {
                FunctionRunFailureReason::FunctionExecutorTerminated
            }
            FunctionExecutorTerminationReason::ExecutorRemoved => {
                FunctionRunFailureReason::ExecutorRemoved
            }
            FunctionExecutorTerminationReason::Oom => FunctionRunFailureReason::OutOfMemory,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FunctionAllowlist {
    pub namespace: Option<String>,
    pub application: Option<String>,
    pub function: Option<String>,
    pub version: Option<String>,
}

impl FunctionAllowlist {
    pub fn matches_function_executor(&self, function_executor: &FunctionExecutor) -> bool {
        self.namespace
            .as_ref()
            .is_none_or(|ns| ns == &function_executor.namespace) &&
            self.application
                .as_ref()
                .is_none_or(|cg_name| cg_name == &function_executor.application_name) &&
            self.function
                .as_ref()
                .is_none_or(|fn_name| fn_name == &function_executor.function_name) &&
            self.version
                .as_ref()
                .is_none_or(|version| version == &function_executor.version)
    }

    pub fn matches_function(&self, function_run: &FunctionRun) -> bool {
        self.namespace
            .as_ref()
            .is_none_or(|ns| ns == &function_run.namespace) &&
            self.application
                .as_ref()
                .is_none_or(|cg_name| cg_name == &function_run.application) &&
            self.function
                .as_ref()
                .is_none_or(|fn_name| fn_name == &function_run.name) &&
            self.version
                .as_ref()
                .is_none_or(|version| version == &function_run.version)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder, Eq, PartialEq)]
pub struct FunctionExecutorResources {
    pub cpu_ms_per_sec: u32,
    pub memory_mb: u64,
    pub ephemeral_disk_mb: u64,
    pub gpu: Option<GPUResources>, // None if no GPU.
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct GcUrl {
    pub url: String,
    pub namespace: String,
    #[builder(default)]
    vector_clock: VectorClock,
}

impl Linearizable for GcUrl {
    fn vector_clock(&self) -> VectorClock {
        self.vector_clock.clone()
    }
}

impl GcUrl {
    pub fn key(&self) -> String {
        format!("{}|{}", self.namespace, self.url)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct FunctionExecutor {
    #[builder(default)]
    pub id: FunctionExecutorId,
    pub namespace: String,
    pub application_name: String,
    pub function_name: String,
    pub version: String,
    pub state: FunctionExecutorState,
    pub resources: FunctionExecutorResources,
    pub max_concurrency: u32,
    #[builder(default)]
    vector_clock: VectorClock,
}

impl Linearizable for FunctionExecutor {
    fn vector_clock(&self) -> VectorClock {
        self.vector_clock.clone()
    }
}

impl PartialEq for FunctionExecutor {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for FunctionExecutor {}

impl Hash for FunctionExecutor {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl FunctionExecutor {
    pub fn fn_uri_str(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            self.namespace, self.application_name, self.function_name, self.version,
        )
    }

    pub fn update(&mut self, other: &FunctionExecutor) {
        // Only update fields that change after self FE was created.
        // Other FE mush represent the same FE.
        self.state = other.state.clone();
    }
}

#[derive(Debug, Clone, Builder)]
pub struct ExecutorServerMetadata {
    pub executor_id: ExecutorId,
    pub function_executors: HashMap<FunctionExecutorId, Box<FunctionExecutorServerMetadata>>,
    pub free_resources: HostResources,
    pub resource_claims: HashMap<FunctionExecutorId, FunctionExecutorResources>,
}

impl Eq for ExecutorServerMetadata {}

impl PartialEq for ExecutorServerMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.executor_id == other.executor_id
    }
}

impl Hash for ExecutorServerMetadata {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.executor_id.hash(state);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct FunctionExecutorServerMetadata {
    pub executor_id: ExecutorId,
    pub function_executor: FunctionExecutor,
    pub desired_state: FunctionExecutorState,
}

impl Eq for FunctionExecutorServerMetadata {}

impl PartialEq for FunctionExecutorServerMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.executor_id == other.executor_id &&
            self.function_executor.id == other.function_executor.id
    }
}

impl Hash for FunctionExecutorServerMetadata {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.executor_id.hash(state);
        self.function_executor.id.hash(state);
    }
}

impl FunctionExecutorServerMetadata {
    pub fn new(
        executor_id: ExecutorId,
        function_executor: FunctionExecutor,
        desired_state: FunctionExecutorState,
    ) -> Self {
        Self {
            executor_id,
            function_executor,
            desired_state,
        }
    }

    pub fn fn_uri_str(&self) -> String {
        self.function_executor.fn_uri_str()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct ExecutorMetadata {
    pub id: ExecutorId,
    #[serde(default = "default_executor_ver")]
    pub executor_version: String,
    pub function_allowlist: Option<Vec<FunctionAllowlist>>,
    pub addr: String,
    pub labels: HashMap<String, String>,
    pub function_executors: HashMap<FunctionExecutorId, FunctionExecutor>,
    pub host_resources: HostResources,
    pub state: ExecutorState,
    #[builder(default)]
    pub tombstoned: bool,
    pub state_hash: String,
    #[builder(default)]
    pub clock: u64,
    #[builder(default)]
    vector_clock: VectorClock,
}

impl Linearizable for ExecutorMetadata {
    fn vector_clock(&self) -> VectorClock {
        self.vector_clock.clone()
    }
}

impl ExecutorMetadata {
    pub fn is_function_allowed(&self, function_run: &FunctionRun) -> bool {
        if let Some(function_allowlist) = &self.function_allowlist {
            function_allowlist
                .iter()
                .any(|allowlist| allowlist.matches_function(function_run))
        } else {
            true
        }
    }

    pub fn update(&mut self, update: ExecutorMetadata) {
        self.function_allowlist = update.function_allowlist;
        self.function_executors = update.function_executors;
        self.state = update.state;
        self.state_hash = update.state_hash;
        self.clock = update.clock;
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
pub struct InvokeApplicationEvent {
    pub request_id: String,
    pub namespace: String,
    pub application: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct GraphUpdates {
    pub graph_updates: Vec<ComputeOp>,
    // The ID of the function call that produces the final output
    // value of the call graph defined in GraphUpdates.
    pub output_function_call_id: FunctionCallId,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct AllocationOutputIngestedEvent {
    pub namespace: String,
    pub application: String,
    pub function: String,
    pub request_id: String,
    pub function_call_id: FunctionCallId,
    pub data_payload: Option<DataPayload>,
    pub graph_updates: Option<GraphUpdates>,
    pub allocation_key: String,
    pub request_exception: Option<DataPayload>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct ExecutorRemovedEvent {
    pub executor_id: ExecutorId,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct TombstoneApplicationEvent {
    pub namespace: String,
    pub application: String,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct TombstoneRequestEvent {
    pub namespace: String,
    pub application: String,
    pub request_id: String,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct ExecutorUpsertedEvent {
    pub executor_id: ExecutorId,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ChangeType {
    InvokeApplication(InvokeApplicationEvent),
    AllocationOutputsIngested(Box<AllocationOutputIngestedEvent>),
    TombstoneApplication(TombstoneApplicationEvent),
    TombstoneRequest(TombstoneRequestEvent),
    ExecutorUpserted(ExecutorUpsertedEvent),
    TombStoneExecutor(ExecutorRemovedEvent),
}

impl fmt::Display for ChangeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeType::InvokeApplication(ev) => {
                write!(
                    f,
                    "InvokeApplication namespace: {}, request_id: {}, app: {}",
                    ev.namespace, ev.request_id, ev.application
                )
            }
            ChangeType::AllocationOutputsIngested(ev) => write!(
                f,
                "AllocationOutputsIngested namespace: {}, request_id: {}, app: {}, fn_call_id: {}",
                ev.namespace, ev.request_id, ev.application, ev.function_call_id,
            ),
            ChangeType::TombstoneApplication(ev) => write!(
                f,
                "TombstoneApplication namespace: {}, app: {}",
                ev.namespace, ev.application
            ),
            ChangeType::ExecutorUpserted(e) => {
                write!(f, "ExecutorAdded, executor_id: {}", e.executor_id)
            }
            ChangeType::TombStoneExecutor(ev) => {
                write!(f, "TombStoneExecutor, executor_id: {}", ev.executor_id)
            }
            ChangeType::TombstoneRequest(ev) => write!(
                f,
                "TombstoneRequest, namespace: {}, app: {}, request_id: {}",
                ev.namespace, ev.application, ev.request_id
            ),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Copy, PartialOrd)]
pub struct StateChangeId(u64);

impl StateChangeId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

impl From<StateChangeId> for u64 {
    fn from(value: StateChangeId) -> Self {
        value.0
    }
}

impl Display for StateChangeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct UnprocessedStateChanges {
    pub changes: Vec<StateChange>,
    pub last_global_state_change_cursor: Option<Vec<u8>>,
    pub last_namespace_state_change_cursor: Option<Vec<u8>>,
}

#[derive(Clone, Serialize, Deserialize, Debug, Builder)]
pub struct StateChange {
    pub id: StateChangeId,
    pub object_id: String,
    pub change_type: ChangeType,
    pub created_at: u64,
    pub processed_at: Option<u64>,
    pub namespace: Option<String>,
    pub application: Option<String>,
    pub request: Option<String>,
    #[builder(default)]
    vector_clock: VectorClock,
}

impl Linearizable for StateChange {
    fn vector_clock(&self) -> VectorClock {
        self.vector_clock.clone()
    }
}

impl StateChange {
    pub fn key(&self) -> Vec<u8> {
        let mut key = vec![];
        if let Some(ns) = &self.namespace {
            key.extend(format!("ns_{}|", &ns).as_bytes());
        } else {
            key.extend(b"global|");
        }
        key.extend(self.id.0.to_be_bytes());
        key
    }
}

impl Display for StateChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StateChange: namespace:{},  id: {}, change_type: {}, created_at: {}",
            self.namespace.as_ref().unwrap_or(&"global".to_string()),
            self.id,
            self.change_type,
            self.created_at,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct Namespace {
    pub name: String,
    pub created_at: u64,
    pub blob_storage_bucket: Option<String>,
    pub blob_storage_region: Option<String>,
    #[builder(default)]
    vector_clock: VectorClock,
}

impl Linearizable for Namespace {
    fn vector_clock(&self) -> VectorClock {
        self.vector_clock.clone()
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Copy, PartialOrd)]
pub struct AllocationUsageId(u64);

impl AllocationUsageId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

impl From<AllocationUsageId> for u64 {
    fn from(value: AllocationUsageId) -> Self {
        value.0
    }
}

impl Display for AllocationUsageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct AllocationUsage {
    pub id: AllocationUsageId,
    pub namespace: String,
    pub application: String,
    pub request_id: String,
    pub allocation_id: AllocationId,
    pub execution_duration_ms: u64,
    pub cpu_ms_per_second: u32,
    pub memory_mb: u64,
    pub disk_mb: u64,
    pub gpu_used: Vec<GPUResources>,

    #[builder(default)]
    vector_clock: VectorClock,
}

impl AllocationUsage {
    /// Returns a key suitable for use in RocksDB.
    ///
    /// It uses the AllocationUsageId as the key, encoded as big-endian bytes.
    pub fn key(&self) -> [u8; 8] {
        // RocksDB sorts keys in lexicographical order. Using the vector clock
        // as the key ensures that newer versions of the same AllocationUsage
        // will sort after older versions.
        self.id.0.to_be_bytes()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use bytes::Bytes;
    use mock_instant::global::MockClock;

    use super::*;
    use crate::data_model::{Application, ApplicationVersion, test_objects::tests::test_function};

    #[test]
    fn test_application_update() {
        const TEST_NAMESPACE: &str = "namespace1";
        let fn_a = test_function("fn_a", 0);
        let fn_b = test_function("fn_b", 0);
        let fn_c = test_function("fn_c", 0);

        let original_application: Application = ApplicationBuilder::default()
            .namespace(TEST_NAMESPACE.to_string())
            .name("graph1".to_string())
            .tombstoned(false)
            .description("description1".to_string())
            .tags(HashMap::new())
            .state(ApplicationState::Active)
            .functions(HashMap::from([
                ("fn_a".to_string(), fn_a.clone()),
                ("fn_b".to_string(), fn_b.clone()),
                ("fn_c".to_string(), fn_c.clone()),
            ]))
            .version("1".to_string())
            .code(DataPayload {
                id: "code_id".to_string(),
                path: "cgc_path".to_string(),
                size: 23,
                sha256_hash: "hash_code".to_string(),
                encoding: "application/octet-stream".to_string(),
                metadata_size: 0,
                offset: 0,
            })
            .created_at(5)
            .entrypoint(ApplicationEntryPoint {
                function_name: "fn_a".to_string(),
                input_serializer: "json".to_string(),
                output_serializer: "json".to_string(),
                output_type_hints_base64: "".to_string(),
            })
            .build()
            .unwrap();

        let original_version = original_application.to_version().unwrap();
        struct TestCase {
            description: &'static str,
            update: Application,
            expected_graph: Application,
            expected_version: ApplicationVersion,
        }

        let test_cases = [
            TestCase {
                description: "no graph and version changes",
                update: original_application.clone(),
                expected_graph: original_application.clone(),
                expected_version: original_version.clone(),
            },
            TestCase {
                description: "version update",
                update: Application {
                    version: "100".to_string(), // different
                    ..original_application.clone()
                },
                expected_graph: Application {
                    version: "100".to_string(),
                    ..original_application.clone()
                },
                expected_version: ApplicationVersion {
                    version: "100".to_string(),
                    ..original_version.clone()
                },
            },
            TestCase {
                description: "immutable fields should not change when version changed",
                update: Application {
                    namespace: "namespace2".to_string(), // different
                    name: "graph2".to_string(),          // different
                    version: "100".to_string(),          // different
                    created_at: 10,                      // different
                    ..original_application.clone()
                },
                expected_graph: Application {
                    version: "100".to_string(),
                    ..original_application.clone()
                },
                expected_version: ApplicationVersion {
                    version: "100".to_string(),
                    ..original_version.clone()
                },
            },
            // Code.
            TestCase {
                description: "changing code with version change should change code",
                update: Application {
                    version: "2".to_string(), // different
                    code: DataPayload {
                        sha256_hash: "hash_code2".to_string(), // different
                        ..original_application.code.clone()
                    },
                    ..original_application.clone()
                },
                expected_graph: Application {
                    version: "2".to_string(), // different
                    code: DataPayload {
                        sha256_hash: "hash_code2".to_string(), // different
                        ..original_application.code.clone()
                    },
                    ..original_application.clone()
                },
                expected_version: ApplicationVersion {
                    version: "2".to_string(),
                    code: DataPayload {
                        sha256_hash: "hash_code2".to_string(), // different
                        ..original_application.code.clone()
                    },
                    ..original_version.clone()
                },
            },
            // Edges.
            TestCase {
                description: "changing edges with version change should change edges",
                update: Application {
                    version: "2".to_string(), // different
                    ..original_application.clone()
                },
                expected_graph: Application {
                    version: "2".to_string(),
                    ..original_application.clone()
                },
                expected_version: ApplicationVersion {
                    version: "2".to_string(),
                    ..original_version.clone()
                },
            },
            // start_fn.
            TestCase {
                description: "changing start function with entrypoint change should change start function",
                update: Application {
                    version: "2".to_string(), // different
                    entrypoint: ApplicationEntryPoint {
                        function_name: "fn_b".to_string(), // different
                        input_serializer: "json".to_string(),
                        output_serializer: "json".to_string(),
                        output_type_hints_base64: "".to_string(),
                    },
                    ..original_application.clone()
                },
                expected_graph: Application {
                    version: "2".to_string(),
                    entrypoint: ApplicationEntryPoint {
                        function_name: "fn_b".to_string(), // different
                        input_serializer: "json".to_string(),
                        output_serializer: "json".to_string(),
                        output_type_hints_base64: "".to_string(),
                    },
                    ..original_application.clone()
                },
                expected_version: ApplicationVersion {
                    version: "2".to_string(),
                    entrypoint: ApplicationEntryPoint {
                        function_name: "fn_b".to_string(), // different
                        input_serializer: "json".to_string(),
                        output_serializer: "json".to_string(),
                        output_type_hints_base64: "".to_string(),
                    },
                    ..original_version.clone()
                },
            },
            // Adding a node.
            TestCase {
                description: "adding a node with version change should add node",
                update: Application {
                    version: "2".to_string(), // different
                    functions: HashMap::from([
                        ("fn_a".to_string(), fn_a.clone()),
                        ("fn_b".to_string(), fn_b.clone()),
                        ("fn_c".to_string(), fn_c.clone()),
                        ("fn_d".to_string(), test_function("fn_d", 0)), // added
                    ]),
                    ..original_application.clone()
                },
                expected_graph: Application {
                    version: "2".to_string(),
                    functions: HashMap::from([
                        ("fn_a".to_string(), fn_a.clone()),
                        ("fn_b".to_string(), fn_b.clone()),
                        ("fn_c".to_string(), fn_c.clone()),
                        ("fn_d".to_string(), test_function("fn_d", 0)), // added
                    ]),
                    ..original_application.clone()
                },
                expected_version: ApplicationVersion {
                    version: "2".to_string(),
                    functions: HashMap::from([
                        ("fn_a".to_string(), fn_a.clone()),
                        ("fn_b".to_string(), fn_b.clone()),
                        ("fn_c".to_string(), fn_c.clone()),
                        ("fn_d".to_string(), test_function("fn_d", 0)), // added
                    ]),
                    ..original_version.clone()
                },
            },
            // Removing a node.
            TestCase {
                description: "removing a node with version change should remove the node",
                update: Application {
                    version: "2".to_string(), // different
                    functions: HashMap::from([
                        ("fn_a".to_string(), fn_a.clone()),
                        ("fn_b".to_string(), fn_b.clone()),
                        // "fn_c" removed
                    ]),
                    ..original_application.clone()
                },
                expected_graph: Application {
                    version: "2".to_string(),
                    functions: HashMap::from([
                        ("fn_a".to_string(), fn_a.clone()),
                        ("fn_b".to_string(), fn_b.clone()),
                    ]),
                    ..original_application.clone()
                },
                expected_version: ApplicationVersion {
                    version: "2".to_string(),
                    functions: HashMap::from([
                        ("fn_a".to_string(), fn_a.clone()),
                        ("fn_b".to_string(), fn_b.clone()),
                    ]),
                    ..original_version.clone()
                },
            },
            // Changing a node's image.
            TestCase {
                description: "changing a node's image with version change should update the image and version",
                update: Application {
                    version: "2".to_string(), // different
                    functions: HashMap::from([
                        ("fn_a".to_string(), test_function("fn_a", 0)), // different
                        ("fn_b".to_string(), fn_b.clone()),
                        ("fn_c".to_string(), fn_c.clone()),
                    ]),
                    ..original_application.clone()
                },
                expected_graph: Application {
                    version: "2".to_string(),
                    functions: HashMap::from([
                        ("fn_a".to_string(), test_function("fn_a", 0)),
                        ("fn_b".to_string(), fn_b.clone()),
                        ("fn_c".to_string(), fn_c.clone()),
                    ]),
                    ..original_application.clone()
                },
                expected_version: ApplicationVersion {
                    version: "2".to_string(),
                    functions: HashMap::from([
                        ("fn_a".to_string(), test_function("fn_a", 0)),
                        ("fn_b".to_string(), fn_b.clone()),
                        ("fn_c".to_string(), fn_c.clone()),
                    ]),
                    ..original_version.clone()
                },
            },
        ];

        for test_case in test_cases.iter() {
            let mut updated_app = original_application.clone();
            updated_app.update(test_case.update.clone());
            assert_eq!(
                updated_app, test_case.expected_graph,
                "{}",
                test_case.description
            );
            assert_eq!(
                updated_app.to_version().unwrap(),
                test_case.expected_version,
                "different ApplicationVersion for test `{}`",
                test_case.description
            );
        }
    }

    #[test]
    fn test_allocation_builder_build_success() {
        MockClock::set_system_time(Duration::from_nanos(9999999999999999));

        let target = AllocationTarget {
            executor_id: "executor-1".into(),
            function_executor_id: "fe-1".into(),
        };
        let allocation = AllocationBuilder::default()
            .namespace("test-ns".to_string())
            .application("graph".to_string())
            .function("fn".to_string())
            .request_id("invoc-1".to_string())
            .function_call_id("task-1".into())
            .input_args(vec![InputArgs {
                function_call_id: None,
                data_payload: DataPayload {
                    id: "test".to_string(),
                    path: "test".to_string(),
                    metadata_size: 0,
                    encoding: "application/octet-stream".to_string(),
                    size: 23,
                    sha256_hash: "hash1232".to_string(),
                    offset: 0,
                },
            }])
            .target(target.clone())
            .call_metadata(Bytes::new())
            .attempt_number(1)
            .outcome(FunctionRunOutcome::Success)
            .build()
            .expect("Allocation should build successfully");
        assert_eq!(allocation.namespace, "test-ns");
        assert_eq!(allocation.application, "graph");
        assert_eq!(allocation.function, "fn");
        assert_eq!(allocation.request_id, "invoc-1");
        assert_eq!(allocation.function_call_id, "task-1".into());
        assert_eq!(allocation.target.executor_id, target.executor_id);
        assert_eq!(
            allocation.target.function_executor_id,
            target.function_executor_id
        );
        assert_eq!(allocation.attempt_number, 1);
        assert_eq!(allocation.outcome, FunctionRunOutcome::Success);
        assert!(!allocation.id.is_empty());
        assert_eq!(allocation.created_at, 9999999999);
        assert!(allocation.execution_duration_ms.is_none());
        assert_eq!(allocation.vector_clock.value(), 0);

        let json = serde_json::to_string(&allocation).expect("Should serialize allocation to JSON");

        // Check all fields are present and correctly serialized
        assert!(json.contains(&format!("\"namespace\":\"{}\"", allocation.namespace)));
        assert!(json.contains(&format!("\"application\":\"{}\"", allocation.application)));
        assert!(json.contains(&format!("\"function\":\"{}\"", allocation.function)));
        assert!(json.contains(&format!("\"request_id\":\"{}\"", allocation.request_id)));
        assert!(json.contains(&format!(
            "\"function_call_id\":\"{}\"",
            allocation.function_call_id
        )));
        assert!(json.contains(&format!(
            "\"target\":{{\"executor_id\":\"{}\",\"function_executor_id\":\"{}\"}}",
            allocation.target.executor_id.get(),
            allocation.target.function_executor_id.get()
        )));
        assert!(json.contains(&format!("\"attempt_number\":{}", allocation.attempt_number)));
        assert!(json.contains(&"\"outcome\":\"Success\"".to_string()));
        assert!(json.contains(&format!("\"id\":\"{}\"", allocation.id)));
        assert!(json.contains(&format!("\"created_at\":{}", allocation.created_at)));
        assert!(json.contains("\"execution_duration_ms\":null"));
        assert!(json.contains("\"vector_clock\":1"));
    }

    #[test]
    fn test_graph_request_ctx_builder_build_success() {
        MockClock::set_system_time(Duration::from_nanos(9999999999999999));

        let namespace = "test-ns".to_string();
        let application_name = "graph".to_string();
        let request_id = "invoc-1".to_string();
        let application_version = "42".to_string();

        // Minimal Application for the builder
        let fn_a = test_function("fn_a", 0);
        let _application = ApplicationBuilder::default()
            .namespace(namespace.clone())
            .name(application_name.clone())
            .tombstoned(false)
            .description("desc".to_string())
            .tags(HashMap::new())
            .state(ApplicationState::Active)
            .functions(HashMap::from([("fn_a".to_string(), fn_a.clone())]))
            .version(application_version.clone())
            .code(DataPayload {
                id: "code_id".to_string(),
                encoding: "application/octet-stream".to_string(),
                metadata_size: 0,
                offset: 0,
                path: "path".to_string(),
                size: 1,
                sha256_hash: "hash".to_string(),
            })
            .created_at(123)
            .entrypoint(ApplicationEntryPoint {
                function_name: "fn_a".to_string(),
                input_serializer: "json".to_string(),
                output_serializer: "json".to_string(),
                output_type_hints_base64: "".to_string(),
            })
            .build()
            .unwrap();

        let ctx = RequestCtxBuilder::default()
            .namespace(namespace.clone())
            .application_name(application_name.clone())
            .request_id(request_id.clone())
            .function_calls(HashMap::new())
            .function_runs(HashMap::new())
            .application_version(application_version.clone())
            .build()
            .expect("RequestCtxBuilder should build successfully");

        assert_eq!(ctx.namespace, namespace);
        assert_eq!(ctx.application_name, application_name);
        assert_eq!(ctx.request_id, request_id);
        assert_eq!(ctx.application_version, application_version);
        assert_eq!(ctx.outcome, None);
        assert!(ctx.request_error.is_none());
        assert_eq!(ctx.created_at, 9999999999);
        assert_eq!(ctx.vector_clock.value(), 0);

        // Check key format
        let key = ctx.key();
        assert_eq!(
            key,
            format!(
                "{}|{}|{}",
                ctx.namespace, ctx.application_name, ctx.request_id
            )
        );

        // Check JSON serialization
        let json = serde_json::to_string(&ctx).expect("Should serialize RequestCtx to JSON");
        assert!(json.contains(&format!("\"namespace\":\"{}\"", ctx.namespace)));
        assert!(json.contains(&format!(
            "\"application_name\":\"{}\"",
            ctx.application_name
        )));
        assert!(json.contains(&format!("\"request_id\":\"{}\"", ctx.request_id)));
        assert!(json.contains(&format!(
            "\"application_version\":\"{}\"",
            ctx.application_version
        )));
        assert!(json.contains("\"outcome\":null"));
        assert!(json.contains("\"created_at\":"));
        assert!(json.contains("\"request_error\":null"));
        assert!(json.contains("\"vector_clock\":1"));
    }

    #[test]
    fn test_function_executor_builder_build_success() {
        let id: FunctionExecutorId = "fe-123".into();
        let namespace = "test-ns".to_string();
        let application_name = "graph".to_string();
        let function_name = "fn".to_string();
        let version = "1".to_string();
        let state = FunctionExecutorState::Running;
        let resources = FunctionExecutorResources {
            cpu_ms_per_sec: 2000,
            memory_mb: 4096,
            ephemeral_disk_mb: 2048,
            gpu: Some(GPUResources {
                count: 2,
                model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
            }),
        };
        let max_concurrency = 4;

        let mut builder = FunctionExecutorBuilder::default();
        builder
            .id(id.clone())
            .namespace(namespace.clone())
            .application_name(application_name.clone())
            .function_name(function_name.clone())
            .version(version.clone())
            .state(state.clone())
            .resources(resources.clone())
            .max_concurrency(max_concurrency);

        let fe = builder.build().expect("Should build FunctionExecutor");

        assert_eq!(fe.id, id);
        assert_eq!(fe.namespace, namespace);
        assert_eq!(fe.application_name, application_name);
        assert_eq!(fe.function_name, function_name);
        assert_eq!(fe.version, version);
        assert_eq!(fe.state, state);
        assert_eq!(fe.resources, resources);
        assert_eq!(fe.max_concurrency, max_concurrency);
        assert_eq!(fe.vector_clock.value(), 0);

        // Check serialization
        let json = serde_json::to_string(&fe).expect("Should serialize FunctionExecutor to JSON");
        assert!(json.contains(&format!("\"id\":\"{}\"", fe.id.get())));
        assert!(json.contains(&format!("\"namespace\":\"{}\"", fe.namespace)));
        assert!(json.contains(&format!("\"application_name\":\"{}\"", fe.application_name)));
        assert!(json.contains(&format!("\"function_name\":\"{}\"", fe.function_name)));
        assert!(json.contains(&format!("\"version\":\"{}\"", fe.version)));
        assert!(json.contains(&format!("\"state\":\"{}\"", fe.state.as_ref())));
        assert!(json.contains(&format!("\"max_concurrency\":{}", fe.max_concurrency)));
        assert!(json.contains("\"resources\":{"));
        assert!(json.contains(&format!("\"cpu_ms_per_sec\":{}", resources.cpu_ms_per_sec)));
        assert!(json.contains(&format!("\"memory_mb\":{}", resources.memory_mb)));
        assert!(json.contains(&format!(
            "\"ephemeral_disk_mb\":{}",
            resources.ephemeral_disk_mb
        )));
        assert!(json.contains("\"gpu\":{"));
        assert!(json.contains(&format!(
            "\"count\":{}",
            resources.gpu.as_ref().unwrap().count
        )));
        assert!(json.contains(&format!(
            "\"model\":\"{}\"",
            resources.gpu.as_ref().unwrap().model
        )));
        assert!(json.contains("\"vector_clock\":1"));
    }

    #[test]
    fn test_executor_metadata_builder_build_success() {
        let id = ExecutorId::from("executor-1");
        let executor_version = "0.2.17".to_string();
        let function_allowlist = Some(vec![FunctionAllowlist {
            namespace: Some("ns".to_string()),
            application: Some("graph".to_string()),
            function: Some("fn".to_string()),
            version: Some("1".to_string()),
        }]);
        let addr = "127.0.0.1:8080".to_string();
        let labels = HashMap::from([("role".to_string(), "worker".to_string())]);
        let fe_id = FunctionExecutorId::from("fe-1");
        let function_executors = HashMap::from([(
            fe_id.clone(),
            FunctionExecutorBuilder::default()
                .id(fe_id.clone())
                .namespace("ns".to_string())
                .application_name("graph".to_string())
                .function_name("fn".to_string())
                .version("1".to_string())
                .state(FunctionExecutorState::Running)
                .resources(FunctionExecutorResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu: None,
                })
                .max_concurrency(2)
                .build()
                .expect("Should build FunctionExecutor"),
        )]);
        let host_resources = HostResources {
            cpu_ms_per_sec: 4000,
            memory_bytes: 8 * 1024 * 1024 * 1024,
            disk_bytes: 100 * 1024 * 1024 * 1024,
            gpu: None,
        };
        let state = ExecutorState::Running;
        let tombstoned = false;
        let state_hash = "abc123".to_string();
        let clock = 42u64;

        let mut builder = ExecutorMetadataBuilder::default();
        builder
            .id(id.clone())
            .executor_version(executor_version.clone())
            .function_allowlist(function_allowlist.clone())
            .addr(addr.clone())
            .labels(labels.clone())
            .function_executors(function_executors.clone())
            .host_resources(host_resources.clone())
            .state(state.clone())
            .tombstoned(tombstoned)
            .state_hash(state_hash.clone())
            .clock(clock);

        let metadata = builder.build().expect("Should build ExecutorMetadata");

        assert_eq!(metadata.id, id);
        assert_eq!(metadata.executor_version, executor_version);
        assert_eq!(metadata.function_allowlist, function_allowlist);
        assert_eq!(metadata.addr, addr);
        assert_eq!(metadata.labels, labels);
        assert_eq!(metadata.function_executors, function_executors);
        assert_eq!(metadata.host_resources, host_resources);
        assert_eq!(metadata.state, state);
        assert_eq!(metadata.tombstoned, tombstoned);
        assert_eq!(metadata.state_hash, state_hash);
        assert_eq!(metadata.clock, clock);
        assert_eq!(metadata.vector_clock.value(), 0);

        // Check serialization
        let json =
            serde_json::to_string(&metadata).expect("Should serialize ExecutorMetadata to JSON");
        assert!(json.contains(&format!("\"id\":\"{}\"", metadata.id.get())));
        assert!(json.contains(&format!(
            "\"executor_version\":\"{}\"",
            metadata.executor_version
        )));
        assert!(json.contains(&format!("\"addr\":\"{}\"", metadata.addr)));
        assert!(json.contains(&format!("\"state\":\"{}\"", metadata.state.as_ref())));
        assert!(json.contains(&format!("\"tombstoned\":{}", metadata.tombstoned)));
        assert!(json.contains(&format!("\"state_hash\":\"{}\"", metadata.state_hash)));
        assert!(json.contains("\"clock\":42"));
        assert!(json.contains("\"vector_clock\":1"));
    }

    #[test]
    fn test_allocation_id() {
        let id = AllocationId::default();
        let id_str = id.to_string();
        let id_deref = id.deref();

        assert_eq!(&id_str, id_deref);
        assert_eq!(id_str.len(), 21); // nanoid length
        assert_eq!(format!("\"{id_str}\""), serde_json::to_string(&id).unwrap());
    }
}

#[cfg(test)]
mod host_resources_tests;
