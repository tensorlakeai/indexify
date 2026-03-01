pub mod clocks;
pub mod filter;
pub mod test_objects;

use std::{
    collections::{HashMap, HashSet},
    fmt::{self, Display},
    hash::Hash,
    ops::{Deref, Range},
    str,
    vec,
};

use anyhow::{Result, anyhow};
use derive_builder::Builder;
use filter::LabelsFilter;
use nanoid::nanoid;
use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;
use strum::Display;
use tracing::info;

#[derive(Debug, Clone, PartialEq)]
pub struct JsonValue(pub serde_json::Value);

impl Serialize for JsonValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            self.0.serialize(serializer)
        } else {
            let json_string = serde_json::to_string(&self.0).map_err(serde::ser::Error::custom)?;
            json_string.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for JsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            serde_json::Value::deserialize(deserializer).map(JsonValue)
        } else {
            let json_string = String::deserialize(deserializer)?;
            serde_json::from_str(&json_string)
                .map(JsonValue)
                .map_err(serde::de::Error::custom)
        }
    }
}

impl From<serde_json::Value> for JsonValue {
    fn from(v: serde_json::Value) -> Self {
        JsonValue(v)
    }
}

impl From<JsonValue> for serde_json::Value {
    fn from(v: JsonValue) -> Self {
        v.0
    }
}

use crate::{
    config::DEFAULT_SANDBOX_IMAGE,
    data_model::clocks::VectorClock,
    utils::{get_epoch_time_in_ms, get_epoch_time_in_ns},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineMetadata {
    pub db_version: u64,
    pub last_change_idx: u64,
    #[serde(default)]
    pub last_usage_idx: u64,
    #[serde(default)]
    pub last_request_event_idx: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct ExecutorId(String);

impl Display for ExecutorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.0)
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

impl From<String> for ExecutorId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct AllocationTarget {
    pub executor_id: ExecutorId,
    pub container_id: ContainerId,
}

impl AllocationTarget {
    pub fn new(executor_id: ExecutorId, container_id: ContainerId) -> Self {
        Self {
            executor_id,
            container_id,
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
    pub application_version: String,
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

    pub fn vector_clock(&self) -> &VectorClock {
        &self.vector_clock
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

impl From<String> for FunctionCallId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionCall {
    pub function_call_id: FunctionCallId,
    pub inputs: Vec<FunctionArgs>,
    pub fn_name: String,
    pub call_metadata: bytes::Bytes,
    pub parent_function_call_id: Option<FunctionCallId>,
}

impl FunctionCall {
    pub fn key_for_request(
        namespace: &str,
        application: &str,
        request_id: &str,
        function_call_id: &FunctionCallId,
    ) -> String {
        format!("{namespace}|{application}|{request_id}|{function_call_id}")
    }

    pub fn key_prefix_for_request(namespace: &str, application: &str, request_id: &str) -> String {
        format!("{namespace}|{application}|{request_id}|")
    }
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
    // This is the application version
    pub version: String,
    pub name: String,
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
    #[serde(default)]
    created_at_clock: Option<u64>,
    #[builder(default)]
    #[serde(default)]
    updated_at_clock: Option<u64>,
    pub call_metadata: bytes::Bytes,

    #[builder(default)]
    pub request_error: Option<DataPayload>,
}

impl FunctionRunBuilder {
    fn default_creation_time_ns(&self) -> u128 {
        get_epoch_time_in_ns()
    }
}

impl FunctionRun {
    pub fn key_application_version(&self, application_name: &str) -> String {
        format!("{}|{}|{}", self.namespace, application_name, self.version)
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

    /// Returns true if this function run has never been persisted to the
    /// database.
    pub fn is_new(&self) -> bool {
        self.created_at_clock.is_none()
    }

    /// Prepares the function run for persistence by setting the server clock.
    /// Sets created_at_clock only on first persistence, updated_at_clock on
    /// every persistence.
    pub fn prepare_for_persistence(&mut self, clock: u64) {
        if self.created_at_clock.is_none() {
            self.created_at_clock = Some(clock);
        }
        self.updated_at_clock = Some(clock);
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

impl FunctionResources {
    pub fn can_be_handled_by_catalog_entry(
        &self,
        catalog_entry: &crate::config::ExecutorCatalogEntry,
    ) -> bool {
        // Convert catalog entry to HostResources and use existing validation
        let catalog_resources = HostResources::from_catalog_entry(catalog_entry);

        // Use existing can_handle_function_resources method
        catalog_resources
            .can_handle_function_resources(self)
            .is_ok()
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

#[serde_inline_default]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct Function {
    pub name: String,
    pub description: String,
    pub placement_constraints: LabelsFilter,
    #[serde_inline_default("cloudpickle".to_string())]
    pub input_encoder: String,
    #[serde_inline_default("cloudpickle".to_string())]
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
    pub return_type: Option<JsonValue>,
    #[serde_inline_default(1)]
    pub max_concurrency: u32,
    #[serde(default)]
    pub min_containers: Option<u32>,
    #[serde(default)]
    pub max_containers: Option<u32>,
    /// Number of idle containers to maintain as a buffer for faster scheduling.
    /// Buffer + active containers must not exceed max_containers.
    #[serde(default)]
    pub warm_containers: Option<u32>,
}

impl Function {
    pub fn create_function_call(
        &self,
        function_call_id: FunctionCallId,
        inputs: Vec<DataPayload>,
        call_metadata: bytes::Bytes,
        parent_function_call_id: Option<FunctionCallId>,
    ) -> FunctionCall {
        FunctionCall {
            function_call_id,
            inputs: inputs.into_iter().map(FunctionArgs::DataPayload).collect(),
            fn_name: self.name.clone(),
            call_metadata,
            parent_function_call_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ParameterMetadata {
    pub name: String,
    pub description: Option<String>,
    pub required: bool,
    pub data_type: JsonValue,
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
    #[serde(default)]
    pub inputs_base64: String,
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
    #[serde(default)]
    #[builder(default)]
    pub code: Option<DataPayload>,
    #[serde(default)]
    #[builder(default)]
    pub functions: HashMap<String, Function>,
    #[serde(default)]
    #[builder(default)]
    pub state: ApplicationState,
    #[builder(default)]
    #[serde(default)]
    created_at_clock: Option<u64>,
    #[builder(default)]
    #[serde(default)]
    updated_at_clock: Option<u64>,
    #[serde(default)]
    #[builder(default)]
    pub entrypoint: Option<ApplicationEntryPoint>,
}

impl ApplicationBuilder {
    fn default_created_at(&self) -> u64 {
        get_epoch_time_in_ms()
    }
}

impl Application {
    pub fn key(&self) -> String {
        Application::key_from(&self.namespace, &self.name)
    }

    /// Prepares the application for persistence by setting the server clock.
    pub fn prepare_for_persistence(&mut self, clock: u64) {
        if self.created_at_clock.is_none() {
            self.created_at_clock = Some(clock);
        }
        self.updated_at_clock = Some(clock);
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
                if node
                    .placement_constraints
                    .matches(&entry.labels.clone().into())
                {
                    met_placement_constraints = true;
                }

                has_cpu = (node.resources.cpu_ms_per_sec / 1000) <= entry.cpu_cores;
                has_mem = node.resources.memory_mb <= entry.memory_gb * 1024;
                has_disk = node.resources.ephemeral_disk_mb <= entry.disk_gb * 1024;
                has_gpu_models = if node.resources.gpu_configs.is_empty() {
                    true // No GPU required
                } else {
                    // Check if any requested GPU matches the catalog entry's GPU model
                    entry.gpu_model.as_ref().is_some_and(|catalog_gpu| {
                        node.resources.gpu_configs.iter().any(|gpu| {
                            gpu.model == catalog_gpu.name && gpu.count <= catalog_gpu.count
                        })
                    })
                };

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
    /// Code payload - required if functions exist
    #[serde(default)]
    #[builder(default)]
    pub code: Option<DataPayload>,
    /// Entrypoint - required if functions exist
    #[serde(default)]
    #[builder(default)]
    pub entrypoint: Option<ApplicationEntryPoint>,
    #[serde(default)]
    #[builder(default)]
    pub functions: HashMap<String, Function>,
    #[builder(default)]
    pub edges: HashMap<String, Vec<String>>,
    #[serde(default)]
    #[builder(default)]
    pub state: ApplicationState,
    #[builder(default)]
    #[serde(default)]
    created_at_clock: Option<u64>,
    #[builder(default)]
    #[serde(default)]
    updated_at_clock: Option<u64>,
}

impl ApplicationVersion {
    /// Prepares the application version for persistence by setting the server
    /// clock.
    pub fn prepare_for_persistence(&mut self, clock: u64) {
        if self.created_at_clock.is_none() {
            self.created_at_clock = Some(clock);
        }
        self.updated_at_clock = Some(clock);
    }

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
                parent_function_call_id: fn_call.parent_function_call_id.clone(),
            }))
            .input_args(input_args)
            .name(fn_call.fn_name.clone())
            .request_id(request_id.to_string())
            .status(FunctionRunStatus::Pending)
            .attempt_number(0)
            .call_metadata(fn_call.call_metadata.clone())
            .creation_time_ns(get_epoch_time_in_ns())
            // created_at_clock and updated_at_clock default to None (not yet persisted)
            .outcome(None)
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

impl DataPayload {
    pub fn request_key_prefix(namespace: &str, application: &str, request_id: &str) -> String {
        format!("{namespace}/{application}/{request_id}")
    }

    pub fn data_size(&self) -> u64 {
        self.size - self.metadata_size
    }

    fn data_offset(&self) -> u64 {
        self.offset + self.metadata_size
    }

    pub fn data_range(&self) -> Range<u64> {
        let offset = self.data_offset();
        offset..offset + self.data_size()
    }

    pub fn full_range(&self) -> Range<u64> {
        self.offset..self.offset + self.size
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RequestOutcome {
    #[serde(alias = "Unknown")]
    #[default]
    Unknown,
    #[serde(alias = "Success")]
    Success,
    #[serde(alias = "Failure")]
    Failure(RequestFailureReason),
}

impl From<FunctionRunOutcome> for RequestOutcome {
    fn from(outcome: FunctionRunOutcome) -> Self {
        match outcome {
            FunctionRunOutcome::Success => RequestOutcome::Success,
            FunctionRunOutcome::Failure(ref failure_reason) => {
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

impl RequestOutcome {
    pub fn is_success(&self) -> bool {
        matches!(self, RequestOutcome::Success)
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestFailureReason {
    // Used when request didn't finish yet and when request finished successfully
    #[serde(alias = "Unknown")]
    #[default]
    Unknown,
    // Internal error on Executor aka platform error.
    #[serde(alias = "InternalError")]
    InternalError,
    // Clear function code failure typically by raising an exception from the function code.
    #[serde(alias = "FunctionError")]
    FunctionError,
    // One of the functions timed out.
    #[serde(alias = "FunctionTimeout")]
    FunctionTimeout,
    // Function code raised RequestError to mark the request as permanently failed.
    #[serde(alias = "RequestError")]
    RequestError,
    // A graph function cannot be scheduled given the specified constraints.
    #[serde(alias = "ConstraintUnsatisfiable")]
    ConstraintUnsatisfiable,
    // Cancelled.
    #[serde(alias = "Cancelled")]
    Cancelled,
    // Out of memory.
    #[serde(alias = "OutOfMemory")]
    OutOfMemory,
}

impl Display for RequestFailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str_val = match self {
            RequestFailureReason::Unknown => "Unknown",
            RequestFailureReason::InternalError => "InternalError",
            RequestFailureReason::FunctionError => "FunctionError",
            RequestFailureReason::RequestError => "RequestError",
            RequestFailureReason::ConstraintUnsatisfiable => "ConstraintUnsatisfiable",
            RequestFailureReason::FunctionTimeout => "FunctionTimeout",
            RequestFailureReason::Cancelled => "Cancelled",
            RequestFailureReason::OutOfMemory => "OutOfMemory",
        };
        write!(f, "{str_val}")
    }
}

impl From<&FunctionRunFailureReason> for RequestFailureReason {
    fn from(failure_reason: &FunctionRunFailureReason) -> Self {
        match failure_reason {
            FunctionRunFailureReason::Unknown => RequestFailureReason::Unknown,
            FunctionRunFailureReason::InternalError => RequestFailureReason::InternalError,
            FunctionRunFailureReason::FunctionError => RequestFailureReason::FunctionError,
            FunctionRunFailureReason::ContainerStartupFunctionError => {
                RequestFailureReason::FunctionError
            }
            FunctionRunFailureReason::ContainerStartupFunctionTimeout => {
                RequestFailureReason::FunctionTimeout
            }
            FunctionRunFailureReason::ContainerStartupInternalError => {
                RequestFailureReason::InternalError
            }
            FunctionRunFailureReason::FunctionTimeout => RequestFailureReason::FunctionTimeout,
            FunctionRunFailureReason::RequestError => RequestFailureReason::RequestError,
            FunctionRunFailureReason::FunctionRunCancelled => RequestFailureReason::Cancelled,
            FunctionRunFailureReason::FunctionExecutorTerminated |
            FunctionRunFailureReason::ExecutorRemoved => RequestFailureReason::InternalError,
            FunctionRunFailureReason::ConstraintUnsatisfiable => {
                RequestFailureReason::ConstraintUnsatisfiable
            }
            FunctionRunFailureReason::OutOfMemory => RequestFailureReason::OutOfMemory,
            FunctionRunFailureReason::ContainerStartupBadImage => {
                RequestFailureReason::InternalError
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct RequestError {
    pub function_name: String,
    pub payload: DataPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RequestCtxKey(pub String);

impl Display for RequestCtxKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl RequestCtxKey {
    pub fn new(namespace: &str, application_name: &str, request_id: &str) -> Self {
        Self(format!("{namespace}|{application_name}|{request_id}"))
    }
}

impl From<String> for RequestCtxKey {
    fn from(key: String) -> Self {
        RequestCtxKey(key)
    }
}

impl From<&String> for RequestCtxKey {
    fn from(key: &String) -> Self {
        RequestCtxKey(key.clone())
    }
}

impl From<&RequestCtx> for RequestCtxKey {
    fn from(ctx: &RequestCtx) -> Self {
        RequestCtxKey(ctx.key())
    }
}

impl From<&FunctionRun> for RequestCtxKey {
    fn from(function_run: &FunctionRun) -> Self {
        RequestCtxKey(format!(
            "{}|{}|{}",
            function_run.namespace, function_run.application, function_run.request_id
        ))
    }
}

impl From<FunctionRun> for RequestCtxKey {
    fn from(function_run: FunctionRun) -> Self {
        RequestCtxKey(format!(
            "{}|{}|{}",
            function_run.namespace, function_run.application, function_run.request_id
        ))
    }
}

impl From<Box<FunctionRun>> for RequestCtxKey {
    fn from(function_run: Box<FunctionRun>) -> Self {
        RequestCtxKey(format!(
            "{}|{}|{}",
            function_run.namespace, function_run.application, function_run.request_id
        ))
    }
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
    #[serde(default)]
    created_at_clock: Option<u64>,
    #[builder(default)]
    #[serde(default)]
    updated_at_clock: Option<u64>,

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

    pub fn pending_function_calls(&self) -> HashSet<FunctionCallId> {
        let function_call_ids = self.function_calls.keys().collect::<HashSet<_>>();
        let function_run_ids = self.function_runs.keys().collect::<HashSet<_>>();

        function_call_ids
            .difference(&function_run_ids)
            .map(|&id| id.clone())
            .collect::<HashSet<_>>()
    }

    pub fn is_request_completed(&self) -> bool {
        if self.outcome.is_some() {
            return true;
        }
        if self.function_calls.len() != self.function_runs.len() {
            return false;
        }
        self.function_runs
            .values()
            .all(|function_run| matches!(function_run.status, FunctionRunStatus::Completed))
    }

    /// Prepares the RequestCtx and all its function runs for persistence.
    /// Sets created_at_clock only on first persistence, updated_at_clock on
    /// every persistence.
    pub fn prepare_for_persistence(&mut self, clock: u64) {
        // Set RequestCtx's own clock values
        if self.created_at_clock.is_none() {
            self.created_at_clock = Some(clock);
        }
        self.updated_at_clock = Some(clock);

        // Prepare all function runs
        for function_run in self.function_runs.values_mut() {
            function_run.prepare_for_persistence(clock);
        }
    }

    pub fn from_persisted(
        persisted: PersistedRequestCtx,
        function_runs: HashMap<FunctionCallId, FunctionRun>,
        function_calls: HashMap<FunctionCallId, FunctionCall>,
    ) -> Self {
        Self {
            namespace: persisted.namespace,
            application_name: persisted.application_name,
            application_version: persisted.application_version,
            request_id: persisted.request_id,
            outcome: persisted.outcome,
            created_at: persisted.created_at,
            request_error: persisted.request_error,
            created_at_clock: persisted.created_at_clock,
            updated_at_clock: persisted.updated_at_clock,
            function_runs,
            function_calls,
            child_function_calls: HashMap::new(),
        }
    }
}

/// A persistence-optimized version of `RequestCtx` that does not embed
/// function_runs or function_calls. Those are stored in their own column
/// families.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedRequestCtx {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
    #[serde(default)]
    pub outcome: Option<RequestOutcome>,
    pub created_at: u64,
    #[serde(default)]
    pub request_error: Option<RequestError>,
    #[serde(default)]
    created_at_clock: Option<u64>,
    #[serde(default)]
    updated_at_clock: Option<u64>,
    pub function_runs_count: usize,
    pub function_calls_count: usize,
}

impl From<&RequestCtx> for PersistedRequestCtx {
    fn from(ctx: &RequestCtx) -> Self {
        Self {
            namespace: ctx.namespace.clone(),
            application_name: ctx.application_name.clone(),
            application_version: ctx.application_version.clone(),
            request_id: ctx.request_id.clone(),
            outcome: ctx.outcome.clone(),
            created_at: ctx.created_at,
            request_error: ctx.request_error.clone(),
            created_at_clock: ctx.created_at_clock,
            updated_at_clock: ctx.updated_at_clock,
            function_runs_count: ctx.function_runs.len(),
            function_calls_count: ctx.function_calls.len(),
        }
    }
}

impl PersistedRequestCtx {
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
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
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

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub enum FunctionRunFailureReason {
    #[default]
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

    // Container startup failed.
    ContainerStartupFunctionError,

    // Container startup timed out.
    ContainerStartupFunctionTimeout,

    // Container startup internal error.
    ContainerStartupInternalError,

    OutOfMemory,

    // Container startup failed due to bad/missing image.
    ContainerStartupBadImage,
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
            FunctionRunFailureReason::ContainerStartupFunctionError => {
                "ContainerStartupFunctionError"
            }
            FunctionRunFailureReason::ContainerStartupFunctionTimeout => {
                "ContainerStartupFunctionTimeout"
            }
            FunctionRunFailureReason::ContainerStartupInternalError => {
                "ContainerStartupInternalError"
            }
            FunctionRunFailureReason::ContainerStartupBadImage => "ContainerStartupBadImage",
        };
        write!(f, "{str_val}")
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
                FunctionRunFailureReason::OutOfMemory |
                FunctionRunFailureReason::ContainerStartupFunctionError |
                FunctionRunFailureReason::ContainerStartupFunctionTimeout |
                FunctionRunFailureReason::ContainerStartupInternalError
        )
    }

    pub fn should_count_against_function_run_retry_attempts(&self) -> bool {
        // Most retriable failures count against retry attempts. This
        // prevents infinite retry loops when containers keep crashing.
        //
        // Reasons that DON'T count (free retries — pure infrastructure):
        //   ExecutorRemoved, FunctionExecutorTerminated
        //   (container killed externally, allocation was an innocent victim)
        //
        // Reasons that DO count (user-attributable or startup failures):
        //   InternalError, FunctionError, FunctionTimeout, OutOfMemory,
        //   ContainerStartupFunctionError, ContainerStartupFunctionTimeout,
        //   ContainerStartupInternalError
        matches!(
            self,
            FunctionRunFailureReason::InternalError |
                FunctionRunFailureReason::FunctionError |
                FunctionRunFailureReason::FunctionTimeout |
                FunctionRunFailureReason::OutOfMemory |
                FunctionRunFailureReason::ContainerStartupFunctionError |
                FunctionRunFailureReason::ContainerStartupFunctionTimeout |
                FunctionRunFailureReason::ContainerStartupInternalError
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct RunningFunctionRunStatus {
    pub allocation_id: AllocationId,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub enum FunctionRunStatus {
    /// Function run is waiting for execution
    Pending,
    /// Function run is running
    Running(RunningFunctionRunStatus),
    /// Function run is completed
    #[default]
    Completed,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FunctionURI {
    pub namespace: String,
    pub application: String,
    pub function: String,
    pub version: String,
}

impl From<&Allocation> for FunctionURI {
    fn from(allocation: &Allocation) -> Self {
        FunctionURI {
            namespace: allocation.namespace.clone(),
            application: allocation.application.clone(),
            function: allocation.function.clone(),
            version: allocation.application_version.clone(),
        }
    }
}

impl From<&ContainerServerMetadata> for FunctionURI {
    fn from(fe_meta: &ContainerServerMetadata) -> Self {
        FunctionURI {
            namespace: fe_meta.function_container.namespace.clone(),
            application: fe_meta.function_container.application_name.clone(),
            function: fe_meta.function_container.function_name.clone(),
            version: fe_meta.function_container.version.clone(),
        }
    }
}

impl From<&Box<ContainerServerMetadata>> for FunctionURI {
    fn from(fe_meta: &Box<ContainerServerMetadata>) -> Self {
        FunctionURI {
            namespace: fe_meta.function_container.namespace.clone(),
            application: fe_meta.function_container.application_name.clone(),
            function: fe_meta.function_container.function_name.clone(),
            version: fe_meta.function_container.version.clone(),
        }
    }
}

impl From<&Container> for FunctionURI {
    fn from(fe: &Container) -> Self {
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
    fn from_catalog_entry(catalog_entry: &crate::config::ExecutorCatalogEntry) -> Self {
        let gpu = catalog_entry.gpu_model.as_ref().map(|model| GPUResources {
            count: model.count, // Unlimited count since catalog doesn't specify
            model: model.name.clone(),
        });

        HostResources {
            cpu_ms_per_sec: catalog_entry.cpu_cores * 1000,
            memory_bytes: catalog_entry.memory_gb * 1024 * 1024 * 1024,
            disk_bytes: catalog_entry.disk_gb * 1024 * 1024 * 1024,
            gpu,
        }
    }

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
    pub fn can_handle_fe_resources(&self, request: &ContainerResources) -> Result<()> {
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
        let fe_resources_no_gpu = ContainerResources {
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

    pub fn consume_fe_resources(&mut self, request: &ContainerResources) -> Result<()> {
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

    /// Force-consume resources without checking availability.
    /// Uses saturating subtraction to prevent underflow.
    /// Used when reconciling containers that already exist on the executor.
    pub fn force_consume_fe_resources(&mut self, request: &ContainerResources) {
        self.cpu_ms_per_sec = self.cpu_ms_per_sec.saturating_sub(request.cpu_ms_per_sec);
        self.memory_bytes = self
            .memory_bytes
            .saturating_sub(request.memory_mb * 1024 * 1024);
        self.disk_bytes = self
            .disk_bytes
            .saturating_sub(request.ephemeral_disk_mb * 1024 * 1024);
        if let Some(requested_gpu) = &request.gpu &&
            let Some(available_gpu) = &mut self.gpu
        {
            available_gpu.count = available_gpu.count.saturating_sub(requested_gpu.count);
        }
    }

    pub fn consume_function_resources(
        &mut self,
        request: &FunctionResources,
    ) -> Result<ContainerResources> {
        let fe_resources_no_gpu = ContainerResources {
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

    pub fn free(&mut self, allocated_resources: &ContainerResources) -> Result<()> {
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
pub struct ContainerId(String);
impl ContainerId {
    pub fn new(id: String) -> Self {
        Self(id)
    }

    pub fn get(&self) -> &str {
        &self.0
    }
}
impl Default for ContainerId {
    fn default() -> Self {
        Self::new(nanoid::nanoid!())
    }
}

impl From<&str> for ContainerId {
    fn from(s: &str) -> Self {
        Self::new(s.to_string())
    }
}

impl From<SandboxId> for ContainerId {
    fn from(id: SandboxId) -> Self {
        Self::new(id.0)
    }
}

impl From<&SandboxId> for ContainerId {
    fn from(id: &SandboxId) -> Self {
        Self::new(id.0.clone())
    }
}

impl fmt::Display for ContainerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", &self.0)
    }
}

#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Default, strum::AsRefStr, Display, Eq, Hash,
)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum ContainerState {
    #[default]
    Unknown,
    // Function Executor is being created.
    Pending,
    // Function Executor is running and ready to accept tasks.
    Running,
    // Function Executor is terminated, all resources are freed.
    Terminated {
        reason: ContainerTerminationReason,
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
pub enum ContainerTerminationReason {
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
    ProcessCrash,
    StartupFailedBadImage,
}

impl From<&ContainerTerminationReason> for FunctionRunFailureReason {
    fn from(reason: &ContainerTerminationReason) -> Self {
        match reason {
            ContainerTerminationReason::Unknown => {
                FunctionRunFailureReason::FunctionExecutorTerminated
            }
            ContainerTerminationReason::StartupFailedInternalError => {
                FunctionRunFailureReason::ContainerStartupInternalError
            }
            ContainerTerminationReason::StartupFailedFunctionError => {
                FunctionRunFailureReason::ContainerStartupFunctionError
            }
            ContainerTerminationReason::StartupFailedFunctionTimeout => {
                FunctionRunFailureReason::ContainerStartupFunctionTimeout
            }
            ContainerTerminationReason::StartupFailedBadImage => {
                FunctionRunFailureReason::ContainerStartupBadImage
            }
            ContainerTerminationReason::Unhealthy => FunctionRunFailureReason::FunctionTimeout,
            ContainerTerminationReason::InternalError => FunctionRunFailureReason::InternalError,
            ContainerTerminationReason::FunctionError => FunctionRunFailureReason::FunctionError,
            ContainerTerminationReason::FunctionTimeout => {
                FunctionRunFailureReason::FunctionTimeout
            }
            ContainerTerminationReason::FunctionCancelled => {
                FunctionRunFailureReason::FunctionRunCancelled
            }
            ContainerTerminationReason::DesiredStateRemoved => {
                FunctionRunFailureReason::FunctionExecutorTerminated
            }
            ContainerTerminationReason::ExecutorRemoved => {
                FunctionRunFailureReason::ExecutorRemoved
            }
            ContainerTerminationReason::Oom => FunctionRunFailureReason::OutOfMemory,
            ContainerTerminationReason::ProcessCrash => FunctionRunFailureReason::FunctionError,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FunctionAllowlist {
    pub namespace: Option<String>,
    pub application: Option<String>,
    pub function: Option<String>,
}

impl FunctionAllowlist {
    pub fn matches_function(&self, ns: &str, app: &str, function: &Function) -> bool {
        self.namespace
            .as_ref()
            .is_none_or(|namespace| namespace == ns) &&
            self.application
                .as_ref()
                .is_none_or(|application| application == app) &&
            self.function
                .as_ref()
                .is_none_or(|function_name| function_name == &function.name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder, Eq, PartialEq)]
pub struct ContainerResources {
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
    #[serde(default)]
    created_at_clock: Option<u64>,
    #[builder(default)]
    #[serde(default)]
    updated_at_clock: Option<u64>,
}

impl GcUrl {
    pub fn key(&self) -> String {
        format!("{}|{}", self.namespace, self.url)
    }

    /// Prepares for persistence by setting the server clock.
    pub fn prepare_for_persistence(&mut self, clock: u64) {
        if self.created_at_clock.is_none() {
            self.created_at_clock = Some(clock);
        }
        self.updated_at_clock = Some(clock);
    }
}

#[serde_inline_default]
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct Container {
    #[builder(default)]
    pub id: ContainerId,
    pub namespace: String,
    pub application_name: String,
    pub function_name: String,
    pub version: String,
    pub state: ContainerState,
    pub resources: ContainerResources,
    pub max_concurrency: u32,
    #[builder(default)]
    #[serde(default)]
    pub container_type: ContainerType,
    /// docker image to use for the sandbox
    #[builder(default)]
    #[serde_inline_default(Some(DEFAULT_SANDBOX_IMAGE.to_string()))]
    pub image: Option<String>,
    #[builder(default)]
    #[serde(default)]
    pub secret_names: Vec<String>,
    /// Timeout in seconds for sandbox containers. 0 means no timeout.
    /// Only applicable to sandbox-type containers.
    #[builder(default)]
    #[serde_inline_default(300)]
    pub timeout_secs: u64,
    /// Optional entrypoint command for sandbox containers.
    #[builder(default)]
    #[serde(default)]
    pub entrypoint: Vec<String>,
    /// Network access control policy for sandbox containers.
    #[builder(default)]
    #[serde(default)]
    pub network_policy: Option<NetworkPolicy>,
    /// The sandbox this container is serving (if any).
    /// For sandbox containers: None when warm, Some when claimed/allocated.
    #[builder(default)]
    #[serde(default)]
    pub sandbox_id: Option<SandboxId>,
    /// The pool this container belongs to.
    /// Some(pool_id) for function and shared sandbox pool containers.
    /// None for standalone sandbox containers (created on-demand, 1:1 with a
    /// sandbox).
    #[builder(default)]
    #[serde(default)]
    pub pool_id: Option<ContainerPoolId>,
    /// Snapshot URI to restore from (e.g. "s3://bucket/snap.tar.zst")
    #[builder(default)]
    #[serde(default)]
    pub snapshot_uri: Option<String>,
    #[builder(default)]
    #[serde(default)]
    created_at_clock: Option<u64>,
    #[builder(default)]
    #[serde(default)]
    updated_at_clock: Option<u64>,
}

// Note: FunctionExecutor is stored in memory, not persisted to RocksDB,
// so created_at_clock and updated_at_clock are currently unused but kept
// for future use if ExecutorMetadata becomes persisted.

impl PartialEq for Container {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Container {}

impl Hash for Container {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Container {
    pub fn update(&mut self, other: &Container) {
        // Only update fields that change after self FE was created.
        // Other FE must represent the same FE.
        self.state = other.state.clone();
    }

    /// Check if this container belongs to the given pool
    pub fn belongs_to_pool(&self, pool_key: &ContainerPoolKey) -> bool {
        self.pool_id
            .as_ref()
            .is_some_and(|pid| *pid == pool_key.pool_id && self.namespace == pool_key.namespace)
    }

    /// Get the pool key for this container.
    /// Returns None for standalone sandbox containers that have no pool.
    pub fn pool_key(&self) -> Option<ContainerPoolKey> {
        self.pool_id
            .as_ref()
            .map(|pid| ContainerPoolKey::new(&self.namespace, pid))
    }

    /// Get the created_at_clock value
    pub fn created_at_clock(&self) -> Option<u64> {
        self.created_at_clock
    }
}

#[derive(Debug, Clone, Builder, Serialize, Deserialize)]
pub struct ExecutorServerMetadata {
    pub executor_id: ExecutorId,
    pub function_container_ids: imbl::HashSet<ContainerId>,
    pub free_resources: HostResources,
    pub resource_claims: imbl::HashMap<ContainerId, ContainerResources>,
}

impl ExecutorServerMetadata {
    pub fn remove_container(&mut self, container: &Container) -> Result<()> {
        self.function_container_ids.remove(&container.id);
        if let Some(existing_claim) = self.resource_claims.get(&container.id) {
            self.free_resources.free(existing_claim)?;
        }
        self.resource_claims.remove(&container.id);
        Ok(())
    }

    pub fn add_container(&mut self, container: &Container) -> Result<()> {
        self.function_container_ids.insert(container.id.clone());
        if let Some(_existing_claim) = self.resource_claims.get(&container.id) {
            return Ok(());
        }
        self.resource_claims
            .insert(container.id.clone(), container.resources.clone());
        self.free_resources
            .consume_fe_resources(&container.resources)
    }

    /// Force-add a container without checking resource availability.
    /// Used when reconciling containers that already exist on the executor.
    /// Uses saturating subtraction to prevent underflow if resources are
    /// overcommitted.
    pub fn force_add_container(&mut self, container: &Container) {
        self.function_container_ids.insert(container.id.clone());
        if self.resource_claims.contains_key(&container.id) {
            return;
        }
        self.resource_claims
            .insert(container.id.clone(), container.resources.clone());
        self.free_resources
            .force_consume_fe_resources(&container.resources);
    }
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub enum ContainerType {
    Sandbox,
    #[default]
    Function,
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct ContainerServerMetadata {
    pub executor_id: ExecutorId,
    pub function_container: Container,
    pub desired_state: ContainerState,
    #[builder(default)]
    pub container_type: ContainerType,
    #[builder(default)]
    #[serde(default)]
    pub allocations: imbl::HashSet<AllocationId>,
    /// When the container last became idle (no allocations).
    /// - `Some(instant)` = container is idle since this time
    /// - `None` = container is currently busy (has allocations)
    #[serde(skip)]
    #[builder(default)]
    pub idle_since: Option<tokio::time::Instant>,
}

impl Eq for ContainerServerMetadata {}

impl PartialEq for ContainerServerMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.executor_id == other.executor_id &&
            self.function_container.id == other.function_container.id
    }
}

impl Hash for ContainerServerMetadata {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.executor_id.hash(state);
        self.function_container.id.hash(state);
    }
}

impl ContainerServerMetadata {
    pub fn new(
        executor_id: ExecutorId,
        function_executor: Container,
        desired_state: ContainerState,
    ) -> Self {
        let container_type = function_executor.container_type.clone();
        Self {
            executor_id,
            function_container: function_executor,
            desired_state,
            container_type,
            allocations: imbl::HashSet::new(),
            idle_since: Some(tokio::time::Instant::now()),
        }
    }

    pub fn can_be_removed(&self) -> bool {
        // A container can only be vacuumed if:
        // 1. It's a Function container (not a Sandbox)
        // 2. AND it has no running allocations
        self.container_type == ContainerType::Function && self.allocations.is_empty()
    }
}

#[serde_inline_default]
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct ExecutorMetadata {
    pub id: ExecutorId,
    // FIXME Remove in next release
    #[serde_inline_default("0.2.17".to_string())]
    pub executor_version: String,
    pub function_allowlist: Option<Vec<FunctionAllowlist>>,
    pub addr: String,
    pub labels: imbl::HashMap<String, String>,
    pub containers: imbl::HashMap<ContainerId, Container>,
    pub host_resources: HostResources,
    pub state: ExecutorState,
    #[builder(default)]
    pub tombstoned: bool,
    pub state_hash: String,
    #[builder(default)]
    pub clock: u64,
    #[builder(default)]
    pub catalog_name: Option<String>,
    #[builder(default)]
    #[serde(default)]
    created_at_clock: Option<u64>,
    #[builder(default)]
    #[serde(default)]
    updated_at_clock: Option<u64>,
    #[builder(default)]
    #[serde(default)]
    pub proxy_address: Option<String>,
}

// Note: ExecutorMetadata is stored in memory, not persisted to RocksDB,
// so created_at_clock and updated_at_clock are currently unused but kept
// for future use if ExecutorMetadata becomes persisted.

impl ExecutorMetadata {
    pub fn is_function_allowed(
        &self,
        namespace: &str,
        application: &str,
        function: &Function,
    ) -> bool {
        if let Some(function_allowlist) = &self.function_allowlist {
            function_allowlist
                .iter()
                .any(|allowlist| allowlist.matches_function(namespace, application, function))
        } else {
            true
        }
    }

    pub fn is_namespace_allowed(&self, namespace: &str) -> bool {
        self.function_allowlist
            .as_ref()
            .map(|allowlist| {
                allowlist.iter().any(|allowlist| {
                    allowlist
                        .namespace
                        .as_ref()
                        .is_some_and(|ns| ns == namespace)
                })
            })
            .unwrap_or(true)
    }

    #[allow(dead_code)]
    pub fn update(&mut self, update: ExecutorMetadata) {
        self.function_allowlist = update.function_allowlist;
        self.containers = update.containers;
        self.state = update.state;
        self.state_hash = update.state_hash;
        self.clock = update.clock;
        self.tombstoned = false;
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
    pub request_exception: Option<DataPayload>,
    pub allocation_id: AllocationId,
    pub allocation_target: AllocationTarget,
    pub allocation_outcome: FunctionRunOutcome,
    pub execution_duration_ms: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct FunctionCallEvent {
    pub namespace: String,
    pub application: String,
    pub request_id: String,
    pub source_function_call_id: FunctionCallId,
    pub graph_updates: GraphUpdates,
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

/// Info about a container state change reported by the dataplane.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ContainerStateUpdateInfo {
    pub container_id: ContainerId,
    pub termination_reason: Option<ContainerTerminationReason>,
}

/// Event triggered when a v2 dataplane reports allocation results
/// and container state changes together for atomic processing.
/// Carries all data needed by the ApplicationProcessor handler so
/// container terminations and allocation results are processed atomically.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DataplaneResultsIngestedEvent {
    pub executor_id: ExecutorId,
    pub allocation_events: Vec<AllocationOutputIngestedEvent>,
    pub container_state_updates: Vec<ContainerStateUpdateInfo>,
    pub container_started_ids: Vec<ContainerId>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ChangeType {
    InvokeApplication(InvokeApplicationEvent),
    CreateFunctionCall(FunctionCallEvent),
    AllocationOutputsIngested(Box<AllocationOutputIngestedEvent>),
    TombstoneApplication(TombstoneApplicationEvent),
    TombstoneRequest(TombstoneRequestEvent),
    ExecutorUpserted(ExecutorUpsertedEvent),
    TombStoneExecutor(ExecutorRemovedEvent),
    CreateSandbox(CreateSandboxEvent),
    TerminateSandbox(TerminateSandboxEvent),
    SnapshotSandbox(SnapshotSandboxEvent),
    CreateContainerPool(CreateContainerPoolEvent),
    UpdateContainerPool(UpdateContainerPoolEvent),
    DeleteContainerPool(DeleteContainerPoolEvent),
    DataplaneResultsIngested(DataplaneResultsIngestedEvent),
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
            ChangeType::CreateFunctionCall(ev) => {
                write!(
                    f,
                    "CreateFunctionCall, request_id: {}, source_function_call_id: {}",
                    ev.request_id, ev.source_function_call_id
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
            ChangeType::CreateSandbox(ev) => write!(
                f,
                "CreateSandbox, namespace: {}, sandbox_id: {}",
                ev.namespace, ev.sandbox_id
            ),
            ChangeType::TerminateSandbox(ev) => write!(
                f,
                "TerminateSandbox, namespace: {}, sandbox_id: {}",
                ev.namespace, ev.sandbox_id
            ),
            ChangeType::SnapshotSandbox(ev) => write!(
                f,
                "SnapshotSandbox, namespace: {}, sandbox_id: {}, snapshot_id: {}",
                ev.namespace, ev.sandbox_id, ev.snapshot_id
            ),
            ChangeType::CreateContainerPool(ev) => write!(
                f,
                "CreateContainerPool, namespace: {}, pool_id: {}",
                ev.namespace, ev.pool_id
            ),
            ChangeType::UpdateContainerPool(ev) => write!(
                f,
                "UpdateContainerPool, namespace: {}, pool_id: {}",
                ev.namespace, ev.pool_id
            ),
            ChangeType::DeleteContainerPool(ev) => write!(
                f,
                "DeleteContainerPool, namespace: {}, pool_id: {}",
                ev.namespace, ev.pool_id
            ),
            ChangeType::DataplaneResultsIngested(ev) => write!(
                f,
                "DataplaneResultsIngested, executor_id: {}",
                ev.executor_id
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

impl AsRef<u64> for StateChangeId {
    fn as_ref(&self) -> &u64 {
        &self.0
    }
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
    #[serde(default)]
    created_at_clock: Option<u64>,
    #[builder(default)]
    #[serde(default)]
    updated_at_clock: Option<u64>,
}

impl Namespace {
    /// Prepares for persistence by setting the server clock.
    pub fn prepare_for_persistence(&mut self, clock: u64) {
        if self.created_at_clock.is_none() {
            self.created_at_clock = Some(clock);
        }
        self.updated_at_clock = Some(clock);
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
pub struct AllocationUsageEvent {
    pub id: AllocationUsageId,
    pub namespace: String,
    pub application: String,
    pub application_version: String,
    pub request_id: String,
    pub allocation_id: AllocationId,
    pub execution_duration_ms: u64,
    pub function: String,

    #[builder(default)]
    #[serde(default)]
    created_at_clock: Option<u64>,
    #[builder(default)]
    #[serde(default)]
    updated_at_clock: Option<u64>,
}

impl AllocationUsageEvent {
    /// Prepares for persistence by setting the server clock.
    pub fn prepare_for_persistence(&mut self, clock: u64) {
        if self.created_at_clock.is_none() {
            self.created_at_clock = Some(clock);
        }
        self.updated_at_clock = Some(clock);
    }

    /// Returns a key suitable for use in RocksDB.
    ///
    /// It uses the AllocationUsageId as the key, encoded as big-endian bytes.
    pub fn key(&self) -> [u8; 8] {
        // RocksDB sorts keys in lexicographical order. Using the id
        // as the key ensures that newer versions of the same AllocationUsage
        // will sort after older versions.
        self.id.0.to_be_bytes()
    }
}

// ================================
// Sandbox Types
// ================================

/// Unique identifier for a sandbox instance
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SandboxId(pub String);

impl SandboxId {
    pub fn new(id: String) -> Self {
        Self(id)
    }

    pub fn get(&self) -> &str {
        &self.0
    }
}

/// DNS-safe alphabet for sandbox IDs (no underscores, lowercase only).
const SANDBOX_ID_ALPHABET: &[char] = &[
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
    'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

impl Default for SandboxId {
    fn default() -> Self {
        Self(nanoid!(21, SANDBOX_ID_ALPHABET))
    }
}

impl Display for SandboxId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for SandboxId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for SandboxId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Status of a sandbox instance
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxStatus {
    /// Sandbox is waiting for executor allocation
    Pending { reason: SandboxPendingReason },
    /// Sandbox container is running
    Running,
    /// Sandbox container has terminated
    Terminated,
    /// Sandbox is being snapshotted; will be terminated after completion
    Snapshotting { snapshot_id: SnapshotId },
}

impl Default for SandboxStatus {
    fn default() -> Self {
        SandboxStatus::Pending {
            reason: SandboxPendingReason::default(),
        }
    }
}

impl SandboxStatus {
    /// Returns true if the status is any Pending variant.
    pub fn is_pending(&self) -> bool {
        matches!(self, SandboxStatus::Pending { .. })
    }
}

impl Display for SandboxStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SandboxStatus::Pending { .. } => write!(f, "Pending"),
            SandboxStatus::Running => write!(f, "Running"),
            SandboxStatus::Snapshotting { .. } => write!(f, "Snapshotting"),
            SandboxStatus::Terminated => write!(f, "Terminated"),
        }
    }
}

/// Reason why a sandbox is in Pending status
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxPendingReason {
    #[default]
    Scheduling,
    WaitingForContainer,
    NoExecutorsAvailable,
    NoResourcesAvailable,
    PoolAtCapacity,
}

impl Display for SandboxPendingReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SandboxPendingReason::Scheduling => write!(f, "scheduling"),
            SandboxPendingReason::WaitingForContainer => write!(f, "waiting_for_container"),
            SandboxPendingReason::NoExecutorsAvailable => write!(f, "no_executors_available"),
            SandboxPendingReason::NoResourcesAvailable => write!(f, "no_resources_available"),
            SandboxPendingReason::PoolAtCapacity => write!(f, "pool_at_capacity"),
        }
    }
}

/// Outcome of a terminated sandbox
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxOutcome {
    #[default]
    Unknown,
    /// Sandbox completed successfully
    Success(SandboxSuccessReason),
    /// Sandbox failed due to an error
    Failure(SandboxFailureReason),
}

impl Display for SandboxOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SandboxOutcome::Unknown => write!(f, "Unknown"),
            SandboxOutcome::Success(reason) => write!(f, "Success({reason})"),
            SandboxOutcome::Failure(reason) => write!(f, "Failure({reason})"),
        }
    }
}

/// Reason for successful sandbox termination
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxSuccessReason {
    #[default]
    Unknown,
    /// User explicitly terminated the sandbox
    UserTerminated,
    /// Sandbox ran until its timeout expired
    Timeout,
}

impl Display for SandboxSuccessReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SandboxSuccessReason::Unknown => write!(f, "Unknown"),
            SandboxSuccessReason::UserTerminated => write!(f, "UserTerminated"),
            SandboxSuccessReason::Timeout => write!(f, "Timeout"),
        }
    }
}

/// Reason for sandbox failure
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxFailureReason {
    #[default]
    Unknown,
    /// Internal platform error
    InternalError,
    /// Placeholder — preserves postcard variant index for existing data.
    #[doc(hidden)]
    _Reserved,
    /// No executor could satisfy placement constraints
    ConstraintUnsatisfiable,
    /// Executor was removed
    ExecutorRemoved,
    /// Out of memory
    OutOfMemory,
    /// Container startup failed
    ContainerStartupFailed,
    /// Container terminated (with reason from executor)
    ContainerTerminated(ContainerTerminationReason),
    /// Pool was deleted while sandbox was using it
    PoolDeleted,
}

impl Display for SandboxFailureReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SandboxFailureReason::Unknown => write!(f, "Unknown"),
            SandboxFailureReason::InternalError => write!(f, "InternalError"),
            SandboxFailureReason::_Reserved => write!(f, "Reserved"),
            SandboxFailureReason::ConstraintUnsatisfiable => write!(f, "ConstraintUnsatisfiable"),
            SandboxFailureReason::ExecutorRemoved => write!(f, "ExecutorRemoved"),
            SandboxFailureReason::OutOfMemory => write!(f, "OutOfMemory"),
            SandboxFailureReason::ContainerStartupFailed => write!(f, "ContainerStartupFailed"),
            SandboxFailureReason::ContainerTerminated(reason) => {
                write!(f, "ContainerTerminated({reason})")
            }
            SandboxFailureReason::PoolDeleted => write!(f, "PoolDeleted"),
        }
    }
}

/// Key for sandbox storage: namespace|sandbox_id
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SandboxKey(pub String);

impl SandboxKey {
    pub fn new(namespace: &str, sandbox_id: &str) -> Self {
        Self(format!("{namespace}|{sandbox_id}"))
    }

    pub fn from_sandbox(sandbox: &Sandbox) -> Self {
        Self::new(&sandbox.namespace, sandbox.id.get())
    }

    /// Extract the namespace portion of the key (before the `|` separator).
    pub fn namespace(&self) -> &str {
        self.0.split('|').next().unwrap_or("")
    }

    /// Extract the sandbox_id portion of the key (after the `|` separator).
    pub fn sandbox_id(&self) -> &str {
        self.0.split('|').nth(1).unwrap_or("")
    }
}

impl Display for SandboxKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&Sandbox> for SandboxKey {
    fn from(sandbox: &Sandbox) -> Self {
        SandboxKey::from_sandbox(sandbox)
    }
}

impl From<String> for SandboxKey {
    fn from(s: String) -> Self {
        SandboxKey(s)
    }
}

impl From<&String> for SandboxKey {
    fn from(s: &String) -> Self {
        SandboxKey(s.clone())
    }
}

/// Network access control policy for sandbox containers.
/// Rules are applied using host-level iptables on the DOCKER-USER chain.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct NetworkPolicy {
    /// If false, all outbound internet access is blocked by default (DROP).
    /// If true (default), outbound is allowed unless explicitly denied.
    #[serde(default = "default_true")]
    pub allow_internet_access: bool,
    /// List of allowed destination IPs/CIDRs (e.g., "8.8.8.8", "10.0.0.0/8").
    /// Allow rules take precedence over deny rules.
    #[serde(default)]
    pub allow_out: Vec<String>,
    /// List of denied destination IPs/CIDRs (e.g., "192.168.1.100").
    #[serde(default)]
    pub deny_out: Vec<String>,
}

fn default_true() -> bool {
    true
}

/// A sandbox instance that provides an interactive container environment
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Builder)]
pub struct Sandbox {
    pub id: SandboxId,
    pub namespace: String,
    /// Docker image to use
    pub image: String,
    #[builder(default)]
    pub status: SandboxStatus,
    #[builder(default)]
    pub outcome: Option<SandboxOutcome>,
    #[builder(default = "self.default_creation_time_ns()")]
    pub creation_time_ns: u128,
    #[builder(default)]
    #[serde(default)]
    created_at_clock: Option<u64>,
    #[builder(default)]
    #[serde(default)]
    updated_at_clock: Option<u64>,
    pub resources: ContainerResources,
    #[builder(default)]
    pub secret_names: Vec<String>,
    #[builder(default)]
    pub timeout_secs: u64,
    /// The Executor ID where the sandbox is running
    #[builder(default)]
    pub executor_id: Option<ExecutorId>,
    /// Optional entrypoint command to run when sandbox starts
    #[builder(default)]
    pub entrypoint: Option<Vec<String>>,
    /// Network access control policy for this sandbox.
    #[builder(default)]
    #[serde(default)]
    pub network_policy: Option<NetworkPolicy>,
    /// If created from a pool, the pool ID
    #[builder(default)]
    #[serde(default)]
    pub pool_id: Option<ContainerPoolId>,
    /// The container ID serving this sandbox (set when sandbox becomes Running)
    #[builder(default)]
    #[serde(default)]
    pub container_id: Option<ContainerId>,
    /// If created from a snapshot, the snapshot ID
    #[builder(default)]
    #[serde(default)]
    pub snapshot_id: Option<SnapshotId>,
}

impl SandboxBuilder {
    fn default_creation_time_ns(&self) -> u128 {
        get_epoch_time_in_ns()
    }
}

impl Sandbox {
    /// Returns the storage key for this sandbox
    pub fn key(&self) -> String {
        format!("{}|{}", self.namespace, self.id.0)
    }

    /// Returns true if a container has already been scheduled for this sandbox.
    /// A sandbox with a container assigned is waiting for the container to
    /// report Running and should not be re-allocated.
    pub fn has_scheduled(&self) -> bool {
        self.container_id.is_some()
    }

    /// Prepares the sandbox for persistence by setting the server clock
    pub fn prepare_for_persistence(&mut self, clock: u64) {
        if self.created_at_clock.is_none() {
            self.created_at_clock = Some(clock);
        }
        self.updated_at_clock = Some(clock);
    }
}

/// Event for creating a sandbox
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct CreateSandboxEvent {
    pub namespace: String,
    pub sandbox_id: SandboxId,
}

/// Event for terminating a sandbox
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct TerminateSandboxEvent {
    pub namespace: String,
    pub sandbox_id: SandboxId,
}

/// Event for snapshotting a sandbox
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct SnapshotSandboxEvent {
    pub namespace: String,
    pub sandbox_id: SandboxId,
    pub snapshot_id: SnapshotId,
    pub upload_uri: String,
}

// ================================
// Snapshot Types
// ================================

/// Unique identifier for a filesystem snapshot
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(transparent)]
pub struct SnapshotId(pub String);

impl SnapshotId {
    pub fn new(id: String) -> Self {
        Self(id)
    }

    pub fn get(&self) -> &str {
        &self.0
    }
}

impl Default for SnapshotId {
    fn default() -> Self {
        Self(nanoid!(21, SANDBOX_ID_ALPHABET))
    }
}

impl Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for SnapshotId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for SnapshotId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Key for snapshot storage: namespace|snapshot_id
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SnapshotKey(pub String);

impl SnapshotKey {
    pub fn new(namespace: &str, snapshot_id: &str) -> Self {
        Self(format!("{namespace}|{snapshot_id}"))
    }

    pub fn from_snapshot(snapshot: &Snapshot) -> Self {
        Self::new(&snapshot.namespace, snapshot.id.get())
    }
}

impl Display for SnapshotKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&Snapshot> for SnapshotKey {
    fn from(snapshot: &Snapshot) -> Self {
        SnapshotKey::from_snapshot(snapshot)
    }
}

impl From<String> for SnapshotKey {
    fn from(s: String) -> Self {
        SnapshotKey(s)
    }
}

impl From<&String> for SnapshotKey {
    fn from(s: &String) -> Self {
        SnapshotKey(s.clone())
    }
}

/// Status of a snapshot
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotStatus {
    /// Snapshot is being created (container being exported + uploaded)
    InProgress,
    /// Snapshot completed successfully
    Completed,
    /// Snapshot creation failed
    Failed { error: String },
}

impl Display for SnapshotStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SnapshotStatus::InProgress => write!(f, "in_progress"),
            SnapshotStatus::Completed => write!(f, "completed"),
            SnapshotStatus::Failed { .. } => write!(f, "failed"),
        }
    }
}

/// A filesystem snapshot of a sandbox container
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Snapshot {
    pub id: SnapshotId,
    pub namespace: String,
    /// The sandbox that was snapshotted
    pub sandbox_id: SandboxId,
    /// Base image of the sandbox at snapshot time
    pub base_image: String,
    pub status: SnapshotStatus,
    /// URI of the stored snapshot blob (set on completion)
    #[serde(default)]
    pub snapshot_uri: Option<String>,
    /// Size of the snapshot in bytes (set on completion)
    #[serde(default)]
    pub size_bytes: Option<u64>,
    pub creation_time_ns: u128,
    /// Resources of the original sandbox (inherited by restored sandboxes)
    pub resources: ContainerResources,
    /// Entrypoint of the original sandbox
    #[serde(default)]
    pub entrypoint: Option<Vec<String>>,
    /// Secret names from the original sandbox
    #[serde(default)]
    pub secret_names: Vec<String>,
}

/// Event for creating a container pool
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct CreateContainerPoolEvent {
    pub namespace: String,
    pub pool_id: ContainerPoolId,
}

/// Event for updating a container pool
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct UpdateContainerPoolEvent {
    pub namespace: String,
    pub pool_id: ContainerPoolId,
}

/// Event for deleting a container pool
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct DeleteContainerPoolEvent {
    pub namespace: String,
    pub pool_id: ContainerPoolId,
}

// ================================
// Container Pool Types (Unified)
// ================================

/// Unique identifier for a container pool.
/// Pool IDs contain only the meaningful identifier (no type prefix):
/// - `{app}|{fn}|{ver}` for function pools (stored in FunctionPools CF)
/// - `{sandbox_id}` for synthetic sandbox tracking (in-memory only)
/// - `{nanoid}` for user-created shared sandbox pools (stored in SandboxPools
///   CF)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContainerPoolId(String);

impl ContainerPoolId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn get(&self) -> &str {
        &self.0
    }

    /// Create pool ID for a function: `{app}|{fn}|{ver}`
    pub fn for_function(app: &str, function: &str, version: &str) -> Self {
        Self(format!("{}|{}|{}", app, function, version))
    }

    /// Create pool ID for a user-created sandbox pool
    pub fn for_pool() -> Self {
        Self(nanoid!())
    }
}

impl Display for ContainerPoolId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for ContainerPoolId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for ContainerPoolId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Composite key for container pool (namespace + pool_id)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContainerPoolKey {
    pub namespace: String,
    pub pool_id: ContainerPoolId,
}

impl ContainerPoolKey {
    pub fn new(namespace: &str, pool_id: &ContainerPoolId) -> Self {
        Self {
            namespace: namespace.to_string(),
            pool_id: pool_id.clone(),
        }
    }

    pub fn key(&self) -> String {
        format!("{}|{}", self.namespace, self.pool_id.0)
    }
}

impl Display for ContainerPoolKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}|{}", self.namespace, self.pool_id.0)
    }
}

impl From<&ContainerPool> for ContainerPoolKey {
    fn from(pool: &ContainerPool) -> Self {
        ContainerPoolKey::new(&pool.namespace, &pool.id)
    }
}

/// Whether a pool is for functions or sandboxes.
/// Determines which CF it's stored in and how the buffer reconciler handles it.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ContainerPoolType {
    Function,
    Sandbox,
}

/// A pool of containers for functions or sandboxes.
/// Pools define container templates and scaling settings (min/max/buffer).
#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
pub struct ContainerPool {
    pub id: ContainerPoolId,
    pub namespace: String,

    /// Whether this is a function pool or sandbox pool
    #[builder(default = "ContainerPoolType::Sandbox")]
    #[serde(default = "ContainerPool::default_pool_type")]
    pub pool_type: ContainerPoolType,

    /// Docker image for containers in this pool
    pub image: String,

    /// Resource allocation for each container
    pub resources: ContainerResources,

    /// Optional entrypoint command
    #[builder(default)]
    #[serde(default)]
    pub entrypoint: Option<Vec<String>>,

    /// Network access policy
    #[builder(default)]
    #[serde(default)]
    pub network_policy: Option<NetworkPolicy>,

    /// Secret names to inject
    #[builder(default)]
    #[serde(default)]
    pub secret_names: Vec<String>,

    /// Timeout in seconds for containers from this pool
    #[builder(default)]
    #[serde(default)]
    pub timeout_secs: u64,

    /// Minimum containers to maintain (floor)
    #[serde(default)]
    #[builder(default)]
    pub min_containers: Option<u32>,

    /// Maximum containers allowed (ceiling)
    #[serde(default)]
    #[builder(default)]
    pub max_containers: Option<u32>,

    /// Number of warm (idle/unclaimed) containers to maintain as buffer
    #[serde(default)]
    #[builder(default)]
    pub buffer_containers: Option<u32>,

    /// Timestamp of creation
    #[builder(default = "self.default_created_at()")]
    pub created_at: u64,

    /// Marked true when the pool is being deleted. User-facing reads
    /// treat tombstoned pools as non-existent; the scheduler eventually
    /// issues the actual RocksDB delete.
    #[builder(default)]
    #[serde(default)]
    pub tombstoned: bool,

    #[builder(default)]
    #[serde(default)]
    created_at_clock: Option<u64>,
    #[builder(default)]
    #[serde(default)]
    updated_at_clock: Option<u64>,
}

impl ContainerPoolBuilder {
    fn default_created_at(&self) -> u64 {
        get_epoch_time_in_ms()
    }
}

impl ContainerPool {
    /// Default maximum containers per function pool
    pub const DEFAULT_MAX_CONTAINERS: u32 = 10;

    /// Default for serde deserialization of old data without pool_type
    fn default_pool_type() -> ContainerPoolType {
        ContainerPoolType::Sandbox
    }

    pub fn is_function_pool(&self) -> bool {
        self.pool_type == ContainerPoolType::Function
    }

    pub fn key(&self) -> ContainerPoolKey {
        ContainerPoolKey::from(self)
    }

    /// Validate pool configuration
    pub fn validate(&self) -> Result<()> {
        let min = self.min_containers.unwrap_or(0);
        let max = self.max_containers.unwrap_or(u32::MAX);
        let buffer = self.buffer_containers.unwrap_or(0);

        if min > max {
            anyhow::bail!(
                "min_containers ({}) cannot exceed max_containers ({})",
                min,
                max
            );
        }
        if buffer > max {
            anyhow::bail!(
                "buffer_containers ({}) cannot exceed max_containers ({})",
                buffer,
                max
            );
        }
        // Function pools don't require an image (they use code payloads)
        if self.image.is_empty() && !self.is_function_pool() {
            anyhow::bail!("image is required");
        }
        if self.resources.cpu_ms_per_sec == 0 {
            anyhow::bail!("cpu must be greater than 0");
        }
        if self.resources.memory_mb == 0 {
            anyhow::bail!("memory must be greater than 0");
        }
        Ok(())
    }

    /// Create a ContainerPool from a Function.
    /// Every function gets a pool with sensible defaults.
    pub fn from_function(
        namespace: &str,
        app_name: &str,
        version: &str,
        function: &Function,
    ) -> Self {
        ContainerPoolBuilder::default()
            .id(ContainerPoolId::for_function(
                app_name,
                &function.name,
                version,
            ))
            .namespace(namespace.to_string())
            .pool_type(ContainerPoolType::Function)
            .image(String::new()) // Function pools don't use images
            .resources(ContainerResources {
                cpu_ms_per_sec: function.resources.cpu_ms_per_sec,
                memory_mb: function.resources.memory_mb,
                ephemeral_disk_mb: function.resources.ephemeral_disk_mb,
                gpu: function.resources.gpu_configs.first().cloned(),
            })
            .secret_names(function.secret_names.clone().unwrap_or_default())
            .timeout_secs((function.timeout.0 / 1000) as u64) // Convert ms to secs
            .min_containers(function.min_containers)
            .max_containers(Some(
                function
                    .max_containers
                    .unwrap_or(Self::DEFAULT_MAX_CONTAINERS),
            ))
            .buffer_containers(function.warm_containers)
            .build()
            .expect("ContainerPool builder should not fail with valid function")
    }

    /// Prepares the pool for persistence by setting the server clock
    pub fn prepare_for_persistence(&mut self, clock: u64) {
        if self.created_at_clock.is_none() {
            self.created_at_clock = Some(clock);
        }
        self.updated_at_clock = Some(clock);
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
            .code(Some(DataPayload {
                id: "code_id".to_string(),
                path: "cgc_path".to_string(),
                size: 23,
                sha256_hash: "hash_code".to_string(),
                encoding: "application/octet-stream".to_string(),
                metadata_size: 0,
                offset: 0,
            }))
            .created_at(5)
            .entrypoint(Some(ApplicationEntryPoint {
                function_name: "fn_a".to_string(),
                input_serializer: "json".to_string(),
                inputs_base64: "".to_string(),
                output_serializer: "json".to_string(),
                output_type_hints_base64: "".to_string(),
            }))
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
                    code: Some(DataPayload {
                        sha256_hash: "hash_code2".to_string(), // different
                        ..original_application.code.clone().unwrap()
                    }),
                    ..original_application.clone()
                },
                expected_graph: Application {
                    version: "2".to_string(), // different
                    code: Some(DataPayload {
                        sha256_hash: "hash_code2".to_string(), // different
                        ..original_application.code.clone().unwrap()
                    }),
                    ..original_application.clone()
                },
                expected_version: ApplicationVersion {
                    version: "2".to_string(),
                    code: Some(DataPayload {
                        sha256_hash: "hash_code2".to_string(), // different
                        ..original_application.code.clone().unwrap()
                    }),
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
                    entrypoint: Some(ApplicationEntryPoint {
                        function_name: "fn_b".to_string(), // different
                        input_serializer: "json".to_string(),
                        inputs_base64: "".to_string(),
                        output_serializer: "json".to_string(),
                        output_type_hints_base64: "".to_string(),
                    }),
                    ..original_application.clone()
                },
                expected_graph: Application {
                    version: "2".to_string(),
                    entrypoint: Some(ApplicationEntryPoint {
                        function_name: "fn_b".to_string(), // different
                        input_serializer: "json".to_string(),
                        inputs_base64: "".to_string(),
                        output_serializer: "json".to_string(),
                        output_type_hints_base64: "".to_string(),
                    }),
                    ..original_application.clone()
                },
                expected_version: ApplicationVersion {
                    version: "2".to_string(),
                    entrypoint: Some(ApplicationEntryPoint {
                        function_name: "fn_b".to_string(), // different
                        input_serializer: "json".to_string(),
                        inputs_base64: "".to_string(),
                        output_serializer: "json".to_string(),
                        output_type_hints_base64: "".to_string(),
                    }),
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
            container_id: "fe-1".into(),
        };
        let allocation = AllocationBuilder::default()
            .namespace("test-ns".to_string())
            .application("graph".to_string())
            .function("fn".to_string())
            .request_id("invoc-1".to_string())
            .function_call_id("task-1".into())
            .application_version("42".to_string())
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
        assert_eq!(allocation.target.container_id, target.container_id);
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
            "\"target\":{{\"executor_id\":\"{}\",\"container_id\":\"{}\"}}",
            allocation.target.executor_id.get(),
            allocation.target.container_id.get()
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
            .code(Some(DataPayload {
                id: "code_id".to_string(),
                encoding: "application/octet-stream".to_string(),
                metadata_size: 0,
                offset: 0,
                path: "path".to_string(),
                size: 1,
                sha256_hash: "hash".to_string(),
            }))
            .created_at(123)
            .entrypoint(Some(ApplicationEntryPoint {
                function_name: "fn_a".to_string(),
                input_serializer: "json".to_string(),
                inputs_base64: "".to_string(),
                output_serializer: "json".to_string(),
                output_type_hints_base64: "".to_string(),
            }))
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
        assert_eq!(ctx.created_at_clock, None);
        assert_eq!(ctx.updated_at_clock, None);

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
        // created_at_clock and updated_at_clock are None by default, so they
        // may or may not be serialized
    }

    #[test]
    fn test_function_executor_builder_build_success() {
        let id: ContainerId = "fe-123".into();
        let namespace = "test-ns".to_string();
        let application_name = "graph".to_string();
        let function_name = "fn".to_string();
        let version = "1".to_string();
        let state = ContainerState::Running;
        let resources = ContainerResources {
            cpu_ms_per_sec: 2000,
            memory_mb: 4096,
            ephemeral_disk_mb: 2048,
            gpu: Some(GPUResources {
                count: 2,
                model: GPU_MODEL_NVIDIA_A100_40GB.to_string(),
            }),
        };
        let max_concurrency = 4;

        let pool_id = ContainerPoolId::for_function(&application_name, &function_name, &version);

        let mut builder = ContainerBuilder::default();
        builder
            .id(id.clone())
            .namespace(namespace.clone())
            .application_name(application_name.clone())
            .function_name(function_name.clone())
            .version(version.clone())
            .state(state.clone())
            .resources(resources.clone())
            .max_concurrency(max_concurrency)
            .pool_id(Some(pool_id));

        let fe = builder.build().expect("Should build FunctionExecutor");

        assert_eq!(fe.id, id);
        assert_eq!(fe.namespace, namespace);
        assert_eq!(fe.application_name, application_name);
        assert_eq!(fe.function_name, function_name);
        assert_eq!(fe.version, version);
        assert_eq!(fe.state, state);
        assert_eq!(fe.resources, resources);
        assert_eq!(fe.max_concurrency, max_concurrency);
        assert_eq!(fe.created_at_clock, None);
        assert_eq!(fe.updated_at_clock, None);

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
        // created_at_clock and updated_at_clock are None by default
    }

    #[test]
    fn test_executor_metadata_builder_build_success() {
        let id = ExecutorId::from("executor-1");
        let executor_version = "0.2.17".to_string();
        let function_allowlist = Some(vec![FunctionAllowlist {
            namespace: Some("ns".to_string()),
            application: Some("graph".to_string()),
            function: Some("fn".to_string()),
        }]);
        let addr = "127.0.0.1:8080".to_string();
        let labels =
            imbl::HashMap::from(HashMap::from([("role".to_string(), "worker".to_string())]));
        let fe_id = ContainerId::from("fe-1");
        let function_executors = imbl::HashMap::from(HashMap::from([(
            fe_id.clone(),
            ContainerBuilder::default()
                .id(fe_id.clone())
                .namespace("ns".to_string())
                .application_name("graph".to_string())
                .function_name("fn".to_string())
                .version("1".to_string())
                .state(ContainerState::Running)
                .resources(ContainerResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                    gpu: None,
                })
                .max_concurrency(2)
                .pool_id(Some(ContainerPoolId::for_function("graph", "fn", "1")))
                .build()
                .expect("Should build FunctionExecutor"),
        )]));
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
            .containers(function_executors.clone())
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
        assert_eq!(metadata.containers, function_executors);
        assert_eq!(metadata.host_resources, host_resources);
        assert_eq!(metadata.state, state);
        assert_eq!(metadata.tombstoned, tombstoned);
        assert_eq!(metadata.state_hash, state_hash);
        assert_eq!(metadata.clock, clock);
        assert_eq!(metadata.created_at_clock, None);
        assert_eq!(metadata.updated_at_clock, None);

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
        // created_at_clock and updated_at_clock are None by default
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

    #[test]
    fn test_data_size() {
        let payload = DataPayload {
            id: "test".to_string(),
            path: "/path/to/data".to_string(),
            metadata_size: 100,
            offset: 0,
            size: 500,
            sha256_hash: "hash".to_string(),
            encoding: "application/octet-stream".to_string(),
        };

        // data_size = size - metadata_size
        assert_eq!(payload.data_size(), 400);
    }

    #[test]
    fn test_data_size_with_different_sizes() {
        let payload = DataPayload {
            id: "test".to_string(),
            path: "/path/to/data".to_string(),
            metadata_size: 50,
            offset: 0,
            size: 1000,
            sha256_hash: "hash".to_string(),
            encoding: "application/octet-stream".to_string(),
        };

        assert_eq!(payload.data_size(), 950);
    }

    #[test]
    fn test_data_offset() {
        let payload = DataPayload {
            id: "test".to_string(),
            path: "/path/to/data".to_string(),
            metadata_size: 100,
            offset: 500,
            size: 600,
            sha256_hash: "hash".to_string(),
            encoding: "application/octet-stream".to_string(),
        };

        // data_offset = offset + metadata_size
        assert_eq!(payload.data_offset(), 600);
    }

    #[test]
    fn test_data_offset_with_zero_offset() {
        let payload = DataPayload {
            id: "test".to_string(),
            path: "/path/to/data".to_string(),
            metadata_size: 100,
            offset: 0,
            size: 500,
            sha256_hash: "hash".to_string(),
            encoding: "application/octet-stream".to_string(),
        };

        assert_eq!(payload.data_offset(), 100);
    }

    #[test]
    fn test_data_range() {
        let payload = DataPayload {
            id: "test".to_string(),
            path: "/path/to/data".to_string(),
            metadata_size: 100,
            offset: 0,
            size: 500,
            sha256_hash: "hash".to_string(),
            encoding: "application/octet-stream".to_string(),
        };

        let range = payload.data_range();
        // start = offset + metadata_size = 0 + 100 = 100
        // end = start + data_size = 100 + 400 = 500
        assert_eq!(range.start, 100);
        assert_eq!(range.end, 500);
        assert_eq!(range.end - range.start, 400);
    }

    #[test]
    fn test_data_range_with_non_zero_offset() {
        let payload = DataPayload {
            id: "test".to_string(),
            path: "/path/to/data".to_string(),
            metadata_size: 50,
            offset: 1000,
            size: 200,
            sha256_hash: "hash".to_string(),
            encoding: "application/octet-stream".to_string(),
        };

        let range = payload.data_range();
        // start = offset + metadata_size = 1000 + 50 = 1050
        // end = start + data_size = 1050 + 150 = 1200
        assert_eq!(range.start, 1050);
        assert_eq!(range.end, 1200);
        assert_eq!(range.end - range.start, 150);
    }

    #[test]
    fn test_full_range() {
        let payload = DataPayload {
            id: "test".to_string(),
            path: "/path/to/data".to_string(),
            metadata_size: 100,
            offset: 0,
            size: 500,
            sha256_hash: "hash".to_string(),
            encoding: "application/octet-stream".to_string(),
        };

        let range = payload.full_range();
        // start = offset = 0
        // end = offset + size = 0 + 500 = 500
        assert_eq!(range.start, 0);
        assert_eq!(range.end, 500);
        assert_eq!(range.end - range.start, 500);
    }

    #[test]
    fn test_full_range_with_non_zero_offset() {
        let payload = DataPayload {
            id: "test".to_string(),
            path: "/path/to/data".to_string(),
            metadata_size: 75,
            offset: 2000,
            size: 300,
            sha256_hash: "hash".to_string(),
            encoding: "application/octet-stream".to_string(),
        };

        let range = payload.full_range();
        // start = offset = 2000
        // end = offset + size = 2000 + 300 = 2300
        assert_eq!(range.start, 2000);
        assert_eq!(range.end, 2300);
        assert_eq!(range.end - range.start, 300);
    }

    #[test]
    fn test_data_range_and_full_range_relationship() {
        // Test that data_range is contained within full_range
        let payload = DataPayload {
            id: "test".to_string(),
            path: "/path/to/data".to_string(),
            metadata_size: 100,
            offset: 500,
            size: 600,
            sha256_hash: "hash".to_string(),
            encoding: "application/octet-stream".to_string(),
        };

        let full = payload.full_range();
        let data = payload.data_range();

        // full_range: [500, 1100]
        // data_range: [600, 1100]
        assert_eq!(full.start, 500);
        assert_eq!(full.end, 1100);
        assert_eq!(data.start, 600);
        assert_eq!(data.end, 1100);

        // Verify data_range is within full_range
        assert!(data.start >= full.start);
        assert!(data.end <= full.end);

        // Verify the gap at the beginning equals metadata_size
        assert_eq!(data.start - full.start, 100);
    }

    #[test]
    fn test_zero_metadata_size() {
        // When metadata_size is 0, data_range should equal full_range
        let payload = DataPayload {
            id: "test".to_string(),
            path: "/path/to/data".to_string(),
            metadata_size: 0,
            offset: 100,
            size: 500,
            sha256_hash: "hash".to_string(),
            encoding: "application/octet-stream".to_string(),
        };

        let full = payload.full_range();
        let data = payload.data_range();

        assert_eq!(data.start, full.start);
        assert_eq!(data.end, full.end);
        assert_eq!(data.end - data.start, full.end - full.start);
    }

    #[test]
    fn test_metadata_equals_size() {
        // When metadata_size equals size, data_size should be 0
        let payload = DataPayload {
            id: "test".to_string(),
            path: "/path/to/data".to_string(),
            metadata_size: 500,
            offset: 0,
            size: 500,
            sha256_hash: "hash".to_string(),
            encoding: "application/octet-stream".to_string(),
        };

        assert_eq!(payload.data_size(), 0);
        let data_range = payload.data_range();
        assert_eq!(data_range.end - data_range.start, 0);
        assert_eq!(data_range.start, data_range.end);
    }
}

#[cfg(test)]
mod host_resources_tests;
