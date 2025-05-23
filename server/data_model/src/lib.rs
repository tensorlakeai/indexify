pub mod filter;
pub mod test_objects;

use std::{
    collections::HashMap,
    error::Error,
    fmt::{self, Display},
    hash::{DefaultHasher, Hash, Hasher},
    str,
    time::{SystemTime, UNIX_EPOCH},
    vec,
};

use anyhow::{anyhow, Result};
use derive_builder::Builder;
use filter::LabelsFilter;
use indexify_utils::{default_creation_time, get_epoch_time_in_ms};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use strum::Display;
use tracing::warn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineMetadata {
    pub db_version: u64,
    pub last_change_idx: u64,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId(String);

impl TaskId {
    pub fn get(&self) -> &str {
        &self.0
    }
}

impl Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for TaskId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Builder)]
#[builder(build_fn(skip))]
pub struct Allocation {
    pub id: String,
    pub executor_id: ExecutorId,
    pub function_executor_id: FunctionExecutorId,
    pub task_id: TaskId,
    pub namespace: String,
    pub compute_graph: String,
    pub compute_fn: String,
    pub invocation_id: String,
    pub created_at: u128,
}

impl Allocation {
    pub fn key(&self) -> String {
        Allocation::key_from(
            &self.namespace,
            &self.compute_graph,
            &self.invocation_id,
            &self.compute_fn,
            &self.task_id,
            &self.executor_id,
        )
    }

    pub fn key_from(
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
        compute_fn: &str,
        task_id: &TaskId,
        executor_id: &ExecutorId,
    ) -> String {
        format!(
            "{}|{}|{}|{}|{}|{}",
            namespace,
            compute_graph,
            invocation_id,
            compute_fn,
            task_id.get(),
            executor_id.get(),
        )
    }

    pub fn key_prefix_from_invocation(
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
    ) -> String {
        format!("{}|{}|{}|", namespace, compute_graph, invocation_id)
    }

    pub fn task_key(&self) -> String {
        Task::key_from(
            &self.namespace,
            &self.compute_graph,
            &self.invocation_id,
            &self.compute_fn,
            &self.task_id.to_string(),
        )
    }

    pub fn fn_uri(&self) -> String {
        format!(
            "{}|{}|{}",
            self.namespace, self.compute_graph, self.compute_fn
        )
    }
}

impl AllocationBuilder {
    pub fn build(&mut self) -> Result<Allocation> {
        let namespace = self
            .namespace
            .clone()
            .ok_or(anyhow!("namespace is required"))?;
        let compute_graph = self
            .compute_graph
            .clone()
            .ok_or(anyhow!("compute_graph_name is required"))?;
        let compute_fn = self
            .compute_fn
            .clone()
            .ok_or(anyhow!("compute fn is required"))?;
        let invocation_id = self
            .invocation_id
            .clone()
            .ok_or(anyhow!("invocation_id is required"))?;
        let task_id = self.task_id.clone().ok_or(anyhow!("task_id is required"))?;
        let function_executor_id = self
            .function_executor_id
            .clone()
            .ok_or(anyhow!("function_executor_id is required"))?;
        let executor_id = self
            .executor_id
            .clone()
            .ok_or(anyhow!("executor_id is required"))?;
        let created_at: u128 = get_epoch_time_in_ms() as u128;

        let mut hasher = DefaultHasher::new();
        namespace.hash(&mut hasher);
        compute_graph.hash(&mut hasher);
        compute_fn.hash(&mut hasher);
        task_id.get().hash(&mut hasher);
        invocation_id.hash(&mut hasher);
        executor_id.get().hash(&mut hasher);
        let id = format!("{:x}", hasher.finish());

        Ok(Allocation {
            id,
            function_executor_id,
            executor_id,
            task_id,
            namespace,
            compute_graph,
            compute_fn,
            invocation_id,
            created_at,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Default)]
pub struct ImageInformation {
    pub image_name: String,
    pub tag: String,
    pub base_image: String,
    pub run_strs: Vec<String>,
    pub image_hash: String,
    pub image_uri: Option<String>,
    pub sdk_version: Option<String>,
}

impl ImageInformation {
    pub fn new(
        image_name: String,
        image_hash: String,
        image_uri: Option<String>,
        tag: String,
        base_image: String,
        run_strs: Vec<String>,
        sdk_version: Option<String>,
    ) -> Self {
        let mut compat_image_hash: String = image_hash;
        if compat_image_hash == "" {
            // Preserve backwards compatibility with old hash calculation
            let mut image_hasher = Sha256::new();
            image_hasher.update(image_name.clone());
            image_hasher.update(base_image.clone());
            image_hasher.update(run_strs.clone().join(""));
            image_hasher.update(sdk_version.clone().unwrap_or("".to_string())); // Igh.....
            compat_image_hash = format!("{:x}", image_hasher.finalize())
        }

        ImageInformation {
            image_name,
            tag,
            base_image,
            run_strs,
            image_hash: compat_image_hash,
            image_uri,
            sdk_version,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeTimeoutMS(pub u32);

impl Default for NodeTimeoutMS {
    fn default() -> Self {
        NodeTimeoutMS(5 * 60 * 1000) // 5 minutes
    }
}

impl Into<NodeTimeoutMS> for u32 {
    fn into(self) -> NodeTimeoutMS {
        NodeTimeoutMS(self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeGPUConfig {
    pub count: u32,
    pub model: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeResources {
    // 1000 CPU ms per sec is one full CPU core.
    // 2000 CPU ms per sec is two full CPU cores.
    pub cpu_ms_per_sec: u32,
    pub memory_mb: u64,
    pub ephemeral_disk_mb: u64,
    // The list is ordered from most to least preferred GPU configuration.
    pub gpu_configs: Vec<NodeGPUConfig>,
}

impl Default for NodeResources {
    fn default() -> Self {
        NodeResources {
            cpu_ms_per_sec: 1000,        // 1 full CPU core
            memory_mb: 1024,             // 1 GB
            ephemeral_disk_mb: 1 * 1024, // 1 GB
            gpu_configs: vec![],         // No GPUs by default
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeRetryPolicy {
    pub max_retries: u32,
    pub initial_delay_ms: u32,
    pub max_delay_ms: u32,
    // The multiplier value is 1000x of the actual value to avoid working with floating point.
    pub delay_multiplier: u32,
}

impl Default for NodeRetryPolicy {
    fn default() -> Self {
        NodeRetryPolicy {
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

#[derive(Default, Debug, Clone, Serialize, Deserialize, Builder, PartialEq, Eq)]
pub struct DynamicEdgeRouter {
    pub name: String,
    pub description: String,
    pub source_fn: String,
    pub target_functions: Vec<String>,
    #[serde(default = "default_data_encoder")]
    pub input_encoder: String,
    #[serde(default = "default_data_encoder")]
    pub output_encoder: String,
    pub image_information: ImageInformation,
    pub secret_names: Option<Vec<String>>,
    #[serde(default)]
    pub timeout: NodeTimeoutMS,
    #[serde(default)]
    pub resources: NodeResources,
    #[serde(default)]
    pub retry_policy: NodeRetryPolicy,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ComputeFn {
    pub name: String,
    pub description: String,
    pub placement_constraints: LabelsFilter,
    pub fn_name: String,
    pub reducer: bool,
    #[serde(default = "default_data_encoder")]
    pub input_encoder: String,
    #[serde(default = "default_data_encoder")]
    pub output_encoder: String,
    pub image_information: ImageInformation,
    pub secret_names: Option<Vec<String>>,
    #[serde(default)]
    pub timeout: NodeTimeoutMS,
    #[serde(default)]
    pub resources: NodeResources,
    #[serde(default)]
    pub retry_policy: NodeRetryPolicy,
    pub cache_key: Option<CacheKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Node {
    Router(DynamicEdgeRouter),
    Compute(ComputeFn),
}

impl Node {
    pub fn name(&self) -> &str {
        match self {
            Node::Router(router) => &router.name,
            Node::Compute(compute) => &compute.name,
        }
    }

    pub fn image_name(&self) -> &str {
        match self {
            Node::Router(router) => &router.image_information.image_name,
            Node::Compute(compute) => &compute.image_information.image_name,
        }
    }

    pub fn image_hash(&self) -> &str {
        match self {
            Node::Router(router) => &router.image_information.image_hash,
            Node::Compute(compute) => &compute.image_information.image_hash,
        }
    }

    pub fn image_uri(&self) -> Option<String> {
        match self {
            Node::Router(router) => router.image_information.image_uri.clone(),
            Node::Compute(compute) => compute.image_information.image_uri.clone(),
        }
    }

    pub fn reducer(&self) -> bool {
        match self {
            Node::Router(_) => false,
            Node::Compute(compute) => compute.reducer,
        }
    }

    pub fn secret_names(&self) -> Vec<String> {
        match self {
            Node::Router(router) => router.secret_names.clone().unwrap_or_default(),
            Node::Compute(compute) => compute.secret_names.clone().unwrap_or_default(),
        }
    }

    pub fn timeout(&self) -> NodeTimeoutMS {
        match self {
            Node::Router(router) => router.timeout.clone(),
            Node::Compute(compute) => compute.timeout.clone(),
        }
    }

    pub fn resources(&self) -> NodeResources {
        match self {
            Node::Router(router) => router.resources.clone(),
            Node::Compute(compute) => compute.resources.clone(),
        }
    }

    pub fn retry_policy(&self) -> NodeRetryPolicy {
        match self {
            Node::Router(router) => router.retry_policy.clone(),
            Node::Compute(compute) => compute.retry_policy.clone(),
        }
    }
}

impl Node {
    pub fn create_task(
        &self,
        namespace: &str,
        compute_graph_name: &str,
        invocation_id: &str,
        input_key: &str,
        reducer_output_id: Option<String>,
        graph_version: &GraphVersion,
    ) -> Result<Task> {
        let name = match self {
            Node::Router(router) => router.name.clone(),
            Node::Compute(compute) => compute.name.clone(),
        };
        let cache_key = match self {
            Node::Router(_) => None,
            Node::Compute(compute) => compute.cache_key.as_ref().and_then(|v| Some(v.clone())),
        };
        let task = TaskBuilder::default()
            .namespace(namespace.to_string())
            .compute_fn_name(name)
            .compute_graph_name(compute_graph_name.to_string())
            .invocation_id(invocation_id.to_string())
            .input_node_output_key(input_key.to_string())
            .reducer_output_id(reducer_output_id)
            .graph_version(graph_version.clone())
            .cache_key(cache_key)
            .build()?;
        Ok(task)
    }

    pub fn reducer_task(
        &self,
        namespace: &str,
        compute_graph_name: &str,
        invocation_id: &str,
        task_id: &str,
        task_output_key: &str,
    ) -> ReduceTask {
        let name = match self {
            Node::Router(router) => router.name.clone(),
            Node::Compute(compute) => compute.name.clone(),
        };
        ReduceTask {
            namespace: namespace.to_string(),
            compute_graph_name: compute_graph_name.to_string(),
            invocation_id: invocation_id.to_string(),
            compute_fn_name: name,
            task_id: task_id.to_string(),
            task_output_key: task_output_key.to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComputeGraphCode {
    pub path: String,
    pub size: u64,
    // FIXME: this is a random string right now because CloudPickle hashes are different for the
    // same code.
    pub sha256_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GraphVersion(pub String);

impl Default for GraphVersion {
    fn default() -> Self {
        Self("1".to_string())
    }
}

impl From<&str> for GraphVersion {
    fn from(item: &str) -> Self {
        GraphVersion(item.to_string())
    }
}

impl Display for GraphVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Ord, Eq, Copy)]
pub struct ImageVersion(pub u32);

impl ImageVersion {
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl Default for ImageVersion {
    fn default() -> Self {
        Self(1)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeInformation {
    pub major_version: u8,
    pub minor_version: u8,
    #[serde(default)]
    pub sdk_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComputeGraph {
    pub namespace: String,
    pub name: String,
    pub tombstoned: bool,
    pub description: String,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    #[serde(default)]
    pub replaying: bool,
    pub created_at: u64,
    // Fields below are versioned. The version field is currently managed manually by users
    pub version: GraphVersion,
    pub code: ComputeGraphCode,
    pub start_fn: Node,
    pub nodes: HashMap<String, Node>,
    pub edges: HashMap<String, Vec<String>>,
    pub runtime_information: RuntimeInformation,
}

impl ComputeGraph {
    pub fn key(&self) -> String {
        ComputeGraph::key_from(&self.namespace, &self.name)
    }

    pub fn key_from(namespace: &str, name: &str) -> String {
        format!("{}|{}", namespace, name)
    }

    pub fn key_prefix_from(namespace: &str, name: &str) -> String {
        format!("{}|{}|", namespace, name)
    }

    /// Update the compute graph from all the supplied Graph fields.
    ///
    /// Assumes validated update values.
    pub fn update(&mut self, update: ComputeGraph) {
        // immutable fields
        // self.namespace = other.namespace;
        // self.name = other.name;
        // self.created_at = other.created_at;
        // self.replaying = other.replaying;

        self.version = update.version;
        self.code = update.code;
        self.edges = update.edges;
        self.start_fn = update.start_fn;
        self.runtime_information = update.runtime_information;
        self.nodes = update.nodes.clone();
        self.description = update.description;
        self.tags = update.tags;
    }

    pub fn into_version(&self) -> ComputeGraphVersion {
        ComputeGraphVersion {
            namespace: self.namespace.clone(),
            compute_graph_name: self.name.clone(),
            created_at: self.created_at,
            version: self.version.clone(),
            code: self.code.clone(),
            start_fn: self.start_fn.clone(),
            nodes: self.nodes.clone(),
            edges: self.edges.clone(),
            runtime_information: self.runtime_information.clone(),
        }
    }
}

#[derive(Debug, strum::Display, PartialEq)]
pub enum ComputeGraphError {
    VersionExists,
}

impl Error for ComputeGraphError {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComputeGraphVersion {
    // Graph is currently versioned manually by users.
    pub namespace: String,
    pub compute_graph_name: String,
    pub created_at: u64,
    pub version: GraphVersion,
    pub code: ComputeGraphCode,
    pub start_fn: Node,
    pub nodes: HashMap<String, Node>,
    pub edges: HashMap<String, Vec<String>>,
    pub runtime_information: RuntimeInformation,
}

impl ComputeGraphVersion {
    pub fn key(&self) -> String {
        ComputeGraphVersion::key_from(&self.namespace, &self.compute_graph_name, &self.version)
    }

    pub fn key_from(namespace: &str, compute_graph_name: &str, version: &GraphVersion) -> String {
        format!("{}|{}|{}", namespace, compute_graph_name, version.0)
    }

    pub fn key_prefix_from(namespace: &str, name: &str) -> String {
        format!("{}|{}|", namespace, name)
    }

    pub fn get_compute_parent_nodes(&self, node_name: &str) -> Vec<String> {
        // Find parent of the node
        self.edges
            .iter()
            .filter(|&(_, successors)| successors.contains(&node_name.to_string()))
            .map(|(parent, _)| parent.as_str())
            // Filter for compute node parent, traversing through routers
            .flat_map(|parent_name| match self.nodes.get(parent_name) {
                Some(Node::Compute(_)) => vec![parent_name.to_string()],
                Some(Node::Router(_)) => self.get_compute_parent_nodes(parent_name),
                None => vec![],
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RouterOutput {
    pub edges: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataPayload {
    pub path: String,
    pub size: u64,
    pub sha256_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskDiagnostics {
    pub stdout: Option<DataPayload>,
    pub stderr: Option<DataPayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OutputPayload {
    Router(RouterOutput),
    Fn(DataPayload),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Builder)]
#[builder(build_fn(skip))]
pub struct NodeOutput {
    pub id: String,
    pub namespace: String,
    pub compute_graph_name: String,
    pub compute_fn_name: String,
    pub invocation_id: String,
    pub payload: OutputPayload,
    pub errors: Option<DataPayload>,
    pub reduced_state: bool,
    pub created_at: u64,
    pub encoding: String,
}

impl NodeOutput {
    pub fn key(&self, invocation_id: &str) -> String {
        NodeOutput::key_from(
            &self.namespace,
            &self.compute_graph_name,
            invocation_id,
            &self.compute_fn_name,
            &self.id,
        )
    }

    pub fn key_from(
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
        compute_fn: &str,
        id: &str,
    ) -> String {
        format!(
            "{}|{}|{}|{}|{}",
            namespace, compute_graph, invocation_id, compute_fn, id
        )
    }

    pub fn key_prefix_from(namespace: &str, compute_graph: &str, invocation_id: &str) -> String {
        format!("{}|{}|{}|", namespace, compute_graph, invocation_id)
    }
}

impl NodeOutputBuilder {
    pub fn build(&mut self) -> Result<NodeOutput> {
        let ns = self
            .namespace
            .clone()
            .ok_or(anyhow!("namespace is required"))?;
        let cg_name = self
            .compute_graph_name
            .clone()
            .ok_or(anyhow!("compute_graph_name is required"))?;
        let fn_name = self
            .compute_fn_name
            .clone()
            .ok_or(anyhow!("compute_fn_name is required"))?;
        let invocation_id = self
            .invocation_id
            .clone()
            .ok_or(anyhow!("invocation_id is required"))?;
        let encoding = self
            .encoding
            .clone()
            .unwrap_or_else(|| "application/octet-stream".to_string());
        let payload = self.payload.clone().ok_or(anyhow!("payload is required"))?;
        let reduced_state = self.reduced_state.clone().unwrap_or(false);
        let created_at: u64 = get_epoch_time_in_ms();
        let mut hasher = DefaultHasher::new();
        ns.hash(&mut hasher);
        cg_name.hash(&mut hasher);
        fn_name.hash(&mut hasher);
        invocation_id.hash(&mut hasher);
        match &payload {
            OutputPayload::Router(router) => router.edges.hash(&mut hasher),
            OutputPayload::Fn(data) => {
                data.path.hash(&mut hasher);
            }
        }
        let errors = self.errors.clone().flatten();

        let id = format!("{:x}", hasher.finish());
        Ok(NodeOutput {
            id,
            namespace: ns,
            compute_graph_name: cg_name,
            invocation_id,
            compute_fn_name: fn_name,
            payload,
            errors,
            reduced_state,
            created_at,
            encoding,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Builder)]
#[builder(build_fn(skip))]
pub struct InvocationPayload {
    pub id: String,
    pub namespace: String,
    pub compute_graph_name: String,
    pub payload: DataPayload,
    pub created_at: u64,
    pub encoding: String,
}

impl InvocationPayload {
    pub fn key(&self) -> String {
        InvocationPayload::key_from(&self.namespace, &self.compute_graph_name, &self.id)
    }

    pub fn key_from(ns: &str, cg: &str, id: &str) -> String {
        format!("{}|{}|{}", ns, cg, id)
    }
}

impl InvocationPayloadBuilder {
    pub fn build(&mut self) -> Result<InvocationPayload> {
        let ns = self
            .namespace
            .clone()
            .ok_or(anyhow!("namespace is required"))?;
        let cg_name = self
            .compute_graph_name
            .clone()
            .ok_or(anyhow!("compute_graph_name is required"))?;
        let encoding = self
            .encoding
            .clone()
            .ok_or(anyhow!("content_type is required"))?;
        let created_at: u64 = get_epoch_time_in_ms();
        let payload = self.payload.clone().ok_or(anyhow!("payload is required"))?;
        let mut hasher = DefaultHasher::new();
        ns.hash(&mut hasher);
        cg_name.hash(&mut hasher);
        payload.sha256_hash.hash(&mut hasher);
        payload.path.hash(&mut hasher);
        let id = format!("{:x}", hasher.finish());
        Ok(InvocationPayload {
            id,
            namespace: ns,
            compute_graph_name: cg_name,
            payload,
            encoding,
            created_at,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum GraphInvocationOutcome {
    Undefined,
    Success,
    Failure,
}

impl Default for GraphInvocationOutcome {
    fn default() -> Self {
        Self::Undefined
    }
}

impl From<TaskOutcome> for GraphInvocationOutcome {
    fn from(outcome: TaskOutcome) -> Self {
        match outcome {
            TaskOutcome::Success => GraphInvocationOutcome::Success,
            TaskOutcome::Failure => GraphInvocationOutcome::Failure,
            TaskOutcome::Unknown => GraphInvocationOutcome::Undefined,
        }
    }
}

impl Display for GraphInvocationOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str_val = match self {
            GraphInvocationOutcome::Success => "Success",
            GraphInvocationOutcome::Failure => "Failure",
            GraphInvocationOutcome::Undefined => "Undefined",
        };
        write!(f, "{}", str_val)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Builder)]
#[builder(build_fn(skip))]
pub struct GraphInvocationCtx {
    pub namespace: String,
    pub compute_graph_name: String,
    pub graph_version: GraphVersion,
    pub invocation_id: String,
    pub completed: bool,
    #[serde(default)]
    pub outcome: GraphInvocationOutcome,
    pub outstanding_tasks: u64,
    pub fn_task_analytics: HashMap<String, TaskAnalytics>,
    #[serde(default = "get_epoch_time_in_ms")]
    pub created_at: u64,
}

impl GraphInvocationCtx {
    pub fn create_tasks(&mut self, tasks: &Vec<Task>) {
        for task in tasks {
            let fn_name = task.compute_fn_name.clone();
            self.fn_task_analytics
                .entry(fn_name.clone())
                .or_insert_with(|| TaskAnalytics::default())
                .pending();
        }
        self.outstanding_tasks += tasks.len() as u64;
    }

    pub fn update_analytics(&mut self, task: &Task) {
        let fn_name = task.compute_fn_name.clone();
        if let Some(analytics) = self.fn_task_analytics.get_mut(&fn_name) {
            match task.outcome {
                TaskOutcome::Success => analytics.success(),
                TaskOutcome::Failure => analytics.fail(),
                _ => {
                    warn!("Task outcome shouldn't be unknown: {:?}", task)
                }
            }
        }
        self.outstanding_tasks -= 1;
    }

    pub fn complete_invocation(&mut self, force_complete: bool, outcome: GraphInvocationOutcome) {
        if self.outstanding_tasks == 0 || force_complete {
            self.completed = true;
            self.outcome = outcome;
        }
    }

    pub fn key(&self) -> String {
        format!(
            "{}|{}|{}",
            self.namespace, self.compute_graph_name, self.invocation_id
        )
    }

    pub fn secondary_index_key(&self) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(self.namespace.as_bytes());
        key.push(b'|');
        key.extend_from_slice(self.compute_graph_name.as_bytes());
        key.push(b'|');
        key.extend_from_slice(&self.created_at.to_be_bytes());
        key.push(b'|');
        key.extend_from_slice(self.invocation_id.as_bytes());
        key
    }

    pub fn get_invocation_id_from_secondary_index_key(key: &[u8]) -> Option<String> {
        key.split(|&b| b == b'|')
            .nth(3)
            .map(|s| String::from_utf8_lossy(s).into_owned())
    }

    pub fn key_from(ns: &str, cg: &str, id: &str) -> String {
        format!("{}|{}|{}", ns, cg, id)
    }

    pub fn get_task_analytics(&self, compute_fn: &str) -> Option<&TaskAnalytics> {
        self.fn_task_analytics.get(compute_fn)
    }

    pub fn key_prefix_for_compute_graph(namespace: &str, compute_graph: &str) -> String {
        format!("{}|{}|", namespace, compute_graph)
    }
}

impl GraphInvocationCtxBuilder {
    pub fn build(&mut self, compute_graph: ComputeGraph) -> Result<GraphInvocationCtx> {
        let namespace = self
            .namespace
            .clone()
            .ok_or(anyhow!("namespace is required"))?;
        let cg_name = self
            .compute_graph_name
            .clone()
            .ok_or(anyhow!("compute_graph_name is required"))?;
        let invocation_id = self
            .invocation_id
            .clone()
            .ok_or(anyhow!("ingested_data_object_id is required"))?;
        let mut fn_task_analytics = HashMap::new();
        for (fn_name, _node) in compute_graph.nodes.iter() {
            fn_task_analytics.insert(fn_name.clone(), TaskAnalytics::default());
        }
        let graph_version = self
            .graph_version
            .clone()
            .ok_or(anyhow!("graph version is required"))?;
        let created_at = self.created_at.unwrap_or_else(|| get_epoch_time_in_ms());
        Ok(GraphInvocationCtx {
            namespace,
            graph_version,
            compute_graph_name: cg_name,
            invocation_id,
            completed: false,
            outcome: GraphInvocationOutcome::Undefined,
            fn_task_analytics,
            outstanding_tasks: 0,
            created_at,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReduceTask {
    pub namespace: String,
    pub compute_graph_name: String,
    pub invocation_id: String,
    pub compute_fn_name: String,

    // The task for which we are need to create the reduce task
    pub task_id: String,
    pub task_output_key: String,
}

impl ReduceTask {
    pub fn key(&self) -> String {
        format!(
            "{}|{}|{}|{}|{}|{}",
            self.namespace,
            self.compute_graph_name,
            self.invocation_id,
            self.compute_fn_name,
            self.task_id,
            self.task_output_key,
        )
    }

    pub fn key_prefix_from(
        namespace: &str,
        compute_graph_name: &str,
        invocation_id: &str,
        compute_fn_name: &str,
    ) -> String {
        format!(
            "{}|{}|{}|{}|",
            namespace, compute_graph_name, invocation_id, compute_fn_name,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskOutputsIngestionStatus {
    /// Outputs are not ingested yet.
    Pending,
    /// Outputs were ingested.
    Ingested,
}

impl TaskOutputsIngestionStatus {
    fn pending() -> Self {
        Self::Pending
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskOutcome {
    Unknown,
    Success,
    Failure,
}

impl TaskOutcome {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Success | Self::Failure)
    }
}

impl Display for TaskOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str_val = match self {
            TaskOutcome::Success => "Success",
            TaskOutcome::Failure => "Failure",
            TaskOutcome::Unknown => "Unknown",
        };
        write!(f, "{}", str_val)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum TaskStatus {
    /// Task is waiting for execution
    Pending,
    /// Task is running
    Running,
    /// Task is completed
    Completed,
}

impl Default for TaskStatus {
    fn default() -> Self {
        Self::Completed
    }
}

impl Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str_val = match self {
            TaskStatus::Pending => "Pending",
            TaskStatus::Running => "Running",
            TaskStatus::Completed => "Completed",
        };
        write!(f, "{}", str_val)
    }
}

#[derive(Serialize, Debug, Deserialize, Clone, PartialEq, Builder)]
#[builder(build_fn(skip))]
pub struct Task {
    pub id: TaskId,
    pub namespace: String,
    pub compute_fn_name: String,
    pub compute_graph_name: String,
    pub invocation_id: String,
    pub input_node_output_key: String,
    #[serde(default = "TaskOutputsIngestionStatus::pending")]
    pub output_status: TaskOutputsIngestionStatus,
    #[serde(default)]
    pub status: TaskStatus,
    pub outcome: TaskOutcome,
    #[serde(default = "default_creation_time")]
    pub creation_time: SystemTime,
    pub creation_time_ns: u128,
    pub diagnostics: Option<TaskDiagnostics>,
    pub reducer_output_id: Option<String>,
    pub graph_version: GraphVersion,
    pub cache_key: Option<CacheKey>,
}

impl Task {
    pub fn is_terminal(&self) -> bool {
        self.status == TaskStatus::Completed || self.outcome.is_terminal()
    }

    pub fn key_prefix_for_namespace(namespace: &str) -> String {
        format!("{}|", namespace)
    }

    pub fn key_prefix_for_compute_graph(namespace: &str, compute_graph: &str) -> String {
        format!("{}|{}|", namespace, compute_graph)
    }

    pub fn key_compute_graph_version(&self) -> String {
        format!(
            "{}|{}|{}",
            self.namespace, self.compute_graph_name, self.graph_version.0,
        )
    }

    pub fn function_uri(&self) -> FunctionURI {
        FunctionURI {
            namespace: self.namespace.clone(),
            compute_graph_name: self.compute_graph_name.clone(),
            compute_fn_name: self.compute_fn_name.clone(),
            version: self.graph_version.clone(),
        }
    }

    pub fn key_prefix_for_invocation(
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
    ) -> String {
        format!("{}|{}|{}|", namespace, compute_graph, invocation_id)
    }

    pub fn key_prefix_for_fn(
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
        compute_fn_name: &str,
    ) -> String {
        format!(
            "{}|{}|{}|{}",
            namespace, compute_graph, invocation_id, compute_fn_name
        )
    }

    pub fn key(&self) -> String {
        // <namespace>_<compute_graph_name>_<invocation_id>_<fn_name>_<task_id>
        format!(
            "{}|{}|{}|{}|{}",
            self.namespace,
            self.compute_graph_name,
            self.invocation_id,
            self.compute_fn_name,
            self.id
        )
    }

    pub fn key_from(
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
        fn_name: &str,
        id: &str,
    ) -> String {
        format!(
            "{}|{}|{}|{}|{}",
            namespace, compute_graph, invocation_id, fn_name, id
        )
    }

    pub fn key_output(&self, output_id: &str) -> String {
        Task::key_output_from(&self.namespace, &self.id, output_id)
    }

    pub fn key_output_from(namespace: &str, id: &TaskId, output_id: &str) -> String {
        format!("{}|{}|{}", namespace, id, output_id)
    }

    pub fn key_output_prefix_from(namespace: &str, id: &str) -> String {
        format!("{}|{}|", namespace, id)
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Task(id: {}, compute_fn_name: {}, compute_graph_name: {}, input_key: {}, outcome: {:?})",
            self.id, self.compute_fn_name, self.compute_graph_name, self.input_node_output_key, self.outcome
        )
    }
}

impl TaskBuilder {
    pub fn build(&self) -> Result<Task> {
        let namespace = self
            .namespace
            .clone()
            .ok_or(anyhow!("namespace is not present"))?;
        let cg_name = self
            .compute_graph_name
            .clone()
            .ok_or(anyhow!("compute graph name is not present"))?;
        let compute_fn_name = self
            .compute_fn_name
            .clone()
            .ok_or(anyhow!("compute fn name is not present"))?;
        let input_key = self
            .input_node_output_key
            .clone()
            .ok_or(anyhow!("input data object id is not present"))?;
        let invocation_id = self
            .invocation_id
            .clone()
            .ok_or(anyhow!("ingestion data object id is not present"))?;
        let graph_version = self
            .graph_version
            .clone()
            .ok_or(anyhow!("graph version is not present"))?;
        let reducer_output_id = self.reducer_output_id.clone().flatten();
        let current_time = SystemTime::now();
        let duration = current_time.duration_since(UNIX_EPOCH).unwrap();
        let creation_time_ns = duration.as_nanos();
        let id = uuid::Uuid::new_v4().to_string();
        let cache_key = self.cache_key.clone().flatten();
        let task = Task {
            id: TaskId(id),
            compute_graph_name: cg_name,
            compute_fn_name,
            input_node_output_key: input_key,
            invocation_id,
            namespace,
            output_status: TaskOutputsIngestionStatus::Pending,
            status: TaskStatus::Pending,
            outcome: TaskOutcome::Unknown,
            creation_time: current_time,
            diagnostics: None,
            reducer_output_id,
            graph_version,
            creation_time_ns,
            cache_key,
        };
        Ok(task)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TaskAnalytics {
    pub pending_tasks: u64,
    pub successful_tasks: u64,
    pub failed_tasks: u64,
}

impl TaskAnalytics {
    pub fn pending(&mut self) {
        self.pending_tasks += 1;
    }

    pub fn success(&mut self) {
        self.successful_tasks += 1;
        // This is for upgrade path from older versions
        if self.pending_tasks > 0 {
            self.pending_tasks -= 1;
        }
    }

    pub fn fail(&mut self) {
        self.failed_tasks += 1;
        if self.pending_tasks > 0 {
            self.pending_tasks -= 1;
        }
    }
}

// FIXME Remove in next release
fn default_executor_ver() -> String {
    "0.2.17".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct FunctionURI {
    pub namespace: String,
    pub compute_graph_name: String,
    pub compute_fn_name: String,
    pub version: GraphVersion,
}

impl From<FunctionExecutorServerMetadata> for FunctionURI {
    fn from(fe_meta: FunctionExecutorServerMetadata) -> Self {
        FunctionURI {
            namespace: fe_meta.function_executor.namespace.clone(),
            compute_graph_name: fe_meta.function_executor.compute_graph_name.clone(),
            compute_fn_name: fe_meta.function_executor.compute_fn_name.clone(),
            version: fe_meta.function_executor.version.clone(),
        }
    }
}

impl From<Box<FunctionExecutorServerMetadata>> for FunctionURI {
    fn from(fe_meta: Box<FunctionExecutorServerMetadata>) -> Self {
        FunctionURI::from(*fe_meta)
    }
}

impl From<&Task> for FunctionURI {
    fn from(task: &Task) -> Self {
        FunctionURI {
            namespace: task.namespace.clone(),
            compute_graph_name: task.compute_graph_name.clone(),
            compute_fn_name: task.compute_fn_name.clone(),
            version: task.graph_version.clone(),
        }
    }
}

impl FunctionURI {
    pub fn matches_task(&self, task: &Task) -> bool {
        self.namespace == task.namespace &&
            self.compute_graph_name == task.compute_graph_name &&
            self.compute_fn_name == task.compute_fn_name &&
            self.version == task.graph_version
    }
}

impl Display for FunctionURI {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}",
            self.namespace, self.compute_graph_name, self.compute_fn_name, self.version
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GpuResources {
    pub count: u32,
    pub model: String,
}

impl GpuResources {
    pub fn can_handle(&self, requested_resources: &NodeGPUConfig) -> bool {
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
pub const ALL_GPU_MODELS: [&str; 6] = [
    GPU_MODEL_NVIDIA_H100_80GB,
    GPU_MODEL_NVIDIA_A100_40GB,
    GPU_MODEL_NVIDIA_A100_80GB,
    GPU_MODEL_NVIDIA_TESLA_T4,
    GPU_MODEL_NVIDIA_A6000,
    GPU_MODEL_NVIDIA_A10,
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HostResources {
    pub cpu_count: u32,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    // Not all Executors have GPUs.
    pub gpu: Option<GpuResources>,
}

impl Default for HostResources {
    fn default() -> Self {
        // There are no sensible defaults for these values.
        // If the defaults are ever used then it means that the Executor is not
        // schedulable.
        Self {
            cpu_count: 0,
            memory_bytes: 0,
            disk_bytes: 0,
            gpu: None,
        }
    }
}

impl HostResources {
    pub fn can_handle(&self, requested_resources: &NodeResources) -> bool {
        let memory_bytes = requested_resources.memory_mb as u64 * 1024 * 1024;
        let disk_bytes = requested_resources.ephemeral_disk_mb as u64 * 1024 * 1024;
        self.cpu_count >= requested_resources.cpu_ms_per_sec / 1000 &&
            self.memory_bytes >= memory_bytes &&
            self.disk_bytes >= disk_bytes &&
            self.gpu.as_ref().map_or(true, |gpu| {
                // TODO: Match functions to GPU models according to prioritized order in
                // gpu_configs.
                requested_resources
                    .gpu_configs
                    .iter()
                    .any(|g| gpu.can_handle(g))
            })
    }

    pub fn consume(&mut self, requested_resources: &NodeResources) -> Result<()> {
        let memory_bytes = requested_resources.memory_mb as u64 * 1024 * 1024;
        let disk_bytes = requested_resources.ephemeral_disk_mb as u64 * 1024 * 1024;
        if self.cpu_count < requested_resources.cpu_ms_per_sec / 1000 {
            return Err(anyhow!(
                "Not enough CPU resources, {} < {}",
                self.cpu_count,
                requested_resources.cpu_ms_per_sec / 1000
            ));
        }
        if self.memory_bytes < memory_bytes {
            return Err(anyhow!(
                "Not enough memory resources, {} < {}",
                self.memory_bytes,
                requested_resources.memory_mb * 1024 * 1024
            ));
        }
        if self.disk_bytes < disk_bytes {
            return Err(anyhow!("Not enough disk resources"));
        }
        self.cpu_count -= requested_resources.cpu_ms_per_sec / 1000;
        self.disk_bytes -= disk_bytes;
        self.memory_bytes -= memory_bytes;
        if let Some(gpu) = &mut self.gpu {
            for gpu_config in requested_resources.gpu_configs.iter() {
                if gpu.model == gpu_config.model {
                    if gpu.count < gpu_config.count {
                        return Err(anyhow!(
                            "Not enough GPU resources, {} < {}",
                            gpu.count,
                            gpu_config.count
                        ));
                    }
                    gpu.count -= gpu_config.count;
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn free(&mut self, requested_resources: &NodeResources) -> Result<()> {
        self.cpu_count += requested_resources.cpu_ms_per_sec / 1000;
        self.memory_bytes += (requested_resources.memory_mb * 1024 * 1024) as u64;
        self.disk_bytes += (requested_resources.ephemeral_disk_mb * 1024 * 1024) as u64;
        if let Some(gpu) = &mut self.gpu {
            for gpu_config in requested_resources.gpu_configs.iter() {
                if gpu.model == gpu_config.model {
                    gpu.count += gpu_config.count;
                    break;
                }
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
    Stopping,
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
pub enum FunctionExecutorState {
    #[default]
    Unknown,
    Pending,
    Running,
    Terminated,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FunctionAllowlist {
    pub namespace: Option<String>,
    pub compute_graph_name: Option<String>,
    pub compute_fn_name: Option<String>,
    pub version: Option<GraphVersion>,
}

impl FunctionAllowlist {
    pub fn matches_function_executor(&self, function_executor: &FunctionExecutor) -> bool {
        self.namespace
            .as_ref()
            .map_or(true, |ns| ns == &function_executor.namespace) &&
            self.compute_graph_name.as_ref().map_or(true, |cg_name| {
                cg_name == &function_executor.compute_graph_name
            }) &&
            self.compute_fn_name.as_ref().map_or(true, |fn_name| {
                fn_name == &function_executor.compute_fn_name
            }) &&
            self.version
                .as_ref()
                .map_or(true, |version| version == &function_executor.version)
    }

    pub fn matches_task(&self, task: &Task) -> bool {
        self.namespace
            .as_ref()
            .map_or(true, |ns| ns == &task.namespace) &&
            self.compute_graph_name
                .as_ref()
                .map_or(true, |cg_name| cg_name == &task.compute_graph_name) &&
            self.compute_fn_name
                .as_ref()
                .map_or(true, |fn_name| fn_name == &task.compute_fn_name) &&
            self.version
                .as_ref()
                .map_or(true, |version| version == &task.graph_version)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[builder(build_fn(skip))]
pub struct FunctionExecutor {
    pub id: FunctionExecutorId,
    pub namespace: String,
    pub compute_graph_name: String,
    pub compute_fn_name: String,
    pub version: GraphVersion,
    pub state: FunctionExecutorState,
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
            self.namespace, self.compute_graph_name, self.compute_fn_name, self.version,
        )
    }

    /// Checks if this FunctionExecutor matches the given FunctionURI.
    ///
    /// A match occurs when:
    /// 1. The namespace, compute_graph_name, and compute_fn_name are equal
    /// 2. Either the FunctionURI has no version specified (None), OR the
    ///    versions match
    pub fn matches_fn_uri(&self, uri: &FunctionURI) -> bool {
        // Check if namespace, compute_graph_name, and compute_fn_name match
        let basic_match = self.namespace == uri.namespace &&
            self.compute_graph_name == uri.compute_graph_name &&
            self.compute_fn_name == uri.compute_fn_name;

        // If the basic fields match, then check the version
        if basic_match {
            // If the URI version is None, it matches any version
            uri.version == self.version
        } else {
            false
        }
    }

    /// Checks if this FunctionExecutor matches the given Task.
    pub fn matches_task(&self, task: &Task) -> bool {
        self.namespace == task.namespace &&
            self.compute_graph_name == task.compute_graph_name &&
            self.compute_fn_name == task.compute_fn_name &&
            self.version == task.graph_version
    }

    /// Checks if this FunctionExecutor matches another FunctionExecutor.
    pub fn matches(&self, other: &FunctionExecutor) -> bool {
        self.namespace == other.namespace &&
            self.compute_graph_name == other.compute_graph_name &&
            self.compute_fn_name == other.compute_fn_name &&
            self.version == other.version
    }
}

impl FunctionExecutorBuilder {
    pub fn build(&mut self) -> Result<FunctionExecutor> {
        let id = self.id.clone().unwrap_or(FunctionExecutorId::default());
        let namespace = self
            .namespace
            .clone()
            .ok_or(anyhow!("namespace is required"))?;
        let compute_graph_name = self
            .compute_graph_name
            .clone()
            .ok_or(anyhow!("compute_graph_name is required"))?;
        let compute_fn_name = self
            .compute_fn_name
            .clone()
            .ok_or(anyhow!("compute_fn_name is required"))?;
        let version = self.version.clone().ok_or(anyhow!("version is required"))?;
        let state = self.state.clone().ok_or(anyhow!("state is required"))?;
        Ok(FunctionExecutor {
            id,
            namespace,
            compute_graph_name,
            compute_fn_name,
            version,
            state,
        })
    }
}

#[derive(Debug, Clone, Builder)]
#[builder(build_fn(skip))]
pub struct ExecutorServerMetadata {
    pub executor_id: ExecutorId,
    pub function_executors: im::HashMap<FunctionExecutorId, Box<FunctionExecutorServerMetadata>>,
    pub free_resources: HostResources,
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
#[builder(build_fn(skip))]
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

    /// Checks if this FunctionExecutor matches another FunctionExecutor.
    pub fn matches(&self, other: &FunctionExecutor) -> bool {
        self.function_executor.matches(other)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Builder)]
#[builder(build_fn(skip))]
pub struct ExecutorMetadata {
    pub id: ExecutorId,
    #[serde(default = "default_executor_ver")]
    pub executor_version: String,
    pub function_allowlist: Option<Vec<FunctionAllowlist>>,
    pub addr: String,
    pub labels: HashMap<String, serde_json::Value>,
    pub function_executors: HashMap<FunctionExecutorId, FunctionExecutor>,
    pub host_resources: HostResources,
    pub state: ExecutorState,
    pub tombstoned: bool,
    pub state_hash: String,
    pub clock: u64,
}

impl ExecutorMetadata {
    pub fn is_task_allowed(&self, task: &Task) -> bool {
        if let Some(function_allowlist) = &self.function_allowlist {
            function_allowlist
                .iter()
                .any(|allowlist| allowlist.matches_task(task))
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

impl ExecutorMetadataBuilder {
    pub fn build(&mut self) -> Result<ExecutorMetadata> {
        let id = self.id.clone().ok_or(anyhow!("id is required"))?;
        let executor_version = self
            .executor_version
            .clone()
            .ok_or(anyhow!("executor_version is required"))?;
        let function_allowlist = self
            .function_allowlist
            .clone()
            .ok_or(anyhow!("function_allowlist is required"))?;
        let addr = self.addr.clone().ok_or(anyhow!("addr is required"))?;
        let labels = self.labels.clone().ok_or(anyhow!("labels is required"))?;
        let function_executors = self
            .function_executors
            .clone()
            .ok_or(anyhow!("function_executors is required"))?;
        let host_resources = self
            .host_resources
            .clone()
            .ok_or(anyhow!("host_resources is required"))?;
        let state = self.state.clone().ok_or(anyhow!("state is required"))?;
        let state_hash = self
            .state_hash
            .clone()
            .ok_or(anyhow!("state_hash is required"))?;
        let tombstoned = self.tombstoned.unwrap_or(false);
        let clock = self.clock.unwrap_or(0);
        Ok(ExecutorMetadata {
            id,
            executor_version,
            function_allowlist,
            addr,
            labels,
            function_executors,
            host_resources,
            state,
            tombstoned,
            state_hash,
            clock,
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
pub struct InvokeComputeGraphEvent {
    pub invocation_id: String,
    pub namespace: String,
    pub compute_graph: String,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct TaskFinalizedEvent {
    pub namespace: String,
    pub compute_graph: String,
    pub compute_fn: String,
    pub invocation_id: String,
    pub task_id: TaskId,
    pub executor_id: ExecutorId,
}

impl fmt::Display for TaskFinalizedEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TaskFinishedEvent(namespace: {}, compute_graph: {}, compute_fn: {}, task_id: {})",
            self.namespace, self.compute_graph, self.compute_fn, self.task_id,
        )
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct TaskOutputsIngestedEvent {
    pub namespace: String,
    pub compute_graph: String,
    pub compute_fn: String,
    pub invocation_id: String,
    pub task_id: TaskId,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct TaskCreatedEvent {
    pub task: Task,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct ExecutorRemovedEvent {
    pub executor_id: ExecutorId,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct TombstoneComputeGraphEvent {
    pub namespace: String,
    pub compute_graph: String,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct TombstoneInvocationEvent {
    pub namespace: String,
    pub compute_graph: String,
    pub invocation_id: String,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct ExecutorAddedEvent {
    pub executor_id: ExecutorId,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum ChangeType {
    InvokeComputeGraph(InvokeComputeGraphEvent),
    TaskOutputsIngested(TaskOutputsIngestedEvent),
    TombstoneComputeGraph(TombstoneComputeGraphEvent),
    TombstoneInvocation(TombstoneInvocationEvent),
    ExecutorUpserted(ExecutorAddedEvent),
    TombStoneExecutor(ExecutorRemovedEvent),
    ExecutorRemoved(ExecutorRemovedEvent),
}

impl fmt::Display for ChangeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeType::InvokeComputeGraph(ev) => {
                write!(
                    f,
                    "InvokeComputeGraph ns: {}, invocation: {}, compute_graph: {}",
                    ev.namespace, ev.invocation_id, ev.compute_graph
                )
            }
            ChangeType::TaskOutputsIngested(ev) => write!(
                f,
                "TaskOutputsIngested ns: {}, invocation: {}, compute_graph: {}, task: {}",
                ev.namespace, ev.invocation_id, ev.compute_graph, ev.task_id,
            ),
            ChangeType::TombstoneComputeGraph(ev) => write!(
                f,
                "TombstoneComputeGraph ns: {}, compute_graph: {}",
                ev.namespace, ev.compute_graph
            ),
            ChangeType::ExecutorUpserted(e) => {
                write!(f, "ExecutorAdded, executor_id: {}", e.executor_id)
            }
            ChangeType::TombStoneExecutor(ev) => {
                write!(f, "TombStoneExecutor, executor_id: {}", ev.executor_id)
            }
            ChangeType::ExecutorRemoved(ev) => {
                write!(f, "ExecutorRemoved, executor_id: {}", ev.executor_id)
            }
            ChangeType::TombstoneInvocation(ev) => write!(
                f,
                "TombstoneInvocation, ns: {}, compute_graph: {}, invocation_id: {}",
                ev.namespace, ev.compute_graph, ev.invocation_id
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
    pub compute_graph: Option<String>,
    pub invocation: Option<String>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Namespace {
    pub name: String,
    pub created_at: u64,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::{
        test_objects::tests::test_compute_fn,
        ComputeGraph,
        ComputeGraphCode,
        ComputeGraphVersion,
        DynamicEdgeRouter,
        GraphVersion,
        Node,
        RuntimeInformation,
    };

    #[test]
    fn test_compute_graph_update() {
        const TEST_NAMESPACE: &str = "namespace1";
        let fn_a = test_compute_fn("fn_a", "some_hash_fn_a".to_string());
        let fn_b = test_compute_fn("fn_b", "some_hash_fn_b".to_string());
        let fn_c = test_compute_fn("fn_c", "some_hash_fn_c".to_string());
        let original_graph: ComputeGraph = ComputeGraph {
            namespace: TEST_NAMESPACE.to_string(),
            name: "graph1".to_string(),
            tombstoned: false,
            description: "description1".to_string(),
            tags: HashMap::new(),
            nodes: HashMap::from([
                ("fn_a".to_string(), Node::Compute(fn_a.clone())),
                ("fn_b".to_string(), Node::Compute(fn_b.clone())),
                ("fn_c".to_string(), Node::Compute(fn_c.clone())),
            ]),
            version: crate::GraphVersion::from("1"),
            edges: HashMap::from([(
                "fn_a".to_string(),
                vec!["fn_b".to_string(), "fn_c".to_string()],
            )]),
            code: ComputeGraphCode {
                path: "cgc_path".to_string(),
                size: 23,
                sha256_hash: "hash_code".to_string(),
            },
            created_at: 5,
            start_fn: Node::Compute(fn_a.clone()),
            runtime_information: RuntimeInformation {
                major_version: 3,
                minor_version: 10,
                sdk_version: "1.2.3".to_string(),
            },
            replaying: false,
        };

        struct TestCase {
            description: &'static str,
            update: ComputeGraph,
            expected_graph: ComputeGraph,
            expected_version: ComputeGraphVersion,
        }

        let test_cases = [
            TestCase {
                description: "no graph and version changes",
                update: original_graph.clone(),
                expected_graph: original_graph.clone(),
                expected_version: original_graph.into_version(),
            },
            TestCase {
                description: "version update",
                update: ComputeGraph {
                    version: crate::GraphVersion::from("100"),   // different
                    ..original_graph.clone()
                },
                expected_graph: ComputeGraph {
                    version: crate::GraphVersion::from("100"),
                    ..original_graph.clone()
                },
                expected_version: ComputeGraphVersion {
                    version: GraphVersion::from("100"),
                    ..original_graph.into_version()
                },
            },
            TestCase {
                description: "immutable fields should not change when version changed",
                update: ComputeGraph {
                    namespace: "namespace2".to_string(),         // different
                    name: "graph2".to_string(),                  // different
                    version: crate::GraphVersion::from("100"),   // different
                    created_at: 10,                              // different
                    replaying: true,                             // different
                    ..original_graph.clone()
                },
                expected_graph: ComputeGraph {
                    version: crate::GraphVersion::from("100"),
                    ..original_graph.clone()
                },
                expected_version:ComputeGraphVersion {
                    version: GraphVersion::from("100"),
                    ..original_graph.into_version()
                },
            },
            // Runtime information.
            TestCase {
                description: "changing runtime information with version change should change runtime information",
                update: ComputeGraph {
                    version: GraphVersion::from("2"), // different
                    runtime_information: RuntimeInformation {
                        minor_version: 12, // different
                        ..original_graph.runtime_information.clone()
                    },
                    ..original_graph.clone()
                },
                expected_graph: ComputeGraph {
                    version: GraphVersion::from("2"), // different
                    runtime_information: RuntimeInformation {
                        minor_version: 12, // different
                        ..original_graph.runtime_information.clone()
                    },
                    ..original_graph.clone()
                },
                expected_version: ComputeGraphVersion {
                    version: GraphVersion::from("2"),
                    runtime_information: RuntimeInformation {
                        minor_version: 12, // different
                        ..original_graph.runtime_information.clone()
                    },
                    ..original_graph.into_version()
                },
            },
            // Code.
            TestCase {
                description: "changing code with version change should change code",
                update: ComputeGraph {
                    version: GraphVersion::from("2"), // different
                    code: ComputeGraphCode {
                        sha256_hash: "hash_code2".to_string(), // different
                        ..original_graph.code.clone()
                    },
                    ..original_graph.clone()
                },
                expected_graph: ComputeGraph {
                    version: GraphVersion::from("2"), // different
                    code: ComputeGraphCode {
                        sha256_hash: "hash_code2".to_string(), // different
                        ..original_graph.code.clone()
                    },
                    ..original_graph.clone()
                },
                expected_version: ComputeGraphVersion {
                    version: GraphVersion::from("2"),
                    code: ComputeGraphCode {
                        sha256_hash: "hash_code2".to_string(), // different
                        ..original_graph.code.clone()
                    },
                    ..original_graph.into_version()
                },
            },
            // Edges.
            TestCase {
                description: "changing edges with version change should change edges",
                update: ComputeGraph {
                    version: GraphVersion::from("2"), // different
                    edges: HashMap::from([(
                        "fn_a".to_string(),
                        vec!["fn_c".to_string(), "fn_b".to_string()], // c and b swapped
                    )]),
                    ..original_graph.clone()
                },
                expected_graph: ComputeGraph {
                    version: GraphVersion::from("2"),
                    edges: HashMap::from([(
                        "fn_a".to_string(),
                        vec!["fn_c".to_string(), "fn_b".to_string()],
                    )]),
                    ..original_graph.clone()
                },
                expected_version: ComputeGraphVersion {
                    version: GraphVersion::from("2"),
                    edges: HashMap::from([(
                        "fn_a".to_string(),
                        vec!["fn_c".to_string(), "fn_b".to_string()],
                    )]),
                    ..original_graph.into_version()
                },
            },
            // start_fn.
            TestCase {
                description: "changing start function with version change should change start function",
                update: ComputeGraph {
                    version: GraphVersion::from("2"), // different
                    start_fn: Node::Compute(fn_b.clone()), // different
                    ..original_graph.clone()
                },
                expected_graph: ComputeGraph {
                    version: GraphVersion::from("2"),
                    start_fn: Node::Compute(fn_b.clone()),
                    ..original_graph.clone()
                },
                expected_version: ComputeGraphVersion {
                    version: GraphVersion::from("2"),
                    start_fn: Node::Compute(fn_b.clone()),
                    ..original_graph.into_version()
                },
            },
            // Adding a node.
            TestCase {
                description: "adding a node with version change should add node",
                update: ComputeGraph {
                    version: GraphVersion::from("2"), // different
                    nodes: HashMap::from([
                        ("fn_a".to_string(), Node::Compute(fn_a.clone())),
                        ("fn_b".to_string(), Node::Compute(fn_b.clone())),
                        ("fn_c".to_string(), Node::Compute(fn_c.clone())),
                        ("fn_d".to_string(), Node::Compute(test_compute_fn("fn_d", "some_hash_fn_d".to_string()))), // added
                    ]),
                    ..original_graph.clone()
                },
                expected_graph: ComputeGraph {
                    version: GraphVersion::from("2"),
                    nodes: HashMap::from([
                        ("fn_a".to_string(), Node::Compute(fn_a.clone())),
                        ("fn_b".to_string(), Node::Compute(fn_b.clone())),
                        ("fn_c".to_string(), Node::Compute(fn_c.clone())),
                        ("fn_d".to_string(), Node::Compute(test_compute_fn("fn_d", "some_hash_fn_d".to_string()))), // added
                    ]),
                    ..original_graph.clone()
                },
                expected_version: ComputeGraphVersion {
                    version: GraphVersion::from("2"),
                    nodes: HashMap::from([
                        ("fn_a".to_string(), Node::Compute(fn_a.clone())),
                        ("fn_b".to_string(), Node::Compute(fn_b.clone())),
                        ("fn_c".to_string(), Node::Compute(fn_c.clone())),
                        ("fn_d".to_string(), Node::Compute(test_compute_fn("fn_d", "some_hash_fn_d".to_string()))), // added
                    ]),
                    ..original_graph.into_version()
                },
            },
            // Removing a node.
            TestCase {
                description: "removing a node with version change should remove the node",
                update: ComputeGraph {
                    version: GraphVersion::from("2"), // different
                    nodes: HashMap::from([
                        ("fn_a".to_string(), Node::Compute(fn_a.clone())),
                        ("fn_b".to_string(), Node::Compute(fn_b.clone())),
                        // "fn_c" removed
                    ]),
                    ..original_graph.clone()
                },
                expected_graph: ComputeGraph {
                    version: GraphVersion::from("2"),
                    nodes: HashMap::from([
                        ("fn_a".to_string(), Node::Compute(fn_a.clone())),
                        ("fn_b".to_string(), Node::Compute(fn_b.clone())),
                    ]),
                    ..original_graph.clone()
                },
                expected_version: ComputeGraphVersion {
                    version: GraphVersion::from("2"),
                    nodes: HashMap::from([
                        ("fn_a".to_string(), Node::Compute(fn_a.clone())),
                        ("fn_b".to_string(), Node::Compute(fn_b.clone())),
                    ]),
                    ..original_graph.into_version()
                },
            },
            // Changing a node's image.
            TestCase {
                description: "changing a node's image with version change should update the image and version",
                update: ComputeGraph {
                    version: GraphVersion::from("2"), // different
                    nodes: HashMap::from([
                        ("fn_a".to_string(), Node::Compute(test_compute_fn("fn_a", "some_hash_fn_a_updated".to_string()))), // different
                        ("fn_b".to_string(), Node::Compute(fn_b.clone())),
                        ("fn_c".to_string(), Node::Compute(fn_c.clone())),
                    ]),
                    ..original_graph.clone()
                },
                expected_graph: ComputeGraph {
                    version: GraphVersion::from("2"),
                    nodes: HashMap::from([
                        ("fn_a".to_string(), Node::Compute(test_compute_fn("fn_a", "some_hash_fn_a_updated".to_string()))),
                        ("fn_b".to_string(), Node::Compute(fn_b.clone())),
                        ("fn_c".to_string(), Node::Compute(fn_c.clone())),
                    ]),
                    ..original_graph.clone()
                },
                expected_version: ComputeGraphVersion {
                    version: GraphVersion::from("2"),
                    nodes: HashMap::from([
                        ("fn_a".to_string(), Node::Compute(test_compute_fn("fn_a", "some_hash_fn_a_updated".to_string()))),
                        ("fn_b".to_string(), Node::Compute(fn_b.clone())),
                        ("fn_c".to_string(), Node::Compute(fn_c.clone())),
                    ]),
                    ..original_graph.into_version()
                },
            },
        ];

        for test_case in test_cases.iter() {
            let mut updated_graph = original_graph.clone();
            updated_graph.update(test_case.update.clone());
            assert_eq!(
                updated_graph, test_case.expected_graph,
                "{}",
                test_case.description
            );
            assert_eq!(
                updated_graph.into_version(),
                test_case.expected_version.clone(),
                "{}",
                test_case.description
            );
        }
    }

    // Check function pattern
    fn check_compute_parent<F>(node: &str, mut expected_parents: Vec<&str>, configure_graph: F)
    where
        F: FnOnce(&mut ComputeGraphVersion),
    {
        fn create_test_graph_version() -> ComputeGraphVersion {
            let fn_a = test_compute_fn("fn_a", "some_hash_fn_a".to_string());
            ComputeGraphVersion {
                namespace: String::new(),
                compute_graph_name: String::new(),
                created_at: 0,
                version: GraphVersion::default(),
                code: ComputeGraphCode {
                    path: String::new(),
                    size: 0,
                    sha256_hash: String::new(),
                },
                start_fn: Node::Compute(fn_a),
                nodes: HashMap::new(),
                edges: HashMap::new(),
                runtime_information: RuntimeInformation {
                    major_version: 0,
                    minor_version: 0,
                    sdk_version: "1.2.3".to_string(),
                },
            }
        }

        let mut graph_version = create_test_graph_version();
        configure_graph(&mut graph_version);

        let mut parent_nodes = graph_version.get_compute_parent_nodes(node);
        parent_nodes.sort();
        expected_parents.sort();

        assert_eq!(parent_nodes, expected_parents, "Failed for node: {}", node);
    }

    #[test]
    fn test_get_compute_parent_scenarios() {
        check_compute_parent("compute2", vec!["compute1"], |graph| {
            graph.edges = HashMap::from([("compute1".to_string(), vec!["compute2".to_string()])]);
            graph.nodes = HashMap::from([
                (
                    "compute1".to_string(),
                    Node::Compute(test_compute_fn("compute1", "image_hash".to_string())),
                ),
                (
                    "compute2".to_string(),
                    Node::Compute(test_compute_fn("compute2", "image_hash".to_string())),
                ),
            ]);
        });
        check_compute_parent("router2", vec!["compute4"], |graph| {
            graph.edges = HashMap::from([("compute4".to_string(), vec!["router2".to_string()])]);
            graph.nodes = HashMap::from([
                (
                    "compute4".to_string(),
                    Node::Compute(test_compute_fn("compute4", "image_hash".to_string())),
                ),
                (
                    "router2".to_string(),
                    Node::Router(DynamicEdgeRouter {
                        name: "router2".to_string(),
                        ..Default::default()
                    }),
                ),
            ]);
        });
        check_compute_parent("nonexistent", vec![], |_| {});

        // More complex routing scenarios
        check_compute_parent("compute2", vec!["compute1"], |graph| {
            graph.edges = HashMap::from([
                ("compute1".to_string(), vec!["router1".to_string()]),
                ("router1".to_string(), vec!["compute2".to_string()]),
            ]);
            graph.nodes = HashMap::from([
                (
                    "compute1".to_string(),
                    Node::Compute(test_compute_fn("compute1", "image_hash".to_string())),
                ),
                (
                    "router1".to_string(),
                    Node::Router(DynamicEdgeRouter {
                        name: "router1".to_string(),
                        ..Default::default()
                    }),
                ),
                (
                    "compute2".to_string(),
                    Node::Compute(test_compute_fn("compute2", "image_hash".to_string())),
                ),
            ]);
        });

        check_compute_parent("compute2", vec!["compute3"], |graph| {
            graph.edges = HashMap::from([
                ("compute3".to_string(), vec!["router1".to_string()]),
                ("router1".to_string(), vec!["compute2".to_string()]),
            ]);
            graph.nodes = HashMap::from([
                (
                    "compute3".to_string(),
                    Node::Compute(test_compute_fn("compute3", "image_hash".to_string())),
                ),
                (
                    "router1".to_string(),
                    Node::Router(DynamicEdgeRouter {
                        name: "router1".to_string(),
                        ..Default::default()
                    }),
                ),
                (
                    "compute2".to_string(),
                    Node::Compute(test_compute_fn("compute2", "image_hash".to_string())),
                ),
            ]);
        });

        check_compute_parent("compute2", vec!["compute3"], |graph| {
            graph.edges = HashMap::from([
                ("compute3".to_string(), vec!["router1".to_string()]),
                ("router1".to_string(), vec!["compute2".to_string()]),
            ]);
            graph.nodes = HashMap::from([
                (
                    "compute3".to_string(),
                    Node::Compute(test_compute_fn("compute3", "image_hash".to_string())),
                ),
                (
                    "router1".to_string(),
                    Node::Router(DynamicEdgeRouter {
                        name: "router1".to_string(),
                        ..Default::default()
                    }),
                ),
                (
                    "compute2".to_string(),
                    Node::Compute(test_compute_fn("compute2", "image_hash".to_string())),
                ),
            ]);
        });

        // test multiple parents
        check_compute_parent(
            "compute5",
            vec!["compute1", "compute2", "compute3", "compute4"],
            |graph| {
                graph.edges = HashMap::from([
                    ("compute1".to_string(), vec!["compute5".to_string()]),
                    ("compute2".to_string(), vec!["compute5".to_string()]),
                    ("compute3".to_string(), vec!["compute5".to_string()]),
                    ("compute4".to_string(), vec!["compute5".to_string()]),
                ]);
                graph.nodes = HashMap::from([
                    (
                        "compute1".to_string(),
                        Node::Compute(test_compute_fn("compute1", "image_hash".to_string())),
                    ),
                    (
                        "compute2".to_string(),
                        Node::Compute(test_compute_fn("compute1", "image_hash".to_string())),
                    ),
                    (
                        "compute3".to_string(),
                        Node::Compute(test_compute_fn("compute1", "image_hash".to_string())),
                    ),
                    (
                        "compute4".to_string(),
                        Node::Compute(test_compute_fn("compute1", "image_hash".to_string())),
                    ),
                    (
                        "compute5".to_string(),
                        Node::Compute(test_compute_fn("compute1", "image_hash".to_string())),
                    ),
                    (
                        "compute6".to_string(),
                        Node::Compute(test_compute_fn("compute1", "image_hash".to_string())),
                    ),
                ]);
            },
        );
    }
}
