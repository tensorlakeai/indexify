pub mod filter;
pub mod test_objects;

use std::{
    collections::HashMap,
    fmt::{self, Display},
    hash::{DefaultHasher, Hash, Hasher},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Result};
use derive_builder::Builder;
use filter::LabelsFilter;
use indexify_utils::{default_creation_time, get_epoch_time_in_ms};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use strum::AsRefStr;

// Invoke graph for all existing payloads
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemTask {
    pub namespace: String,
    pub compute_graph_name: String,
    pub graph_version: GraphVersion,
    pub waiting_for_running_invocations: bool,
    /// key for the next invocation id to process
    pub restart_key: Option<Vec<u8>>,
    /// Number of currently running invocations for this system task.
    pub num_running_invocations: usize,
}

impl SystemTask {
    pub fn new(namespace: String, compute_graph_name: String, graph_version: GraphVersion) -> Self {
        Self {
            namespace,
            compute_graph_name,
            waiting_for_running_invocations: false,
            graph_version,
            restart_key: None,
            num_running_invocations: 0,
        }
    }

    pub fn key(&self) -> String {
        SystemTask::key_from(&self.namespace, &self.compute_graph_name)
    }

    pub fn key_from(namespace: &str, compute_graph: &str) -> String {
        format!("{}|{}", namespace, compute_graph)
    }
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

    pub fn executor_key(&self) -> String {
        self.0.to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId(String);

impl TaskId {
    pub fn new(id: String) -> Self {
        Self(id)
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
        tag: String,
        base_image: String,
        run_strs: Vec<String>,
        sdk_version: Option<String>,
    ) -> Self {
        let mut image_hasher = Sha256::new();
        image_hasher.update(image_name.clone());
        image_hasher.update(base_image.clone());
        image_hasher.update(run_strs.clone().join(""));
        image_hasher.update(sdk_version.clone().unwrap_or("".to_string())); // Igh.....

        ImageInformation {
            image_name,
            tag,
            base_image,
            run_strs,
            image_hash: format!("{:x}", image_hasher.finalize()),
            image_uri: None,
            sdk_version,
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

    pub fn matches_executor(
        &self,
        executor: &ExecutorMetadata,
        diagnostic_msgs: &mut Vec<String>,
    ) -> bool {
        if executor.image_name != self.image_name() {
            diagnostic_msgs.push(format!(
                "executor {}, image name: {} does not match function image name {}. Make sure the executor is running the latest image.",
                executor.id,
                executor.image_name,
                self.image_name()
            ));
            return false;
        }

        // Empty executor image hash means that the executor can accept any image
        // version. This is needed for backwards compatibility.
        if !executor.image_hash.is_empty() && executor.image_hash != self.image_hash() {
            diagnostic_msgs.push(format!(
                "executor {}, image hash: {} does not match function image hash {}. Make sure the executor is running the latest image.",
                executor.id,
                executor.image_hash,
                self.image_hash()
            ));
            return false;
        }

        true
    }

    pub fn reducer(&self) -> bool {
        match self {
            Node::Router(_) => false,
            Node::Compute(compute) => compute.reducer,
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
        graph_version: GraphVersion,
    ) -> Result<Task> {
        let name = match self {
            Node::Router(router) => router.name.clone(),
            Node::Compute(compute) => compute.name.clone(),
        };
        let task = TaskBuilder::default()
            .namespace(namespace.to_string())
            .compute_fn_name(name)
            .compute_graph_name(compute_graph_name.to_string())
            .invocation_id(invocation_id.to_string())
            .input_node_output_key(input_key.to_string())
            .reducer_output_id(reducer_output_id)
            .graph_version(graph_version)
            .image_uri(self.image_uri())
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
    pub sha256_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Ord, Eq, Copy)]
pub struct GraphVersion(pub u32);

impl GraphVersion {
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl Default for GraphVersion {
    fn default() -> Self {
        Self(1)
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
    pub description: String,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    #[serde(default)]
    pub replaying: bool,
    pub created_at: u64,
    // Fields below are versioned
    pub version: GraphVersion, // Version incremented with code update
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

    /// Update a compute graph by computing fields and ignoring immutable
    /// fields.
    ///
    /// Assumes validated update values.
    pub fn update(&mut self, update: ComputeGraph) -> (&Self, Option<ComputeGraphVersion>) {
        // immutable fields
        // self.namespace = other.namespace;
        // self.name = other.name;
        // self.created_at = other.created_at;
        // self.replaying = other.replaying;

        self.description = update.description;
        self.tags = update.tags;

        let mut graph_version: Option<ComputeGraphVersion> = None;

        if self.code.sha256_hash != update.code.sha256_hash ||
            self.runtime_information != update.runtime_information ||
            self.edges != update.edges ||
            self.start_fn != update.start_fn ||
            self.nodes.len() != update.nodes.len() ||
            self.nodes.iter().any(|(k, v)| {
                update
                    .nodes
                    .get(k)
                    .map_or(true, |n| n.image_hash() != v.image_hash())
            })
        {
            // if the code has changed, increment the version.
            self.version = self.version.next();
            self.code = update.code;
            self.edges = update.edges;
            self.start_fn = update.start_fn;
            self.runtime_information = update.runtime_information;
            self.nodes = update.nodes.clone();

            graph_version = Some(self.into_version());
        }

        (self, graph_version)
    }

    pub fn into_version(&self) -> ComputeGraphVersion {
        ComputeGraphVersion {
            namespace: self.namespace.clone(),
            compute_graph_name: self.name.clone(),
            created_at: self.created_at,
            version: self.version,
            code: self.code.clone(),
            start_fn: self.start_fn.clone(),
            nodes: self.nodes.clone(),
            edges: self.edges.clone(),
            runtime_information: self.runtime_information.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComputeGraphVersion {
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
        ComputeGraphVersion::key_from(&self.namespace, &self.compute_graph_name, self.version)
    }

    pub fn key_from(namespace: &str, compute_graph_name: &str, version: GraphVersion) -> String {
        format!("{}|{}|{}", namespace, compute_graph_name, version.0)
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
        format!("{}|{}|{}", self.namespace, self.compute_graph_name, self.id)
    }

    pub fn key_from(ns: &str, cg: &str, id: &str) -> String {
        format!("{}|{}|{}", ns, cg, id)
    }

    pub fn invocation_context_key(&self) -> String {
        format!("{}|{}|{}", self.namespace, self.compute_graph_name, self.id)
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Builder)]
#[builder(build_fn(skip))]
pub struct GraphInvocationCtx {
    pub namespace: String,
    pub compute_graph_name: String,
    pub graph_version: GraphVersion,
    pub invocation_id: String,
    pub completed: bool,
    pub outstanding_tasks: u64,
    pub fn_task_analytics: HashMap<String, TaskAnalytics>,
    pub is_system_task: bool,
}

impl GraphInvocationCtx {
    pub fn key(&self) -> String {
        format!(
            "{}|{}|{}",
            self.namespace, self.compute_graph_name, self.invocation_id
        )
    }

    pub fn key_from(ns: &str, cg: &str, id: &str) -> String {
        format!("{}|{}|{}", ns, cg, id)
    }

    pub fn get_task_analytics(&self, compute_fn: &str) -> Option<&TaskAnalytics> {
        self.fn_task_analytics.get(compute_fn)
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
        let is_system_task = self.is_system_task.unwrap_or(false);
        Ok(GraphInvocationCtx {
            namespace,
            graph_version,
            compute_graph_name: cg_name,
            invocation_id,
            completed: false,
            fn_task_analytics,
            outstanding_tasks: 1, // Starts with 1 for the initial state change event
            is_system_task,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskOutcome {
    Unknown,
    Success,
    Failure,
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
    pub outcome: TaskOutcome,
    #[serde(default = "default_creation_time")]
    pub creation_time: SystemTime,
    pub diagnostics: Option<TaskDiagnostics>,
    pub reducer_output_id: Option<String>,
    pub graph_version: GraphVersion,
    pub image_uri: Option<String>,
}

impl Task {
    pub fn terminal_state(&self) -> bool {
        self.outcome != TaskOutcome::Unknown
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
        format!("{}|{}|{}", self.namespace, self.id, output_id)
    }

    pub fn make_allocation_key(&self, executor_id: &ExecutorId) -> String {
        let duration = self.creation_time.duration_since(UNIX_EPOCH).unwrap();
        let secs = duration.as_secs() as u128;
        let nsecs = duration.subsec_nanos() as u128;
        let nsecs = secs * 1_000_000_000 + nsecs;
        format!("{}|{}|{}", executor_id, nsecs, self.key())
    }

    pub fn key_from_allocation_key(allocation_key: &[u8]) -> Result<Vec<u8>> {
        let pos_1 = allocation_key
            .iter()
            .position(|&x| x == b'|')
            .ok_or(anyhow!("invalid executor key"))?;
        let pos_2 = allocation_key[pos_1 + 1..]
            .iter()
            .position(|&x| x == b'|')
            .ok_or(anyhow!("invalid executor key"))?;
        Ok(allocation_key[pos_1 + 1 + pos_2 + 1..].to_vec())
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
            .ok_or(anyhow!("graph version is not present"))?;
        let reducer_output_id = self.reducer_output_id.clone().flatten();
        let id = uuid::Uuid::new_v4().to_string();
        let task = Task {
            id: TaskId(id),
            compute_graph_name: cg_name,
            compute_fn_name,
            input_node_output_key: input_key,
            invocation_id,
            namespace,
            outcome: TaskOutcome::Unknown,
            creation_time: SystemTime::now(),
            diagnostics: None,
            reducer_output_id,
            graph_version,
            image_uri: self.image_uri.clone().flatten(),
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExecutorMetadata {
    pub id: ExecutorId,
    #[serde(default = "default_executor_ver")]
    pub executor_version: String,
    pub image_name: String,
    #[serde(default)]
    pub image_hash: String,
    pub addr: String,
    pub labels: HashMap<String, serde_json::Value>,
}

impl ExecutorMetadata {
    pub fn key(&self) -> String {
        self.id.executor_key()
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct InvokeComputeGraphEvent {
    pub invocation_id: String,
    pub namespace: String,
    pub compute_graph: String,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct TaskFinishedEvent {
    pub namespace: String,
    pub compute_graph: String,
    pub compute_fn: String,
    pub invocation_id: String,
    pub task_id: TaskId,
}

impl fmt::Display for TaskFinishedEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TaskFinishedEvent(namespace: {}, compute_graph: {}, compute_fn: {}, task_id: {})",
            self.namespace, self.compute_graph, self.compute_fn, self.task_id
        )
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, AsRefStr)]
pub enum ChangeType {
    InvokeComputeGraph(InvokeComputeGraphEvent),
    TaskFinished(TaskFinishedEvent),
    TombstoneIngestedData,
    TombstoneComputeGraph,
    ExecutorAdded,
    ExecutorRemoved,
    TaskCreated,
}

impl fmt::Display for ChangeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeType::InvokeComputeGraph(_) => write!(f, "InvokeComputeGraph"),
            ChangeType::TaskFinished(_) => write!(f, "TaskFinished"),
            ChangeType::TombstoneIngestedData => write!(f, "TombstoneIngestedData"),
            ChangeType::TombstoneComputeGraph => write!(f, "TombstoneComputeGraph"),
            ChangeType::ExecutorAdded => write!(f, "ExecutorAdded"),
            ChangeType::ExecutorRemoved => write!(f, "ExecutorRemoved"),
            ChangeType::TaskCreated => write!(f, "TaskCreated"),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Copy, Ord, PartialOrd)]
pub struct StateChangeId(u64);

impl StateChangeId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Return key to store in k/v db
    pub fn to_key(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    pub fn from_key(key: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(key))
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

#[derive(Clone, Serialize, Deserialize, Debug, Builder)]
pub struct StateChange {
    pub id: StateChangeId,
    pub object_id: String,
    pub change_type: ChangeType,
    pub created_at: u64,
    pub processed_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Namespace {
    pub name: String,
    pub created_at: u64,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        test_objects::tests::test_compute_fn,
        ComputeFn,
        ComputeGraph,
        ComputeGraphCode,
        ComputeGraphVersion,
        DynamicEdgeRouter,
        ExecutorMetadata,
        GraphVersion,
        ImageInformation,
        Node,
        RuntimeInformation,
    };

    #[test]
    fn test_image_hash_consistency() {
        let image_info = ImageInformation::new(
            "test".to_string(),
            "test".to_string(),
            "static_base_image".to_string(),
            vec!["pip install all_the_things".to_string()],
            Some("1.2.3".to_string()),
        );

        assert_eq!(
            image_info.image_hash,
            "229514da1c19e40fda77e8b4a4990f69ce1ec460f025f4e1367bb2219f6abea1",
            "image hash should not change"
        );
    }

    #[test]
    fn test_node_matches_executor_scenarios() {
        fn check(
            test_name: &str,
            image_name: &str,
            image_hash: &str,
            executor_image_name: &str,
            executor_image_hash: &str,
            expected: bool,
        ) {
            let executor_metadata = ExecutorMetadata {
                image_name: executor_image_name.to_string(),
                image_hash: executor_image_hash.to_string(),
                ..Default::default()
            };
            let mut diagnostic_msgs = vec![];

            let compute_fn = ComputeFn {
                name: "fn1".to_string(),
                image_information: ImageInformation {
                    image_name: image_name.to_string(),
                    image_hash: image_hash.to_string(),
                    ..Default::default()
                },
                ..Default::default()
            };

            let router = DynamicEdgeRouter {
                name: "router1".to_string(),
                image_information: ImageInformation {
                    image_name: image_name.to_string(),
                    image_hash: image_hash.to_string(),
                    ..Default::default()
                },
                ..Default::default()
            };

            print!("{:?}", executor_metadata);

            assert_eq!(
                Node::Compute(compute_fn)
                    .matches_executor(&executor_metadata, &mut diagnostic_msgs),
                expected,
                "Failed for test: {}, {}",
                test_name,
                diagnostic_msgs.join(", ")
            );

            assert_eq!(
                Node::Router(router).matches_executor(&executor_metadata, &mut diagnostic_msgs),
                expected,
                "Failed for test: {}, {}",
                test_name,
                diagnostic_msgs.join(", ")
            );
        }

        // Test case: Image name does not match
        check(
            "Image name does not match",
            "some_image_name",
            "some_image_hash",
            "some_image_name1",
            "some_image_hash",
            false,
        );

        // Test case: Image hash does not match
        check(
            "Image hash does not match",
            "some_image_name",
            "some_image_hash",
            "some_image_name",
            "different_image_hash",
            false,
        );

        // Test case: Image name and hash match
        check(
            "Image name and hash match",
            "some_image_name",
            "some_image_hash",
            "some_image_name",
            "some_image_hash",
            true,
        );

        // Test case: Executor Image hash is empty so it should match any image hash
        check(
            "Executor Image hash is empty",
            "some_image_name",
            "some_image_hash",
            "some_image_name",
            "", // empty hash
            true,
        );
    }

    #[test]
    fn test_compute_graph_update() {
        const TEST_NAMESPACE: &str = "namespace1";
        fn check<F>(graph_update: F, check_fn: impl Fn(&ComputeGraph, Option<ComputeGraphVersion>))
        where
            F: FnOnce(ComputeGraph) -> ComputeGraph,
        {
            let fn_a = test_compute_fn("fn_a", "some_hash_fn_a".to_string());
            let fn_b = test_compute_fn("fn_b", "some_hash_fn_b".to_string());
            let fn_c = test_compute_fn("fn_c", "some_hash_fn_c".to_string());

            let mut graph = ComputeGraph {
                namespace: TEST_NAMESPACE.to_string(),
                name: "graph1".to_string(),
                description: "description1".to_string(),
                tags: HashMap::new(),
                nodes: HashMap::from([
                    ("fn_a".to_string(), Node::Compute(fn_a.clone())),
                    ("fn_b".to_string(), Node::Compute(fn_b.clone())),
                    ("fn_c".to_string(), Node::Compute(fn_c.clone())),
                ]),
                version: crate::GraphVersion(1),
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

            let update = graph_update(graph.clone());
            let (update, version) = graph.update(update);
            check_fn(update, version);
        }

        // immutable fields should not change
        check(
            |graph| -> ComputeGraph {
                ComputeGraph {
                    namespace: "namespace2".to_string(), // different
                    name: "graph2".to_string(),          // different
                    version: crate::GraphVersion(100),   // different
                    created_at: 10,                      // different
                    replaying: true,                     // different
                    ..graph
                }
            },
            |graph, version| {
                assert_eq!(
                    graph.namespace, TEST_NAMESPACE,
                    "namespace should not change"
                );
                assert_eq!(graph.name, "graph1", "name should not change");
                assert_eq!(graph.version, GraphVersion(1), "should not update version");
                assert_eq!(graph.created_at, 5, "created_at should not change");
                assert!(!graph.replaying, "replaying should not change");
                assert!(version.is_none(), "version should not be updated");
            },
        );

        // updating without changing anything should not change the version
        check(
            |graph| -> ComputeGraph { graph },
            |graph, version| {
                assert_eq!(graph.version, GraphVersion(1), "should not update version");
                assert!(version.is_none(), "version should not be updated");
            },
        );

        // changing runtime information should increase the version
        check(
            |graph| -> ComputeGraph {
                ComputeGraph {
                    runtime_information: RuntimeInformation {
                        major_version: 3,
                        minor_version: 12, // updated
                        sdk_version: "1.2.3".to_string(),
                    },
                    ..graph
                }
            },
            |graph, version| {
                assert_eq!(graph.version, GraphVersion(2), "update version");
                assert_eq!(
                    graph.runtime_information.minor_version, 12,
                    "update runtime_information"
                );

                let version = version.expect("version should be created");
                assert_eq!(version.version, GraphVersion(2), "update version");
                assert_eq!(
                    version.runtime_information.minor_version, 12,
                    "version runtime_information"
                );
            },
        );

        // changing code should increase the version
        check(
            |graph| -> ComputeGraph {
                ComputeGraph {
                    code: ComputeGraphCode {
                        path: "cgc_path2".to_string(),
                        size: 23,
                        sha256_hash: "hash_code2".to_string(), // different
                    },
                    ..graph
                }
            },
            |graph, version| {
                assert_eq!(graph.version, GraphVersion(2), "update version");
                assert_eq!(graph.code.sha256_hash, "hash_code2", "update code");

                let version = version.expect("version should be created");
                assert_eq!(version.version, GraphVersion(2), "update version");
                assert_eq!(version.code.sha256_hash, "hash_code2", "version code");
            },
        );

        // changing edges should increase the version
        check(
            |graph| -> ComputeGraph {
                ComputeGraph {
                    edges: HashMap::from([(
                        "fn_a".to_string(),
                        vec!["fn_c".to_string(), "fn_b".to_string()], // c and b swapped
                    )]),
                    ..graph
                }
            },
            |graph, version| {
                assert_eq!(graph.version, GraphVersion(2), "update version");
                assert_eq!(graph.edges["fn_a"], vec!["fn_c", "fn_b"], "update edges");

                let version = version.expect("version should be created");
                assert_eq!(version.version, GraphVersion(2), "update version");
                assert_eq!(version.edges["fn_a"], vec!["fn_c", "fn_b"], "version edges");
            },
        );

        // changing start_fn should increase the version
        check(
            |graph| -> ComputeGraph {
                let fn_b = test_compute_fn("fn_b", "some_hash_fn_a".to_string());
                ComputeGraph {
                    start_fn: Node::Compute(fn_b),
                    ..graph
                }
            },
            |graph, version| {
                assert_eq!(graph.version, GraphVersion(2), "update version");
                assert_eq!(graph.start_fn.name(), "fn_b", "update start_fn");

                let version = version.expect("version should be created");
                assert_eq!(version.version, GraphVersion(2), "update version");
                assert_eq!(version.start_fn.name(), "fn_b", "version start_fn");
            },
        );

        // adding a node should increase the version
        check(
            |graph| -> ComputeGraph {
                let fn_a = test_compute_fn("fn_a", "some_hash_fn_a".to_string());
                let fn_b = test_compute_fn("fn_b", "some_hash_fn_b".to_string());
                let fn_c = test_compute_fn("fn_c", "some_hash_fn_c".to_string());
                let fn_d = test_compute_fn("fn_d", "some_hash_fn_d".to_string());
                ComputeGraph {
                    nodes: HashMap::from([
                        ("fn_a".to_string(), Node::Compute(fn_a.clone())),
                        ("fn_b".to_string(), Node::Compute(fn_b.clone())),
                        ("fn_c".to_string(), Node::Compute(fn_c.clone())),
                        ("fn_d".to_string(), Node::Compute(fn_d.clone())), // added
                    ]),
                    ..graph
                }
            },
            |graph, version| {
                assert_eq!(graph.version, GraphVersion(2), "update version");
                assert_eq!(graph.nodes.len(), 4, "update nodes");

                let version = version.expect("version should be created");
                assert_eq!(version.version, GraphVersion(2), "update version");
                assert_eq!(version.nodes.len(), 4, "version nodes");
            },
        );

        // removing a node should increase the version
        check(
            |graph| -> ComputeGraph {
                let fn_a = test_compute_fn("fn_a", "some_hash_fn_a".to_string());
                let fn_b = test_compute_fn("fn_b", "some_hash_fn_b".to_string());
                ComputeGraph {
                    nodes: HashMap::from([
                        ("fn_a".to_string(), Node::Compute(fn_a.clone())),
                        ("fn_b".to_string(), Node::Compute(fn_b.clone())),
                    ]),
                    ..graph
                }
            },
            |graph, version| {
                assert_eq!(graph.version, GraphVersion(2), "update version");
                assert_eq!(graph.nodes.len(), 2, "update nodes");

                let version = version.expect("version should be created");
                assert_eq!(version.version, GraphVersion(2), "update version");
                assert_eq!(version.nodes.len(), 2, "version nodes");
            },
        );

        // changing a node's image should increase the version
        check(
            |graph| -> ComputeGraph {
                let fn_a = test_compute_fn("fn_a", "some_hash_fn_a_updated".to_string());
                let fn_b = test_compute_fn("fn_b", "some_hash_fn_b".to_string());
                let fn_c = test_compute_fn("fn_c", "some_hash_fn_c".to_string());
                ComputeGraph {
                    nodes: HashMap::from([
                        ("fn_a".to_string(), Node::Compute(fn_a.clone())),
                        ("fn_b".to_string(), Node::Compute(fn_b.clone())),
                        ("fn_c".to_string(), Node::Compute(fn_c.clone())),
                    ]),
                    ..graph
                }
            },
            |graph, version| {
                assert_eq!(graph.version, GraphVersion(2), "update version");
                assert_eq!(graph.nodes.len(), 3, "update nodes");
                assert_eq!(
                    graph.nodes["fn_a"].image_hash(),
                    "some_hash_fn_a_updated",
                    "update node"
                );

                assert_eq!(graph.created_at, 5, "created_at should not change");

                let version = version.expect("version should be created");
                assert_eq!(version.version, GraphVersion(2), "update version");
                assert_eq!(version.nodes.len(), 3, "version nodes");
                assert_eq!(
                    version.nodes["fn_a"].image_hash(),
                    "some_hash_fn_a_updated",
                    "version node"
                );
            },
        );
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
