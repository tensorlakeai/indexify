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
use indexify_utils::default_creation_time;
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
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

#[derive(Debug, Clone, Serialize, Deserialize, Builder, PartialEq, Eq)]
pub struct DynamicEdgeRouter {
    pub name: String,
    pub description: String,
    pub source_fn: String,
    pub target_functions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComputeFn {
    pub name: String,
    pub description: String,
    pub placement_constraints: LabelsFilter,
    pub fn_name: String,
}

impl ComputeFn {
    pub fn matches_executor(&self, executor: &ExecutorMetadata) -> bool {
        self.placement_constraints.matches(&executor.labels)
    }
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

    pub fn matches_executor(&self, executor: &ExecutorMetadata) -> bool {
        match self {
            Node::Router(_) => true,
            Node::Compute(compute) => compute.matches_executor(executor),
        }
    }
}

impl Node {
    pub fn create_task(
        &self,
        namespace: &str,
        compute_graph_name: &str,
        input_id: &str,
        invocation_id: &str,
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
            .input_data_id(input_id.to_string())
            .build()?;
        Ok(task)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComputeGraphCode {
    pub path: String,
    pub size: u64,
    pub sha256_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComputeGraph {
    pub namespace: String,
    pub name: String,
    pub description: String,
    pub code: ComputeGraphCode,
    pub create_at: u64,
    pub tomb_stoned: bool,
    pub start_fn: Node,
    pub nodes: HashMap<String, Node>,
    pub edges: HashMap<String, Vec<String>>,
}

impl ComputeGraph {
    pub fn key(&self) -> String {
        format!("{}|{}", self.namespace, self.name)
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
}

impl NodeOutput {
    pub fn key(&self, invocation_id: &str) -> String {
        format!(
            "{}|{}|{}|{}|{}",
            self.namespace, self.compute_graph_name, invocation_id, self.compute_fn_name, self.id
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
        let payload = self.payload.clone().ok_or(anyhow!("payload is required"))?;
        let mut hasher = DefaultHasher::new();
        ns.hash(&mut hasher);
        cg_name.hash(&mut hasher);
        fn_name.hash(&mut hasher);
        invocation_id.hash(&mut hasher);
        match &payload {
            OutputPayload::Router(router) => router.edges.hash(&mut hasher),
            OutputPayload::Fn(data) => {
                data.sha256_hash.hash(&mut hasher);
                data.path.hash(&mut hasher);
            }
        }
        let id = format!("{:x}", hasher.finish());
        Ok(NodeOutput {
            id,
            namespace: ns,
            compute_graph_name: cg_name,
            invocation_id,
            compute_fn_name: fn_name,
            payload,
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
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Builder)]
#[builder(build_fn(skip))]
pub struct GraphInvocationCtx {
    pub namespace: String,
    pub compute_graph_name: String,
    pub invocation_id: String,
    pub completed: bool,
    pub outstanding_tasks: u16,
    pub fn_task_analytics: HashMap<String, TaskAnalytics>,
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
}

impl GraphInvocationCtxBuilder {
    pub fn build(&mut self) -> Result<GraphInvocationCtx> {
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
        Ok(GraphInvocationCtx {
            namespace,
            compute_graph_name: cg_name,
            invocation_id,
            completed: false,
            outstanding_tasks: 0,
            fn_task_analytics: HashMap::new(),
        })
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
    pub input_data_id: String,
    pub outcome: TaskOutcome,
    #[serde(default = "default_creation_time")]
    pub creation_time: SystemTime,
}

impl Task {
    pub fn terminal_state(&self) -> bool {
        self.outcome != TaskOutcome::Unknown
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

    pub fn key_output(&self, output_id: &str) -> String {
        format!("{}|{}|{}", self.namespace, self.id, output_id)
    }

    pub fn make_allocation_key(&self, executor_id: &ExecutorId) -> Vec<u8> {
        let duration = self.creation_time.duration_since(UNIX_EPOCH).unwrap();
        let secs = duration.as_secs() as u128;
        let nsecs = duration.subsec_nanos() as u128;
        let nsecs = secs * 1_000_000_000 + nsecs;
        format!("{}|{}|{}", executor_id, nsecs, self.key(),).into()
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
            "Task(id: {}, compute_fn_name: {}, compute_graph_name: {}, content_id: {}, outcome: {:?})",
            self.id, self.compute_fn_name, self.compute_graph_name, self.input_data_id, self.outcome
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
        let input_data_id = self
            .input_data_id
            .clone()
            .ok_or(anyhow!("input data object id is not present"))?;
        let invocation_id = self
            .invocation_id
            .clone()
            .ok_or(anyhow!("ingestion data object id is not present"))?;
        let mut hasher = DefaultHasher::new();
        cg_name.hash(&mut hasher);
        compute_fn_name.hash(&mut hasher);
        input_data_id.hash(&mut hasher);
        invocation_id.hash(&mut hasher);
        namespace.hash(&mut hasher);
        let id = format!("{:x}", hasher.finish());
        let task = Task {
            id: TaskId(id),
            compute_graph_name: cg_name,
            compute_fn_name,
            input_data_id,
            invocation_id,
            namespace,
            outcome: TaskOutcome::Unknown,
            creation_time: SystemTime::now(),
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExecutorMetadata {
    pub id: ExecutorId,
    pub runner_name: String,
    pub addr: String,
    pub labels: HashMap<String, serde_json::Value>,
}

impl ExecutorMetadata {
    pub fn key(&self) -> String {
        format!("{}", self.id)
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

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
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
