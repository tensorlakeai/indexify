use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Result};
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
pub mod filter;
use std::fmt::{self, Display};

use filter::LabelsFilter;
use indexify_proto::indexify_coordinator;

pub type ExecutorId = String;
pub type TaskId = String;

fn default_creation_time() -> SystemTime {
    UNIX_EPOCH
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
    pub fn create_task(
        &self,
        namespace: &str,
        compute_graph_name: &str,
        input_id: &str,
        ingested_id: &str,
    ) -> Result<Task> {
        let name = match self {
            Node::Router(router) => router.name.clone(),
            Node::Compute(compute) => compute.name.clone(),
        };
        let task = TaskBuilder::default()
            .namespace(namespace.to_string())
            .compute_fn_name(name)
            .compute_graph_name(compute_graph_name.to_string())
            .ingested_data_id(ingested_id.to_string())
            .input_data_id(input_id.to_string())
            .build()?;
        Ok(task)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ComputeGraph {
    pub namespace: String,
    pub name: String,
    pub description: String,
    pub code_path: String,
    pub create_at: u64,
    pub tomb_stoned: bool,
    pub start_fn: Node,
    pub edges: HashMap<String, Vec<Node>>,
}

impl ComputeGraph {
    pub fn key(&self) -> String {
        format!("{}_{}", self.namespace, self.name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Builder)]
#[builder(build_fn(skip))]
pub struct DataObject {
    pub id: String,
    pub namespace: String,
    pub compute_graph_name: String,
    pub compute_fn_name: String,
    pub payload_url: String,
    pub payload_hash: [u8; 32],
}

impl DataObject {
    pub fn ingestion_object_key(&self) -> String {
        let mut hasher = DefaultHasher::new();
        self.namespace.hash(&mut hasher);
        self.compute_graph_name.hash(&mut hasher);
        self.payload_hash.hash(&mut hasher);
        self.payload_url.hash(&mut hasher);
        let id = format!("{:x}", hasher.finish());
        format!("{}_{}_{}", self.namespace, self.compute_graph_name, id)
    }

    pub fn fn_output_key(&self, ingestion_object_id: &str) -> String {
        let mut hasher = DefaultHasher::new();
        self.namespace.hash(&mut hasher);
        self.compute_graph_name.hash(&mut hasher);
        self.compute_fn_name.hash(&mut hasher);
        self.payload_hash.hash(&mut hasher);
        self.payload_url.hash(&mut hasher);
        ingestion_object_id.hash(&mut hasher);
        let id = format!("{:x}", hasher.finish());
        format!(
            "{}_{}_{}_{}_{}",
            self.namespace, self.compute_graph_name, ingestion_object_id, self.compute_fn_name, id
        )
    }
}

impl DataObjectBuilder {
    pub fn build(&mut self) -> Result<DataObject> {
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
        let payload_hash = self
            .payload_hash
            .clone()
            .ok_or(anyhow!("payload_hash is required"))?;
        let payload_url = self
            .payload_url
            .clone()
            .ok_or(anyhow!("payload_url is required"))?;
        let mut hasher = DefaultHasher::new();
        ns.hash(&mut hasher);
        cg_name.hash(&mut hasher);
        fn_name.hash(&mut hasher);
        payload_hash.hash(&mut hasher);
        payload_url.hash(&mut hasher);
        let id = format!("{:x}", hasher.finish());
        Ok(DataObject {
            id,
            namespace: ns,
            compute_graph_name: cg_name,
            compute_fn_name: fn_name,
            payload_url,
            payload_hash,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Builder)]
#[builder(build_fn(skip))]
pub struct GraphInvocationCtx {
    pub namespace: String,
    pub compute_graph_name: String,
    pub ingested_data_object_id: String,
    pub fn_task_analytics: HashMap<String, TaskAnalytics>,
}

impl GraphInvocationCtx {
    pub fn key(&self) -> String {
        format!(
            "{}_{}_{}",
            self.namespace, self.compute_graph_name, self.ingested_data_object_id
        )
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
        let ingested_data_object_id = self
            .ingested_data_object_id
            .clone()
            .ok_or(anyhow!("ingested_data_object_id is required"))?;
        Ok(GraphInvocationCtx {
            namespace,
            compute_graph_name: cg_name,
            ingested_data_object_id,
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
    pub id: String,
    pub namespace: String,
    pub compute_fn_name: String,
    pub compute_graph_name: String,
    pub ingested_data_id: String,
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
        format!(
            "{}_{}_{}_{}",
            self.namespace, self.compute_graph_name, self.compute_fn_name, self.id
        )
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
        let ingested_data_id = self
            .ingested_data_id
            .clone()
            .ok_or(anyhow!("ingestion data object id is not present"))?;
        let mut hasher = DefaultHasher::new();
        cg_name.hash(&mut hasher);
        compute_fn_name.hash(&mut hasher);
        input_data_id.hash(&mut hasher);
        ingested_data_id.hash(&mut hasher);
        namespace.hash(&mut hasher);
        let id = format!("{:x}", hasher.finish());
        let task = Task {
            id,
            compute_graph_name: cg_name,
            compute_fn_name,
            input_data_id,
            ingested_data_id,
            namespace,
            outcome: TaskOutcome::Unknown,
            creation_time: default_creation_time(),
        };
        Ok(task)
    }
}

impl TryFrom<Task> for indexify_coordinator::Task {
    type Error = anyhow::Error;

    fn try_from(value: Task) -> Result<Self> {
        Ok(indexify_coordinator::Task {
            id: value.id,
            namespace: value.namespace,
            input_data_object_id: value.input_data_id,
            compute_graph_name: value.compute_graph_name,
            compute_fn_name: value.compute_fn_name,
        })
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
    pub id: String,
    pub addr: String,
    pub labels: HashMap<String, serde_json::Value>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct InvokeComputeGraphEvent {
    pub ingested_data_id: String,
    pub namespace: String,
    pub compute_graph: String,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct TaskFinishedEvent {
    pub namespace: String,
    pub compute_graph: String,
    pub compute_fn: String,
    pub task_id: String,
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

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StateChange {
    pub id: StateChangeId,
    pub object_id: String,
    pub change_type: ChangeType,
    pub created_at: u64,
    pub processed_at: Option<u64>,
}

impl StateChange {
    pub fn new(object_id: String, change_type: ChangeType, created_at: u64) -> Self {
        Self {
            id: StateChangeId(0),
            object_id,
            change_type,
            created_at,
            processed_at: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Namespace {
    pub name: String,
    pub created_at: u64,
}
