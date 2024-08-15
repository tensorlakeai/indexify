use core::fmt;
use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, BTreeSet, HashMap, HashSet, VecDeque},
    sync::{Arc, RwLock},
    time::SystemTime,
};

use anyhow::{anyhow, Result};
use indexify_internal_api::{
    self as internal_api,
    ContentOffset,
    ExtractionGraphNode,
    ServerTaskType,
    TaskAnalytics,
};
use internal_api::{
    v1,
    ContentMetadata,
    ContentMetadataId,
    ContentSource,
    ExecutorMetadata,
    ExtractionGraph,
    ExtractionGraphLink,
    ExtractionPolicy,
    ExtractionPolicyName,
    ExtractorDescription,
    Index,
    StateChange,
    StructuredDataSchema,
    Task,
    TaskOutcome,
};
use itertools::Itertools;
use opentelemetry::metrics::AsyncInstrument;
use rocksdb::{IteratorMode, OptimisticTransactionDB, Transaction};
use serde::de::DeserializeOwned;
use tokio::sync::broadcast;
use tracing::warn;

use super::{
    requests::{RequestPayload, StateChangeProcessed, StateMachineUpdateRequest},
    serializer::JsonEncode,
    ExecutorId,
    ExtractionGraphId,
    ExtractionPolicyId,
    ExtractorName,
    JsonEncoder,
    NamespaceName,
    SchemaId,
    StateChangeId,
    StateMachineColumns,
    StateMachineError,
    TaskId,
};
use crate::state::{store::get_from_cf, NodeId};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct UnassignedTasks {
    unassigned_tasks: Arc<RwLock<HashMap<TaskId, SystemTime>>>,
}

impl UnassignedTasks {
    pub fn insert(&self, task_id: &TaskId, creation_time: SystemTime) {
        let mut guard = self.unassigned_tasks.write().unwrap();
        guard.insert(task_id.into(), creation_time);
    }

    pub fn remove(&self, task_id: &TaskId) -> Option<UnfinishedTask> {
        let mut guard = self.unassigned_tasks.write().unwrap();
        guard.remove(task_id).map(|creation_time| UnfinishedTask {
            id: task_id.clone(),
            creation_time,
        })
    }

    pub fn inner(&self) -> HashMap<TaskId, SystemTime> {
        let guard = self.unassigned_tasks.read().unwrap();
        guard.clone()
    }

    pub fn set(&self, tasks: HashMap<TaskId, SystemTime>) {
        let mut guard = self.unassigned_tasks.write().unwrap();
        *guard = tasks;
    }

    pub fn count(&self) -> usize {
        let guard = self.unassigned_tasks.read().unwrap();
        guard.len()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct UnprocessedStateChanges {
    unprocessed_state_changes: Arc<RwLock<HashSet<StateChangeId>>>,
}

impl UnprocessedStateChanges {
    pub fn insert(&self, state_change_id: StateChangeId) {
        let mut guard = self.unprocessed_state_changes.write().unwrap();
        guard.insert(state_change_id);
    }

    pub fn remove(&self, state_change_id: &StateChangeId) {
        let mut guard = self.unprocessed_state_changes.write().unwrap();
        guard.remove(state_change_id);
    }

    pub fn inner(&self) -> HashSet<StateChangeId> {
        let guard = self.unprocessed_state_changes.read().unwrap();
        guard.clone()
    }
}

impl From<HashSet<StateChangeId>> for UnprocessedStateChanges {
    fn from(state_changes: HashSet<StateChangeId>) -> Self {
        let unprocessed_state_changes = Arc::new(RwLock::new(state_changes));
        Self {
            unprocessed_state_changes,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct ContentNamespaceTable {
    content_namespace_table: Arc<RwLock<HashMap<NamespaceName, HashSet<ContentMetadataId>>>>,
}

impl ContentNamespaceTable {
    pub fn insert(&self, namespace: &NamespaceName, content_id: &ContentMetadataId) {
        let mut guard = self.content_namespace_table.write().unwrap();
        guard
            .entry(namespace.clone())
            .or_default()
            .insert(content_id.clone());
    }

    pub fn remove(&self, namespace: &NamespaceName, content_id: &ContentMetadataId) {
        let mut guard = self.content_namespace_table.write().unwrap();
        guard
            .entry(namespace.clone())
            .or_default()
            .remove(content_id);
    }

    pub fn inner(&self) -> HashMap<NamespaceName, HashSet<ContentMetadataId>> {
        let guard = self.content_namespace_table.read().unwrap();
        guard.clone()
    }
}

impl From<HashMap<NamespaceName, HashSet<ContentMetadataId>>> for ContentNamespaceTable {
    fn from(content_namespace_table: HashMap<NamespaceName, HashSet<ContentMetadataId>>) -> Self {
        let content_namespace_table = Arc::new(RwLock::new(content_namespace_table));
        Self {
            content_namespace_table,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct ExtractorExecutorsTable {
    extractor_executors_table: Arc<RwLock<HashMap<ExtractorName, HashSet<ExecutorId>>>>,
}

impl ExtractorExecutorsTable {
    pub fn insert(&self, extractor: &ExtractorName, executor_id: &ExecutorId) {
        let mut guard = self.extractor_executors_table.write().unwrap();
        guard
            .entry(extractor.clone())
            .or_default()
            .insert(executor_id.clone());
    }

    pub fn remove(&self, extractor: &ExtractorName, executor_id: &ExecutorId) {
        let mut guard = self.extractor_executors_table.write().unwrap();
        guard
            .entry(extractor.clone())
            .or_default()
            .remove(executor_id);
    }

    pub fn inner(&self) -> HashMap<ExtractorName, HashSet<ExecutorId>> {
        let guard = self.extractor_executors_table.read().unwrap();
        guard.clone()
    }
}

impl From<HashMap<ExtractorName, HashSet<ExecutorId>>> for ExtractorExecutorsTable {
    fn from(extractor_executors_table: HashMap<ExtractorName, HashSet<ExecutorId>>) -> Self {
        let extractor_executors_table = Arc::new(RwLock::new(extractor_executors_table));
        Self {
            extractor_executors_table,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct NamespaceIndexTable {
    namespace_index_table: Arc<RwLock<HashMap<NamespaceName, HashSet<String>>>>,
}

impl NamespaceIndexTable {
    pub fn insert(&self, namespace: &NamespaceName, index_id: &str) {
        let mut guard = self.namespace_index_table.write().unwrap();
        guard
            .entry(namespace.clone())
            .or_default()
            .insert(index_id.to_owned());
    }

    pub fn remove(&self, namespace: &NamespaceName, index_id: &String) {
        let mut guard = self.namespace_index_table.write().unwrap();
        guard.entry(namespace.clone()).or_default().remove(index_id);
    }

    pub fn inner(&self) -> HashMap<NamespaceName, HashSet<String>> {
        let guard = self.namespace_index_table.read().unwrap();
        guard.clone()
    }
}

impl From<HashMap<NamespaceName, HashSet<String>>> for NamespaceIndexTable {
    fn from(namespace_index_table: HashMap<NamespaceName, HashSet<String>>) -> Self {
        let namespace_index_table = Arc::new(RwLock::new(namespace_index_table));
        Self {
            namespace_index_table,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct UnfinishedTasksByExtractor {
    unfinished_tasks_by_extractor: Arc<RwLock<HashMap<ExtractorName, HashSet<TaskId>>>>,
}

impl UnfinishedTasksByExtractor {
    pub fn insert(&self, extractor: &ExtractorName, task_id: &TaskId) {
        let mut guard = self.unfinished_tasks_by_extractor.write().unwrap();
        guard
            .entry(extractor.clone())
            .or_default()
            .insert(task_id.clone());
    }

    pub fn remove(&self, extractor: &ExtractorName, task_id: &TaskId) {
        let mut guard = self.unfinished_tasks_by_extractor.write().unwrap();
        guard.entry(extractor.clone()).or_default().remove(task_id);
    }

    pub fn inner(&self) -> HashMap<ExtractorName, HashSet<TaskId>> {
        let guard = self.unfinished_tasks_by_extractor.read().unwrap();
        guard.clone()
    }

    pub fn observe_task_counts(&self, observer: &dyn AsyncInstrument<u64>) {
        let guard = self.unfinished_tasks_by_extractor.read().unwrap();
        for (extractor, tasks) in guard.iter() {
            observer.observe(
                tasks.len() as u64,
                &[opentelemetry::KeyValue::new(
                    "extractor",
                    extractor.to_string(),
                )],
            );
        }
    }
}

impl From<HashMap<ExtractorName, HashSet<TaskId>>> for UnfinishedTasksByExtractor {
    fn from(unfinished_tasks_by_extractor: HashMap<ExtractorName, HashSet<TaskId>>) -> Self {
        let unfinished_tasks_by_extractor = Arc::new(RwLock::new(unfinished_tasks_by_extractor));
        Self {
            unfinished_tasks_by_extractor,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct ExecutorRunningTaskCount {
    executor_running_task_count: Arc<RwLock<HashMap<ExecutorId, u64>>>,
}

impl ExecutorRunningTaskCount {
    pub fn new() -> Self {
        Self {
            executor_running_task_count: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get(&self, executor_id: &ExecutorId) -> Option<u64> {
        let guard = self.executor_running_task_count.read().unwrap();
        guard.get(executor_id).copied()
    }

    pub fn insert(&self, executor_id: &ExecutorId, count: u64) {
        let mut guard = self.executor_running_task_count.write().unwrap();
        guard.insert(executor_id.clone(), count);
    }

    pub fn remove(&self, executor_id: &ExecutorId) {
        let mut guard = self.executor_running_task_count.write().unwrap();
        guard.remove(executor_id);
    }

    pub fn inner(&self) -> HashMap<ExecutorId, u64> {
        let guard = self.executor_running_task_count.read().unwrap();
        guard.clone()
    }

    pub fn increment_running_task_count(&self, executor_id: &ExecutorId) {
        let mut executor_load = self.executor_running_task_count.write().unwrap();
        let load = executor_load.entry(executor_id.clone()).or_insert(0);
        *load += 1;
    }

    pub fn decrement_running_task_count(&self, executor_id: &ExecutorId) {
        let mut executor_load = self.executor_running_task_count.write().unwrap();
        if let Some(load) = executor_load.get_mut(executor_id) {
            if *load > 0 {
                *load -= 1;
            } else {
                warn!("Tried to decrement load below 0. This is a bug because the state machine shouldn't allow it.");
            }
        } else {
            // Add the executor to the load map if it's not there, with an initial load of
            // 0.
            executor_load.insert(executor_id.clone(), 0);
        }
    }

    pub fn executor_count(&self) -> usize {
        let guard = self.executor_running_task_count.read().unwrap();
        guard.len()
    }
}

impl From<HashMap<ExecutorId, u64>> for ExecutorRunningTaskCount {
    fn from(executor_running_task_count: HashMap<ExecutorId, u64>) -> Self {
        let executor_running_task_count = Arc::new(RwLock::new(executor_running_task_count));
        Self {
            executor_running_task_count,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct ExtractionGraphTable {
    eg_by_namespace: Arc<RwLock<HashMap<NamespaceName, HashSet<ExtractionGraphId>>>>,
}

impl ExtractionGraphTable {
    pub fn get(&self, namespace: &NamespaceName) -> HashSet<ExtractionGraphId> {
        let guard = self.eg_by_namespace.read().unwrap();
        guard.get(namespace).cloned().unwrap_or_default()
    }

    pub fn insert(&self, namespace: &NamespaceName, id: &ExtractionGraphId) {
        let mut guard = self.eg_by_namespace.write().unwrap();
        guard
            .entry(namespace.clone())
            .or_default()
            .insert(id.clone());
    }

    pub fn remove(&self, namespace: &NamespaceName, id: &SchemaId) {
        let mut guard = self.eg_by_namespace.write().unwrap();
        guard.entry(namespace.clone()).or_default().remove(id);
    }

    pub fn inner(&self) -> HashMap<NamespaceName, HashSet<SchemaId>> {
        let guard = self.eg_by_namespace.read().unwrap();
        guard.clone()
    }
}

impl From<HashMap<NamespaceName, HashSet<ExtractionGraphId>>> for ExtractionGraphTable {
    fn from(eg_by_namespace: HashMap<NamespaceName, HashSet<SchemaId>>) -> Self {
        let eg_by_namespace = Arc::new(RwLock::new(eg_by_namespace));
        Self { eg_by_namespace }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct SchemasByNamespace {
    schemas_by_namespace: Arc<RwLock<HashMap<NamespaceName, HashSet<SchemaId>>>>,
}

impl SchemasByNamespace {
    pub fn insert(&self, namespace: &NamespaceName, schema_id: &SchemaId) {
        let mut guard = self.schemas_by_namespace.write().unwrap();
        guard
            .entry(namespace.clone())
            .or_default()
            .insert(schema_id.clone());
    }

    pub fn remove(&self, namespace: &NamespaceName, schema_id: &SchemaId) {
        let mut guard = self.schemas_by_namespace.write().unwrap();
        guard
            .entry(namespace.clone())
            .or_default()
            .remove(schema_id);
    }

    pub fn inner(&self) -> HashMap<NamespaceName, HashSet<SchemaId>> {
        let guard = self.schemas_by_namespace.read().unwrap();
        guard.clone()
    }
}

impl From<HashMap<NamespaceName, HashSet<SchemaId>>> for SchemasByNamespace {
    fn from(schemas_by_namespace: HashMap<NamespaceName, HashSet<SchemaId>>) -> Self {
        let schemas_by_namespace = Arc::new(RwLock::new(schemas_by_namespace));
        Self {
            schemas_by_namespace,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct ContentChildrenTable {
    content_children_table: Arc<RwLock<HashMap<ContentMetadataId, HashSet<ContentMetadataId>>>>,
}

impl ContentChildrenTable {
    pub fn insert(&self, parent_id: &ContentMetadataId, child_id: &ContentMetadataId) {
        let mut guard = self.content_children_table.write().unwrap();
        guard
            .entry(parent_id.clone())
            .or_default()
            .insert(child_id.clone());
    }

    pub fn remove(&self, parent_id: &ContentMetadataId, child_id: &ContentMetadataId) {
        let mut guard = self.content_children_table.write().unwrap();
        if let Some(children) = guard.get_mut(parent_id) {
            children.remove(child_id);
            if children.is_empty() {
                guard.remove(parent_id);
            }
        }
    }

    pub fn remove_all(&self, parent_id: &ContentMetadataId) {
        let mut guard = self.content_children_table.write().unwrap();
        guard.remove(parent_id);
    }

    pub fn get_children(&self, parent_id: &ContentMetadataId) -> HashSet<ContentMetadataId> {
        let guard = self.content_children_table.read().unwrap();
        guard.get(parent_id).cloned().unwrap_or_default()
    }

    pub fn replace_parent(
        &self,
        old_parent_id: &ContentMetadataId,
        new_parent_id: &ContentMetadataId,
    ) {
        let mut guard = self.content_children_table.write().unwrap();
        let children = guard.remove(old_parent_id).unwrap_or_default();
        guard.insert(new_parent_id.clone(), children);
    }

    pub fn inner(&self) -> HashMap<ContentMetadataId, HashSet<ContentMetadataId>> {
        let guard = self.content_children_table.read().unwrap();
        guard.clone()
    }
}

impl From<HashMap<ContentMetadataId, HashSet<ContentMetadataId>>> for ContentChildrenTable {
    fn from(
        content_children_table: HashMap<ContentMetadataId, HashSet<ContentMetadataId>>,
    ) -> Self {
        let content_children_table = Arc::new(RwLock::new(content_children_table));
        Self {
            content_children_table,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct Metrics {
    /// Number of tasks total
    pub tasks_completed: u64,

    /// Number of tasks completed with errors
    pub tasks_completed_with_errors: u64,

    /// Number of contents uploaded
    pub content_uploads: u64,

    /// Total number of bytes in uploaded contents
    pub content_bytes: u64,

    /// Number of contents extracted
    pub content_extracted: u64,

    /// Total number of bytes in extracted contents
    pub content_extracted_bytes: u64,
}

impl Metrics {
    pub fn update_task_completion(&mut self, outcome: TaskOutcome) {
        match outcome {
            TaskOutcome::Success => self.tasks_completed += 1,
            TaskOutcome::Failed => self.tasks_completed_with_errors += 1,
            _ => (),
        }
    }
}

#[derive(Debug, Default)]
struct TaskCount {
    count: u64,
    notify: Option<broadcast::Sender<()>>,
}

#[derive(Debug, Clone)]
pub struct UnfinishedTask {
    pub id: TaskId,
    pub creation_time: SystemTime,
}

impl PartialEq for UnfinishedTask {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for UnfinishedTask {}

impl PartialOrd for UnfinishedTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Sort unfinished tasks by creation time so we can process them in order
impl Ord for UnfinishedTask {
    fn cmp(&self, other: &Self) -> Ordering {
        self.creation_time
            .cmp(&other.creation_time)
            .then_with(|| self.id.cmp(&other.id))
    }
}

fn max_content_offset(db: &OptimisticTransactionDB) -> Result<u64, StateMachineError> {
    let mut iter = db.iterator_cf(
        StateMachineColumns::ChangeIdContentIndex.cf(db),
        IteratorMode::End,
    );
    match iter.next() {
        Some(Ok((key, _))) => {
            let key_bytes: Result<[u8; 8], _> = key.as_ref().try_into();
            match key_bytes {
                Ok(bytes) => Ok(u64::from_be_bytes(bytes) + 1),
                Err(e) => Err(StateMachineError::DatabaseError(format!(
                    "Failed to decode key: {}",
                    e
                ))),
            }
        }
        Some(Err(e)) => Err(StateMachineError::DatabaseError(format!(
            "Failed to read content index: {}",
            e
        ))),
        None => Ok(1),
    }
}

#[derive(Debug)]
pub struct ExecutorUnfinishedTasks {
    pub tasks: BTreeSet<UnfinishedTask>,
    pub new_task_channel: broadcast::Sender<()>,
}

impl ExecutorUnfinishedTasks {
    pub fn new() -> Self {
        let (new_task_channel, _) = broadcast::channel(1);
        Self {
            tasks: BTreeSet::new(),
            new_task_channel,
        }
    }

    pub fn insert(&mut self, task: UnfinishedTask) {
        self.tasks.insert(task);
        let _ = self.new_task_channel.send(());
    }

    // Send notification for remove because new task can be available if
    // were at maximum number of tasks before task completion.
    pub fn remove(&mut self, task: &UnfinishedTask) {
        self.tasks.remove(task);
        let _ = self.new_task_channel.send(());
    }

    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.new_task_channel.subscribe()
    }
}

impl Default for ExecutorUnfinishedTasks {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(thiserror::Error, Debug)]
pub struct IndexifyState {
    // Reverse Indexes
    /// The tasks that are currently unassigned
    pub unassigned_tasks: UnassignedTasks,

    /// State changes that have not been processed yet
    pub unprocessed_state_changes: UnprocessedStateChanges,

    /// Extractor -> Executors table
    pub extractor_executors_table: ExtractorExecutorsTable,

    /// Namespace -> Index id
    pub namespace_index_table: NamespaceIndexTable,

    /// Tasks that are currently unfinished, by extractor. Once they are
    /// finished, they are removed from this set.
    /// Extractor name -> Task Ids
    pub unfinished_tasks_by_extractor: UnfinishedTasksByExtractor,

    /// Pending tasks per executor, sorted by creation time
    pub unfinished_tasks_by_executor: Arc<RwLock<HashMap<ExecutorId, ExecutorUnfinishedTasks>>>,

    /// Namespace -> Schemas
    pub schemas_by_namespace: SchemasByNamespace,

    /// Parent content id -> children content id's
    pub content_children_table: ContentChildrenTable,

    /// Number of tasks pending for root content
    root_task_counts: RwLock<HashMap<String, TaskCount>>,

    /// Metrics
    pub metrics: std::sync::Mutex<Metrics>,

    /// Namespace -> Extraction Graph ID
    extraction_graphs_by_ns: ExtractionGraphTable,

    /// Next change id
    pub change_id: std::sync::Mutex<u64>,

    /// Graph+ContentSource->Policy Id
    pub graph_links: Arc<RwLock<HashMap<ExtractionGraphNode, HashSet<ExtractionPolicyId>>>>,

    pub new_content_channel: broadcast::Sender<()>,

    /// Next content offset
    pub content_offset: std::sync::Mutex<ContentOffset>,
}

impl fmt::Display for IndexifyState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IndexifyState {{ unassigned_tasks: {:?}, unprocessed_state_changes: {:?}, extractor_executors_table: {:?}, namespace_index_table: {:?}, unfinished_tasks_by_extractor: {:?}, unfinished_tasks_by_executor: {:?}, schemas_by_namespace: {:?} }}, content_children_table: {:?}",
            self.unassigned_tasks,
            self.unprocessed_state_changes,
            self.extractor_executors_table,
            self.namespace_index_table,
            self.unfinished_tasks_by_extractor,
            self.unfinished_tasks_by_executor,
            self.schemas_by_namespace,
            self.content_children_table,
        )
    }
}

impl Default for IndexifyState {
    fn default() -> Self {
        let new_content_channel = broadcast::channel(1).0;
        Self {
            unassigned_tasks: Default::default(),
            unprocessed_state_changes: Default::default(),
            extractor_executors_table: Default::default(),
            namespace_index_table: Default::default(),
            unfinished_tasks_by_extractor: Default::default(),
            unfinished_tasks_by_executor: Default::default(),
            schemas_by_namespace: Default::default(),
            content_children_table: Default::default(),
            root_task_counts: Default::default(),
            metrics: Default::default(),
            extraction_graphs_by_ns: Default::default(),
            change_id: Default::default(),
            graph_links: Default::default(),
            new_content_channel,
            content_offset: std::sync::Mutex::new(ContentOffset(1)),
        }
    }
}

impl IndexifyState {
    pub fn next_content_offset(&self) -> ContentOffset {
        let mut offset_guard = self.content_offset.lock().unwrap();
        let offset = *offset_guard;
        *offset_guard = offset.next();
        offset
    }

    // Complete value of link is stored in db key since multiple graphs
    // can be linked to the same node.
    fn set_extraction_graph_link(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        link: &ExtractionGraphLink,
    ) -> Result<(), StateMachineError> {
        let key = JsonEncoder::encode(link)?;
        txn.put_cf(&StateMachineColumns::ExtractionGraphLinks.cf(db), key, [])
            .map_err(|e| StateMachineError::DatabaseError(e.to_string()))
    }

    fn set_extraction_graph(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        extraction_graph: &ExtractionGraph,
        structured_data_schema: &StructuredDataSchema,
    ) -> Result<(), StateMachineError> {
        let serialized_eg = JsonEncoder::encode(extraction_graph)?;
        txn.put_cf(
            &StateMachineColumns::ExtractionGraphs.cf(db),
            &extraction_graph.id,
            serialized_eg,
        )
        .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
        for ep in extraction_graph.extraction_policies.to_owned() {
            self.set_extraction_policy(db, txn, &ep)?;
        }
        self.set_schema(db, txn, structured_data_schema)?;
        Ok(())
    }

    fn set_new_state_changes(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        state_changes: &mut Vec<StateChange>,
    ) -> Result<(), StateMachineError> {
        let mut change_id = self.get_next_change_ids(state_changes.len());
        for change in state_changes {
            if let Some(refcnt_object_id) = change.refcnt_object_id.as_ref() {
                self.inc_root_ref_count(refcnt_object_id);
            }
            change.id = StateChangeId::new(change_id);
            change_id += 1;
            let serialized_change = JsonEncoder::encode(&change)?;
            txn.put_cf(
                StateMachineColumns::StateChanges.cf(db),
                change.id.to_key(),
                &serialized_change,
            )
            .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
        }
        Ok(())
    }

    fn set_processed_state_changes(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        state_changes: &Vec<StateChangeProcessed>,
    ) -> Result<Vec<StateChange>, StateMachineError> {
        let state_changes_cf = StateMachineColumns::StateChanges.cf(db);
        let mut changes = Vec::new();

        for change in state_changes {
            let key = change.state_change_id.to_key();
            let result = txn
                .get_cf(state_changes_cf, key)
                .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
            let result = result.ok_or_else(|| {
                StateMachineError::DatabaseError(format!(
                    "State change {:?} not found",
                    change.state_change_id
                ))
            })?;

            let mut state_change = JsonEncoder::decode::<StateChange>(&result)?;
            state_change.processed_at = Some(change.processed_at);
            let serialized_change = JsonEncoder::encode(&state_change)?;
            txn.put_cf(state_changes_cf, key, &serialized_change)
                .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
            changes.push(state_change);
        }
        Ok(changes)
    }

    fn set_index(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        index: &Index,
        id: &String,
    ) -> Result<(), StateMachineError> {
        let serialized_index = JsonEncoder::encode(index)?;
        txn.put_cf(StateMachineColumns::IndexTable.cf(db), id, serialized_index)
            .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    fn update_tasks(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        tasks: Vec<Task>,
        update_time: SystemTime,
    ) -> Result<(), StateMachineError> {
        for task in tasks {
            let serialized_task = JsonEncoder::encode(&task)?;
            let prev_task: Option<Task> = get_from_cf(db, StateMachineColumns::Tasks, &task.id)
                .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
            txn.put_cf(
                StateMachineColumns::Tasks.cf(db),
                task.id.clone(),
                &serialized_task,
            )
            .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;

            // Mark the completion time if the task is terminal
            // If the task is newly created, we need to add an entry to the content metadata
            // that a new task was created for that content.
            if task.terminal_state() || update_time == SystemTime::UNIX_EPOCH {
                let extraction_policy_id = ExtractionPolicy::create_id(
                    &task.extraction_graph_name,
                    &task.extraction_policy_name,
                    &task.namespace,
                );
                self.update_content_extraction_policy_state(
                    db,
                    txn,
                    &task.content_metadata.id,
                    &extraction_policy_id,
                    update_time,
                )?;
            }

            self.update_task_analytics(db, txn, prev_task, &task)
                .map_err(|e| {
                    StateMachineError::DatabaseError(format!(
                        "Error updating task analytics for task {}: {}",
                        task.id, e
                    ))
                })?;
        }
        Ok(())
    }

    fn update_task_analytics(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        prev_task: Option<Task>,
        new_task: &Task,
    ) -> Result<(), StateMachineError> {
        let key = format!(
            "{}_{}_{}",
            new_task.namespace, new_task.extraction_graph_name, new_task.extraction_policy_name
        );
        let task_analytics = db
            .get_cf(StateMachineColumns::TaskAnalytics.cf(db), key.clone())
            .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
        let mut task_analytics: TaskAnalytics = task_analytics
            .map(|db_vec| {
                JsonEncoder::decode(&db_vec)
                    .map_err(|e| StateMachineError::DatabaseError(e.to_string()))
            })
            .unwrap_or_else(|| Ok(TaskAnalytics::default()))?;
        match prev_task {
            Some(prev_task) => {
                if !prev_task.terminal_state() && new_task.terminal_state() {
                    if new_task.outcome == TaskOutcome::Success {
                        task_analytics.success();
                    } else {
                        task_analytics.fail();
                    }
                }
            }
            None => {
                task_analytics.pending();
            }
        }

        let serialized_task_analytics = JsonEncoder::encode(&task_analytics)?;
        txn.put_cf(
            StateMachineColumns::TaskAnalytics.cf(db),
            key,
            &serialized_task_analytics,
        )
        .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    fn set_garbage_collection_tasks(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        garbage_collection_tasks: &[impl AsRef<internal_api::GarbageCollectionTask>],
    ) -> Result<(), StateMachineError> {
        for gc_task in garbage_collection_tasks {
            let gc_task = gc_task.as_ref();
            let serialized_gc_task = JsonEncoder::encode(gc_task)?;
            txn.put_cf(
                StateMachineColumns::GarbageCollectionTasks.cf(db),
                &gc_task.id,
                &serialized_gc_task,
            )
            .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
        }
        Ok(())
    }

    fn update_garbage_collection_tasks(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        garbage_collection_tasks: &Vec<&internal_api::GarbageCollectionTask>,
    ) -> Result<(), StateMachineError> {
        for gc_task in garbage_collection_tasks {
            let serialized_gc_task = JsonEncoder::encode(gc_task)?;
            txn.put_cf(
                StateMachineColumns::GarbageCollectionTasks.cf(db),
                gc_task.id.clone(),
                &serialized_gc_task,
            )
            .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
        }
        Ok(())
    }

    fn get_task_assignments_for_executor(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        executor_id: &str,
    ) -> Result<HashSet<TaskId>, StateMachineError> {
        let value = txn
            .get_cf(StateMachineColumns::TaskAssignments.cf(db), executor_id)
            .map_err(|e| {
                StateMachineError::DatabaseError(format!("Error reading task assignments: {}", e))
            })?;
        match value {
            Some(existing_value) => {
                let existing_value: HashSet<TaskId> = JsonEncoder::decode(&existing_value)
                    .map_err(|e| {
                        StateMachineError::DatabaseError(format!(
                            "Error deserializing task assignments: {}",
                            e
                        ))
                    })?;
                Ok(existing_value)
            }
            None => Ok(HashSet::new()),
        }
    }

    /// Set the list of tasks that have been assigned to some executor
    fn set_task_assignments(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        task_assignments: &HashMap<String, HashSet<TaskId>>,
    ) -> Result<(), StateMachineError> {
        let task_assignment_cf = StateMachineColumns::TaskAssignments.cf(db);
        for (executor_id, task_ids) in task_assignments {
            txn.put_cf(
                task_assignment_cf,
                executor_id,
                JsonEncoder::encode(&task_ids)?,
            )
            .map_err(|e| {
                StateMachineError::DatabaseError(format!("Error writing task assignments: {}", e))
            })?;
        }
        Ok(())
    }

    // FIXME USE MULTI-GET HERE
    fn delete_task_assignments_for_executor(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        executor_id: &str,
    ) -> Result<Vec<TaskId>, StateMachineError> {
        let task_assignment_cf = StateMachineColumns::TaskAssignments.cf(db);
        let task_ids: Vec<TaskId> = txn
            .get_cf(task_assignment_cf, executor_id)
            .map_err(|e| {
                StateMachineError::DatabaseError(format!(
                    "Error reading task assignments for executor: {}",
                    e
                ))
            })?
            .map(|db_vec| {
                JsonEncoder::decode(&db_vec).map_err(|e| {
                    StateMachineError::DatabaseError(format!(
                        "Error deserializing task assignments for executor: {}",
                        e
                    ))
                })
            })
            .unwrap_or_else(|| Ok(Vec::new()))?;

        txn.delete_cf(task_assignment_cf, executor_id)
            .map_err(|e| {
                StateMachineError::DatabaseError(format!(
                    "Error deleting task assignments for executor: {}",
                    e
                ))
            })?;

        Ok(task_ids)
    }

    fn set_content<'a>(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        contents_vec: impl IntoIterator<Item = &'a ContentMetadata>,
    ) -> Result<(), StateMachineError> {
        for content in contents_vec {
            let mut content = content.clone();
            content.change_offset = self.next_content_offset();
            let serialized_content = JsonEncoder::encode(&content)?;
            txn.put_cf(
                StateMachineColumns::ContentTable.cf(db),
                content.id_key(),
                &serialized_content,
            )
            .map_err(|e| {
                StateMachineError::DatabaseError(format!("error writing content: {}", e))
            })?;
            txn.put_cf(
                StateMachineColumns::ChangeIdContentIndex.cf(db),
                content.change_offset.0.to_be_bytes(),
                &content.id.id,
            )
            .map_err(|e| {
                StateMachineError::DatabaseError(format!("error writing content: {}", e))
            })?;
        }
        Ok(())
    }

    fn tombstone_content_tree<'a>(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        content_metadata: impl IntoIterator<Item = &'a ContentMetadata>,
    ) -> Result<(), StateMachineError> {
        for content in content_metadata {
            let cf = StateMachineColumns::ContentTable.cf(db);
            let mut content = content.clone();
            // If updating latest version of root node, the key will change so delete from
            // previous location.
            if content.latest && content.parent_id.is_none() {
                txn.delete_cf(cf, &content.id.id)
                    .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;
                content.latest = false;
            }
            let serialized_content = JsonEncoder::encode(&content)?;
            txn.put_cf(cf, content.id_key(), &serialized_content)
                .map_err(|e| {
                    StateMachineError::DatabaseError(format!("error writing content: {}", e))
                })?;
        }

        Ok(())
    }

    /// Function to delete content based on content ids
    fn delete_content(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        content_id: ContentMetadataId,
        latest: bool,
        change_offset: ContentOffset,
    ) -> Result<(), StateMachineError> {
        let key = if latest {
            content_id.id.clone()
        } else {
            format!("{}::v{}", content_id.id.clone(), content_id.version)
        };
        let res = txn
            .get_cf(StateMachineColumns::ContentTable.cf(db), &key)
            .map_err(|e| {
                StateMachineError::TransactionError(format!(
                    "error in txn while trying to delete content: {}",
                    e
                ))
            })?;
        if res.is_none() {
            warn!("Content with id {} not found", content_id);
        }
        txn.delete_cf(StateMachineColumns::ContentTable.cf(db), &key)
            .map_err(|e| {
                StateMachineError::TransactionError(format!(
                    "error in txn while trying to delete content: {}",
                    e
                ))
            })?;
        txn.delete_cf(
            StateMachineColumns::ChangeIdContentIndex.cf(db),
            change_offset.0.to_be_bytes(),
        )
        .map_err(|e| {
            StateMachineError::TransactionError(format!(
                "error in txn while trying to delete content: {}",
                e
            ))
        })?;
        Ok(())
    }

    fn set_executor(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        addr: String,
        executor_id: &str,
        extractors: &Vec<ExtractorDescription>,
        ts_secs: &u64,
    ) -> Result<(), StateMachineError> {
        let serialized_executor = JsonEncoder::encode(&ExecutorMetadata {
            id: executor_id.into(),
            last_seen: *ts_secs,
            addr: addr.clone(),
            extractors: extractors.clone(),
        })?;
        txn.put_cf(
            StateMachineColumns::Executors.cf(db),
            executor_id,
            serialized_executor,
        )
        .map_err(|e| StateMachineError::DatabaseError(format!("Error writing executor: {}", e)))?;
        Ok(())
    }

    fn delete_executor(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        executor_id: &str,
    ) -> Result<Option<ExecutorMetadata>, StateMachineError> {
        //  Get a handle on the executor before deleting it from the DB
        let executors_cf = StateMachineColumns::Executors.cf(db);
        match txn.get_cf(executors_cf, executor_id).map_err(|e| {
            StateMachineError::DatabaseError(format!("Error reading executor: {}", e))
        })? {
            Some(executor) => {
                let executor_meta = JsonEncoder::decode::<ExecutorMetadata>(&executor)?;
                txn.delete_cf(executors_cf, executor_id).map_err(|e| {
                    StateMachineError::DatabaseError(format!("Error deleting executor: {}", e))
                })?;
                Ok(Some(executor_meta))
            }
            None => Ok(None),
        }
    }

    fn set_extractors(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        extractors: &Vec<ExtractorDescription>,
    ) -> Result<(), StateMachineError> {
        for extractor in extractors {
            let serialized_extractor = JsonEncoder::encode(extractor)?;
            txn.put_cf(
                StateMachineColumns::Extractors.cf(db),
                &extractor.name,
                serialized_extractor,
            )
            .map_err(|e| {
                StateMachineError::DatabaseError(format!("Error writing extractor: {}", e))
            })?;
        }
        Ok(())
    }

    fn set_extraction_policy(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        extraction_policy: &ExtractionPolicy,
    ) -> Result<(), StateMachineError> {
        let serialized_extraction_policy = JsonEncoder::encode(extraction_policy)?;
        txn.put_cf(
            &StateMachineColumns::ExtractionPolicies.cf(db),
            extraction_policy.id.clone(),
            serialized_extraction_policy,
        )
        .map_err(|e| {
            StateMachineError::DatabaseError(format!("Error writing extraction policy: {}", e))
        })?;
        Ok(())
    }

    fn set_namespace(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        namespace: &NamespaceName,
    ) -> Result<(), StateMachineError> {
        let serialized_name = JsonEncoder::encode(namespace)?;
        txn.put_cf(
            &StateMachineColumns::Namespaces.cf(db),
            namespace,
            serialized_name,
        )
        .map_err(|e| StateMachineError::DatabaseError(format!("Error writing namespace: {}", e)))?;
        Ok(())
    }

    fn set_schema(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        schema: &StructuredDataSchema,
    ) -> Result<(), StateMachineError> {
        let serialized_schema = JsonEncoder::encode(schema)?;
        txn.put_cf(
            &StateMachineColumns::StructuredDataSchemas.cf(db),
            schema.id.clone(),
            serialized_schema,
        )
        .map_err(|e| StateMachineError::DatabaseError(format!("Error writing schema: {}", e)))?;
        Ok(())
    }

    pub fn update_content_extraction_policy_state(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        content_id: &ContentMetadataId,
        extraction_policy_id: &str,
        policy_completion_time: SystemTime,
    ) -> Result<(), StateMachineError> {
        let value = txn
            .get_cf(StateMachineColumns::ContentTable.cf(db), &content_id.id)
            .map_err(|e| {
                StateMachineError::DatabaseError(format!(
                    "Error getting the content policies applied on content id {}: {}",
                    content_id, e
                ))
            })?
            .ok_or_else(|| {
                StateMachineError::DatabaseError(format!(
                    "Content not found while updating applied extraction policies {}",
                    content_id
                ))
            })?;
        let mut content_meta = JsonEncoder::decode::<ContentMetadata>(&value)?;
        let epoch_time = policy_completion_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| {
                StateMachineError::DatabaseError(format!(
                    "Error converting policy completion time to u64: {}",
                    e
                ))
            })?
            .as_secs();
        content_meta
            .extraction_policy_ids
            .insert(extraction_policy_id.to_string(), epoch_time);
        let data = JsonEncoder::encode(&content_meta)?;
        txn.put_cf(
            StateMachineColumns::ContentTable.cf(db),
            &content_id.id,
            data,
        )
        .map_err(|e| {
            StateMachineError::DatabaseError(format!(
                "Error writing content policies applied on content for id {}: {}",
                content_id, e
            ))
        })?;

        Ok(())
    }

    pub fn set_coordinator_addr(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        node_id: NodeId,
        coordinator_addr: &str,
    ) -> Result<(), StateMachineError> {
        let serialized_coordinator_addr = JsonEncoder::encode(&coordinator_addr)?;
        txn.put_cf(
            StateMachineColumns::CoordinatorAddress.cf(db),
            node_id.to_string(),
            serialized_coordinator_addr,
        )
        .map_err(|e| {
            StateMachineError::DatabaseError(format!(
                "Error writing coordinator address for node {}: {}",
                node_id, e
            ))
        })?;
        Ok(())
    }

    pub fn mark_state_changes_processed(
        &self,
        state_change: &StateChangeProcessed,
        _processed_at: u64,
    ) {
        self.unprocessed_state_changes
            .remove(&state_change.state_change_id);
    }

    fn update_extraction_graph_reverse_idx(
        &self,
        extraction_graph: &ExtractionGraph,
        structured_data_schema: StructuredDataSchema,
    ) {
        for ep in &extraction_graph.extraction_policies {
            let key = ExtractionGraphNode {
                namespace: ep.namespace.clone(),
                graph_name: extraction_graph.name.clone(),
                source: ep.content_source.clone(),
            };
            self.graph_links
                .write()
                .unwrap()
                .entry(key)
                .or_default()
                .insert(ep.id.clone());
        }
        self.extraction_graphs_by_ns
            .insert(&extraction_graph.namespace, &extraction_graph.id);
        self.schemas_by_namespace.insert(
            &structured_data_schema.namespace,
            &structured_data_schema.id,
        );
    }

    fn add_graph_to_content(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        content_ids: &Vec<String>,
        namespace: &str,
        extraction_graph: &str,
    ) -> Result<(), StateMachineError> {
        let res =
            self.get_extraction_graphs_by_name(namespace, &[extraction_graph.to_string()], db)?;
        if res.first().is_none() {
            warn!(
                "extraction graph {} not found in namespace {}",
                extraction_graph, namespace
            );
            return Ok(());
        };
        for content_id in content_ids {
            let content = self.get_latest_version_of_content(content_id, db, txn)?;
            match content {
                Some(content) if !content.tombstoned => {
                    if content.namespace != namespace {
                        warn!(
                            "content {} does not belong to namespace {}",
                            content_id, namespace
                        );
                        continue;
                    }
                    let mut content = content;
                    content
                        .extraction_graph_names
                        .push(extraction_graph.to_string());
                    let serialized_content = JsonEncoder::encode(&content)?;
                    txn.put_cf(
                        StateMachineColumns::ContentTable.cf(db),
                        content.id_key(),
                        &serialized_content,
                    )
                    .map_err(|e| {
                        StateMachineError::DatabaseError(format!("error writing content: {}", e))
                    })?;
                }
                _ => {
                    warn!("Content with id {} not found", content_id);
                }
            }
        }
        Ok(())
    }

    /// This method will make all state machine forward index writes to RocksDB
    pub fn apply_state_machine_updates(
        &self,
        mut request: StateMachineUpdateRequest,
        db: &OptimisticTransactionDB,
        txn: Transaction<OptimisticTransactionDB>,
    ) -> Result<Option<StateChangeId>, StateMachineError> {
        self.set_new_state_changes(db, &txn, &mut request.new_state_changes)?;
        let mut state_changes_processed =
            self.set_processed_state_changes(db, &txn, &request.state_changes_processed)?;

        match &request.payload {
            RequestPayload::DeleteExtractionGraph { graph_id, gc_task } => {
                self.set_garbage_collection_tasks(db, &txn, &[gc_task])?;
                self.delete_extraction_graph(db, &txn, graph_id)?;
            }
            RequestPayload::AddGraphToContent {
                content_ids,
                extraction_graph,
                namespace,
            } => {
                self.add_graph_to_content(db, &txn, content_ids, namespace, extraction_graph)?;
            }
            RequestPayload::SetIndex { indexes } => {
                for index in indexes {
                    self.set_index(db, &txn, index, &index.id)?;
                }
            }
            RequestPayload::CreateTasks { tasks } => {
                self.update_tasks(db, &txn, tasks.clone(), SystemTime::UNIX_EPOCH)?;
                for task in tasks {
                    self.inc_root_ref_count(task.content_metadata.get_root_id());
                }
            }
            RequestPayload::CreateOrAssignGarbageCollectionTask { gc_tasks } => {
                self.set_garbage_collection_tasks(db, &txn, gc_tasks)?;
            }
            RequestPayload::UpdateGarbageCollectionTask {
                gc_task,
                mark_finished,
            } => {
                if *mark_finished {
                    tracing::info!("Marking garbage collection task as finished: {:?}", gc_task);
                    self.update_garbage_collection_tasks(db, &txn, &vec![gc_task])?;
                    if gc_task.task_type == ServerTaskType::Delete ||
                        gc_task.task_type == ServerTaskType::DeleteBlobStore
                    {
                        self.delete_content(
                            db,
                            &txn,
                            gc_task.content_id.clone(),
                            gc_task.latest,
                            gc_task.change_offset,
                        )?;
                    }
                }
            }
            RequestPayload::AssignTask { assignments } => {
                let assignments: HashMap<&String, HashSet<TaskId>> =
                    assignments
                        .iter()
                        .fold(HashMap::new(), |mut acc, (task_id, executor_id)| {
                            acc.entry(executor_id).or_default().insert(task_id.clone());
                            acc
                        });

                // FIXME - Write a test which assigns tasks mutliple times to the same executor
                // and make sure it's additive.

                for (executor_id, tasks) in assignments.iter() {
                    let mut existing_tasks =
                        self.get_task_assignments_for_executor(db, &txn, executor_id)?;
                    existing_tasks.extend(tasks.clone());
                    let task_assignment =
                        HashMap::from([(executor_id.to_string(), existing_tasks)]);
                    self.set_task_assignments(db, &txn, &task_assignment)?;
                }
            }
            RequestPayload::UpdateTask {
                task,
                executor_id,
                update_time,
            } => {
                self.update_tasks(db, &txn, vec![task.clone()], *update_time)?;

                if task.terminal_state() {
                    self.metrics
                        .lock()
                        .unwrap()
                        .update_task_completion(task.outcome);

                    //  If the task is meant to be marked finished and has an executor id, remove it
                    // from the list of tasks assigned to an executor
                    if let Some(executor_id) = executor_id {
                        let mut existing_tasks =
                            self.get_task_assignments_for_executor(db, &txn, executor_id)?;
                        existing_tasks.remove(&task.id);
                        let new_task_assignment =
                            HashMap::from([(executor_id.to_string(), existing_tasks)]);
                        self.set_task_assignments(db, &txn, &new_task_assignment)?;
                    }
                    self.dec_root_ref_count(task.content_metadata.get_root_id());
                }
            }
            RequestPayload::RegisterExecutor {
                addr,
                executor_id,
                extractors,
                ts_secs,
            } => {
                //  Insert the executor
                self.set_executor(db, &txn, addr.into(), executor_id, extractors, ts_secs)?;

                //  Insert the associated extractors
                self.set_extractors(db, &txn, extractors)?;
            }
            RequestPayload::RemoveExecutor { executor_id } => {
                //  NOTE: Special case where forward and reverse indexes are updated together

                //  Get a handle on the executor before deleting it from the DB
                let executor_meta = self.delete_executor(db, &txn, executor_id)?;

                // Remove all tasks assigned to this executor
                self.delete_task_assignments_for_executor(db, &txn, executor_id)?;

                txn.commit()
                    .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;

                //  Remove the extractors from the executor -> extractor mapping table
                if let Some(executor_meta) = executor_meta {
                    for extractor in &executor_meta.extractors {
                        self.extractor_executors_table
                            .remove(&extractor.name, &executor_meta.id);
                    }
                }

                let executor_tasks = self
                    .unfinished_tasks_by_executor
                    .write()
                    .unwrap()
                    .remove(executor_id);

                //  Put the tasks of the deleted executor into the unassigned tasks list
                if let Some(executor_tasks) = executor_tasks {
                    for task in executor_tasks.tasks {
                        self.unassigned_tasks.insert(&task.id, task.creation_time);
                    }
                }

                return Ok(request.new_state_changes.last().map(|sc| sc.id));
            }
            RequestPayload::CreateOrUpdateContent { entries } => {
                self.set_content(db, &txn, entries.iter().map(|e| &e.content))?;
            }
            RequestPayload::TombstoneContentTree { content_metadata } => {
                self.tombstone_content_tree(db, &txn, content_metadata)?;
            }
            RequestPayload::TombstoneContent { content_metadata } => {
                let updated_content: Vec<_> =
                    content_metadata.iter().filter(|c| !c.tombstoned).collect();
                let deleted_content: Vec<_> =
                    content_metadata.iter().filter(|c| c.tombstoned).collect();

                self.set_content(db, &txn, updated_content)?;
                self.tombstone_content_tree(db, &txn, deleted_content)?;
            }
            RequestPayload::CreateNamespace { name } => {
                self.set_namespace(db, &txn, name)?;
            }
            RequestPayload::MarkStateChangesProcessed { state_changes } => {
                let payload_changes_processed =
                    self.set_processed_state_changes(db, &txn, state_changes)?;
                state_changes_processed.extend(payload_changes_processed);
            }
            RequestPayload::JoinCluster {
                node_id,
                address: _,
                coordinator_addr,
            } => {
                self.set_coordinator_addr(db, &txn, *node_id, coordinator_addr)?;
            }
            RequestPayload::CreateExtractionGraph {
                extraction_graph,
                structured_data_schema,
                indexes,
            } => {
                self.set_extraction_graph(db, &txn, extraction_graph, structured_data_schema)?;
                for index in indexes {
                    self.set_index(db, &txn, index, &index.id)?;
                }
            }
            RequestPayload::CreateExtractionGraphLink {
                extraction_graph_link,
            } => {
                self.set_extraction_graph_link(db, &txn, extraction_graph_link)?;
                let linked_graph = self.get_extraction_graphs_by_name(
                    &extraction_graph_link.node.namespace,
                    &[extraction_graph_link.graph_name.clone()],
                    db,
                )?;
                let mut graph_links = self.graph_links.write().unwrap();
                if let Some(Some(linked_graph)) = linked_graph.first() {
                    for policy in &linked_graph.extraction_policies {
                        if policy.content_source == ContentSource::Ingestion {
                            graph_links
                                .entry(extraction_graph_link.node.clone())
                                .or_default()
                                .insert(policy.id.clone());
                        }
                    }
                }
            }
        };

        let unprocessed_changes = self.get_unprocessed_state_changes();
        for state_change in state_changes_processed {
            if unprocessed_changes.contains(&state_change.id) {
                if let Some(refcnt_object_id) = &state_change.refcnt_object_id {
                    self.dec_root_ref_count(refcnt_object_id);
                }
            }
        }

        let last_change_id = request.new_state_changes.last().map(|sc| sc.id);

        self.update_reverse_indexes(request).map_err(|e| {
            StateMachineError::ExternalError(anyhow!(
                "Error while applying reverse index updates: {}",
                e
            ))
        })?;

        txn.commit()
            .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;

        Ok(last_change_id)
    }

    /// This method handles all reverse index writes. All reverse indexes are
    /// written in memory
    pub fn update_reverse_indexes(&self, request: StateMachineUpdateRequest) -> Result<()> {
        for change in request.new_state_changes {
            self.unprocessed_state_changes.insert(change.id);
        }
        for change in request.state_changes_processed {
            self.mark_state_changes_processed(&change, change.processed_at);
        }
        match request.payload {
            RequestPayload::AddGraphToContent { .. } => Ok(()),
            RequestPayload::RegisterExecutor {
                addr,
                executor_id,
                extractors,
                ts_secs,
            } => {
                // Inserts the executor list of extractors to the executor -> extractor mapping
                // table
                for extractor in &extractors {
                    self.extractor_executors_table
                        .insert(&extractor.name, &executor_id);
                }

                let _executor_info = ExecutorMetadata {
                    id: executor_id.clone(),
                    last_seen: ts_secs,
                    addr: addr.clone(),
                    extractors: extractors.clone(),
                };
                // initialize executor tasks
                self.unfinished_tasks_by_executor
                    .write()
                    .unwrap()
                    .entry(executor_id.clone())
                    .or_default();

                Ok(())
            }
            RequestPayload::CreateTasks { tasks } => {
                for task in tasks {
                    self.unassigned_tasks.insert(&task.id, task.creation_time);
                    self.unfinished_tasks_by_extractor
                        .insert(&task.extractor, &task.id);
                }
                Ok(())
            }
            RequestPayload::AssignTask { assignments } => {
                for (task_id, executor_id) in assignments {
                    let task = self.unassigned_tasks.remove(&task_id);
                    if let Some(task) = task {
                        self.unfinished_tasks_by_executor
                            .write()
                            .unwrap()
                            .entry(executor_id.clone())
                            .or_default()
                            .insert(task);
                    }
                }
                Ok(())
            }
            RequestPayload::UpdateGarbageCollectionTask {
                gc_task,
                mark_finished,
            } => {
                if mark_finished &&
                    (gc_task.task_type == ServerTaskType::Delete ||
                        gc_task.task_type == ServerTaskType::DeleteBlobStore)
                {
                    self.content_children_table.remove_all(&gc_task.content_id);
                }
                Ok(())
            }
            RequestPayload::CreateOrUpdateContent { entries } => {
                for entry in entries {
                    let mut guard = self.metrics.lock().unwrap();
                    if let Some(prev_parent) = entry.previous_parent {
                        self.content_children_table
                            .remove(&prev_parent, &entry.content.id);
                    }
                    if let Some(parent_id) = entry.content.parent_id {
                        self.content_children_table
                            .insert(&parent_id, &entry.content.id);
                        guard.content_extracted += 1;
                        guard.content_extracted_bytes += entry.content.size_bytes;
                    } else {
                        guard.content_uploads += 1;
                        guard.content_bytes += entry.content.size_bytes;
                    }
                    let _ = self.new_content_channel.send(());
                }
                Ok(())
            }
            RequestPayload::CreateExtractionGraph {
                extraction_graph,
                structured_data_schema,
                indexes,
            } => {
                self.update_extraction_graph_reverse_idx(&extraction_graph, structured_data_schema);
                for index in indexes {
                    self.namespace_index_table
                        .insert(&index.namespace, &index.id);
                }
                Ok(())
            }
            RequestPayload::UpdateTask {
                task,
                executor_id,
                update_time: _,
            } => {
                if task.terminal_state() {
                    self.unassigned_tasks.remove(&task.id);
                    self.unfinished_tasks_by_extractor
                        .remove(&task.extractor, &task.id);
                    if let Some(ref executor_id) = executor_id {
                        self.unfinished_tasks_by_executor
                            .write()
                            .unwrap()
                            .get_mut(executor_id)
                            .map(|tasks| {
                                tasks.remove(&UnfinishedTask {
                                    id: task.id.clone(),
                                    creation_time: task.creation_time,
                                })
                            });
                    }
                }
                Ok(())
            }
            RequestPayload::MarkStateChangesProcessed { state_changes } => {
                for state_change in state_changes {
                    self.mark_state_changes_processed(&state_change, state_change.processed_at);
                }
                Ok(())
            }
            RequestPayload::DeleteExtractionGraph { .. } |
            RequestPayload::CreateOrAssignGarbageCollectionTask { .. } |
            RequestPayload::CreateExtractionGraphLink { .. } |
            RequestPayload::CreateNamespace { .. } |
            RequestPayload::JoinCluster { .. } |
            RequestPayload::RemoveExecutor { .. } |
            RequestPayload::SetIndex { .. } |
            RequestPayload::TombstoneContent { .. } |
            RequestPayload::TombstoneContentTree { .. } => Ok(()),
        }
    }

    //  START READER METHODS FOR ROCKSDB FORWARD INDEXES

    pub fn get_latest_version_of_content(
        &self,
        content_id: &str,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
    ) -> Result<Option<ContentMetadata>, StateMachineError> {
        txn.get_cf(StateMachineColumns::ContentTable.cf(db), content_id)
            .map_err(|e| StateMachineError::TransactionError(e.to_string()))?
            .map(|data| JsonEncoder::decode::<ContentMetadata>(&data))
            .transpose()
    }

    fn delete_schema_for_graph(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        graph: &ExtractionGraph,
    ) -> Result<(), StateMachineError> {
        let cf = StateMachineColumns::StructuredDataSchemas.cf(db);
        for item in txn.iterator_cf(cf, IteratorMode::Start) {
            let (key, value) = item.map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
            let schema = JsonEncoder::decode::<StructuredDataSchema>(&value)?;
            if schema.namespace == graph.namespace && schema.extraction_graph_name == graph.name {
                self.schemas_by_namespace
                    .remove(&schema.namespace, &schema.id);
                let _ = txn.delete_cf(cf, key);
                break;
            }
        }
        Ok(())
    }

    fn delete_indexes_for_graph(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        graph: &ExtractionGraph,
    ) -> Result<(), StateMachineError> {
        let cf = StateMachineColumns::IndexTable.cf(db);
        for item in txn.iterator_cf(cf, IteratorMode::Start) {
            let (key, value) = item.map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
            let index = JsonEncoder::decode::<Index>(&value)?;
            if index.namespace == graph.namespace && index.graph_name == graph.name {
                self.namespace_index_table
                    .remove(&index.namespace, &index.id);
                let _ = txn.delete_cf(cf, key);
            }
        }
        Ok(())
    }

    fn delete_extraction_graph(
        &self,
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
        graph_id: &str,
    ) -> Result<(), StateMachineError> {
        let graph = self.get_extraction_graphs(&[graph_id.to_string()], db, txn)?;
        let graph = match graph.first() {
            Some(Some(graph)) => graph,
            _ => {
                warn!("Extraction graph with id {} not found", graph_id);
                return Ok(());
            }
        };
        self.delete_schema_for_graph(db, txn, graph)?;
        self.delete_indexes_for_graph(db, txn, graph)?;
        txn.delete_cf(StateMachineColumns::ExtractionGraphs.cf(db), &graph_id)
            .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;
        self.extraction_graphs_by_ns
            .remove(&graph.namespace, &graph.id);
        Ok(())
    }

    /// This method is used to get the tasks assigned to an executor
    /// It does this by looking up the TaskAssignments CF to get the task id's
    /// and then using those id's to look up tasks via Tasks CF
    pub fn get_tasks_for_executor(
        &self,
        executor_id: &str,
        limit: Option<u64>,
        db: &OptimisticTransactionDB,
    ) -> Result<Vec<Task>, StateMachineError> {
        let limit = limit.unwrap_or(u64::MAX);
        let tasks: Vec<_> = match self
            .unfinished_tasks_by_executor
            .read()
            .unwrap()
            .get(executor_id)
        {
            Some(tasks) => tasks
                .tasks
                .iter()
                .take(limit as usize)
                .map(|task| task.id.clone())
                .collect(),
            None => return Ok(Vec::new()),
        };

        let txn = db.transaction();

        let tasks: Result<Vec<Task>, StateMachineError> = tasks
            .iter()
            .map(|task_id| {
                let task_bytes = txn
                    .get_cf(StateMachineColumns::Tasks.cf(db), task_id.as_bytes())
                    .map_err(|e| StateMachineError::TransactionError(e.to_string()))?
                    .ok_or_else(|| {
                        StateMachineError::DatabaseError(format!("Task {} not found", task_id))
                    })?;
                JsonEncoder::decode(&task_bytes).map_err(StateMachineError::from)
            })
            .collect();
        tasks
    }

    /// This method will fetch indexes based on the id's of the indexes provided
    pub fn get_indexes_from_ids(
        &self,
        task_ids: HashSet<TaskId>,
        db: &OptimisticTransactionDB,
    ) -> Result<Vec<Index>, StateMachineError> {
        let txn = db.transaction();
        let indexes: Result<Vec<Index>, StateMachineError> = task_ids
            .into_iter()
            .map(|task_id| {
                let index_bytes = txn
                    .get_cf(StateMachineColumns::IndexTable.cf(db), task_id.as_bytes())
                    .map_err(|e| StateMachineError::TransactionError(e.to_string()))?
                    .ok_or_else(|| {
                        StateMachineError::DatabaseError(format!("Index {} not found", task_id))
                    })?;
                JsonEncoder::decode(&index_bytes).map_err(StateMachineError::from)
            })
            .collect();
        indexes
    }

    /// This method will fetch the executors from RocksDB CF based on the
    /// executor id's provided
    pub fn get_executors_from_ids(
        &self,
        executor_ids: HashSet<String>,
        db: &OptimisticTransactionDB,
    ) -> Result<Vec<ExecutorMetadata>, StateMachineError> {
        let txn = db.transaction();
        let executors: Result<Vec<ExecutorMetadata>, StateMachineError> = executor_ids
            .into_iter()
            .map(|executor_id| {
                let executor_bytes = txn
                    .get_cf(
                        StateMachineColumns::Executors.cf(db),
                        executor_id.as_bytes(),
                    )
                    .map_err(|e| StateMachineError::TransactionError(e.to_string()))?
                    .ok_or_else(|| {
                        StateMachineError::DatabaseError(format!(
                            "Executor {} not found",
                            executor_id
                        ))
                    })?;
                JsonEncoder::decode(&executor_bytes).map_err(StateMachineError::from)
            })
            .collect();
        executors
    }

    pub fn get_content_by_id_and_version(
        &self,
        db: &OptimisticTransactionDB,
        content_id: &ContentMetadataId,
    ) -> Result<Option<ContentMetadata>, StateMachineError> {
        let txn = db.transaction();
        let content_metadata_bytes = txn
            .get_cf(
                StateMachineColumns::ContentTable.cf(db),
                format!("{}::v{}", content_id.id, content_id.version),
            )
            .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;
        if content_metadata_bytes.is_none() {
            return Ok(None);
        }
        let content_metadata =
            JsonEncoder::decode::<ContentMetadata>(&content_metadata_bytes.unwrap())?;
        Ok(Some(content_metadata))
    }

    /// This method will fetch content based on the id's provided. It will look
    /// for the latest version for each piece of content It will skip any
    /// that cannot be found and expect the consumer to decide what to do in
    /// that case.
    pub fn get_content_from_ids(
        &self,
        content_ids: impl IntoIterator<Item = String>,
        db: &OptimisticTransactionDB,
    ) -> Result<Vec<ContentMetadata>, StateMachineError> {
        let txn = db.transaction();
        let mut contents = Vec::new();
        let cf_handle = StateMachineColumns::ContentTable.cf(db);
        let cf_keys = content_ids
            .into_iter()
            .map(|id| (cf_handle, id))
            .collect_vec();
        let results = txn.multi_get_cf(cf_keys);
        for res in results {
            match res {
                Ok(Some(value)) => {
                    contents.push(JsonEncoder::decode::<ContentMetadata>(&value)?);
                }
                Ok(None) => {}
                Err(e) => {
                    return Err(StateMachineError::DatabaseError(format!(
                        "error reading content: {}",
                        e
                    )))
                }
            }
        }
        Ok(contents)
    }

    // Root of the tree can be either latest version, or an overwritten/deleted
    // content with a version supplied. The rest of the elements in the tree are
    // always version 1.
    fn get_content_tree_metadata_inner(
        &self,
        content_id: &str,
        version: Option<u64>,
        db: &OptimisticTransactionDB,
    ) -> Result<Vec<ContentMetadata>, StateMachineError> {
        let txn = db.transaction();
        let mut collected_content_metadata = Vec::new();
        let content_key = ContentMetadata::make_id_key(content_id, version);
        let cf_handle = StateMachineColumns::ContentTable.cf(db);
        let val = txn
            .get_cf(cf_handle, content_key)
            .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;

        let content = match val {
            None => return Ok(collected_content_metadata),
            Some(bytes) => JsonEncoder::decode::<ContentMetadata>(&bytes)?,
        };

        let mut cf_ids = Vec::new();

        let mut queue = VecDeque::new();
        queue.push_back(content.id.clone());
        collected_content_metadata.push(content);
        while let Some(current_root) = queue.pop_front() {
            let children = self.content_children_table.get_children(&current_root);
            cf_ids.extend(children.iter().map(|id| (cf_handle, id.id.clone())));
            queue.extend(children.into_iter());
        }

        let content_metadata_bytes = txn.multi_get_cf(cf_ids);

        for res in content_metadata_bytes {
            if let Ok(Some(value)) = res {
                let content = JsonEncoder::decode::<ContentMetadata>(&value)?;
                collected_content_metadata.push(content);
            }
        }

        Ok(collected_content_metadata)
    }

    /// This method will fetch all pieces of content metadata for the tree
    /// rooted at latest version of content_id.
    pub fn get_content_tree_metadata(
        &self,
        content_id: &str,
        db: &OptimisticTransactionDB,
    ) -> Result<Vec<ContentMetadata>, StateMachineError> {
        self.get_content_tree_metadata_inner(content_id, None, db)
    }

    /// This method will fetch all pieces of content metadata for the tree
    /// rooted at overwritten/deleted content_id. It will look for a specfic
    /// version of the node.
    pub fn get_content_tree_metadata_with_version(
        &self,
        content_id: &ContentMetadataId,
        db: &OptimisticTransactionDB,
    ) -> Result<Vec<ContentMetadata>, StateMachineError> {
        self.get_content_tree_metadata_inner(&content_id.id, Some(content_id.version), db)
    }

    /// This method tries to retrieve all policies based on id's. If it cannot
    /// find any, it skips them. If it encounters an error at any point
    /// during the transaction, it returns out immediately
    pub fn get_extraction_policies_from_ids(
        &self,
        extraction_policy_ids: HashSet<String>,
        db: &OptimisticTransactionDB,
    ) -> Result<Vec<ExtractionPolicy>, StateMachineError> {
        let txn = db.transaction();

        let mut policies = Vec::new();
        for id in extraction_policy_ids.iter() {
            let bytes_opt = txn
                .get_cf(
                    StateMachineColumns::ExtractionPolicies.cf(db),
                    id.as_bytes(),
                )
                .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;

            if let Some(bytes) = bytes_opt {
                let policy = serde_json::from_slice::<ExtractionPolicy>(&bytes).map_err(|e| {
                    StateMachineError::SerializationError(format!(
                        "get_extraction_policies from id: unable to deserialize json, {}",
                        e
                    ))
                })?;
                policies.push(policy);
            }
            // If None, the policy is not found; we simply skip it.
        }
        Ok(policies)
    }

    pub fn get_extraction_policy_by_names(
        &self,
        namespace: &str,
        graph_name: &str,
        policy_names: &HashSet<ExtractionPolicyName>,
        db: &OptimisticTransactionDB,
    ) -> Result<Vec<Option<ExtractionPolicy>>, StateMachineError> {
        let mut policies = Vec::new();
        for policy_name in policy_names {
            let extraction_policy_id =
                ExtractionPolicy::create_id(graph_name, policy_name, namespace);
            let extraction_policy_bytes = db
                .get_cf(
                    StateMachineColumns::ExtractionPolicies.cf(db),
                    extraction_policy_id.as_bytes(),
                )
                .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;
            match extraction_policy_bytes {
                Some(bytes) => {
                    let extraction_policy = JsonEncoder::decode::<ExtractionPolicy>(&bytes)?;
                    policies.push(Some(extraction_policy))
                }
                None => policies.push(None),
            }
        }
        Ok(policies)
    }

    /// This method gets all task assignments stored in the relevant CF
    pub fn get_all_task_assignments(
        &self,
        db: &OptimisticTransactionDB,
    ) -> Result<HashMap<TaskId, ExecutorId>, StateMachineError> {
        let mut assignments = HashMap::new();
        let iter = db.iterator_cf(
            StateMachineColumns::TaskAssignments.cf(db),
            IteratorMode::Start,
        );
        for item in iter {
            let (key, value) = item.map_err(|e| {
                StateMachineError::DatabaseError(format!(
                    "unable to get values from task assignment {}",
                    e
                ))
            })?;
            let executor_id = String::from_utf8(key.to_vec()).map_err(|e| {
                StateMachineError::DatabaseError(format!(
                    "unable to get executor id from task assignment {}",
                    e
                ))
            })?;
            let task_ids: HashSet<TaskId> = JsonEncoder::decode(&value).map_err(|e| {
                StateMachineError::DatabaseError(format!(
                    "unable to decoded task hashset from task assignment {}",
                    e
                ))
            })?;
            for task_id in task_ids {
                assignments.insert(task_id, executor_id.clone());
            }
        }
        Ok(assignments)
    }

    /// Allocate a block of state change ids
    pub fn get_next_change_ids(&self, num: usize) -> u64 {
        let mut guard = self.change_id.lock().unwrap();
        let next_id = *guard;
        *guard = next_id + num as u64;
        next_id
    }

    /// This method will get the namespace based on the key provided
    pub fn namespace_exists(&self, namespace: &str, db: &OptimisticTransactionDB) -> Result<bool> {
        Ok(get_from_cf::<String, &str>(db, StateMachineColumns::Namespaces, namespace)?.is_some())
    }

    pub fn get_schemas(
        &self,
        ids: HashSet<String>,
        db: &OptimisticTransactionDB,
    ) -> Result<Vec<StructuredDataSchema>> {
        let txn = db.transaction();
        let keys = ids
            .iter()
            .map(|id| (StateMachineColumns::StructuredDataSchemas.cf(db), id))
            .collect_vec();
        let schema_bytes = txn.multi_get_cf(keys);
        let mut schemas = vec![];
        for schema in schema_bytes {
            let schema = schema
                .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?
                .ok_or(StateMachineError::DatabaseError("Schema not found".into()))?;
            let schema = JsonEncoder::decode(&schema)?;
            schemas.push(schema);
        }
        Ok(schemas)
    }

    pub fn get_extraction_graphs(
        &self,
        extraction_graph_ids: &[impl AsRef<str>],
        db: &OptimisticTransactionDB,
        txn: &Transaction<OptimisticTransactionDB>,
    ) -> Result<Vec<Option<ExtractionGraph>>, StateMachineError> {
        let cf = StateMachineColumns::ExtractionGraphs.cf(db);
        let keys: Vec<(&rocksdb::ColumnFamily, &[u8])> = extraction_graph_ids
            .iter()
            .map(|egid| (cf, egid.as_ref().as_bytes()))
            .collect();
        let serialized_graphs = txn.multi_get_cf(keys);
        let mut graphs: Vec<Option<ExtractionGraph>> = Vec::new();
        for serialized_graph in serialized_graphs {
            match serialized_graph {
                Ok(Some(graph)) => {
                    graphs.push(Some(JsonEncoder::decode::<ExtractionGraph>(&graph)?));
                }
                Ok(None) => {
                    graphs.push(None);
                }
                Err(e) => {
                    return Err(StateMachineError::TransactionError(e.to_string()));
                }
            }
        }
        Ok(graphs)
    }

    pub fn get_extraction_graphs_by_name(
        &self,
        namespace: &str,
        graph_names: &[impl AsRef<str>],
        db: &OptimisticTransactionDB,
    ) -> Result<Vec<Option<ExtractionGraph>>, StateMachineError> {
        let eg_ids: Vec<String> = graph_names
            .iter()
            .map(|name| ExtractionGraph::create_id(name.as_ref(), namespace))
            .collect();
        let cf = StateMachineColumns::ExtractionGraphs.cf(db);
        let keys: Vec<(&rocksdb::ColumnFamily, &[u8])> =
            eg_ids.iter().map(|egid| (cf, egid.as_bytes())).collect();
        let serialized_graphs = db.multi_get_cf(keys);
        let mut graphs: Vec<Option<ExtractionGraph>> = Vec::new();
        for serialized_graph in serialized_graphs {
            match serialized_graph {
                Ok(graph) => {
                    if graph.is_some() {
                        let deserialized_graph =
                            JsonEncoder::decode::<ExtractionGraph>(&graph.unwrap())?;
                        graphs.push(Some(deserialized_graph));
                    } else {
                        graphs.push(None);
                    }
                }
                Err(e) => {
                    return Err(StateMachineError::TransactionError(e.to_string()));
                }
            }
        }
        Ok(graphs)
    }

    pub fn get_coordinator_addr(
        &self,
        node_id: NodeId,
        db: &OptimisticTransactionDB,
    ) -> Result<Option<String>> {
        get_from_cf(
            db,
            StateMachineColumns::CoordinatorAddress,
            node_id.to_string(),
        )
    }

    pub fn iter_cf<'a, V>(
        &self,
        db: &'a OptimisticTransactionDB,
        column: StateMachineColumns,
    ) -> impl Iterator<Item = Result<(Box<[u8]>, V), StateMachineError>> + 'a
    where
        V: DeserializeOwned,
    {
        let cf_handle = db
            .cf_handle(column.as_ref())
            .ok_or(StateMachineError::DatabaseError(format!(
                "Failed to get column family {}",
                column
            )))
            .unwrap();
        db.iterator_cf(cf_handle, IteratorMode::Start).map(|item| {
            item.map_err(|e| StateMachineError::DatabaseError(e.to_string()))
                .and_then(|(key, value)| match JsonEncoder::decode::<V>(&value) {
                    Ok(value) => Ok((key, value)),
                    Err(e) => Err(StateMachineError::SerializationError(e.to_string())),
                })
        })
    }

    /// Test utility method to get all key-value pairs from a column family
    pub fn get_all_rows_from_cf<V>(
        &self,
        column: StateMachineColumns,
        db: &OptimisticTransactionDB,
    ) -> Result<Vec<(String, V)>, StateMachineError>
    where
        V: DeserializeOwned,
    {
        let cf_handle = db
            .cf_handle(column.as_ref())
            .ok_or(StateMachineError::DatabaseError(format!(
                "Failed to get column family {}",
                column
            )))?;
        let iter = db.iterator_cf(cf_handle, IteratorMode::Start);

        iter.map(|item| {
            item.map_err(|e| StateMachineError::DatabaseError(e.to_string()))
                .and_then(|(key, value)| {
                    match column {
                        StateMachineColumns::StateChanges => {
                            // let key = u64::from_be_bytes(key).to_string();
                            let key =
                                StateChangeId::from_key(key.to_vec()[..8].try_into().map_err(
                                    |e: std::array::TryFromSliceError| {
                                        StateMachineError::DatabaseError(e.to_string())
                                    },
                                )?)
                                .to_string();
                            let value = JsonEncoder::decode(&value)
                                .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
                            Ok((key, value))
                        }
                        _ => {
                            let key = String::from_utf8(key.to_vec())
                                .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
                            let value = JsonEncoder::decode(&value)
                                .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
                            Ok((key, value))
                        }
                    }
                })
        })
        .collect::<Result<Vec<(String, V)>, _>>()
    }

    pub fn list_active_contents(
        &self,
        db: &OptimisticTransactionDB,
        namespace: &str,
    ) -> Result<Vec<String>, StateMachineError> {
        let root_content_guard = self.root_task_counts.read().unwrap();
        let root_content_ids: Vec<String> = root_content_guard.keys().cloned().collect();
        drop(root_content_guard);
        let content = self.get_content_from_ids(root_content_ids, db)?;
        let content_ids = content
            .into_iter()
            .filter(|content| content.namespace == namespace)
            .map(|content| content.id.id)
            .collect_vec();
        Ok(content_ids)
    }

    //  END READER METHODS FOR ROCKSDB FORWARD INDEXES

    //  START READER METHODS FOR REVERSE INDEXES
    pub fn get_unassigned_tasks(&self) -> HashMap<TaskId, SystemTime> {
        self.unassigned_tasks.inner()
    }

    pub fn get_unprocessed_state_changes(&self) -> HashSet<StateChangeId> {
        self.unprocessed_state_changes.inner()
    }

    pub fn get_extractor_executors_table(&self) -> HashMap<ExtractorName, HashSet<ExecutorId>> {
        self.extractor_executors_table.inner()
    }

    pub fn get_namespace_index_table(&self) -> HashMap<NamespaceName, HashSet<String>> {
        self.namespace_index_table.inner()
    }

    pub fn get_unfinished_tasks_by_extractor(&self) -> HashMap<ExtractorName, HashSet<TaskId>> {
        self.unfinished_tasks_by_extractor.inner()
    }

    pub fn get_executor_running_task_count(&self) -> HashMap<ExecutorId, u64> {
        self.unfinished_tasks_by_executor
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.tasks.len() as u64))
            .collect()
    }

    pub fn get_schemas_by_namespace(&self) -> HashMap<NamespaceName, HashSet<SchemaId>> {
        self.schemas_by_namespace.inner()
    }

    pub fn get_content_children_table(
        &self,
    ) -> HashMap<ContentMetadataId, HashSet<ContentMetadataId>> {
        self.content_children_table.inner()
    }

    fn inc_root_ref_count(&self, content_id: &str) {
        let mut root_task_counts = self.root_task_counts.write().unwrap();
        root_task_counts
            .entry(content_id.to_string())
            .and_modify(|c| c.count += 1)
            .or_insert(TaskCount {
                count: 1,
                notify: None,
            });
    }

    fn dec_root_ref_count(&self, content_id: &str) {
        let mut root_task_counts = self.root_task_counts.write().unwrap();
        match root_task_counts.entry(content_id.to_string()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().count -= 1;
                if entry.get().count == 0 {
                    let notify = entry.get().notify.clone();
                    entry.remove_entry();
                    drop(root_task_counts);

                    if let Some(tx) = notify {
                        let _ = tx.send(());
                    }
                }
            }
            Entry::Vacant(_) => {
                // this should never happen
            }
        }
    }

    pub async fn wait_root_task_count_zero(&self, content_id: &str) {
        loop {
            let mut receiver = {
                let mut root_task_counts = self.root_task_counts.write().unwrap();
                match root_task_counts.get_mut(content_id) {
                    Some(tc) => match tc.notify {
                        Some(ref n) => n.subscribe(),
                        None => {
                            let (tx, rx) = broadcast::channel(1);
                            tc.notify = Some(tx);
                            rx
                        }
                    },
                    None => return,
                }
            };
            let _ = receiver.recv().await;
        }
    }

    pub fn are_content_tasks_completed(&self, content_id: &ContentMetadataId) -> bool {
        self.root_task_counts
            .read()
            .unwrap()
            .get(&content_id.id)
            .is_none()
    }

    pub fn executor_count(&self) -> usize {
        self.unfinished_tasks_by_executor.read().unwrap().len()
    }

    //  END READER METHODS FOR REVERSE INDEXES

    // For each linked graph link it's top level policies to the graph node
    fn rebuild_graph_links(
        &self,
        db: &OptimisticTransactionDB,
        graph_links: &mut HashMap<ExtractionGraphNode, HashSet<String>>,
    ) -> Result<(), StateMachineError> {
        let cf_handle = StateMachineColumns::ExtractionGraphLinks.cf(db);
        for v in db.iterator_cf(cf_handle, IteratorMode::Start) {
            let (key, _) = v.map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
            let link: ExtractionGraphLink = JsonEncoder::decode(&key)?;

            let linked_graphs =
                self.get_extraction_graphs_by_name(&link.node.namespace, &[link.graph_name], db)?;
            if let Some(Some(linked_graph)) = linked_graphs.first() {
                for policy in &linked_graph.extraction_policies {
                    if policy.content_source == ContentSource::Ingestion {
                        graph_links
                            .entry(link.node.clone())
                            .or_default()
                            .insert(policy.id.clone());
                    }
                }
            }
        }
        Ok(())
    }

    //  Build the in-memory reverse indexes on startup or when restoring from
    // snapshot.
    pub fn rebuild_reverse_indexes(
        &self,
        db: &OptimisticTransactionDB,
    ) -> Result<(), StateMachineError> {
        let mut unassigned_tasks = self.unassigned_tasks.unassigned_tasks.write().unwrap();
        let mut unprocessed_state_changes_guard = self
            .unprocessed_state_changes
            .unprocessed_state_changes
            .write()
            .unwrap();
        let mut extractor_executors_table = self
            .extractor_executors_table
            .extractor_executors_table
            .write()
            .unwrap();
        let mut namespace_index_table = self
            .namespace_index_table
            .namespace_index_table
            .write()
            .unwrap();
        let mut unfinished_tasks_by_extractor = self
            .unfinished_tasks_by_extractor
            .unfinished_tasks_by_extractor
            .write()
            .unwrap();
        let mut unfinished_tasks_by_executor = self.unfinished_tasks_by_executor.write().unwrap();
        let mut schemas_by_namespace = self
            .schemas_by_namespace
            .schemas_by_namespace
            .write()
            .unwrap();
        let mut content_children_table = self
            .content_children_table
            .content_children_table
            .write()
            .unwrap();
        let mut graphs = self
            .extraction_graphs_by_ns
            .eg_by_namespace
            .write()
            .unwrap();
        let mut graph_links = self.graph_links.write().unwrap();

        self.rebuild_graph_links(db, &mut graph_links)?;

        for eg in self.iter_cf(db, StateMachineColumns::ExtractionGraphs) {
            let eg = eg?;
            let eg: ExtractionGraph = eg.1;
            graphs
                .entry(eg.namespace.clone())
                .or_default()
                .insert(eg.id.clone());
            for policy in eg.extraction_policies {
                graph_links
                    .entry(ExtractionGraphNode {
                        namespace: eg.namespace.clone(),
                        graph_name: eg.name.clone(),
                        source: policy.content_source.clone(),
                    })
                    .or_default()
                    .insert(policy.id.clone());
            }
        }

        for task in self.iter_cf::<Task>(db, StateMachineColumns::Tasks) {
            let (_, task) = task?;
            if !task.terminal_state() {
                unassigned_tasks.insert(task.id.clone(), task.creation_time);
                unfinished_tasks_by_extractor
                    .entry(task.extractor.clone())
                    .or_default()
                    .insert(task.id.clone());
            }
        }

        for task_assignment in
            self.iter_cf::<HashSet<TaskId>>(db, StateMachineColumns::TaskAssignments)
        {
            let (executor_key, task_ids) = task_assignment?;
            let executor_id = String::from_utf8(executor_key.to_vec()).map_err(|e| {
                StateMachineError::DatabaseError(format!("Failed to decode executor key: {}", e))
            })?;
            for task_id in task_ids {
                let creation_time = unassigned_tasks.remove(&task_id);
                if let Some(creation_time) = creation_time {
                    unfinished_tasks_by_executor
                        .entry(executor_id.clone())
                        .or_default()
                        .insert(UnfinishedTask {
                            id: task_id.clone(),
                            creation_time,
                        });
                }
            }
        }

        let mut max_change_id = None;
        for state_change in self.iter_cf::<StateChange>(db, StateMachineColumns::StateChanges) {
            let (_, state_change) = state_change?;
            if state_change.processed_at.is_none() {
                unprocessed_state_changes_guard.insert(state_change.id);
            }
            max_change_id = std::cmp::max(max_change_id, Some(state_change.id));
        }
        *self.change_id.lock().unwrap() = match max_change_id {
            Some(val) => Into::<u64>::into(val) + 1,
            None => 0,
        };

        *self.content_offset.lock().unwrap() = ContentOffset(max_content_offset(db)?);

        for content in self.iter_cf::<ContentMetadata>(db, StateMachineColumns::ContentTable) {
            let (_, content) = content?;
            if let Some(parent_id) = &content.parent_id {
                content_children_table
                    .entry(parent_id.clone())
                    .or_default()
                    .insert(content.id.clone());
            }
        }

        for executor in self.iter_cf::<ExecutorMetadata>(db, StateMachineColumns::Executors) {
            let (_, executor) = executor?;
            for extractor in &executor.extractors {
                extractor_executors_table
                    .entry(extractor.name.clone())
                    .or_default()
                    .insert(executor.id.clone());
            }
        }

        for index in self.iter_cf::<Index>(db, StateMachineColumns::IndexTable) {
            let (_, index) = index?;
            namespace_index_table
                .entry(index.namespace.clone())
                .or_default()
                .insert(index.id.clone());
        }

        for schema in
            self.iter_cf::<StructuredDataSchema>(db, StateMachineColumns::StructuredDataSchemas)
        {
            let (_, schema) = schema?;
            schemas_by_namespace
                .entry(schema.namespace.clone())
                .or_default()
                .insert(schema.id.clone());
        }

        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Default, Debug)]
pub struct V1Snapshot {
    pub executors: HashMap<ExecutorId, ExecutorMetadata>,
    pub tasks: HashMap<TaskId, v1::Task>,
    pub gc_tasks: HashMap<
        indexify_internal_api::GarbageCollectionTaskId,
        indexify_internal_api::GarbageCollectionTask,
    >,
    pub task_assignments: HashMap<ExecutorId, HashSet<TaskId>>,
    pub state_changes: HashMap<StateChangeId, StateChange>,
    pub content_table: HashMap<ContentMetadataId, v1::ContentMetadata>,
    pub extraction_policies: HashMap<ExtractionPolicyId, v1::ExtractionPolicy>,
    pub extractors: HashMap<ExtractorName, ExtractorDescription>,
    pub namespaces: HashSet<NamespaceName>,
    pub index_table: HashMap<String, Index>,
    pub structured_data_schemas: HashMap<String, StructuredDataSchema>,
    pub coordinator_address: HashMap<NodeId, String>,
    pub extraction_graphs: HashMap<ExtractionGraphId, v1::ExtractionGraph>,
    pub metrics: Metrics,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_increment_running_task_count() {
        let executor_running_task_count = ExecutorRunningTaskCount::new();
        let executor_id = "executor_id".to_string();
        executor_running_task_count.insert(&executor_id, 0);
        executor_running_task_count.increment_running_task_count(&executor_id);
        assert_eq!(executor_running_task_count.get(&executor_id).unwrap(), 1);
        executor_running_task_count.increment_running_task_count(&executor_id);
        assert_eq!(executor_running_task_count.get(&executor_id).unwrap(), 2);
    }

    #[test]
    fn test_decrement_running_task_count() {
        let executor_running_task_count = ExecutorRunningTaskCount::new();
        let executor_id = "executor_id".to_string();
        executor_running_task_count.insert(&executor_id, 2);
        executor_running_task_count.decrement_running_task_count(&executor_id);
        assert_eq!(executor_running_task_count.get(&executor_id).unwrap(), 1);
        executor_running_task_count.decrement_running_task_count(&executor_id);
        assert_eq!(executor_running_task_count.get(&executor_id).unwrap(), 0);
    }
}
