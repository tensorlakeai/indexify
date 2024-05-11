use core::fmt;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, RwLock},
    time::SystemTime,
};

use anyhow::{anyhow, Result};
use indexify_internal_api as internal_api;
use internal_api::{
    ContentMetadataId,
    ExtractionGraph,
    ExtractionPolicy,
    ExtractionPolicyName,
    ExtractorDescription,
    StateChange,
    TaskOutcome,
};
use itertools::Itertools;
use opentelemetry::metrics::AsyncInstrument;
use rocksdb::OptimisticTransactionDB;
use serde::de::DeserializeOwned;
use tracing::{error, warn};

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
use crate::state::NodeId;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct UnassignedTasks {
    unassigned_tasks: Arc<RwLock<HashSet<TaskId>>>,
}

impl UnassignedTasks {
    pub fn insert(&self, task_id: &TaskId) {
        let mut guard = self.unassigned_tasks.write().unwrap();
        guard.insert(task_id.into());
    }

    pub fn remove(&self, task_id: &TaskId) {
        let mut guard = self.unassigned_tasks.write().unwrap();
        guard.remove(task_id);
    }

    pub fn inner(&self) -> HashSet<TaskId> {
        let guard = self.unassigned_tasks.read().unwrap();
        guard.clone()
    }

    pub fn set(&self, tasks: HashSet<TaskId>) {
        let mut guard = self.unassigned_tasks.write().unwrap();
        *guard = tasks;
    }

    pub fn count(&self) -> usize {
        let guard = self.unassigned_tasks.read().unwrap();
        guard.len()
    }
}

impl From<HashSet<TaskId>> for UnassignedTasks {
    fn from(tasks: HashSet<TaskId>) -> Self {
        let unassigned_tasks = Arc::new(RwLock::new(tasks));
        Self { unassigned_tasks }
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
pub struct ExtractionPoliciesTable {
    extraction_policies_table: Arc<RwLock<HashMap<NamespaceName, HashSet<String>>>>,
}

impl ExtractionPoliciesTable {
    pub fn get(&self, namespace: &NamespaceName) -> HashSet<String> {
        let guard = self.extraction_policies_table.read().unwrap();
        guard.get(namespace).cloned().unwrap_or_default()
    }

    pub fn insert(&self, namespace: &NamespaceName, extraction_policy_id: &str) {
        let mut guard = self.extraction_policies_table.write().unwrap();
        guard
            .entry(namespace.clone())
            .or_default()
            .insert(extraction_policy_id.to_owned());
    }

    pub fn remove(&self, namespace: &NamespaceName, extraction_policy_id: &str) {
        let mut guard = self.extraction_policies_table.write().unwrap();
        guard
            .entry(namespace.clone())
            .or_default()
            .remove(extraction_policy_id);
    }

    pub fn inner(&self) -> HashMap<NamespaceName, HashSet<String>> {
        let guard = self.extraction_policies_table.read().unwrap();
        guard.clone()
    }
}

impl From<HashMap<NamespaceName, HashSet<String>>> for ExtractionPoliciesTable {
    fn from(extraction_policies_table: HashMap<NamespaceName, HashSet<String>>) -> Self {
        let extraction_policies_table = Arc::new(RwLock::new(extraction_policies_table));
        Self {
            extraction_policies_table,
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
pub struct PendingTasksForContent {
    pending_tasks_for_content:
        Arc<RwLock<HashMap<ContentMetadataId, HashMap<ExtractionPolicyId, HashSet<TaskId>>>>>,
}

impl PendingTasksForContent {
    pub fn insert(
        &self,
        content_id: &ContentMetadataId,
        extraction_policy_id: &ExtractionPolicyId,
        task_id: &TaskId,
    ) {
        let mut guard = self.pending_tasks_for_content.write().unwrap();
        let policies_map = guard.entry(content_id.clone()).or_default();
        let tasks_set = policies_map
            .entry(extraction_policy_id.clone())
            .or_default();
        tasks_set.insert(task_id.clone());
    }

    pub fn remove(
        &self,
        content_id: &ContentMetadataId,
        extraction_policy_id: &ExtractionPolicyId,
        task_id: &TaskId,
    ) {
        let mut guard = self.pending_tasks_for_content.write().unwrap();
        if let Some(extraction_policies_map) = guard.get_mut(content_id) {
            if let Some(task_ids) = extraction_policies_map.get_mut(extraction_policy_id) {
                task_ids.remove(task_id);
                if task_ids.is_empty() {
                    extraction_policies_map.remove(extraction_policy_id);
                }
            }
            if extraction_policies_map.is_empty() {
                guard.remove(content_id);
            }
        }
    }

    pub fn are_content_tasks_completed(&self, content_id: &ContentMetadataId) -> bool {
        let guard = self.pending_tasks_for_content.read().unwrap();
        guard.get(content_id).is_none()
    }

    pub fn inner(
        &self,
    ) -> HashMap<ContentMetadataId, HashMap<ExtractionPolicyId, HashSet<TaskId>>> {
        let guard = self.pending_tasks_for_content.read().unwrap();
        guard.clone()
    }
}

impl From<HashMap<ContentMetadataId, HashMap<ExtractionPolicyId, HashSet<TaskId>>>>
    for PendingTasksForContent
{
    fn from(
        pending_tasks_for_content: HashMap<
            ContentMetadataId,
            HashMap<ExtractionPolicyId, HashSet<TaskId>>,
        >,
    ) -> Self {
        let pending_tasks_for_content = Arc::new(RwLock::new(pending_tasks_for_content));
        Self {
            pending_tasks_for_content,
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

    /// Number of contents extacted
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

#[derive(thiserror::Error, Debug, serde::Serialize, serde::Deserialize, Default)]
pub struct IndexifyState {
    // Reverse Indexes
    /// The tasks that are currently unassigned
    pub unassigned_tasks: UnassignedTasks,

    /// State changes that have not been processed yet
    pub unprocessed_state_changes: UnprocessedStateChanges,

    /// Namespace -> Content ID
    pub content_namespace_table: ContentNamespaceTable,

    /// Namespace -> Extraction policy id
    pub extraction_policies_table: ExtractionPoliciesTable,

    /// Extractor -> Executors table
    pub extractor_executors_table: ExtractorExecutorsTable,

    /// Namespace -> Index id
    pub namespace_index_table: NamespaceIndexTable,

    // extraction_policy_id -> List[IndexIds]
    extraction_policy_index_mapping: HashMap<String, HashSet<String>>,

    /// Tasks that are currently unfinished, by extractor. Once they are
    /// finished, they are removed from this set.
    /// Extractor name -> Task Ids
    pub unfinished_tasks_by_extractor: UnfinishedTasksByExtractor,

    /// Number of tasks currently running on each executor
    /// Executor id -> number of tasks running on executor
    pub executor_running_task_count: ExecutorRunningTaskCount,

    /// Namespace -> Schemas
    pub schemas_by_namespace: SchemasByNamespace,

    /// Parent content id -> children content id's
    pub content_children_table: ContentChildrenTable,

    /// content id -> Map<ExtractionPolicyId, HashSet<TaskId>>
    pub pending_tasks_for_content: PendingTasksForContent,

    /// Metrics
    pub metrics: std::sync::Mutex<Metrics>,

    /// Namespace -> Extraction Graph ID
    extraction_graphs_by_ns: ExtractionGraphTable,
}

impl fmt::Display for IndexifyState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IndexifyState {{ unassigned_tasks: {:?}, unprocessed_state_changes: {:?}, content_namespace_table: {:?}, extraction_policies_table: {:?}, extractor_executors_table: {:?}, namespace_index_table: {:?}, unfinished_tasks_by_extractor: {:?}, executor_running_task_count: {:?}, schemas_by_namespace: {:?} }}, content_children_table: {:?}",
            self.unassigned_tasks,
            self.unprocessed_state_changes,
            self.content_namespace_table,
            self.extraction_policies_table,
            self.extractor_executors_table,
            self.namespace_index_table,
            self.unfinished_tasks_by_extractor,
            self.executor_running_task_count,
            self.schemas_by_namespace,
            self.content_children_table,
        )
    }
}

impl IndexifyState {
    fn set_extraction_graph(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        extraction_graph: &ExtractionGraph,
        structured_data_schema: &internal_api::StructuredDataSchema,
    ) -> Result<(), StateMachineError> {
        let serialized_eg = JsonEncoder::encode(extraction_graph)?;
        let _ = txn
            .put_cf(
                &StateMachineColumns::ExtractionGraphs.cf(db),
                &extraction_graph.id,
                serialized_eg,
            )
            .map_err(|e| StateMachineError::DatabaseError(e.to_string()));
        for ep in extraction_graph.extraction_policies.to_owned() {
            self.set_extraction_policy(db, txn, &ep)?;
        }
        self.set_schema(db, txn, structured_data_schema)?;
        Ok(())
    }

    fn set_new_state_changes(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        state_changes: &Vec<StateChange>,
    ) -> Result<(), StateMachineError> {
        for change in state_changes {
            let serialized_change = JsonEncoder::encode(change)?;
            txn.put_cf(
                StateMachineColumns::StateChanges.cf(db),
                &change.id,
                &serialized_change,
            )
            .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
        }
        Ok(())
    }

    fn set_processed_state_changes(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        state_changes: &Vec<StateChangeProcessed>,
    ) -> Result<(), StateMachineError> {
        let state_changes_cf = StateMachineColumns::StateChanges.cf(db);

        for change in state_changes {
            let result = txn
                .get_cf(state_changes_cf, &change.state_change_id)
                .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
            let result = result
                .ok_or_else(|| StateMachineError::DatabaseError("State change not found".into()))?;

            let mut state_change = JsonEncoder::decode::<StateChange>(&result)?;
            state_change.processed_at = Some(change.processed_at);
            let serialized_change = JsonEncoder::encode(&state_change)?;
            txn.put_cf(
                state_changes_cf,
                &change.state_change_id,
                &serialized_change,
            )
            .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
        }
        Ok(())
    }

    fn set_index(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        index: &internal_api::Index,
        id: &String,
    ) -> Result<(), StateMachineError> {
        let serialized_index = JsonEncoder::encode(index)?;
        txn.put_cf(StateMachineColumns::IndexTable.cf(db), id, serialized_index)
            .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    fn set_tasks(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        tasks: &Vec<internal_api::Task>,
    ) -> Result<(), StateMachineError> {
        // content_id -> Set(Extraction Policy Ids)
        for task in tasks {
            let serialized_task = JsonEncoder::encode(task)?;
            txn.put_cf(
                StateMachineColumns::Tasks.cf(db),
                task.id.clone(),
                &serialized_task,
            )
            .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
            self.update_content_extraction_policy_state(
                db,
                txn,
                &task.content_metadata.id,
                &task.extraction_policy_id,
                SystemTime::UNIX_EPOCH,
            )?;
        }
        Ok(())
    }

    fn update_tasks(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        tasks: Vec<&internal_api::Task>,
        update_time: SystemTime,
    ) -> Result<(), StateMachineError> {
        for task in tasks {
            let serialized_task = JsonEncoder::encode(task)?;
            txn.put_cf(
                StateMachineColumns::Tasks.cf(db),
                task.id.clone(),
                &serialized_task,
            )
            .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
            if task.terminal_state() {
                self.update_content_extraction_policy_state(
                    db,
                    txn,
                    &task.content_metadata.id,
                    &task.extraction_policy_id,
                    update_time,
                )?;
            }
        }
        Ok(())
    }

    fn set_garbage_collection_tasks(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        garbage_collection_tasks: &Vec<internal_api::GarbageCollectionTask>,
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

    fn update_garbage_collection_tasks(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
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
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
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
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
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
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
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

    fn set_content(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        contents_vec: &Vec<internal_api::ContentMetadata>,
    ) -> Result<(), StateMachineError> {
        for content in contents_vec {
            let content_key = format!("{}::v{}", content.id.id, content.id.version);
            let serialized_content = JsonEncoder::encode(content)?;
            txn.put_cf(
                StateMachineColumns::ContentTable.cf(db),
                content_key,
                &serialized_content,
            )
            .map_err(|e| {
                StateMachineError::DatabaseError(format!("error writing content: {}", e))
            })?;
        }
        Ok(())
    }

    fn tombstone_content_tree(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        content_metadata: &Vec<indexify_internal_api::ContentMetadata>,
    ) -> Result<(), StateMachineError> {
        for content in content_metadata {
            let content_key = format!("{}::v{}", content.id.id, content.id.version);
            let serialized_content = JsonEncoder::encode(content)?;
            txn.put_cf(
                StateMachineColumns::ContentTable.cf(db),
                content_key,
                &serialized_content,
            )
            .map_err(|e| {
                StateMachineError::DatabaseError(format!("error writing content: {}", e))
            })?;
        }

        Ok(())
    }

    /// Function to delete content based on content ids
    fn delete_content(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        content_ids: Vec<ContentMetadataId>,
    ) -> Result<(), StateMachineError> {
        for content_id in content_ids {
            txn.delete_cf(
                StateMachineColumns::ContentTable.cf(db),
                &format!("{}::v{}", content_id.id, content_id.version),
            )
            .map_err(|e| {
                StateMachineError::TransactionError(format!(
                    "error in txn while trying to delete content: {}",
                    e
                ))
            })?;
        }
        Ok(())
    }

    fn set_executor(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        addr: String,
        executor_id: &str,
        extractors: &Vec<ExtractorDescription>,
        ts_secs: &u64,
    ) -> Result<(), StateMachineError> {
        let serialized_executor = JsonEncoder::encode(&internal_api::ExecutorMetadata {
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
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        executor_id: &str,
    ) -> Result<Option<internal_api::ExecutorMetadata>, StateMachineError> {
        //  Get a handle on the executor before deleting it from the DB
        let executors_cf = StateMachineColumns::Executors.cf(db);
        match txn.get_cf(executors_cf, executor_id).map_err(|e| {
            StateMachineError::DatabaseError(format!("Error reading executor: {}", e))
        })? {
            Some(executor) => {
                let executor_meta =
                    JsonEncoder::decode::<internal_api::ExecutorMetadata>(&executor)?;
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
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
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
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
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
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
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
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        schema: &internal_api::StructuredDataSchema,
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
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        content_id: &ContentMetadataId,
        extraction_policy_id: &str,
        policy_completion_time: SystemTime,
    ) -> Result<(), StateMachineError> {
        let value = txn
            .get_cf(
                StateMachineColumns::ContentTable.cf(db),
                format!("{}::v{}", content_id.id, content_id.version),
            )
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
        let mut content_meta = JsonEncoder::decode::<internal_api::ContentMetadata>(&value)?;
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
            format!("{}::v{}", content_id.id, content_id.version),
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
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
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

    fn update_schema_reverse_idx(&self, schema: internal_api::StructuredDataSchema) {
        self.schemas_by_namespace
            .insert(&schema.namespace, &schema.id);
    }

    fn update_extraction_graph_reverse_idx(
        &self,
        extraction_graph: &ExtractionGraph,
        schema: internal_api::StructuredDataSchema,
    ) {
        for ep in &extraction_graph.extraction_policies {
            self.extraction_policies_table.insert(&ep.namespace, &ep.id);
        }
        self.update_schema_reverse_idx(schema);
        self.extraction_graphs_by_ns
            .insert(&extraction_graph.namespace, &extraction_graph.id);
    }

    /// This method will make all state machine forward index writes to RocksDB
    pub fn apply_state_machine_updates(
        &self,
        request: StateMachineUpdateRequest,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<(), StateMachineError> {
        let txn = db.transaction();

        self.set_new_state_changes(db, &txn, &request.new_state_changes)?;
        self.set_processed_state_changes(db, &txn, &request.state_changes_processed)?;

        match &request.payload {
            RequestPayload::SetIndex { indexes } => {
                for index in indexes {
                    self.set_index(db, &txn, index, &index.id)?;
                }
            }
            RequestPayload::CreateTasks { tasks } => {
                self.set_tasks(db, &txn, tasks)?;
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
                    self.delete_content(db, &txn, vec![gc_task.content_id.clone()])?;
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
                content_metadata,
                update_time,
            } => {
                self.update_tasks(db, &txn, vec![task], *update_time)?;
                self.set_content(db, &txn, content_metadata)?;

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

                // Remove all tasks assigned to this executor and get a handle on the task ids
                let task_ids = self.delete_task_assignments_for_executor(db, &txn, executor_id)?;

                txn.commit()
                    .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;

                //  Remove the extractors from the executor -> extractor mapping table
                if let Some(executor_meta) = executor_meta {
                    for extractor in &executor_meta.extractors {
                        self.extractor_executors_table
                            .remove(&extractor.name, &executor_meta.id);
                    }
                }

                //  Put the tasks of the deleted executor into the unassigned tasks list
                for task_id in task_ids {
                    self.unassigned_tasks.insert(&task_id);
                }

                // Remove from the executor load table
                self.executor_running_task_count.remove(executor_id);

                return Ok(());
            }
            RequestPayload::CreateContent { content_metadata } => {
                self.set_content(db, &txn, content_metadata)?;
            }
            RequestPayload::UpdateContent { content_metadata } => {
                self.set_content(db, &txn, content_metadata)?;
            }
            RequestPayload::TombstoneContentTree { content_metadata } => {
                self.tombstone_content_tree(db, &txn, content_metadata)?;
            }
            RequestPayload::CreateExtractionPolicy {
                extraction_policy,
                updated_structured_data_schema: _,
                new_structured_data_schema: _,
            } => {
                self.set_extraction_policy(db, &txn, extraction_policy)?;
            }
            RequestPayload::CreateNamespace { name } => {
                self.set_namespace(db, &txn, name)?;
            }
            RequestPayload::MarkStateChangesProcessed { state_changes } => {
                self.set_processed_state_changes(db, &txn, state_changes)?;
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
        };

        self.update_reverse_indexes(request).map_err(|e| {
            StateMachineError::ExternalError(anyhow!(
                "Error while applying reverse index updates: {}",
                e
            ))
        })?;

        txn.commit()
            .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;

        Ok(())
    }

    /// This method handles all reverse index writes. All reverse indexes are
    /// written in memory
    pub fn update_reverse_indexes(&self, request: StateMachineUpdateRequest) -> Result<()> {
        for change in request.new_state_changes {
            self.unprocessed_state_changes.insert(change.id.clone());
        }
        for change in request.state_changes_processed {
            self.mark_state_changes_processed(&change, change.processed_at);
        }
        match request.payload {
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

                let _executor_info = internal_api::ExecutorMetadata {
                    id: executor_id.clone(),
                    last_seen: ts_secs,
                    addr: addr.clone(),
                    extractors: extractors.clone(),
                };
                // initialize executor load at 0
                self.executor_running_task_count.insert(&executor_id, 0);
                Ok(())
            }
            RequestPayload::CreateTasks { tasks } => {
                for task in tasks {
                    self.unassigned_tasks.insert(&task.id);
                    self.unfinished_tasks_by_extractor
                        .insert(&task.extractor, &task.id);
                    self.pending_tasks_for_content.insert(
                        &task.content_metadata.id,
                        &task.extraction_policy_id,
                        &task.id,
                    );
                }
                Ok(())
            }
            RequestPayload::AssignTask { assignments } => {
                for (task_id, executor_id) in assignments {
                    self.unassigned_tasks.remove(&task_id);

                    self.executor_running_task_count
                        .increment_running_task_count(&executor_id);
                }
                Ok(())
            }
            RequestPayload::CreateOrAssignGarbageCollectionTask { gc_tasks: _ } => Ok(()),
            RequestPayload::UpdateGarbageCollectionTask {
                gc_task,
                mark_finished,
            } => {
                if mark_finished {
                    self.content_children_table.remove_all(&gc_task.content_id);
                }
                Ok(())
            }
            RequestPayload::CreateContent { content_metadata } => {
                for content in content_metadata {
                    self.content_namespace_table
                        .insert(&content.namespace, &content.id);
                    let mut guard = self.metrics.lock().unwrap();
                    if let Some(parent_id) = content.parent_id {
                        self.content_children_table.insert(&parent_id, &content.id);
                        guard.content_extracted += 1;
                        guard.content_extracted_bytes += content.size_bytes;
                    } else {
                        guard.content_uploads += 1;
                        guard.content_bytes += content.size_bytes;
                    }
                }
                Ok(())
            }
            RequestPayload::UpdateContent { content_metadata } => {
                for content in content_metadata {
                    if let Some(parent_id) = content.parent_id {
                        let old_parent = ContentMetadataId::new_with_version(
                            &parent_id.id,
                            parent_id.version - 1,
                        );
                        self.content_children_table.remove(&old_parent, &content.id);
                        self.content_children_table.insert(&parent_id, &content.id);
                    }
                }
                Ok(())
            }
            RequestPayload::CreateExtractionPolicy {
                extraction_policy,
                updated_structured_data_schema,
                new_structured_data_schema,
            } => {
                self.extraction_policies_table
                    .insert(&extraction_policy.namespace, &extraction_policy.id);
                if let Some(schema) = updated_structured_data_schema {
                    self.update_schema_reverse_idx(schema);
                }
                self.update_schema_reverse_idx(new_structured_data_schema);
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
            RequestPayload::CreateNamespace { name: _ } => Ok(()),
            RequestPayload::UpdateTask {
                task,
                executor_id,
                content_metadata,
                update_time: _,
            } => {
                if task.terminal_state() {
                    self.unassigned_tasks.remove(&task.id);
                    self.unfinished_tasks_by_extractor
                        .remove(&task.extractor, &task.id);
                    if let Some(ref executor_id) = executor_id {
                        self.executor_running_task_count
                            .decrement_running_task_count(executor_id);
                    }
                    let content_id = task.content_metadata.id;
                    self.pending_tasks_for_content.remove(
                        &content_id,
                        &task.extraction_policy_id,
                        &task.id,
                    );
                }
                for content in content_metadata {
                    self.content_namespace_table
                        .insert(&content.namespace, &content.id);
                }
                Ok(())
            }
            RequestPayload::MarkStateChangesProcessed { state_changes } => {
                for state_change in state_changes {
                    self.mark_state_changes_processed(&state_change, state_change.processed_at);
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    //  START READER METHODS FOR ROCKSDB FORWARD INDEXES

    /// This function is a helper method that will get the latest version of any
    /// piece of content in the database by building a prefix foward iterator
    pub fn get_latest_version_of_content(
        &self,
        content_id: &str,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
    ) -> Result<Option<internal_api::ContentMetadata>, StateMachineError> {
        let prefix = format!("{}::v", content_id);

        let mut read_opts = rocksdb::ReadOptions::default();
        read_opts.set_prefix_same_as_start(true);
        let iter = txn.iterator_cf(
            StateMachineColumns::ContentTable.cf(db),
            rocksdb::IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Forward),
        );

        let mut latest_content_metada: Option<internal_api::ContentMetadata> = None;

        for item in iter {
            match item {
                Ok((key, value)) => {
                    let content_metadata =
                        JsonEncoder::decode::<indexify_internal_api::ContentMetadata>(&value)?;
                    if content_metadata.tombstoned {
                        continue;
                    }
                    if let Ok(key_str) = std::str::from_utf8(&key) {
                        if let Some(version_str) = key_str.strip_prefix(&prefix) {
                            if let Ok(version) = version_str.parse::<u64>() {
                                latest_content_metada = Some(match latest_content_metada {
                                    Some(current_high) if version > current_high.id.version => {
                                        content_metadata
                                    }
                                    None => content_metadata,
                                    Some(current_high) => current_high,
                                });
                            }
                        }
                    }
                }
                Err(e) => return Err(StateMachineError::TransactionError(e.to_string())),
            }
        }

        Ok(latest_content_metada)
    }

    /// This method fetches a key from a specific column family
    pub fn get_from_cf<T, K>(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        column: StateMachineColumns,
        key: K,
    ) -> Result<Option<T>, anyhow::Error>
    where
        T: DeserializeOwned,
        K: AsRef<[u8]>,
    {
        let result_bytes = match db.get_cf(column.cf(db), key)? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        let result = JsonEncoder::decode::<T>(&result_bytes)
            .map_err(|e| anyhow::anyhow!("Deserialization error: {}", e))?;

        Ok(Some(result))
    }

    /// This method is used to get the tasks assigned to an executor
    /// It does this by looking up the TaskAssignments CF to get the task id's
    /// and then using those id's to look up tasks via Tasks CF
    pub fn get_tasks_for_executor(
        &self,
        executor_id: &str,
        limit: Option<u64>,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Vec<indexify_internal_api::Task>, StateMachineError> {
        //  NOTE: Don't do deserialization within the transaction
        let txn = db.transaction();
        let task_ids_bytes = txn
            .get_cf(StateMachineColumns::TaskAssignments.cf(db), executor_id)
            .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;

        let task_ids: Vec<String> = task_ids_bytes
            .map(|task_id_bytes| {
                JsonEncoder::decode(&task_id_bytes)
                    .map_err(StateMachineError::from)
                    .unwrap_or_else(|e| {
                        error!("Failed to deserialize task id: {}", e);
                        Vec::new()
                    })
            })
            .unwrap_or_else(Vec::new);

        // FIXME Use MULTIGET
        let limit = limit.unwrap_or(task_ids.len() as u64) as usize;

        let tasks: Result<Vec<indexify_internal_api::Task>, StateMachineError> = task_ids
            .into_iter()
            .take(limit)
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
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Vec<indexify_internal_api::Index>, StateMachineError> {
        let txn = db.transaction();
        let indexes: Result<Vec<indexify_internal_api::Index>, StateMachineError> = task_ids
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
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Vec<indexify_internal_api::ExecutorMetadata>, StateMachineError> {
        let txn = db.transaction();
        let executors: Result<Vec<indexify_internal_api::ExecutorMetadata>, StateMachineError> =
            executor_ids
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

    /// This method will fetch content based on the id and version provided.
    /// It will skip over any content that it cannot find
    pub fn get_content_from_ids_with_version(
        &self,
        content_ids: Vec<ContentMetadataId>,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Vec<Option<indexify_internal_api::ContentMetadata>>, StateMachineError> {
        let txn = db.transaction();
        let keys = content_ids
            .iter()
            .map(|id| {
                (
                    StateMachineColumns::ContentTable.cf(db),
                    format!("{}::v{}", id.id, id.version),
                )
            })
            .collect_vec();
        let mut content_metadatas = Vec::new();

        let content_metadata_bytes = txn.multi_get_cf(keys);
        for content_metadata in content_metadata_bytes {
            let content_metadata =
                content_metadata.map_err(|e| StateMachineError::TransactionError(e.to_string()))?;
            match content_metadata {
                Some(content_bytes) => {
                    let content = JsonEncoder::decode::<indexify_internal_api::ContentMetadata>(
                        &content_bytes,
                    )
                    .map_err(StateMachineError::from)?;
                    content_metadatas.push(Some(content));
                }
                None => {
                    content_metadatas.push(None);
                }
            }
        }
        Ok(content_metadatas)
    }

    pub fn get_content_by_id_and_version(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        content_id: &ContentMetadataId,
    ) -> Result<Option<indexify_internal_api::ContentMetadata>, StateMachineError> {
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
        let content_metadata = JsonEncoder::decode::<indexify_internal_api::ContentMetadata>(
            &content_metadata_bytes.unwrap(),
        )?;
        Ok(Some(content_metadata))
    }

    /// This method will fetch content based on the id's provided. It will look
    /// for the latest version for each piece of content It will skip any
    /// that cannot be found and expect the consumer to decide what to do in
    /// that case It will also skip any that have been tombstoned
    pub fn get_content_from_ids(
        &self,
        content_ids: HashSet<String>,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Vec<indexify_internal_api::ContentMetadata>, StateMachineError> {
        let txn = db.transaction();
        let mut contents = Vec::new();

        //  For each content id find the highest version, deserialize it and collect it
        for content_id in &content_ids {
            // Construct prefix for content ID to search for all its versions
            let highest_version = self.get_latest_version_of_content(content_id, db, &txn)?;

            if highest_version.is_none() {
                continue;
            }
            contents.push(highest_version.unwrap());
        }
        Ok(contents)
    }

    /// This method will fetch all pieces of content metadata for the tree
    /// rooted at content_id. It will look for the latest version of each node
    pub fn get_content_tree_metadata(
        &self,
        content_id: &str,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Vec<indexify_internal_api::ContentMetadata>, StateMachineError> {
        let txn = db.transaction();
        let mut collected_content_metadata = Vec::new();

        let mut queue = VecDeque::new();
        queue.push_back(content_id.to_string());

        while let Some(current_root) = queue.pop_front() {
            let highest_version = self.get_latest_version_of_content(&current_root, db, &txn)?;
            if highest_version.is_none() {
                continue;
            }
            let highest_version = highest_version.unwrap();
            let content_bytes = txn
                .get_cf(
                    StateMachineColumns::ContentTable.cf(db),
                    &format!("{}::v{}", current_root, highest_version.id.version),
                )
                .map_err(|e| StateMachineError::TransactionError(e.to_string()))?
                .ok_or_else(|| {
                    StateMachineError::DatabaseError(format!(
                        "Content {} not found while fetching content tree",
                        &current_root
                    ))
                })?;
            let content =
                JsonEncoder::decode::<indexify_internal_api::ContentMetadata>(&content_bytes)?;
            collected_content_metadata.push(content.clone());
            let children = self.content_children_table.get_children(&content.id);
            queue.extend(children.into_iter().map(|id| id.id));
        }
        Ok(collected_content_metadata)
    }

    /// This method will fetch all pieces of content metadata for the tree
    /// rooted at content_id. It will look for a specfic version of the node
    pub fn get_content_tree_metadata_with_version(
        &self,
        content_id: &ContentMetadataId,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Vec<indexify_internal_api::ContentMetadata>, StateMachineError> {
        let txn = db.transaction();
        let mut collected_content_metadata = Vec::new();

        let mut queue = VecDeque::new();
        queue.push_back(content_id.clone());

        while let Some(current_root) = queue.pop_front() {
            let content_bytes = txn
                .get_cf(
                    StateMachineColumns::ContentTable.cf(db),
                    &format!("{}::v{}", current_root.id, current_root.version),
                )
                .map_err(|e| StateMachineError::TransactionError(e.to_string()))?
                .ok_or_else(|| {
                    StateMachineError::DatabaseError(format!(
                        "Content {} not found while fetching content tree",
                        &current_root
                    ))
                })?;
            let content =
                JsonEncoder::decode::<indexify_internal_api::ContentMetadata>(&content_bytes)?;
            collected_content_metadata.push(content.clone());
            let children = self.content_children_table.get_children(&content.id);
            queue.extend(children.into_iter());
        }
        Ok(collected_content_metadata)
    }

    /// This method tries to retrieve all policies based on id's. If it cannot
    /// find any, it skips them. If it encounters an error at any point
    /// during the transaction, it returns out immediately
    pub fn get_extraction_policies_from_ids(
        &self,
        extraction_policy_ids: HashSet<String>,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Option<Vec<ExtractionPolicy>>, StateMachineError> {
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

        if policies.is_empty() {
            Ok(None)
        } else {
            Ok(Some(policies))
        }
    }

    pub fn get_extraction_policy_by_names(
        &self,
        namespace: &str,
        graph_name: &str,
        policy_names: &HashSet<ExtractionPolicyName>,
        db: &Arc<OptimisticTransactionDB>,
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
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<HashMap<TaskId, ExecutorId>, StateMachineError> {
        let mut assignments = HashMap::new();
        let iter = db.iterator_cf(
            StateMachineColumns::TaskAssignments.cf(db),
            rocksdb::IteratorMode::Start,
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

    /// This method will get the namespace based on the key provided
    pub fn get_namespace(
        &self,
        namespace: &str,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Option<indexify_internal_api::Namespace>> {
        let ns_name = match self.get_from_cf(db, StateMachineColumns::Namespaces, namespace)? {
            Some(name) => name,
            None => return Ok(None),
        };
        let extraction_graphs_ids = self
            .extraction_graphs_by_ns
            .get(&namespace.to_string())
            .into_iter()
            .collect_vec();
        let extraction_graphs = self
            .get_extraction_graphs(&extraction_graphs_ids, db)?
            .into_iter()
            .filter_map(|eg| eg)
            .collect();

        Ok(Some(indexify_internal_api::Namespace {
            name: ns_name,
            extraction_graphs,
        }))
    }

    pub fn get_schemas(
        &self,
        ids: HashSet<String>,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Vec<internal_api::StructuredDataSchema>> {
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
        extraction_graph_ids: &Vec<ExtractionGraphId>,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Vec<Option<ExtractionGraph>>, StateMachineError> {
        let cf = StateMachineColumns::ExtractionGraphs.cf(db);
        let keys: Vec<(&rocksdb::ColumnFamily, &[u8])> = extraction_graph_ids
            .iter()
            .map(|egid| (cf, egid.as_bytes()))
            .collect();
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

    pub fn get_extraction_graphs_by_name(
        &self,
        namespace: &str,
        graph_names: &[String],
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Vec<Option<ExtractionGraph>>, StateMachineError> {
        let eg_ids: Vec<String> = graph_names
            .iter()
            .map(|name| ExtractionGraph::create_id(name, namespace))
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
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Option<String>> {
        self.get_from_cf(
            db,
            StateMachineColumns::CoordinatorAddress,
            node_id.to_string(),
        )
    }

    /// Test utility method to get all key-value pairs from a column family
    pub fn get_all_rows_from_cf<V>(
        &self,
        column: StateMachineColumns,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Vec<(String, V)>, anyhow::Error>
    where
        V: DeserializeOwned,
    {
        let cf_handle = db.cf_handle(column.as_ref()).ok_or(anyhow::anyhow!(
            "Failed to get column family {}",
            column.to_string()
        ))?;
        let iter = db.iterator_cf(cf_handle, rocksdb::IteratorMode::Start);

        iter.map(|item| {
            item.map_err(|e| anyhow::anyhow!(e))
                .and_then(|(key, value)| {
                    let key = String::from_utf8(key.to_vec())
                        .map_err(|e| anyhow::anyhow!("UTF-8 conversion error for key: {}", e))?;
                    let value = JsonEncoder::decode(&value)
                        .map_err(|e| anyhow::anyhow!("Deserialization error for value: {}", e))?;
                    Ok((key, value))
                })
        })
        .collect::<Result<Vec<(String, V)>, _>>()
    }

    //  END READER METHODS FOR ROCKSDB FORWARD INDEXES

    //  START READER METHODS FOR REVERSE INDEXES
    pub fn get_unassigned_tasks(&self) -> HashSet<TaskId> {
        self.unassigned_tasks.inner()
    }

    pub fn get_unprocessed_state_changes(&self) -> HashSet<StateChangeId> {
        self.unprocessed_state_changes.inner()
    }

    pub fn get_content_namespace_table(
        &self,
    ) -> HashMap<NamespaceName, HashSet<ContentMetadataId>> {
        self.content_namespace_table.inner()
    }

    pub fn get_extraction_policies_table(&self) -> HashMap<NamespaceName, HashSet<String>> {
        self.extraction_policies_table.inner()
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
        self.executor_running_task_count.inner()
    }

    pub fn get_schemas_by_namespace(&self) -> HashMap<NamespaceName, HashSet<SchemaId>> {
        self.schemas_by_namespace.inner()
    }

    pub fn get_content_children_table(
        &self,
    ) -> HashMap<ContentMetadataId, HashSet<ContentMetadataId>> {
        self.content_children_table.inner()
    }

    pub fn get_pending_tasks_for_content(
        &self,
    ) -> HashMap<ContentMetadataId, HashMap<ExtractionPolicyId, HashSet<TaskId>>> {
        self.pending_tasks_for_content.inner()
    }

    pub fn are_content_tasks_completed(&self, content_id: &ContentMetadataId) -> bool {
        self.pending_tasks_for_content
            .are_content_tasks_completed(content_id)
    }

    pub fn executor_count(&self) -> usize {
        self.executor_running_task_count.executor_count()
    }

    //  END READER METHODS FOR REVERSE INDEXES

    //  START WRITER METHODS FOR REVERSE INDEXES
    pub fn insert_executor_running_task_count(&self, executor_id: &str, tasks: u64) {
        self.executor_running_task_count
            .insert(&executor_id.to_string(), tasks);
    }

    //  END WRITER METHODS FOR REVERSE INDEXES

    //  START SNAPSHOT METHODS
    pub fn build_snapshot(&self) -> IndexifyStateSnapshot {
        IndexifyStateSnapshot {
            unassigned_tasks: self.get_unassigned_tasks(),
            unprocessed_state_changes: self.get_unprocessed_state_changes(),
            content_namespace_table: self.get_content_namespace_table(),
            extraction_policies_table: self.get_extraction_policies_table(),
            extractor_executors_table: self.get_extractor_executors_table(),
            namespace_index_table: self.get_namespace_index_table(),
            unfinished_tasks_by_extractor: self.get_unfinished_tasks_by_extractor(),
            executor_running_task_count: self.get_executor_running_task_count(),
            schemas_by_namespace: self.get_schemas_by_namespace(),
            content_children_table: self.get_content_children_table(),
            pending_tasks_for_content: self.get_pending_tasks_for_content(),
        }
    }

    pub fn install_snapshot(&self, snapshot: IndexifyStateSnapshot) {
        let mut unassigned_tasks_guard = self.unassigned_tasks.unassigned_tasks.write().unwrap();
        let mut unprocessed_state_changes_guard = self
            .unprocessed_state_changes
            .unprocessed_state_changes
            .write()
            .unwrap();
        let mut content_namespace_table_guard = self
            .content_namespace_table
            .content_namespace_table
            .write()
            .unwrap();
        let mut extraction_policies_table_guard = self
            .extraction_policies_table
            .extraction_policies_table
            .write()
            .unwrap();
        let mut extractor_executors_table_guard = self
            .extractor_executors_table
            .extractor_executors_table
            .write()
            .unwrap();
        let mut namespace_index_table_guard = self
            .namespace_index_table
            .namespace_index_table
            .write()
            .unwrap();
        let mut unfinished_tasks_by_extractor_guard = self
            .unfinished_tasks_by_extractor
            .unfinished_tasks_by_extractor
            .write()
            .unwrap();
        let mut executor_running_task_count_guard = self
            .executor_running_task_count
            .executor_running_task_count
            .write()
            .unwrap();
        let mut schemas_by_namespace_guard = self
            .schemas_by_namespace
            .schemas_by_namespace
            .write()
            .unwrap();
        let mut content_children_table_guard = self
            .content_children_table
            .content_children_table
            .write()
            .unwrap();

        *unassigned_tasks_guard = snapshot.unassigned_tasks;
        *unprocessed_state_changes_guard = snapshot.unprocessed_state_changes;
        *content_namespace_table_guard = snapshot.content_namespace_table;
        *extraction_policies_table_guard = snapshot.extraction_policies_table;
        *extractor_executors_table_guard = snapshot.extractor_executors_table;
        *namespace_index_table_guard = snapshot.namespace_index_table;
        *unfinished_tasks_by_extractor_guard = snapshot.unfinished_tasks_by_extractor;
        *executor_running_task_count_guard = snapshot.executor_running_task_count;
        *schemas_by_namespace_guard = snapshot.schemas_by_namespace;
        *content_children_table_guard = snapshot.content_children_table;
    }
    //  END SNAPSHOT METHODS
}

#[derive(serde::Serialize, serde::Deserialize, Default, Debug)]
pub struct IndexifyStateSnapshot {
    unassigned_tasks: HashSet<TaskId>,
    unprocessed_state_changes: HashSet<StateChangeId>,
    content_namespace_table: HashMap<NamespaceName, HashSet<ContentMetadataId>>,
    extraction_policies_table: HashMap<NamespaceName, HashSet<String>>,
    extractor_executors_table: HashMap<ExtractorName, HashSet<ExecutorId>>,
    namespace_index_table: HashMap<NamespaceName, HashSet<String>>,
    unfinished_tasks_by_extractor: HashMap<ExtractorName, HashSet<TaskId>>,
    executor_running_task_count: HashMap<ExecutorId, u64>,
    schemas_by_namespace: HashMap<NamespaceName, HashSet<SchemaId>>,
    content_children_table: HashMap<ContentMetadataId, HashSet<ContentMetadataId>>,
    pending_tasks_for_content:
        HashMap<ContentMetadataId, HashMap<ExtractionPolicyId, HashSet<TaskId>>>,
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
