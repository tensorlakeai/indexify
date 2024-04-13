use core::fmt;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, RwLock},
    time::SystemTime,
};

use anyhow::{anyhow, Result};
use indexify_internal_api as internal_api;
use internal_api::{ContentMetadataId, ExtractorDescription, StateChange};
use itertools::Itertools;
use rocksdb::OptimisticTransactionDB;
use serde::de::DeserializeOwned;
use tracing::{error, warn};

use super::{
    requests::{RequestPayload, StateChangeProcessed, StateMachineUpdateRequest},
    serializer::JsonEncode,
    ExecutorId, ExtractionPolicyId, ExtractorName, JsonEncoder, NamespaceName, SchemaId,
    StateChangeId, StateMachineColumns, StateMachineError, TaskId,
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
    executor_running_task_count: Arc<RwLock<HashMap<ExecutorId, usize>>>,
}

impl ExecutorRunningTaskCount {
    pub fn new() -> Self {
        Self {
            executor_running_task_count: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get(&self, executor_id: &ExecutorId) -> Option<usize> {
        let guard = self.executor_running_task_count.read().unwrap();
        guard.get(executor_id).copied()
    }

    pub fn insert(&self, executor_id: &ExecutorId, count: usize) {
        let mut guard = self.executor_running_task_count.write().unwrap();
        guard.insert(executor_id.clone(), count);
    }

    pub fn remove(&self, executor_id: &ExecutorId) {
        let mut guard = self.executor_running_task_count.write().unwrap();
        guard.remove(executor_id);
    }

    pub fn inner(&self) -> HashMap<ExecutorId, usize> {
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
}

impl From<HashMap<ExecutorId, usize>> for ExecutorRunningTaskCount {
    fn from(executor_running_task_count: HashMap<ExecutorId, usize>) -> Self {
        let executor_running_task_count = Arc::new(RwLock::new(executor_running_task_count));
        Self {
            executor_running_task_count,
        }
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
    content_children: Arc<RwLock<HashMap<ContentMetadataId, HashSet<ContentMetadataId>>>>,
}

impl ContentChildrenTable {
    pub fn insert(&self, parent_id: &ContentMetadataId, child_id: &ContentMetadataId) {
        let mut guard = self.content_children.write().unwrap();
        guard
            .entry(parent_id.clone())
            .or_default()
            .insert(child_id.clone());
    }

    pub fn remove(&self, parent_id: &ContentMetadataId, child_id: &ContentMetadataId) {
        let mut guard = self.content_children.write().unwrap();
        if let Some(children) = guard.get_mut(parent_id) {
            children.remove(child_id);
            if children.is_empty() {
                guard.remove(parent_id);
            }
        }
    }

    pub fn remove_all(&self, parent_id: &ContentMetadataId) {
        let mut guard = self.content_children.write().unwrap();
        guard.remove(parent_id);
    }

    pub fn get_children(&self, parent_id: &ContentMetadataId) -> HashSet<ContentMetadataId> {
        let guard = self.content_children.read().unwrap();
        guard.get(parent_id).cloned().unwrap_or_default()
    }

    pub fn replace_parent(
        &self,
        old_parent_id: &ContentMetadataId,
        new_parent_id: &ContentMetadataId,
    ) {
        let mut guard = self.content_children.write().unwrap();
        let children = guard.remove(old_parent_id).unwrap_or_default();
        guard.insert(new_parent_id.clone(), children);
    }

    pub fn inner(&self) -> HashMap<ContentMetadataId, HashSet<ContentMetadataId>> {
        let guard = self.content_children.read().unwrap();
        guard.clone()
    }
}

impl From<HashMap<ContentMetadataId, HashSet<ContentMetadataId>>> for ContentChildrenTable {
    fn from(content_children: HashMap<ContentMetadataId, HashSet<ContentMetadataId>>) -> Self {
        let content_children = Arc::new(RwLock::new(content_children));
        Self { content_children }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct ContentTaskMapping {
    content_task_mapping:
        Arc<RwLock<HashMap<ContentMetadataId, HashMap<ExtractionPolicyId, HashSet<TaskId>>>>>,
}

impl ContentTaskMapping {
    pub fn insert(
        &self,
        content_id: &ContentMetadataId,
        extraction_policy_id: &ExtractionPolicyId,
        new_task_ids: &HashSet<TaskId>,
    ) {
        let mut guard = self.content_task_mapping.write().unwrap();
        let policies_map = guard.entry(content_id.clone()).or_default();
        let tasks_set = policies_map
            .entry(extraction_policy_id.clone())
            .or_default();
        tasks_set.extend(new_task_ids.iter().cloned());
    }

    pub fn remove(
        &self,
        content_id: &ContentMetadataId,
        extraction_policy_id: &ExtractionPolicyId,
        task_id: &TaskId,
    ) {
        let mut guard = self.content_task_mapping.write().unwrap();
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

    pub fn is_content_processed(&self, content_id: &ContentMetadataId) -> bool {
        let guard = self.content_task_mapping.read().unwrap();
        !guard.get(content_id).is_some()
    }

    pub fn inner(
        &self,
    ) -> HashMap<ContentMetadataId, HashMap<ExtractionPolicyId, HashSet<TaskId>>> {
        let guard = self.content_task_mapping.read().unwrap();
        guard.clone()
    }
}

impl From<HashMap<ContentMetadataId, HashMap<ExtractionPolicyId, HashSet<TaskId>>>>
    for ContentTaskMapping
{
    fn from(
        content_task_mapping: HashMap<
            ContentMetadataId,
            HashMap<ExtractionPolicyId, HashSet<TaskId>>,
        >,
    ) -> Self {
        let content_task_mapping = Arc::new(RwLock::new(content_task_mapping));
        Self {
            content_task_mapping,
        }
    }
}

#[derive(thiserror::Error, Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct IndexifyState {
    // Reverse Indexes
    /// The tasks that are currently unassigned
    unassigned_tasks: UnassignedTasks,

    /// State changes that have not been processed yet
    unprocessed_state_changes: UnprocessedStateChanges,

    /// Namespace -> Content ID
    content_namespace_table: ContentNamespaceTable,

    /// Namespace -> Extraction policy id
    extraction_policies_table: ExtractionPoliciesTable,

    /// Extractor -> Executors table
    extractor_executors_table: ExtractorExecutorsTable,

    /// Namespace -> Index id
    namespace_index_table: NamespaceIndexTable,

    /// Tasks that are currently unfinished, by extractor. Once they are
    /// finished, they are removed from this set.
    /// Extractor name -> Task Ids
    unfinished_tasks_by_extractor: UnfinishedTasksByExtractor,

    /// Number of tasks currently running on each executor
    /// Executor id -> number of tasks running on executor
    executor_running_task_count: ExecutorRunningTaskCount,

    /// Namespace -> Schemas
    schemas_by_namespace: SchemasByNamespace,

    /// Parent content id -> children content id's
    content_children_table: ContentChildrenTable,

    /// content id -> Map<ExtractionPolicyId, HashSet<TaskId>>
    content_task_mapping: ContentTaskMapping,
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
            self.content_children_table
        )
    }
}

impl IndexifyState {
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

    fn _get_task(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        task_id: &TaskId,
    ) -> Result<internal_api::Task, StateMachineError> {
        let serialized_task = txn
            .get_cf(StateMachineColumns::Tasks.cf(db), task_id)
            .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?
            .ok_or_else(|| {
                StateMachineError::DatabaseError(format!("Task {} not found", task_id))
            })?;
        let task = JsonEncoder::decode(&serialized_task)?;
        Ok(task)
    }

    fn set_tasks(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        tasks: &Vec<internal_api::Task>,
    ) -> Result<(), StateMachineError> {
        // content_id -> Set(Extraction Policy Ids)
        let _content_extraction_policy_mappings: HashMap<String, HashSet<String>> = HashMap::new();
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

    /// This method will merge two content trees. It does this by swapping the parent pointers of all children of the old node to the new node
    /// It also updates reverse index invariants
    fn merge_content_trees(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        old_content_id: &ContentMetadataId,
        new_content_id: &ContentMetadataId,
    ) -> Result<(), StateMachineError> {
        //  get the children of the old and new nodes
        let old_children = self.content_children_table.get_children(old_content_id);
        let new_children = self.content_children_table.get_children(new_content_id);

        let preserved_ids = old_children
            .intersection(&new_children)
            .cloned()
            .collect::<HashSet<_>>();
        let to_remove_ids = old_children
            .difference(&new_children)
            .cloned()
            .collect::<HashSet<_>>();
        let to_add_ids = new_children
            .difference(&old_children)
            .cloned()
            .collect::<HashSet<_>>();

        for child_id in preserved_ids {
            let child_metadata = txn
                .get_cf(
                    StateMachineColumns::ContentTable.cf(db),
                    format!("{}::v{}", child_id.id, child_id.version),
                )
                .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?
                .ok_or_else(|| {
                    StateMachineError::DatabaseError(format!(
                        "Child content {} not found",
                        child_id
                    ))
                })?;
            let mut child_metadata =
                JsonEncoder::decode::<internal_api::ContentMetadata>(&child_metadata)?;
            child_metadata.parent_id = new_content_id.clone();
            self.content_children_table
                .remove(old_content_id, &child_id);
            self.content_children_table
                .insert(new_content_id, &child_id);
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
        let mut updated_contents = Vec::new();

        //  Update the parents of all the contents to point to the latest version
        for content in contents_vec.iter().cloned() {
            let mut updated_content = content;
            if !updated_content.parent_id.id.is_empty() {
                let parent_latest_version =
                    self.get_latest_version_of_content(&updated_content.parent_id.id, db, txn)?;
                if parent_latest_version == 0 {
                    return Err(StateMachineError::DatabaseError(format!(
                        "Parent content {} not found",
                        updated_content.parent_id.id
                    )));
                }
                // Update the parent_id version to the latest version
                updated_content.parent_id.version = parent_latest_version;
            }

            updated_contents.push(updated_content);
        }

        for updated_content in updated_contents {
            let content_key = format!("{}::v{}", updated_content.id.id, updated_content.id.version);
            let serialized_content = JsonEncoder::encode(&updated_content)?;
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

    /// This method updates existing content. It changes the pointers of the
    /// children of the node being updated to point to the new node and then
    /// creates the new node. It does nothing to the old node
    fn update_content(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        updated_content_map: &HashMap<String, internal_api::ContentMetadata>,
    ) -> Result<(), StateMachineError> {
        for (old_content_key, new_content_data) in updated_content_map.iter() {
            let old_content_key: ContentMetadataId = old_content_key.clone().try_into()?;
            let serialized_content = JsonEncoder::encode(new_content_data)?;

            //  update the children so that they point to the new node
            for child in self.content_children_table.get_children(&old_content_key) {
                let child_content_key = format!("{}::v{}", child.id, child.version);
                let child_content = txn
                    .get_cf(StateMachineColumns::ContentTable.cf(db), &child_content_key)
                    .map_err(|e| {
                        StateMachineError::DatabaseError(format!(
                            "Error reading child content: {}",
                            e
                        ))
                    })?
                    .ok_or_else(|| {
                        StateMachineError::DatabaseError(format!(
                            "Child content {} not found",
                            child_content_key
                        ))
                    })?;
                let mut child_content =
                    JsonEncoder::decode::<internal_api::ContentMetadata>(&child_content)?;
                child_content.parent_id = new_content_data.id.clone();
                let serialized_child_content = JsonEncoder::encode(&child_content)?;
                txn.put_cf(
                    StateMachineColumns::ContentTable.cf(db),
                    child_content_key,
                    &serialized_child_content,
                )
                .map_err(|e| {
                    StateMachineError::DatabaseError(format!("Error writing child content: {}", e))
                })?;
            }

            //  create the new node
            let new_content_key = format!(
                "{}::v{}",
                new_content_data.id.id, new_content_data.id.version
            );
            txn.put_cf(
                StateMachineColumns::ContentTable.cf(db),
                new_content_key,
                &serialized_content,
            )
            .map_err(|e| {
                StateMachineError::DatabaseError(format!("error writing updated content: {}", e))
            })?;
        }
        Ok(())
    }

    fn tombstone_content_tree(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        content_ids: &HashSet<ContentMetadataId>,
    ) -> Result<(), StateMachineError> {
        let mut queue = VecDeque::new();
        for root_content_id in content_ids {
            queue.push_back(root_content_id.clone());
        }

        while let Some(current_root) = queue.pop_front() {
            let stored_key = format!("{}::v{}", current_root.id, current_root.version);
            let serialized_content_metadata = txn
                .get_cf(StateMachineColumns::ContentTable.cf(db), &stored_key)
                .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?
                .ok_or_else(|| {
                    StateMachineError::DatabaseError(format!(
                        "Content {} not found while tombstoning",
                        current_root
                    ))
                })?;
            let mut content_metadata =
                JsonEncoder::decode::<internal_api::ContentMetadata>(&serialized_content_metadata)?;
            content_metadata.tombstoned = true;
            let serialized_content_metadata = JsonEncoder::encode(&content_metadata)?;
            txn.put_cf(
                StateMachineColumns::ContentTable.cf(db),
                stored_key,
                &serialized_content_metadata,
            )
            .map_err(|e| {
                StateMachineError::DatabaseError(format!(
                    "Error writing content back after setting tombstone flag on it for content {}: {}",
                    &current_root, e
                ))
            })?;

            let children = self
                .content_children_table
                .get_children(&content_metadata.id);
            queue.extend(children.iter().cloned());
        }

        Ok(())
    }

    /// Function to delete content based on content ids
    fn delete_content(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        content_ids: Vec<String>,
    ) -> Result<(), StateMachineError> {
        for content_id in content_ids {
            let latest_version = self.get_latest_version_of_content(&content_id, db, txn)?;
            txn.delete_cf(
                StateMachineColumns::ContentTable.cf(db),
                &format!("{}::v{}", content_id, latest_version),
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
        extractor: &ExtractorDescription,
        ts_secs: &u64,
    ) -> Result<(), StateMachineError> {
        let serialized_executor = JsonEncoder::encode(&internal_api::ExecutorMetadata {
            id: executor_id.into(),
            last_seen: *ts_secs,
            addr: addr.clone(),
            extractor: extractor.clone(),
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
    ) -> Result<internal_api::ExecutorMetadata, StateMachineError> {
        //  Get a handle on the executor before deleting it from the DB
        let executors_cf = StateMachineColumns::Executors.cf(db);
        let serialized_executor = txn
            .get_cf(executors_cf, executor_id)
            .map_err(|e| {
                StateMachineError::DatabaseError(format!("Error reading executor: {}", e))
            })?
            .ok_or_else(|| {
                StateMachineError::DatabaseError(format!("Executor {} not found", executor_id))
            })?;
        let executor_meta =
            JsonEncoder::decode::<internal_api::ExecutorMetadata>(&serialized_executor)?;
        txn.delete_cf(executors_cf, executor_id).map_err(|e| {
            StateMachineError::DatabaseError(format!("Error deleting executor: {}", e))
        })?;
        Ok(executor_meta)
    }

    fn set_extractor(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        extractor: &ExtractorDescription,
    ) -> Result<(), StateMachineError> {
        let serialized_extractor = JsonEncoder::encode(extractor)?;
        txn.put_cf(
            StateMachineColumns::Extractors.cf(db),
            &extractor.name,
            serialized_extractor,
        )
        .map_err(|e| StateMachineError::DatabaseError(format!("Error writing extractor: {}", e)))?;
        Ok(())
    }

    fn set_extraction_policy(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        extraction_policy: &internal_api::ExtractionPolicy,
        updated_structured_data_schema: &Option<internal_api::StructuredDataSchema>,
        new_structured_data_schema: &internal_api::StructuredDataSchema,
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
        if let Some(schema) = updated_structured_data_schema {
            self.set_schema(db, txn, schema)?
        }
        self.set_schema(db, txn, new_structured_data_schema)?;
        Ok(())
    }

    fn set_namespace(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        namespace: &NamespaceName,
        structured_data_schema: &internal_api::StructuredDataSchema,
    ) -> Result<(), StateMachineError> {
        let serialized_name = JsonEncoder::encode(namespace)?;
        txn.put_cf(
            &StateMachineColumns::Namespaces.cf(db),
            namespace,
            serialized_name,
        )
        .map_err(|e| StateMachineError::DatabaseError(format!("Error writing namespace: {}", e)))?;
        self.set_schema(db, txn, structured_data_schema)?;
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
        content_id: &str,
        _extraction_policy_id: &str,
        policy_completion_time: SystemTime,
    ) -> Result<(), StateMachineError> {
        let value = txn
            .get_cf(mapping_cf, content_key.clone())
            .map_err(|e| {
                StateMachineError::DatabaseError(format!(
                    "Error getting the content policies applied on content id {}: {}",
                    content_key, e
                ))
            })?
            .ok_or_else(|| {
                StateMachineError::DatabaseError(format!(
                    "No content policies applied on content found for id {}",
                    content_key
                ))
            })?;
        let content_meta = JsonEncoder::decode::<internal_api::ContentMetadata>(&value)?;
        let _epoch_time = policy_completion_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|e| {
                StateMachineError::DatabaseError(format!(
                    "Error converting policy completion time to u64: {}",
                    e
                ))
            })?
            .as_secs();
        let data = JsonEncoder::encode(&content_meta)?;
        txn.put_cf(StateMachineColumns::ContentTable.cf(db), content_id, data)
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
            RequestPayload::CreateIndex {
                index,
                namespace: _,
                id,
            } => {
                self.set_index(db, &txn, index, id)?;
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
                //  NOTE: Special case where forward and reverse indexes are updated together
                // because get_latest_version_of_content requires a txn
                if *mark_finished {
                    tracing::info!("Marking garbage collection task as finished: {:?}", gc_task);
                    self.update_garbage_collection_tasks(db, &txn, &vec![gc_task])?;
                    self.delete_content(db, &txn, vec![gc_task.content_id.clone()])?;
                    let latest_parent_id =
                        self.get_latest_version_of_content(&gc_task.parent_content_id, db, &txn)?;
                    let parent_content_metadata_id = ContentMetadataId {
                        id: gc_task.parent_content_id.clone(),
                        version: latest_parent_id,
                    };
                    let latest_content_id =
                        self.get_latest_version_of_content(&gc_task.content_id, db, &txn)?;
                    let content_metadata_id = ContentMetadataId {
                        id: gc_task.content_id.clone(),
                        version: latest_content_id,
                    };
                    self.content_children_table
                        .remove(&parent_content_metadata_id, &content_metadata_id);
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
                println!("Marking task {} as {}", task.id, mark_finished);
                self.update_tasks(db, &txn, vec![task])?;
                self.set_content(db, &txn, content_metadata)?;

                if task.terminal_state() {
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

                    //  remove the task from the content -> extraction_policy_id -> task_ids mapping reverse index
                    let latest_version =
                        self.get_latest_version_of_content(&task.content_metadata.id.id, db, &txn)?;
                    let content_id = ContentMetadataId {
                        id: task.content_metadata.id.id.clone(),
                        version: latest_version,
                    };
                    self.content_task_mapping.remove(
                        &content_id,
                        &task.extraction_policy_id,
                        &task.id,
                    );
                    if self.content_task_mapping.is_content_processed(&content_id) {
                        println!("The content is processed");
                        //  compare the two sub-trees here
                        if latest_version > 1 {
                            println!("There is a previous version, merge the two sub-trees");
                            self.merge_content_trees(
                                db,
                                &txn,
                                &ContentMetadataId {
                                    id: content_id.id.clone(),
                                    version: content_id.version - 1,
                                },
                                &content_id,
                            )?;
                        }
                    }
                }
            }
            RequestPayload::RegisterExecutor {
                addr,
                executor_id,
                extractor,
                ts_secs,
            } => {
                //  Insert the executor
                self.set_executor(db, &txn, addr.into(), executor_id, extractor, ts_secs)?;

                //  Insert the associated extractor
                self.set_extractor(db, &txn, extractor)?;
            }
            RequestPayload::RemoveExecutor { executor_id } => {
                //  NOTE: Special case where forward and reverse indexes are updated together

                //  Get a handle on the executor before deleting it from the DB
                let executor_meta = self.delete_executor(db, &txn, executor_id)?;

                // Remove all tasks assigned to this executor and get a handle on the task ids
                let task_ids = self.delete_task_assignments_for_executor(db, &txn, executor_id)?;

                txn.commit()
                    .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;

                //  Remove the the extractor from the executor -> extractor mapping table
                self.extractor_executors_table
                    .remove(&executor_meta.extractor.name, &executor_meta.id);

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
            RequestPayload::UpdateContent { updated_content } => {
                //  NOTE: Special case where forward and reverse indexes are updated together so
                // errors can be handled
                self.update_content(db, &txn, updated_content)?;
                for (old_content_key, new_content_data) in updated_content.iter() {
                    let old_content_key: ContentMetadataId = old_content_key.try_into()?;
                    self.content_namespace_table
                        .remove(&new_content_data.namespace, &old_content_key);
                    self.content_namespace_table
                        .insert(&new_content_data.namespace, &new_content_data.id);
                    self.content_children_table
                        .replace_parent(&old_content_key, &new_content_data.id);
                }
            }
            RequestPayload::TombstoneContentTree {
                namespace: _,
                content_ids,
            } => {
                self.tombstone_content_tree(db, &txn, content_ids)?;
            }
            RequestPayload::CreateExtractionPolicy {
                extraction_policy,
                updated_structured_data_schema,
                new_structured_data_schema,
            } => {
                self.set_extraction_policy(
                    db,
                    &txn,
                    extraction_policy,
                    updated_structured_data_schema,
                    new_structured_data_schema,
                )?;
            }
            RequestPayload::CreateNamespace {
                name,
                structured_data_schema,
            } => {
                self.set_namespace(db, &txn, name, structured_data_schema)?;
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
            RequestPayload::SetContentTaskMappings {
                content_task_mappings: _,
            } => {
                //  no forward index writes here
            }
        };

        self.apply(request).map_err(|e| {
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
    pub fn apply(&mut self, request: StateMachineUpdateRequest) -> Result<()> {
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
                extractor,
                ts_secs,
            } => {
                self.extractor_executors_table
                    .insert(&extractor.name, &executor_id);
                let _executor_info = internal_api::ExecutorMetadata {
                    id: executor_id.clone(),
                    last_seen: ts_secs,
                    addr: addr.clone(),
                    extractor: extractor.clone(),
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
            RequestPayload::CreateContent { content_metadata } => {
                for content in content_metadata {
                    self.content_namespace_table
                        .insert(&content.namespace, &content.id);
                    self.content_children_table
                        .insert(&content.parent_id, &content.id);
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
            RequestPayload::CreateNamespace {
                name: _,
                structured_data_schema,
            } => {
                self.update_schema_reverse_idx(structured_data_schema);
                Ok(())
            }
            RequestPayload::CreateIndex {
                index: _,
                namespace,
                id,
            } => {
                self.namespace_index_table.insert(&namespace, &id);
                Ok(())
            }
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
                    if let Some(executor_id) = executor_id {
                        self.executor_running_task_count
                            .decrement_running_task_count(&executor_id);
                    }
                }
                //  TODO: Why do we need this?
                for content in content_metadata {
                    self.content_namespace_table
                        .insert(&content.namespace, &content.id);
                }
                Ok(())
            }
            RequestPayload::SetContentTaskMappings {
                content_task_mappings,
            } => {
                for (content_id, policy_task_mapping) in content_task_mappings {
                    for (extraction_policy_id, task_ids) in policy_task_mapping {
                        self.content_task_mapping.insert(
                            &content_id.clone().try_into()?,
                            &extraction_policy_id,
                            &task_ids,
                        );
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
            _ => Ok(()),
        }
    }

    //  START READER METHODS FOR ROCKSDB FORWARD INDEXES

    /// This function is a helper method that will get the latest version of any
    /// piece of content in the database by building a prefix foward iterator
    /// TODO: Should we be ignoring tombstoned content here for the latest
    /// version?
    pub fn get_latest_version_of_content(
        &self,
        content_id: &str,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
    ) -> Result<u64, StateMachineError> {
        let prefix = format!("{}::v", content_id);

        let mut read_opts = rocksdb::ReadOptions::default();
        read_opts.set_prefix_same_as_start(true);
        let iter = txn.iterator_cf(
            StateMachineColumns::ContentTable.cf(db),
            rocksdb::IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Forward),
        );

        let mut highest_version: u64 = 0;

        for item in iter {
            match item {
                Ok((key, _)) => {
                    if let Ok(key_str) = std::str::from_utf8(&key) {
                        if let Some(version_str) = key_str.strip_prefix(&prefix) {
                            if let Ok(version) = version_str.parse::<u64>() {
                                if version > highest_version {
                                    highest_version = version;
                                }
                            }
                        }
                    }
                }
                Err(e) => return Err(StateMachineError::TransactionError(e.to_string())),
            }
        }

        Ok(highest_version)
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
        content_ids: HashSet<ContentMetadataId>,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Vec<indexify_internal_api::ContentMetadata>, StateMachineError> {
        let txn = db.transaction();

        let content: Result<Vec<indexify_internal_api::ContentMetadata>, StateMachineError> =
            content_ids
                .into_iter()
                .filter_map(|content_id| {
                    match txn.get_cf(
                        StateMachineColumns::ContentTable.cf(db),
                        format!("{}::v{}", content_id.id, content_id.version),
                    ) {
                        Ok(Some(content_bytes)) => match JsonEncoder::decode::<
                            indexify_internal_api::ContentMetadata,
                        >(&content_bytes)
                        {
                            Ok(content) => {
                                if !content.tombstoned {
                                    Some(Ok(content))
                                } else {
                                    None
                                }
                            }
                            Err(e) => Some(Err(StateMachineError::TransactionError(e.to_string()))),
                        },
                        Ok(None) => None,
                        Err(e) => Some(Err(StateMachineError::TransactionError(e.to_string()))),
                    }
                })
                .collect::<Result<Vec<_>, _>>();
        content
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

            // If a key with the highest version is found, decode its content and add to the
            // results
            if highest_version == 0 {
                continue;
            }
            match txn.get_cf(
                StateMachineColumns::ContentTable.cf(db),
                &format!("{}::v{}", content_id, highest_version),
            ) {
                Ok(Some(content_bytes)) => {
                    match JsonEncoder::decode::<indexify_internal_api::ContentMetadata>(
                        &content_bytes,
                    ) {
                        Ok(content) => {
                            if !content.tombstoned {
                                contents.push(content);
                            }
                        }
                        Err(e) => {
                            return Err(StateMachineError::TransactionError(e.to_string()));
                        }
                    }
                }
                Ok(None) => {} // This should technically never happen since we have the key
                Err(e) => return Err(StateMachineError::TransactionError(e.to_string())),
            }
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
            if highest_version == 0 {
                continue;
            }
            let content_bytes = txn
                .get_cf(
                    StateMachineColumns::ContentTable.cf(db),
                    &format!("{}::v{}", current_root, highest_version),
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
    ) -> Result<Option<Vec<indexify_internal_api::ExtractionPolicy>>, StateMachineError> {
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
                let policy =
                    serde_json::from_slice::<indexify_internal_api::ExtractionPolicy>(&bytes)
                        .map_err(StateMachineError::SerializationError)?;
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

        let extraction_policy_ids = self.extraction_policies_table.get(&namespace.to_string());
        let extraction_policies = self
            .get_extraction_policies_from_ids(extraction_policy_ids, db)?
            .unwrap_or_else(Vec::new);

        Ok(Some(indexify_internal_api::Namespace {
            name: ns_name,
            extraction_policies,
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

    pub fn get_executor_running_task_count(&self) -> HashMap<ExecutorId, usize> {
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

    pub fn get_content_task_mapping(
        &self,
    ) -> HashMap<ContentMetadataId, HashMap<ExtractionPolicyId, HashSet<TaskId>>> {
        self.content_task_mapping.inner()
    }

    pub fn is_content_processed(&self, content_id: &ContentMetadataId) -> bool {
        self.content_task_mapping.is_content_processed(content_id)
    }

    //  END READER METHODS FOR REVERSE INDEXES

    //  START WRITER METHODS FOR REVERSE INDEXES
    pub fn insert_executor_running_task_count(&self, executor_id: &str, tasks: u64) {
        self.executor_running_task_count
            .insert(&executor_id.to_string(), tasks as usize);
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
            content_task_mapping: self.get_content_task_mapping(),
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

#[derive(serde::Serialize, serde::Deserialize, Default)]
pub struct IndexifyStateSnapshot {
    unassigned_tasks: HashSet<TaskId>,
    unprocessed_state_changes: HashSet<StateChangeId>,
    content_namespace_table: HashMap<NamespaceName, HashSet<ContentMetadataId>>,
    extraction_policies_table: HashMap<NamespaceName, HashSet<String>>,
    extractor_executors_table: HashMap<ExtractorName, HashSet<ExecutorId>>,
    namespace_index_table: HashMap<NamespaceName, HashSet<String>>,
    unfinished_tasks_by_extractor: HashMap<ExtractorName, HashSet<TaskId>>,
    executor_running_task_count: HashMap<ExecutorId, usize>,
    schemas_by_namespace: HashMap<NamespaceName, HashSet<SchemaId>>,
    content_children_table: HashMap<ContentMetadataId, HashSet<ContentMetadataId>>,
    content_task_mapping: HashMap<ContentMetadataId, HashMap<ExtractionPolicyId, HashSet<TaskId>>>,
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
