use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use indexify_internal_api as internal_api;
use internal_api::{ExtractorDescription, StateChange};
use rocksdb::OptimisticTransactionDB;
use serde::{Deserialize, Serialize};
use tracing::error;

use super::{
    requests::{RequestPayload, StateChangeProcessed, StateMachineUpdateRequest},
    store_utils::{decrement_running_task_count, increment_running_task_count},
    ContentId,
    ExecutorId,
    ExtractorName,
    NamespaceName,
    StateChangeId,
    StateMachineColumns,
    TaskId,
};

#[derive(thiserror::Error, Debug)]
pub enum StateMachineError {
    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("RocksDB transaction error: {0}")]
    TransactionError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexifyState {
    //  TODO: Check whether only id's can be stored in reverse indexes
    // Reverse Indexes
    /// The tasks that are currently unassigned
    pub unassigned_tasks: HashSet<TaskId>,

    /// State changes that have not been processed yet
    pub unprocessed_state_changes: HashSet<StateChangeId>,

    /// Namespace -> Content ID
    pub content_namespace_table: HashMap<NamespaceName, HashSet<ContentId>>,

    /// Namespace -> Extractor bindings
    pub extraction_policies_table: HashMap<NamespaceName, HashSet<internal_api::ExtractionPolicy>>, /* TODO: Change this to store extraction policy id in the second col because we have data elsewhere */

    /// Extractor -> Executors table
    pub extractor_executors_table: HashMap<ExtractorName, HashSet<ExecutorId>>,

    /// Namespace -> Index index
    pub namespace_index_table: HashMap<NamespaceName, HashSet<internal_api::Index>>, /* TODO: Store id in the second column, not the entire index */

    /// Tasks that are currently unfinished, by extractor. Once they are
    /// finished, they are removed from this set.
    pub unfinished_tasks_by_extractor: HashMap<ExtractorName, HashSet<TaskId>>,

    /// Number of tasks currently running on each executor
    pub executor_running_task_count: HashMap<ExecutorId, usize>,
}

impl IndexifyState {
    fn set_new_state_changes(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        state_changes: &Vec<StateChange>,
    ) -> Result<(), StateMachineError> {
        let state_changes_cf = db
            .cf_handle(StateMachineColumns::state_changes.to_string().as_str())
            .ok_or_else(|| {
                StateMachineError::DatabaseError("ColumnFamily 'state_changes' not found".into())
            })?;

        for change in state_changes {
            let serialized_change = serde_json::to_vec(change)?;
            txn.put_cf(state_changes_cf, &change.id, &serialized_change)
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
        let state_changes_cf = db
            .cf_handle(StateMachineColumns::state_changes.to_string().as_str())
            .ok_or_else(|| {
                StateMachineError::DatabaseError("ColumnFamily 'state_changes' not found".into())
            })?;

        for change in state_changes {
            let result = txn
                .get_cf(state_changes_cf, &change.state_change_id)
                .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
            let result = result
                .ok_or_else(|| StateMachineError::DatabaseError("State change not found".into()))?;

            let mut state_change = serde_json::from_slice::<StateChange>(&result)?;
            state_change.processed_at = Some(change.processed_at);
            let serialized_change = serde_json::to_vec(&state_change)?;
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
        let index_table_cf = db
            .cf_handle(StateMachineColumns::index_table.to_string().as_str())
            .ok_or_else(|| {
                StateMachineError::DatabaseError("ColumnFamily 'index_table' not found".into())
            })?;
        let serialized_index = serde_json::to_vec(index)?;
        txn.put_cf(index_table_cf, id, serialized_index)
            .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    fn _get_task(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        task_id: &TaskId,
    ) -> Result<internal_api::Task, StateMachineError> {
        let tasks_cf = db
            .cf_handle(StateMachineColumns::tasks.to_string().as_str())
            .ok_or_else(|| {
                StateMachineError::DatabaseError("ColumnFamily 'tasks' not found".into())
            })?;
        let serialized_task = txn
            .get_cf(tasks_cf, task_id)
            .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?
            .ok_or_else(|| {
                StateMachineError::DatabaseError(format!("Task {} not found", task_id))
            })?;
        let task = serde_json::from_slice(&serialized_task)?;
        Ok(task)
    }

    fn set_tasks(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        tasks: &Vec<internal_api::Task>,
    ) -> Result<(), StateMachineError> {
        let tasks_cf = db
            .cf_handle(StateMachineColumns::tasks.to_string().as_str())
            .ok_or_else(|| {
                StateMachineError::DatabaseError("ColumnFamily 'tasks' not found".into())
            })?;
        for task in tasks {
            let serialized_task = serde_json::to_vec(task)?;
            txn.put_cf(tasks_cf, task.id.clone(), &serialized_task)
                .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
        }
        Ok(())
    }

    fn update_tasks(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        tasks: Vec<&internal_api::Task>,
    ) -> Result<(), StateMachineError> {
        let tasks_cf = db
            .cf_handle(StateMachineColumns::tasks.to_string().as_str())
            .ok_or_else(|| {
                StateMachineError::DatabaseError("ColumnFamily 'tasks' not found".into())
            })?;
        for task in tasks {
            let serialized_task = serde_json::to_vec(task)?;
            txn.put_cf(tasks_cf, task.id.clone(), &serialized_task)
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
        let task_assignment_cf = db
            .cf_handle(StateMachineColumns::task_assignments.to_string().as_str())
            .ok_or_else(|| {
                StateMachineError::DatabaseError("ColumnFamily 'task_assignments' not found".into())
            })?;
        let key = executor_id;
        let value = txn.get_cf(task_assignment_cf, key).map_err(|e| {
            StateMachineError::DatabaseError(format!("Error reading task assignments: {}", e))
        })?;
        match value {
            Some(existing_value) => {
                let existing_value: HashSet<TaskId> = serde_json::from_slice(&existing_value)
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

    fn set_task_assignments(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        task_assignments: &HashMap<&String, HashSet<TaskId>>,
    ) -> Result<(), StateMachineError> {
        let task_assignment_cf = db
            .cf_handle(StateMachineColumns::task_assignments.to_string().as_str())
            .ok_or_else(|| {
                StateMachineError::DatabaseError("ColumnFamily 'task_assignments' not found".into())
            })?;
        for (executor_id, task_ids) in task_assignments {
            let key = executor_id;
            let value = txn.get_cf(task_assignment_cf, key).map_err(|e| {
                StateMachineError::DatabaseError(format!("Error reading task assignments: {}", e))
            })?;

            match value {
                //  Update the hash set of task ids if executor id is already present as key
                Some(existing_value) => {
                    let mut existing_value: HashSet<TaskId> =
                        serde_json::from_slice(&existing_value).map_err(|e| {
                            StateMachineError::DatabaseError(format!(
                                "Error deserializing task assignments: {}",
                                e
                            ))
                        })?;
                    existing_value.extend(task_ids.clone());
                    let new_value = serde_json::to_vec(&existing_value)?;
                    txn.put_cf(task_assignment_cf, key, &new_value)
                        .map_err(|e| {
                            StateMachineError::DatabaseError(format!(
                                "Error writing task assignments: {}",
                                e
                            ))
                        })?;
                }
                None => {
                    //  Create a new hash set of task ids if executor id is not present as
                    // key
                    let new_value = serde_json::to_vec(task_ids)?;
                    txn.put_cf(task_assignment_cf, key, &new_value)
                        .map_err(|e| {
                            StateMachineError::DatabaseError(format!(
                                "Error writing task assignments: {}",
                                e
                            ))
                        })?;
                }
            }
        }
        Ok(())
    }

    fn delete_task_assignments_for_executor(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        executor_id: &str,
    ) -> Result<Vec<TaskId>, StateMachineError> {
        let task_assignment_cf = db
            .cf_handle(StateMachineColumns::task_assignments.to_string().as_str())
            .ok_or_else(|| {
                StateMachineError::DatabaseError("ColumnFamily 'task_assignments' not found".into())
            })?;
        let task_ids: Vec<TaskId> = txn
            .get_cf(task_assignment_cf, executor_id)
            .map_err(|e| {
                StateMachineError::DatabaseError(format!(
                    "Error reading task assignments for executor: {}",
                    e
                ))
            })?
            .map(|db_vec| {
                serde_json::from_slice(&db_vec).map_err(|e| {
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
        let content_table_cf = db
            .cf_handle(StateMachineColumns::content_table.to_string().as_str())
            .ok_or_else(|| {
                StateMachineError::DatabaseError("ColumnFamily 'content_table' not found".into())
            })?;
        for content in contents_vec {
            let serialized_content = serde_json::to_vec(content)?;
            txn.put_cf(content_table_cf, content.id.clone(), &serialized_content)
                .map_err(|e| {
                    StateMachineError::DatabaseError(format!("Error writing content: {}", e))
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
        let executors_cf = db
            .cf_handle(StateMachineColumns::executors.to_string().as_str())
            .ok_or_else(|| {
                StateMachineError::DatabaseError("ColumnFamily 'executors' not found".into())
            })?;
        let serialized_executor = serde_json::to_vec(&internal_api::ExecutorMetadata {
            id: executor_id.into(),
            last_seen: *ts_secs,
            addr: addr.clone(),
            extractor: extractor.clone(),
        })?;
        txn.put_cf(executors_cf, executor_id, serialized_executor)
            .map_err(|e| {
                StateMachineError::DatabaseError(format!("Error writing executor: {}", e))
            })?;
        Ok(())
    }

    fn delete_executor(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        executor_id: &str,
    ) -> Result<internal_api::ExecutorMetadata, StateMachineError> {
        //  Get a handle on the executor before deleting it from the DB
        let executors_cf = db
            .cf_handle(StateMachineColumns::executors.to_string().as_str())
            .ok_or_else(|| {
                StateMachineError::DatabaseError("ColumnFamily 'executors' not found".into())
            })?;
        let serialized_executor = txn
            .get_cf(executors_cf, executor_id)
            .map_err(|e| {
                StateMachineError::DatabaseError(format!("Error reading executor: {}", e))
            })?
            .ok_or_else(|| {
                StateMachineError::DatabaseError(format!("Executor {} not found", executor_id))
            })?;
        let executor_meta =
            serde_json::from_slice::<internal_api::ExecutorMetadata>(&serialized_executor)?;
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
        let extractors_cf = db
            .cf_handle(StateMachineColumns::extractors.to_string().as_str())
            .ok_or_else(|| {
                StateMachineError::DatabaseError("ColumnFamily 'extractors' not found".into())
            })?;
        let serialized_extractor = serde_json::to_vec(extractor)?;
        txn.put_cf(extractors_cf, extractor.name.clone(), serialized_extractor)
            .map_err(|e| {
                StateMachineError::DatabaseError(format!("Error writing extractor: {}", e))
            })?;
        Ok(())
    }

    fn set_extraction_policy(
        &self,
        db: &Arc<OptimisticTransactionDB>,
        txn: &rocksdb::Transaction<OptimisticTransactionDB>,
        extraction_policy: &internal_api::ExtractionPolicy,
    ) -> Result<(), StateMachineError> {
        let extraction_policies_cf = db
            .cf_handle(
                StateMachineColumns::extraction_policies
                    .to_string()
                    .as_str(),
            )
            .ok_or_else(|| {
                StateMachineError::DatabaseError(
                    "ColumnFamily 'extraction_policies' not found".into(),
                )
            })?;
        let serialized_extraction_policy = serde_json::to_vec(extraction_policy)?;
        txn.put_cf(
            extraction_policies_cf,
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
        let namespaces_cf = db
            .cf_handle(StateMachineColumns::namespaces.to_string().as_str())
            .ok_or_else(|| {
                StateMachineError::DatabaseError("ColumnFamily 'namespaces' not found".into())
            })?;
        let serialized_name = serde_json::to_vec(namespace)?;
        txn.put_cf(namespaces_cf, serialized_name, [])
            .map_err(|e| {
                StateMachineError::DatabaseError(format!("Error writing namespace: {}", e))
            })?;
        Ok(())
    }

    /// This method will make all state machine forward index writes to RocksDB
    pub fn apply_state_machine_updates(
        &mut self,
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
            RequestPayload::AssignTask { assignments } => {
                let assignments: HashMap<&String, HashSet<TaskId>> = assignments.iter().fold(
                    HashMap::new(),
                    |mut acc, (task_id, executor_id)| {
                        acc.entry(executor_id)
                            .or_default()
                            .insert(task_id.clone());
                        acc
                    },
                );

                self.set_task_assignments(db, &txn, &assignments)?;
            }
            RequestPayload::UpdateTask {
                task,
                mark_finished,
                executor_id,
                content_metadata,
            } => {
                self.update_tasks(db, &txn, vec![task])?;

                if *mark_finished {
                    //  If the task is meant to be marked finished and has an executor id, remove it
                    // from the list of tasks assigned to an executor
                    if let Some(executor_id) = executor_id {
                        let mut existing_tasks =
                            self.get_task_assignments_for_executor(db, &txn, executor_id)?;
                        existing_tasks.remove(&task.id);
                        let mut new_task_assignment = HashMap::new();
                        new_task_assignment.insert(executor_id, existing_tasks);
                        self.set_task_assignments(db, &txn, &new_task_assignment)?;
                        decrement_running_task_count(
                            &mut self.executor_running_task_count,
                            executor_id,
                        );
                    }
                }

                //  Insert the content metadata into the db
                self.set_content(db, &txn, content_metadata)?;
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
                //  NOTE: Special case of a handler that also remove its own reverse indexes
                // here and returns from this function  Doing this because
                // altering the reverse indexes requires references to the removed items

                //  Get a handle on the executor before deleting it from the DB
                let executor_meta = self.delete_executor(db, &txn, executor_id)?;

                // Remove all tasks assigned to this executor and get a handle on the task ids
                let task_ids = self.delete_task_assignments_for_executor(db, &txn, executor_id)?;

                txn.commit()
                    .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;

                //  Remove the the extractor from the executor -> extractor mapping table
                let executors = self
                    .extractor_executors_table
                    .entry(executor_meta.extractor.name.clone())
                    .or_default();
                executors.remove(&executor_meta.id);

                //  Put the tasks of the deleted executor into the unassigned tasks list
                for task_id in task_ids {
                    self.unassigned_tasks.insert(task_id);
                }

                // Remove from the executor load table
                self.executor_running_task_count.remove(executor_id);

                return Ok(());
            }
            RequestPayload::CreateContent { content_metadata } => {
                self.set_content(db, &txn, content_metadata)?;
            }
            RequestPayload::CreateExtractionPolicy { extraction_policy } => {
                self.set_extraction_policy(db, &txn, extraction_policy)?;
            }
            RequestPayload::CreateNamespace { name } => {
                self.set_namespace(db, &txn, name)?;
            }
            RequestPayload::MarkStateChangesProcessed { state_changes } => {
                self.set_processed_state_changes(db, &txn, state_changes)?;
            }
            _ => (),
        };

        txn.commit()
            .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;

        self.apply(request);

        Ok(())
    }

    /// This method handles all reverse index writes which are in memory
    /// This will only run after the RocksDB transaction to commit the forward
    /// index writes is done
    pub fn apply(&mut self, request: StateMachineUpdateRequest) {
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
                    .entry(extractor.name.clone())
                    .or_default()
                    .insert(executor_id.clone());
                let _executor_info = internal_api::ExecutorMetadata {
                    id: executor_id.clone(),
                    last_seen: ts_secs,
                    addr: addr.clone(),
                    extractor: extractor.clone(),
                };
                // initialize executor load at 0
                self.executor_running_task_count
                    .insert(executor_id.clone(), 0);
            }
            RequestPayload::RemoveExecutor { executor_id: _ } => (),
            RequestPayload::CreateTasks { tasks } => {
                for task in tasks {
                    self.unassigned_tasks.insert(task.id.clone());
                    self.unfinished_tasks_by_extractor
                        .entry(task.extractor.clone())
                        .or_default()
                        .insert(task.id.clone());
                }
            }
            RequestPayload::AssignTask { assignments } => {
                for (task_id, executor_id) in assignments {
                    self.unassigned_tasks.remove(&task_id);

                    increment_running_task_count(
                        &mut self.executor_running_task_count,
                        &executor_id,
                    );
                }
            }
            RequestPayload::CreateContent { content_metadata } => {
                for content in content_metadata {
                    //  The below write is handled in apply_state_machine_updates
                    self.content_namespace_table
                        .entry(content.namespace.clone())
                        .or_default()
                        .insert(content.id.clone());
                }
            }
            RequestPayload::CreateExtractionPolicy { extraction_policy } => {
                self.extraction_policies_table
                    .entry(extraction_policy.namespace.clone())
                    .or_default()
                    .insert(extraction_policy.clone());
            }
            RequestPayload::CreateNamespace { name: _ } => (), /* do nothing no reverse index */
            // change required
            RequestPayload::CreateIndex {
                index,
                namespace,
                id: _,
            } => {
                self.namespace_index_table
                    .entry(namespace.clone())
                    .or_default()
                    .insert(index.clone());
            }
            RequestPayload::UpdateTask {
                task,
                mark_finished,
                executor_id,
                content_metadata,
            } => {
                if mark_finished {
                    self.unassigned_tasks.remove(&task.id);
                    self.unfinished_tasks_by_extractor
                        .entry(task.extractor.clone())
                        .or_default()
                        .remove(&task.id);
                    if let Some(executor_id) = executor_id {
                        decrement_running_task_count(
                            &mut self.executor_running_task_count,
                            &executor_id,
                        );
                    }
                }
                for content in content_metadata {
                    self.content_namespace_table
                        .entry(content.namespace.clone())
                        .or_default()
                        .insert(content.id.clone());
                }
            }
            RequestPayload::MarkStateChangesProcessed { state_changes } => {
                for state_change in state_changes {
                    self.mark_state_changes_processed(&state_change, state_change.processed_at);
                }
            }
            RequestPayload::JoinCluster {
                node_id: _,
                address: _,
            } => {} //  do nothing
        }
    }

    pub fn mark_state_changes_processed(
        &mut self,
        state_change: &StateChangeProcessed,
        _processed_at: u64,
    ) {
        self.unprocessed_state_changes
            .remove(&state_change.state_change_id);
    }

    pub async fn get_tasks_for_executor(
        &self,
        executor_id: &str,
        limit: Option<u64>,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Vec<indexify_internal_api::Task>, StateMachineError> {
        //  NOTE: Don't do deserialization within the transaction
        let txn = db.transaction();
        let task_assignments_cf = db
            .cf_handle(StateMachineColumns::task_assignments.to_string().as_str())
            .ok_or_else(|| {
                StateMachineError::DatabaseError(
                    "Failed to get column family 'task_assignments'".into(),
                )
            })?;

        let task_ids_key = executor_id.as_bytes();
        let task_ids_bytes = txn
            .get_cf(task_assignments_cf, task_ids_key)
            .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;

        let task_ids: Vec<String> = task_ids_bytes
            .map(|task_id_bytes| {
                serde_json::from_slice(&task_id_bytes)
                    .map_err(StateMachineError::from)
                    .unwrap_or_else(|e| {
                        error!("Failed to deserialize task id: {}", e);
                        Vec::new()
                    })
            })
            .unwrap_or_else(Vec::new);

        let tasks_cf = db
            .cf_handle(StateMachineColumns::tasks.to_string().as_str())
            .ok_or_else(|| {
                StateMachineError::DatabaseError("Failed to get column family 'tasks'".into())
            })?;

        let limit = limit.unwrap_or(task_ids.len() as u64) as usize;

        let tasks: Result<Vec<indexify_internal_api::Task>, StateMachineError> = task_ids
            .into_iter()
            .take(limit)
            .map(|task_id| {
                let task_bytes = txn
                    .get_cf(tasks_cf, task_id.as_bytes())
                    .map_err(|e| StateMachineError::TransactionError(e.to_string()))?
                    .ok_or_else(|| {
                        StateMachineError::DatabaseError(format!("Task {} not found", task_id))
                    })?;
                serde_json::from_slice(&task_bytes).map_err(StateMachineError::from)
            })
            .collect();
        tasks
    }

    pub async fn get_executors_from_ids(
        &self,
        executor_ids: HashSet<String>,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Vec<internal_api::ExecutorMetadata>, StateMachineError> {
        let txn = db.transaction();
        let executors_cf = db
            .cf_handle(StateMachineColumns::executors.to_string().as_str())
            .ok_or_else(|| {
                StateMachineError::DatabaseError("Failed to get column family 'executors'".into())
            })?;
        let executors: Result<Vec<internal_api::ExecutorMetadata>, StateMachineError> =
            executor_ids
                .into_iter()
                .map(|executor_id| {
                    let executor_bytes = txn
                        .get_cf(executors_cf, executor_id.as_bytes())
                        .map_err(|e| StateMachineError::TransactionError(e.to_string()))?
                        .ok_or_else(|| {
                            StateMachineError::DatabaseError(format!(
                                "Executor {} not found",
                                executor_id
                            ))
                        })?;
                    serde_json::from_slice(&executor_bytes).map_err(StateMachineError::from)
                })
                .collect();
        executors
    }

    pub async fn get_content_from_ids(
        &self,
        content_ids: HashSet<String>,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Vec<internal_api::ContentMetadata>, StateMachineError> {
        let txn = db.transaction();
        let content_cf = db
            .cf_handle(StateMachineColumns::content_table.to_string().as_str())
            .ok_or_else(|| {
                StateMachineError::DatabaseError(
                    "Failed to get column family 'content_table'".into(),
                )
            })?;
        let content: Result<Vec<internal_api::ContentMetadata>, StateMachineError> = content_ids
            .into_iter()
            .map(|content_id| {
                let content_bytes = txn
                    .get_cf(content_cf, content_id.as_bytes())
                    .map_err(|e| StateMachineError::TransactionError(e.to_string()))?
                    .ok_or_else(|| {
                        StateMachineError::DatabaseError(format!(
                            "Content {} not found",
                            content_id
                        ))
                    })?;
                serde_json::from_slice(&content_bytes).map_err(StateMachineError::from)
            })
            .collect();
        content
    }
}
