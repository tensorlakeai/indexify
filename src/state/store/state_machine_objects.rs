use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use indexify_internal_api as internal_api;
use internal_api::StateChange;
use rocksdb::OptimisticTransactionDB;
use serde::{Deserialize, Serialize};
use tracing::error;

use super::{
    requests::{RequestPayload, StateChangeProcessed, StateMachineUpdateRequest},
    store_utils::{decrement_running_task_count, increment_running_task_count},
    ContentId, ExecutorId, ExtractionPolicyId, ExtractorName, NamespaceName, StateChangeId,
    StateMachineColumns, TaskId,
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
    pub executors: HashMap<ExecutorId, internal_api::ExecutorMetadata>,

    pub tasks: HashMap<TaskId, internal_api::Task>,

    pub task_assignments: HashMap<ExecutorId, HashSet<TaskId>>,

    pub state_changes: HashMap<StateChangeId, StateChange>,

    pub content_table: HashMap<ContentId, internal_api::ContentMetadata>,

    pub extraction_policies: HashMap<ExtractionPolicyId, internal_api::ExtractionPolicy>,

    pub extractors: HashMap<ExtractorName, internal_api::ExtractorDescription>,

    pub namespaces: HashSet<NamespaceName>,

    // pub index_table: HashMap<String, internal_api::Index>,   //  done

    //  Remove this once the coordinator::tests::test_create_extraction_events test isn't failing after removing this
    // pub index_table: HashMap<String, internal_api::Index>,

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
    pub fn apply_state_machine_updates(
        &mut self,
        request: StateMachineUpdateRequest,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<(), StateMachineError> {
        println!("ENTER: IndexifyState::apply_state_machine_updates");
        let txn = db.transaction();

        let state_changes_cf = db
            .cf_handle(StateMachineColumns::state_changes.to_string().as_str())
            .ok_or_else(|| {
                StateMachineError::DatabaseError("ColumnFamily 'state_changes' not found".into())
            })?;

        for change in &request.new_state_changes {
            let serialized_change = serde_json::to_vec(change)?;
            txn.put_cf(state_changes_cf, &change.id, &serialized_change)
                .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
        }

        for change in &request.state_changes_processed {
            let result = txn
                .get_cf(state_changes_cf, &change.state_change_id.to_string())
                .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?
                .ok_or_else(|| StateMachineError::DatabaseError("State change not found".into()))?;
            let mut state_change = serde_json::from_slice::<StateChange>(&result)?;
            state_change.processed_at = Some(change.processed_at);
            let serialized_change = serde_json::to_vec(&state_change)?;
            txn.put_cf(
                state_changes_cf,
                &change.state_change_id.to_string(),
                &serialized_change,
            )
            .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
        }

        match &request.payload {
            RequestPayload::CreateIndex {
                index,
                namespace: _,
                id,
            } => {
                println!("CreateIndex in RocksDB");
                let index_table_cf = db
                    .cf_handle(StateMachineColumns::index_table.to_string().as_str())
                    .ok_or_else(|| {
                        StateMachineError::DatabaseError(
                            "ColumnFamily 'index_table' not found".into(),
                        )
                    })?;
                let serialized_index = serde_json::to_vec(&index)?;
                txn.put_cf(index_table_cf, id, &serialized_index)
                    .map_err(|e| StateMachineError::DatabaseError(e.to_string()))?;
            }
            RequestPayload::CreateTasks { tasks } => {
                println!("CreateTasks in RocksDB");
                let tasks_cf = db
                    .cf_handle(StateMachineColumns::tasks.to_string().as_str())
                    .ok_or_else(|| {
                        StateMachineError::DatabaseError("ColumnFamily 'tasks' not found".into())
                    })?;
                for task in tasks {
                    let serialized_task = serde_json::to_vec(&task)?;
                    txn.put_cf(tasks_cf, task.id.clone(), &serialized_task)
                        .map_err(|e| {
                            StateMachineError::DatabaseError(format!("Error writing task: {}", e))
                        })?;
                }
                println!("CreateTasks in RocksDB done");
            }
            _ => (),
        };

        txn.commit()
            .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;

        self.apply(request);

        println!("EXIT: IndexifyState::apply_state_machine_updates");
        Ok(())
    }

    pub fn apply(&mut self, request: StateMachineUpdateRequest) {
        println!("ENTER: IndexifyState::apply");
        for change in request.new_state_changes {
            //  The below write is handled in store/mod.rs
            // self.state_changes.insert(change.id.clone(), change.clone());
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
                self.extractors
                    .insert(extractor.name.clone(), extractor.clone());
                self.extractor_executors_table
                    .entry(extractor.name.clone())
                    .or_default()
                    .insert(executor_id.clone());
                let executor_info = internal_api::ExecutorMetadata {
                    id: executor_id.clone(),
                    last_seen: ts_secs,
                    addr: addr.clone(),
                    extractor: extractor.clone(),
                };
                self.executors.insert(executor_id.clone(), executor_info);
                // initialize executor load at 0
                self.executor_running_task_count
                    .insert(executor_id.clone(), 0);
            }
            RequestPayload::RemoveExecutor { executor_id } => {
                // Remove this from the executors table
                let executor_meta = self.executors.remove(&executor_id);
                // Remove this from the extractor -> executors table
                if let Some(executor_meta) = executor_meta {
                    let executors = self
                        .extractor_executors_table
                        .entry(executor_meta.extractor.name.clone())
                        .or_default();
                    executors.remove(&executor_meta.id);
                }
                // Remove from the executor load table
                self.executor_running_task_count.remove(&executor_id);

                // Remove all tasks assigned to this executor
                let tasks = self.task_assignments.remove(&executor_id);
                if let Some(tasks) = tasks {
                    for task_id in tasks {
                        self.unassigned_tasks.insert(task_id);
                    }
                }
            }
            RequestPayload::CreateTasks { tasks } => {
                println!("RequestPayload::CreateTasks handler");
                for task in tasks {
                    // self.tasks.insert(task.id.clone(), task.clone());
                    self.unassigned_tasks.insert(task.id.clone());
                    self.unfinished_tasks_by_extractor
                        .entry(task.extractor.clone())
                        .or_default()
                        .insert(task.id.clone());
                }
            }
            RequestPayload::AssignTask { assignments } => {
                for (task_id, executor_id) in assignments {
                    self.task_assignments
                        .entry(executor_id.clone())
                        .or_default()
                        .insert(task_id.clone());
                    self.unassigned_tasks.remove(&task_id);

                    increment_running_task_count(
                        &mut self.executor_running_task_count,
                        &executor_id,
                    );
                }
            }
            RequestPayload::CreateContent { content_metadata } => {
                for content in content_metadata {
                    self.content_table
                        .insert(content.id.clone(), content.clone());
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
                self.extraction_policies
                    .insert(extraction_policy.id.clone(), extraction_policy.clone());
            }
            RequestPayload::CreateNamespace { name } => {
                self.namespaces.insert(name.clone());
            }
            RequestPayload::CreateIndex {
                index,
                namespace,
                id: _,
            } => {
                self.namespace_index_table
                    .entry(namespace.clone())
                    .or_default()
                    .insert(index.clone());
                //  The below write is handled in apply_state_machine_updates
                // self.index_table.insert(id.clone(), index.clone());
            }
            RequestPayload::UpdateTask {
                task,
                mark_finished,
                executor_id,
                content_metadata,
            } => {
                self.tasks.insert(task.id.clone(), task.clone());
                if mark_finished {
                    self.unassigned_tasks.remove(&task.id);
                    self.unfinished_tasks_by_extractor
                        .entry(task.extractor.clone())
                        .or_default()
                        .remove(&task.id);
                    if let Some(executor_id) = executor_id {
                        // remove the task from the executor's task assignments
                        self.task_assignments
                            .entry(executor_id.clone())
                            .or_default()
                            .remove(&task.id);

                        decrement_running_task_count(
                            &mut self.executor_running_task_count,
                            &executor_id,
                        );
                    }
                }
                for content in content_metadata {
                    self.content_table
                        .insert(content.id.clone(), content.clone());
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
        println!("EXIT: IndexifyState::apply");
    }

    pub fn mark_state_changes_processed(
        &mut self,
        state_change: &StateChangeProcessed,
        processed_at: u64,
    ) {
        self.unprocessed_state_changes
            .remove(&state_change.state_change_id);
        //  The below write is handled in apply_state_machine_updates
        // self.state_changes
        //     .entry(state_change.state_change_id.to_string())
        //     .and_modify(|c| {
        //         c.processed_at = Some(processed_at);
        //     });
    }
}
