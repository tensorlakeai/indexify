use std::collections::{HashMap, HashSet};

use indexify_internal_api as internal_api;
use internal_api::StateChange;
use serde::{Deserialize, Serialize};

use super::{
    requests::{Request, RequestPayload, StateChangeProcessed},
    store_utils::{decrement_running_task_count, increment_running_task_count},
    ContentId,
    ExecutorId,
    ExtractionPolicyId,
    ExtractorName,
    NamespaceName,
    StateChangeId,
    TaskId,
};

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

    pub index_table: HashMap<String, internal_api::Index>,

    // Reverse Indexes
    /// The tasks that are currently unassigned
    pub unassigned_tasks: HashSet<TaskId>,

    /// State changes that have not been processed yet
    pub unprocessed_state_changes: HashSet<StateChangeId>,

    /// Namespace -> Content ID
    pub content_namespace_table: HashMap<NamespaceName, HashSet<ContentId>>,

    /// Namespace -> Extractor bindings
    pub extraction_policies_table: HashMap<NamespaceName, HashSet<internal_api::ExtractionPolicy>>,

    /// Extractor -> Executors table
    pub extractor_executors_table: HashMap<ExtractorName, HashSet<ExecutorId>>,

    /// Namespace -> Index index
    pub namespace_index_table: HashMap<NamespaceName, HashSet<internal_api::Index>>,

    /// Tasks that are currently unfinished, by extractor. Once they are
    /// finished, they are removed from this set.
    pub unfinished_tasks_by_extractor: HashMap<ExtractorName, HashSet<TaskId>>,

    /// Number of tasks currently running on each executor
    pub executor_running_task_count: HashMap<ExecutorId, usize>,
}

impl IndexifyState {
    pub fn apply(&mut self, request: Request) {
        for change in request.new_state_changes {
            self.state_changes.insert(change.id.clone(), change.clone());
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
                for task in tasks {
                    self.tasks.insert(task.id.clone(), task.clone());
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
                id,
            } => {
                self.namespace_index_table
                    .entry(namespace.clone())
                    .or_default()
                    .insert(index.clone());
                self.index_table.insert(id.clone(), index.clone());
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
            RequestPayload::JoinClusterMembership {
                node_id: _,
                address: _,
            } => {} //  do nothing
        }
    }

    pub fn mark_state_changes_processed(
        &mut self,
        state_change: &StateChangeProcessed,
        processed_at: u64,
    ) {
        self.unprocessed_state_changes
            .remove(&state_change.state_change_id);
        self.state_changes
            .entry(state_change.state_change_id.to_string())
            .and_modify(|c| {
                c.processed_at = Some(processed_at);
            });
    }
}
