use std::collections::{HashMap, HashSet};

use indexify_internal_api as internal_api;
use internal_api::StateChange;
use serde::{Deserialize, Serialize};

use super::{
    requests::{Request, RequestPayload},
    store_utils::{decrement_running_task_count, increment_running_task_count},
    BindingId,
    ContentId,
    ExecutorId,
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

    pub extractor_bindings: HashMap<BindingId, internal_api::ExtractorBinding>,

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
    pub bindings_table: HashMap<NamespaceName, HashSet<internal_api::ExtractorBinding>>,

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
        for change in request.state_changes {
            self.state_changes.insert(change.id.clone(), change.clone());
            self.unprocessed_state_changes.insert(change.id.clone());
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
                    executors.remove(executor_meta.extractor.name.as_str());
                }
                // Remove from the executor load table
                self.executor_running_task_count.remove(&executor_id);
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
            RequestPayload::CreateBinding { binding } => {
                self.bindings_table
                    .entry(binding.namespace.clone())
                    .or_default()
                    .insert(binding.clone());
                self.extractor_bindings
                    .insert(binding.id.clone(), binding.clone());
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
                for change in state_changes {
                    self.unprocessed_state_changes
                        .remove(&change.state_change_id);
                    self.state_changes
                        .entry(change.state_change_id)
                        .and_modify(|c| {
                            c.processed_at = Some(change.processed_at);
                        });
                }
            }
        }
    }
}
