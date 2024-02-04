use std::collections::{HashMap, HashSet};

use indexify_internal_api as internal_api;
use internal_api::StateChange;
use serde::{Deserialize, Serialize};

use super::{
    requests::{Request, RequestPayload},
    ContentId,
    ExecutorId,
    ExtractorName,
    RepositoryId,
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

    pub extractor_bindings: HashMap<String, internal_api::ExtractorBinding>,

    pub extractors: HashMap<ExtractorName, internal_api::ExtractorDescription>,

    pub repositories: HashSet<String>,

    pub index_table: HashMap<String, internal_api::Index>,

    // Reverse Indexes
    pub unassigned_tasks: HashSet<TaskId>,

    pub unprocessed_state_changes: HashSet<StateChangeId>,

    pub content_repository_table: HashMap<RepositoryId, HashSet<ContentId>>,

    pub bindings_table: HashMap<RepositoryId, HashSet<internal_api::ExtractorBinding>>,

    pub extractor_executors_table: HashMap<ExtractorName, HashSet<ExecutorId>>,

    pub repository_extractors: HashMap<RepositoryId, HashSet<internal_api::Index>>,
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
            }
            RequestPayload::CreateTasks { tasks } => {
                for task in tasks {
                    self.tasks.insert(task.id.clone(), task.clone());
                    self.unassigned_tasks.insert(task.id.clone());
                }
            }
            RequestPayload::AssignTask { assignments } => {
                for (task_id, executor_id) in assignments {
                    self.task_assignments
                        .entry(executor_id.clone())
                        .or_default()
                        .insert(task_id.clone());
                    self.unassigned_tasks.remove(&task_id);
                }
            }
            RequestPayload::CreateContent { content_metadata } => {
                for content in content_metadata {
                    self.content_table
                        .insert(content.id.clone(), content.clone());
                    self.content_repository_table
                        .entry(content.repository.clone())
                        .or_default()
                        .insert(content.id.clone());
                }
            }
            RequestPayload::CreateBinding { binding } => {
                self.bindings_table
                    .entry(binding.repository.clone())
                    .or_default()
                    .insert(binding.clone());
                self.extractor_bindings
                    .insert(binding.id.clone(), binding.clone());
            }
            RequestPayload::CreateRepository { name } => {
                self.repositories.insert(name.clone());
            }
            RequestPayload::CreateIndex {
                index,
                repository,
                id,
            } => {
                self.repository_extractors
                    .entry(repository.clone())
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
                    if let Some(executor_id) = executor_id {
                        self.task_assignments
                            .entry(executor_id.clone())
                            .or_default()
                            .remove(&task.id);
                    }
                }
                for content in content_metadata {
                    self.content_table
                        .insert(content.id.clone(), content.clone());
                    self.content_repository_table
                        .entry(content.repository.clone())
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
