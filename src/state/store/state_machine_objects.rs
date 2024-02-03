use std::collections::{HashMap, HashSet};

use indexify_internal_api as internal_api;
use serde::{Deserialize, Serialize};

use super::{
    requests::Request,
    ContentId,
    ExecutorId,
    ExtractionEventId,
    ExtractorName,
    RepositoryId,
    TaskId,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexifyState {
    pub executors: HashMap<ExecutorId, internal_api::ExecutorMetadata>,

    pub tasks: HashMap<TaskId, internal_api::Task>,

    pub task_assignments: HashMap<ExecutorId, HashSet<TaskId>>,

    pub extraction_events: HashMap<ExtractionEventId, internal_api::ExtractionEvent>,

    pub content_table: HashMap<ContentId, internal_api::ContentMetadata>,

    pub extractor_bindings: HashMap<String, internal_api::ExtractorBinding>,

    pub extractors: HashMap<ExtractorName, internal_api::ExtractorDescription>,

    pub repositories: HashSet<String>,

    pub index_table: HashMap<String, internal_api::Index>,

    // Reverse Indexes
    pub unassigned_tasks: HashSet<TaskId>,

    pub unprocessed_extraction_events: HashSet<ExtractionEventId>,

    pub content_repository_table: HashMap<RepositoryId, HashSet<ContentId>>,

    pub bindings_table: HashMap<RepositoryId, HashSet<internal_api::ExtractorBinding>>,

    pub extractor_executors_table: HashMap<ExtractorName, HashSet<ExecutorId>>,

    pub repository_extractors: HashMap<RepositoryId, HashSet<internal_api::Index>>,
}

impl IndexifyState {
    pub fn apply(&mut self, request: Request) {
        match request {
            Request::RegisterExecutor {
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
            Request::RemoveExecutor { executor_id } => {
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
            Request::CreateTasks { tasks } => {
                for task in tasks {
                    self.tasks.insert(task.id.clone(), task.clone());
                    self.unassigned_tasks.insert(task.id.clone());
                }
            }
            Request::AssignTask { assignments } => {
                for (task_id, executor_id) in assignments {
                    self.task_assignments
                        .entry(executor_id.clone())
                        .or_default()
                        .insert(task_id.clone());
                    self.unassigned_tasks.remove(&task_id);
                }
            }
            Request::AddExtractionEvent { event } => {
                self.extraction_events
                    .insert(event.id.clone(), event.clone());
                self.unprocessed_extraction_events.insert(event.id.clone());
            }
            Request::MarkExtractionEventProcessed { event_id, ts_secs } => {
                self.unprocessed_extraction_events
                    .retain(|id| id != &event_id);
                let event = self.extraction_events.get(&event_id).map(|event| {
                    let mut event = event.to_owned();
                    event.processed_at = Some(ts_secs);
                    event
                });
                if let Some(event) = event {
                    self.extraction_events.insert(event_id.clone(), event);
                }
            }
            Request::CreateContent {
                content_metadata,
                extraction_events,
            } => {
                for content in content_metadata {
                    self.content_table
                        .insert(content.id.clone(), content.clone());
                    self.content_repository_table
                        .entry(content.repository.clone())
                        .or_default()
                        .insert(content.id.clone());
                }
                for event in extraction_events {
                    self.extraction_events
                        .insert(event.id.clone(), event.clone());
                    self.unprocessed_extraction_events.insert(event.id.clone());
                }
            }
            Request::CreateBinding {
                binding,
                extraction_event,
            } => {
                self.bindings_table
                    .entry(binding.repository.clone())
                    .or_default()
                    .insert(binding.clone());
                if let Some(extraction_event) = extraction_event {
                    self.extraction_events
                        .insert(extraction_event.id.clone(), extraction_event.clone());
                    self.unprocessed_extraction_events
                        .insert(extraction_event.id.clone());
                }
            }
            Request::CreateRepository { name } => {
                self.repositories.insert(name.clone());
            }
            Request::CreateIndex {
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
            Request::UpdateTask {
                task,
                mark_finished,
                executor_id,
                content_metadata,
                extraction_events,
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
                for event in extraction_events {
                    self.extraction_events
                        .insert(event.id.clone(), event.clone());
                    self.unprocessed_extraction_events.insert(event.id.clone());
                }
            }
        }
    }
}
