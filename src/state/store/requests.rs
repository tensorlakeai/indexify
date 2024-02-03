use std::collections::HashMap;

use indexify_internal_api as internal_api;
use serde::{Deserialize, Serialize};

use super::{ExecutorId, TaskId};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    RegisterExecutor {
        addr: String,
        executor_id: String,
        extractor: internal_api::ExtractorDescription,
        ts_secs: u64,
    },
    CreateRepository {
        name: String,
    },
    CreateTasks {
        tasks: Vec<internal_api::Task>,
    },
    AssignTask {
        assignments: HashMap<TaskId, ExecutorId>,
    },
    AddExtractionEvent {
        event: internal_api::ExtractionEvent,
    },
    MarkExtractionEventProcessed {
        event_id: String,
        ts_secs: u64,
    },
    CreateContent {
        content_metadata: Vec<internal_api::ContentMetadata>,
        extraction_events: Vec<internal_api::ExtractionEvent>,
    },
    CreateBinding {
        binding: internal_api::ExtractorBinding,
        extraction_event: Option<internal_api::ExtractionEvent>,
    },
    CreateIndex {
        index: internal_api::Index,
        repository: String,
        id: String,
    },
    UpdateTask {
        task: internal_api::Task,
        mark_finished: bool,
        executor_id: Option<String>,
        content_metadata: Vec<internal_api::ContentMetadata>,
        extraction_events: Vec<internal_api::ExtractionEvent>,
    },
    RemoveExecutor {
        executor_id: String,
    },
}
