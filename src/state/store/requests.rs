use std::collections::HashMap;

use indexify_internal_api as internal_api;
use internal_api::StateChange;
use serde::{Deserialize, Serialize};

use super::{ExecutorId, TaskId};

#[derive(Serialize, Deserialize, Clone)]
pub struct Request {
    pub payload: RequestPayload,
    pub state_changes: Vec<StateChange>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StateChangeProcessed {
    pub state_change_id: String,
    pub processed_at: u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum RequestPayload {
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
    CreateContent {
        content_metadata: Vec<internal_api::ContentMetadata>,
    },
    CreateBinding {
        binding: internal_api::ExtractorBinding,
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
    },
    RemoveExecutor {
        executor_id: String,
    },
    MarkStateChangesProcessed {
        state_changes: Vec<StateChangeProcessed>,
    },
}
