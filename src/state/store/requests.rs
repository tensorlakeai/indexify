use std::collections::HashMap;

use indexify_internal_api as internal_api;
use internal_api::StateChange;
use serde::{Deserialize, Serialize};

use super::{ExecutorId, TaskId};
use crate::state::NodeId;

#[derive(Serialize, Deserialize, Clone)]
pub struct StateMachineUpdateRequest {
    pub payload: RequestPayload,
    pub new_state_changes: Vec<StateChange>,
    pub state_changes_processed: Vec<StateChangeProcessed>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StateChangeProcessed {
    pub state_change_id: String,
    pub processed_at: u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum RequestPayload {
    //  NOTE: This isn't strictly a state machine update. It's used to change cluster membership.
    JoinCluster {
        node_id: NodeId,
        address: String,
    },
    RegisterExecutor {
        addr: String,
        executor_id: String,
        extractor: internal_api::ExtractorDescription,
        ts_secs: u64,
    },
    CreateNamespace {
        name: String,
        structured_data_schema: internal_api::StructuredDataSchema,
    },
    CreateTasks {
        tasks: Vec<internal_api::Task>,
    },
    AssignTask {
        assignments: HashMap<TaskId, ExecutorId>,
    },
    UpdateTask {
        task: internal_api::Task,
        mark_finished: bool,
        executor_id: Option<String>,
        content_metadata: Vec<internal_api::ContentMetadata>,
    },
    CreateGarbageCollectionTasks {
        gc_tasks: Vec<internal_api::GarbageCollectionTask>,
    },
    CreateContent {
        content_metadata: Vec<internal_api::ContentMetadata>,
    },
    DeleteContent {
        namespace: String,
        content_id: String,
    },
    CreateExtractionPolicy {
        extraction_policy: internal_api::ExtractionPolicy,
        updated_structured_data_schema: Option<internal_api::StructuredDataSchema>,
        new_structured_data_schema: internal_api::StructuredDataSchema,
    },
    CreateIndex {
        index: internal_api::Index,
        namespace: String,
        id: String,
    },
    RemoveExecutor {
        executor_id: String,
    },
    MarkStateChangesProcessed {
        state_changes: Vec<StateChangeProcessed>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StateMachineUpdateResponse {
    pub handled_by: NodeId,
}
