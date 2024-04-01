use std::{
    collections::{HashMap, HashSet},
    time::SystemTime,
};

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
    RemoveExecutor {
        executor_id: String,
    },
    RegisterIngestionServer {
        ingestion_server_metadata: internal_api::IngestionServerMetadata,
    },
    RemoveIngestionServer {
        ingestion_server_id: String,
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
    CreateGarbageCollectionTasks {
        gc_tasks: Vec<internal_api::GarbageCollectionTask>,
    },
    UpdateGarbageCollectionTask {
        gc_task: internal_api::GarbageCollectionTask,
        mark_finished: bool,
    },
    CreateContent {
        content_metadata: Vec<internal_api::ContentMetadata>,
    },
    TombstoneContent {
        namespace: String,
        content_ids: HashSet<String>,
    },
    RemoveTombstonedContent {
        parent_content_id: String,
        children_content_ids: HashSet<String>,
    },
    CreateExtractionPolicy {
        extraction_policy: internal_api::ExtractionPolicy,
        updated_structured_data_schema: Option<internal_api::StructuredDataSchema>,
        new_structured_data_schema: internal_api::StructuredDataSchema,
    },
    SetContentExtractionPolicyMappings {
        content_extraction_policy_mappings: Vec<internal_api::ContentExtractionPolicyMapping>,
    },
    MarkExtractionPolicyAppliedOnContent {
        content_id: String,
        extraction_policy_id: String,
        policy_completion_time: SystemTime,
    },
    CreateIndex {
        index: internal_api::Index,
        namespace: String,
        id: String,
    },
    UpdateTask {
        task: internal_api::Task,
        mark_finished: bool,
        executor_id: Option<String>,
        content_metadata: Vec<internal_api::ContentMetadata>,
    },
    MarkStateChangesProcessed {
        state_changes: Vec<StateChangeProcessed>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StateMachineUpdateResponse {
    pub handled_by: NodeId,
}
