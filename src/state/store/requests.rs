use std::{collections::HashMap, time::SystemTime};

use indexify_internal_api::{self as internal_api, GarbageCollectionTask};
use internal_api::{ExtractionGraphLink, StateChange, StateChangeId};
use serde::{Deserialize, Serialize};

use super::{ExecutorId, TaskId};
use crate::state::NodeId;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StateMachineUpdateRequest {
    pub payload: RequestPayload,
    pub new_state_changes: Vec<StateChange>,
    pub state_changes_processed: Vec<StateChangeProcessed>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StateChangeProcessed {
    pub state_change_id: StateChangeId,
    pub processed_at: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CreateOrUpdateContentEntry {
    pub content: internal_api::ContentMetadata,
    pub previous_parent: Option<internal_api::ContentMetadataId>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RequestPayload {
    //  NOTE: This isn't strictly a state machine update. It's used to change cluster membership.
    JoinCluster {
        node_id: NodeId,
        address: String,
        coordinator_addr: String,
    },
    RegisterExecutor {
        addr: String,
        executor_id: String,
        extractors: Vec<internal_api::ExtractorDescription>,
        ts_secs: u64,
    },
    RemoveExecutor {
        executor_id: String,
    },
    CreateNamespace {
        name: String,
    },
    CreateTasks {
        tasks: Vec<internal_api::Task>,
    },
    AssignTask {
        assignments: HashMap<TaskId, ExecutorId>,
    },
    CreateOrAssignGarbageCollectionTask {
        gc_tasks: Vec<GarbageCollectionTask>,
    },
    UpdateGarbageCollectionTask {
        gc_task: GarbageCollectionTask,
        mark_finished: bool,
    },
    AddGraphToContent {
        content_ids: Vec<String>,
        namespace: String,
        extraction_graph: String,
    },
    CreateExtractionGraph {
        extraction_graph: internal_api::ExtractionGraph,
        structured_data_schema: internal_api::StructuredDataSchema,
        indexes: Vec<internal_api::Index>,
    },
    DeleteExtractionGraph {
        graph_id: String,
        gc_task: GarbageCollectionTask,
    },
    CreateExtractionGraphLink {
        extraction_graph_link: ExtractionGraphLink,
    },
    CreateOrUpdateContent {
        entries: Vec<CreateOrUpdateContentEntry>,
    },
    TombstoneContentTree {
        content_metadata: Vec<internal_api::ContentMetadata>,
    },
    TombstoneContent {
        content_metadata: Vec<internal_api::ContentMetadata>,
    },
    SetIndex {
        indexes: Vec<internal_api::Index>,
    },
    UpdateTask {
        task: internal_api::Task,
        executor_id: Option<String>,
        update_time: SystemTime,
    },
    MarkStateChangesProcessed {
        state_changes: Vec<StateChangeProcessed>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StateMachineUpdateResponse {
    pub handled_by: NodeId,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct V1StateMachineUpdateRequest {
    pub payload: V1RequestPayload,
    pub new_state_changes: Vec<StateChange>,
    pub state_changes_processed: Vec<StateChangeProcessed>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum V1RequestPayload {
    JoinCluster {
        node_id: NodeId,
        address: String,
        coordinator_addr: String,
    },
    RegisterExecutor {
        addr: String,
        executor_id: String,
        extractors: Vec<internal_api::ExtractorDescription>,
        ts_secs: u64,
    },
    RemoveExecutor {
        executor_id: String,
    },
    CreateNamespace {
        name: String,
    },
    CreateTasks {
        tasks: Vec<internal_api::v1::Task>,
    },
    AssignTask {
        assignments: HashMap<TaskId, ExecutorId>,
    },
    CreateOrAssignGarbageCollectionTask {
        gc_tasks: Vec<GarbageCollectionTask>,
    },
    UpdateGarbageCollectionTask {
        gc_task: GarbageCollectionTask,
        mark_finished: bool,
    },
    CreateExtractionGraph {
        extraction_graph: internal_api::v1::ExtractionGraph,
        structured_data_schema: internal_api::StructuredDataSchema,
        indexes: Vec<internal_api::Index>,
    },
    CreateOrUpdateContent {
        entries: Vec<V1CreateOrUpdateContentEntry>,
    },
    TombstoneContentTree {
        content_metadata: Vec<internal_api::v1::ContentMetadata>,
    },
    SetIndex {
        indexes: Vec<internal_api::Index>,
    },
    UpdateTask {
        task: internal_api::v1::Task,
        executor_id: Option<String>,
        update_time: SystemTime,
    },
    MarkStateChangesProcessed {
        state_changes: Vec<StateChangeProcessed>,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct V1CreateOrUpdateContentEntry {
    pub content: internal_api::v1::ContentMetadata,
    pub previous_parent: Option<internal_api::ContentMetadataId>,
}

impl From<V1CreateOrUpdateContentEntry> for CreateOrUpdateContentEntry {
    fn from(entry: V1CreateOrUpdateContentEntry) -> Self {
        CreateOrUpdateContentEntry {
            content: entry.content.into(),
            previous_parent: entry.previous_parent,
        }
    }
}
