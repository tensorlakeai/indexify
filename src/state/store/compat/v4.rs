use std::{collections::HashMap, time::SystemTime};

use indexify_internal_api::*;
use rocksdb::OptimisticTransactionDB;
use serde::{Deserialize, Serialize};

use crate::state::{
    store::{ExecutorId, TaskId},
    BasicNode,
    CreateOrUpdateContentEntry,
    NodeId,
    StateChangeProcessed,
};

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
        extractors: Vec<ExtractorDescription>,
        ts_secs: u64,
    },
    RemoveExecutor {
        executor_id: String,
    },
    CreateNamespace {
        name: String,
    },
    CreateTasks {
        tasks: Vec<Task>,
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
        extraction_graph: ExtractionGraph,
        structured_data_schema: StructuredDataSchema,
        indexes: Vec<Index>,
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
        content_metadata: Vec<ContentMetadata>,
    },
    TombstoneContent {
        content_metadata: Vec<ContentMetadata>,
    },
    SetIndex {
        indexes: Vec<Index>,
    },
    UpdateTask {
        task: Task,
        executor_id: Option<String>,
        update_time: SystemTime,
    },
    MarkStateChangesProcessed {
        state_changes: Vec<StateChangeProcessed>,
    },
}

pub fn convert_v4_payload(
    payload: RequestPayload,
) -> Result<crate::state::store::RequestPayload, anyhow::Error> {
    Ok(match payload {
        RequestPayload::JoinCluster {
            node_id,
            address,
            coordinator_addr,
        } => crate::state::store::RequestPayload::JoinCluster {
            node_id,
            address,
            coordinator_addr,
        },
        RequestPayload::RegisterExecutor {
            addr,
            executor_id,
            extractors,
            ts_secs: _,
        } => crate::state::store::RequestPayload::RegisterExecutor(ExecutorMetadata {
            addr,
            id: executor_id,
            extractors,
            ..Default::default()
        }),
        RequestPayload::RemoveExecutor { executor_id } => {
            crate::state::store::RequestPayload::RemoveExecutor { executor_id }
        }
        RequestPayload::CreateNamespace { name } => {
            crate::state::store::RequestPayload::CreateNamespace { name }
        }
        RequestPayload::CreateTasks { tasks } => {
            crate::state::store::RequestPayload::CreateTasks { tasks }
        }
        RequestPayload::AssignTask { assignments } => {
            crate::state::store::RequestPayload::AssignTask { assignments }
        }
        RequestPayload::CreateOrAssignGarbageCollectionTask { gc_tasks } => {
            crate::state::store::RequestPayload::CreateOrAssignGarbageCollectionTask { gc_tasks }
        }
        RequestPayload::UpdateGarbageCollectionTask {
            gc_task,
            mark_finished,
        } => crate::state::store::RequestPayload::UpdateGarbageCollectionTask {
            gc_task,
            mark_finished,
        },
        RequestPayload::CreateExtractionGraph {
            extraction_graph,
            structured_data_schema,
            indexes,
        } => crate::state::store::RequestPayload::CreateExtractionGraph {
            extraction_graph,
            structured_data_schema,
            indexes,
        },
        RequestPayload::CreateExtractionGraphLink {
            extraction_graph_link,
        } => crate::state::store::RequestPayload::CreateExtractionGraphLink {
            extraction_graph_link,
        },
        RequestPayload::CreateOrUpdateContent { entries } => {
            crate::state::store::RequestPayload::CreateOrUpdateContent { entries }
        }
        RequestPayload::TombstoneContentTree { content_metadata } => {
            crate::state::store::RequestPayload::TombstoneContentTree { content_metadata }
        }
        RequestPayload::SetIndex { indexes } => {
            crate::state::store::RequestPayload::SetIndex { indexes }
        }
        RequestPayload::UpdateTask {
            task,
            executor_id,
            update_time,
        } => crate::state::store::RequestPayload::UpdateTask {
            task,
            executor_id,
            update_time,
        },
        RequestPayload::MarkStateChangesProcessed { state_changes } => {
            crate::state::store::RequestPayload::MarkStateChangesProcessed { state_changes }
        }
        RequestPayload::AddGraphToContent {
            content_ids,
            namespace,
            extraction_graph,
        } => crate::state::store::RequestPayload::AddGraphToContent {
            content_ids,
            namespace,
            extraction_graph,
        },
        RequestPayload::DeleteExtractionGraph { graph_id, gc_task } => {
            crate::state::store::RequestPayload::DeleteExtractionGraph { graph_id, gc_task }
        }
        RequestPayload::TombstoneContent { content_metadata } => {
            crate::state::store::RequestPayload::TombstoneContent { content_metadata }
        }
    })
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StateMachineUpdateRequest {
    pub payload: RequestPayload,
    pub new_state_changes: Vec<StateChange>,
    pub state_changes_processed: Vec<StateChangeProcessed>,
}

fn convert_v4_sm_request(
    request: StateMachineUpdateRequest,
) -> Result<crate::state::StateMachineUpdateRequest, anyhow::Error> {
    Ok(crate::state::StateMachineUpdateRequest {
        payload: convert_v4_payload(request.payload)?,
        new_state_changes: request.new_state_changes,
        state_changes_processed: request.state_changes_processed,
    })
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = StateMachineUpdateRequest,
        R = (),
        NodeId = NodeId,
        Node = BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = (),
        AsyncRuntime = openraft::TokioRuntime
);

type OldPayload = openraft::EntryPayload<TypeConfig>;
type NewTypeConfig = crate::state::TypeConfig;
type NewPayload = openraft::EntryPayload<NewTypeConfig>;
type OldLogEntry = openraft::Entry<TypeConfig>;
type NewLogEntry = openraft::Entry<NewTypeConfig>;

fn convert_log_payload(payload: OldPayload) -> Result<NewPayload, anyhow::Error> {
    Ok(match payload {
        OldPayload::Blank => NewPayload::Blank,
        OldPayload::Normal(request) => NewPayload::Normal(convert_v4_sm_request(request)?),
        OldPayload::Membership(v) => NewPayload::Membership(v),
    })
}

pub fn convert_log_entry(
    entry: OldLogEntry,
    _db: &OptimisticTransactionDB,
) -> Result<NewLogEntry, anyhow::Error> {
    Ok(NewLogEntry {
        log_id: entry.log_id,
        payload: convert_log_payload(entry.payload)?,
    })
}
