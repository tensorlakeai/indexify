use std::{collections::HashMap, time::SystemTime};

use anyhow::anyhow;
use indexify_internal_api::{
    v2::{ExtractionGraph, Task},
    *,
};
use rocksdb::OptimisticTransactionDB;
use serde::{Deserialize, Serialize};

use crate::state::{
    store::{ExecutorId, JsonEncode, JsonEncoder, StateMachineColumns, TaskId},
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
    CreateExtractionGraph {
        extraction_graph: ExtractionGraph,
        structured_data_schema: StructuredDataSchema,
        indexes: Vec<Index>,
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

pub fn convert_v2_task(
    task: Task,
    db: &OptimisticTransactionDB,
) -> Result<indexify_internal_api::Task, anyhow::Error> {
    let policy = db
        .get_cf(
            StateMachineColumns::ExtractionPolicies.cf(db),
            &task.extraction_policy_id,
        )?
        .ok_or_else(|| anyhow!("Extraction policy not found"))?;
    let policy: ExtractionPolicy =
        JsonEncoder::decode(&policy).map_err(|e| anyhow!("Failed to decode policy: {:?}", e))?;
    Ok(indexify_internal_api::Task {
        id: task.id,
        extractor: task.extractor,
        extraction_policy_name: policy.name,
        extraction_graph_name: task.extraction_graph_name,
        output_index_table_mapping: task.output_index_table_mapping,
        namespace: task.namespace,
        content_metadata: task.content_metadata.into(),
        input_params: task.input_params,
        outcome: task.outcome,
        index_tables: task.index_tables,
        creation_time: task.creation_time,
    })
}

pub fn convert_v2_payload(
    payload: RequestPayload,
    db: &OptimisticTransactionDB,
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
            ts_secs,
        } => crate::state::store::RequestPayload::RegisterExecutor {
            addr,
            executor_id,
            extractors,
            ts_secs,
        },
        RequestPayload::RemoveExecutor { executor_id } => {
            crate::state::store::RequestPayload::RemoveExecutor { executor_id }
        }
        RequestPayload::CreateNamespace { name } => {
            crate::state::store::RequestPayload::CreateNamespace { name }
        }
        RequestPayload::CreateTasks { tasks } => {
            let tasks: Result<Vec<indexify_internal_api::Task>, _> =
                tasks.into_iter().map(|t| convert_v2_task(t, db)).collect();
            crate::state::store::RequestPayload::CreateTasks { tasks: tasks? }
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
            extraction_graph: extraction_graph.into(),
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
            task: convert_v2_task(task, db)?,
            executor_id,
            update_time,
        },
        RequestPayload::MarkStateChangesProcessed { state_changes } => {
            crate::state::store::RequestPayload::MarkStateChangesProcessed { state_changes }
        }
    })
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StateMachineUpdateRequest {
    pub payload: RequestPayload,
    pub new_state_changes: Vec<StateChange>,
    pub state_changes_processed: Vec<StateChangeProcessed>,
}

pub fn convert_v2_sm_request(
    request: StateMachineUpdateRequest,
    db: &OptimisticTransactionDB,
) -> Result<crate::state::StateMachineUpdateRequest, anyhow::Error> {
    Ok(crate::state::StateMachineUpdateRequest {
        payload: convert_v2_payload(request.payload, db)?,
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

fn convert_log_payload(
    payload: OldPayload,
    db: &OptimisticTransactionDB,
) -> Result<NewPayload, anyhow::Error> {
    Ok(match payload {
        OldPayload::Blank => NewPayload::Blank,
        OldPayload::Normal(request) => NewPayload::Normal(convert_v2_sm_request(request, db)?),
        OldPayload::Membership(v) => NewPayload::Membership(v),
    })
}

pub fn convert_log_entry(
    entry: OldLogEntry,
    db: &OptimisticTransactionDB,
) -> Result<NewLogEntry, anyhow::Error> {
    Ok(NewLogEntry {
        log_id: entry.log_id,
        payload: convert_log_payload(entry.payload, db)?,
    })
}
