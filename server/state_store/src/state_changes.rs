use std::{
    sync::atomic::{self, AtomicU64},
    vec,
};

use anyhow::Result;
use data_model::{
    ChangeType,
    ExecutorAddedEvent,
    ExecutorRemovedEvent,
    InvokeComputeGraphEvent,
    StateChange,
    StateChangeBuilder,
    StateChangeId,
    TaskCreatedEvent,
    TaskOutputsIngestedEvent,
    TombstoneComputeGraphEvent,
    TombstoneInvocationEvent,
};
use indexify_utils::get_epoch_time_in_ms;

use crate::requests::{
    DeleteComputeGraphRequest,
    DeleteInvocationRequest,
    DeregisterExecutorRequest,
    IngestTaskOutputsRequest,
    InvokeComputeGraphRequest,
    MutateClusterTopologyRequest,
    NamespaceProcessorUpdateRequest,
    RegisterExecutorRequest,
};

pub fn invoke_compute_graph(
    last_change_id: &AtomicU64,
    request: &InvokeComputeGraphRequest,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .namespace(Some(request.namespace.clone()))
        .compute_graph(Some(request.compute_graph_name.clone()))
        .invocation(Some(request.invocation_payload.id.clone()))
        .change_type(ChangeType::InvokeComputeGraph(InvokeComputeGraphEvent {
            namespace: request.namespace.clone(),
            invocation_id: request.invocation_payload.id.clone(),
            compute_graph: request.compute_graph_name.clone(),
        }))
        .created_at(get_epoch_time_in_ms())
        .object_id(request.invocation_payload.id.clone())
        .id(StateChangeId::new(last_change_id))
        .processed_at(None)
        .build()?;
    Ok(vec![state_change])
}

pub fn tombstone_compute_graph(
    last_change_id: &AtomicU64,
    request: &DeleteComputeGraphRequest,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .id(StateChangeId::new(last_change_id))
        .change_type(ChangeType::TombstoneComputeGraph(
            TombstoneComputeGraphEvent {
                namespace: request.namespace.clone(),
                compute_graph: request.name.clone(),
            },
        ))
        .namespace(Some(request.namespace.clone()))
        .compute_graph(Some(request.name.clone()))
        .created_at(get_epoch_time_in_ms())
        .object_id(request.name.clone())
        .processed_at(None)
        .invocation(None)
        .build()?;
    Ok(vec![state_change])
}

pub fn tombstone_invocation(
    last_change_id: &AtomicU64,
    request: &DeleteInvocationRequest,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .id(StateChangeId::new(last_change_id))
        .change_type(ChangeType::TombstoneInvocation(TombstoneInvocationEvent {
            namespace: request.namespace.clone(),
            compute_graph: request.compute_graph.clone(),
            invocation_id: request.invocation_id.clone(),
        }))
        .namespace(Some(request.namespace.clone()))
        .compute_graph(Some(request.compute_graph.clone()))
        .invocation(Some(request.invocation_id.clone()))
        .created_at(get_epoch_time_in_ms())
        .object_id(request.invocation_id.clone())
        .processed_at(None)
        .build()?;
    Ok(vec![state_change])
}

pub fn task_outputs_ingested(
    last_change_id: &AtomicU64,
    request: &IngestTaskOutputsRequest,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .namespace(Some(request.namespace.clone()))
        .compute_graph(Some(request.compute_graph.clone()))
        .invocation(Some(request.invocation_id.clone()))
        .change_type(ChangeType::TaskOutputsIngested(TaskOutputsIngestedEvent {
            namespace: request.namespace.clone(),
            compute_graph: request.compute_graph.clone(),
            compute_fn: request.compute_fn.clone(),
            invocation_id: request.invocation_id.clone(),
            task_id: request.task_id.clone(),
            executor_id: request.executor_id.clone(),
        }))
        .created_at(get_epoch_time_in_ms())
        .object_id(request.task_id.clone().to_string())
        .id(StateChangeId::new(last_change_id))
        .processed_at(None)
        .build()?;
    Ok(vec![state_change])
}

pub fn change_events_for_namespace_processor_update(
    last_state_change_id: &AtomicU64,
    req: &NamespaceProcessorUpdateRequest,
) -> Result<Vec<StateChange>> {
    let mut state_changes = Vec::new();
    for task in &req.task_requests {
        let last_change_id = last_state_change_id.fetch_add(1, atomic::Ordering::Relaxed);
        let state_change = StateChangeBuilder::default()
            .change_type(ChangeType::TaskCreated(TaskCreatedEvent {
                task: task.clone(),
            }))
            .namespace(Some(task.namespace.clone()))
            .compute_graph(Some(task.compute_graph_name.clone()))
            .invocation(Some(task.invocation_id.clone()))
            .created_at(get_epoch_time_in_ms())
            .object_id(task.id.to_string())
            .id(StateChangeId::new(last_change_id))
            .processed_at(None)
            .build()?;
        state_changes.push(state_change);
    }
    Ok(state_changes)
}

pub fn deregister_executor_events(
    last_state_change_id: &AtomicU64,
    request: &MutateClusterTopologyRequest,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_state_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .change_type(ChangeType::ExecutorRemoved(ExecutorRemovedEvent {
            executor_id: request.executor_removed.clone(),
        }))
        .namespace(None)
        .compute_graph(None)
        .invocation(None)
        .created_at(get_epoch_time_in_ms())
        .object_id(request.executor_removed.get().to_string())
        .id(StateChangeId::new(last_change_id))
        .processed_at(None)
        .build()?;
    Ok(vec![state_change])
}

pub fn tombstone_executor(
    last_state_change_id: &AtomicU64,
    request: &DeregisterExecutorRequest,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_state_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .change_type(ChangeType::TombStoneExecutor(ExecutorRemovedEvent {
            executor_id: request.executor_id.clone(),
        }))
        .namespace(None)
        .compute_graph(None)
        .invocation(None)
        .created_at(get_epoch_time_in_ms())
        .object_id(request.executor_id.get().to_string())
        .id(StateChangeId::new(last_change_id))
        .processed_at(None)
        .build()?;
    Ok(vec![state_change])
}

pub fn register_executor(
    last_state_change_id: &AtomicU64,
    request: &RegisterExecutorRequest,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_state_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .change_type(ChangeType::ExecutorAdded(ExecutorAddedEvent {
            executor_id: request.executor.id.clone(),
        }))
        .created_at(get_epoch_time_in_ms())
        .object_id(request.executor.id.to_string())
        .id(StateChangeId::new(last_change_id))
        .processed_at(None)
        .namespace(None)
        .compute_graph(None)
        .invocation(None)
        .build()?;

    Ok(vec![state_change])
}
