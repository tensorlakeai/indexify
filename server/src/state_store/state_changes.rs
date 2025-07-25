use std::{
    sync::atomic::{self, AtomicU64},
    vec,
};

use anyhow::Result;

use crate::{
    data_model::{
        AllocationOutputIngestedEvent,
        ChangeType,
        ExecutorRemovedEvent,
        ExecutorUpsertedEvent,
        InvokeComputeGraphEvent,
        StateChange,
        StateChangeBuilder,
        StateChangeId,
        TombstoneComputeGraphEvent,
        TombstoneInvocationEvent,
    },
    state_store::requests::{
        AllocationOutput,
        DeleteComputeGraphRequest,
        DeleteInvocationRequest,
        DeregisterExecutorRequest,
        InvokeComputeGraphRequest,
        UpsertExecutorRequest,
    },
    utils::get_epoch_time_in_ms,
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
    request: &AllocationOutput,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .namespace(Some(request.namespace.clone()))
        .compute_graph(Some(request.compute_graph.clone()))
        .invocation(Some(request.invocation_id.clone()))
        .change_type(ChangeType::AllocationOutputsIngested(
            AllocationOutputIngestedEvent {
                namespace: request.namespace.clone(),
                compute_graph: request.compute_graph.clone(),
                compute_fn: request.compute_fn.clone(),
                invocation_id: request.invocation_id.clone(),
                task_id: request.allocation.task_id.clone(),
                node_output_key: request.node_output.key(),
                allocation_key: Some(request.allocation_key.clone()),
            },
        ))
        .created_at(get_epoch_time_in_ms())
        .object_id(request.allocation.task_id.get().to_string())
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

pub fn upsert_executor(
    last_state_change_id: &AtomicU64,
    request: &UpsertExecutorRequest,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_state_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .change_type(ChangeType::ExecutorUpserted(ExecutorUpsertedEvent {
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
