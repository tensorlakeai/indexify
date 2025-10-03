use std::{
    sync::atomic::{self, AtomicU64},
    vec,
};

use anyhow::Result;

use crate::{
    data_model::{
        AllocationOutputIngestedEvent, ChangeType, ExecutorId, ExecutorRemovedEvent,
        ExecutorUpsertedEvent, GraphUpdates, InvokeApplicationEvent, StateChange,
        StateChangeBuilder, StateChangeId, TombstoneComputeGraphEvent, TombstoneInvocationEvent,
    },
    state_store::requests::{
        AllocationOutput, DeleteComputeGraphRequest, DeleteInvocationRequest,
        DeregisterExecutorRequest, InvokeComputeGraphRequest,
    },
    utils::get_epoch_time_in_ms,
};

pub fn invoke_application(
    last_change_id: &AtomicU64,
    request: &InvokeComputeGraphRequest,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .namespace(Some(request.namespace.clone()))
        .application(Some(request.application_name.clone()))
        .invocation(Some(request.ctx.request_id.clone()))
        .change_type(ChangeType::InvokeComputeGraph(InvokeApplicationEvent {
            namespace: request.namespace.clone(),
            invocation_id: request.ctx.request_id.clone(),
            application: request.application_name.clone(),
        }))
        .created_at(get_epoch_time_in_ms())
        .object_id(request.ctx.request_id.clone())
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
                application: request.name.clone(),
            },
        ))
        .namespace(Some(request.namespace.clone()))
        .application(Some(request.name.clone()))
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
            application: request.application.clone(),
            invocation_id: request.invocation_id.clone(),
        }))
        .namespace(Some(request.namespace.clone()))
        .application(Some(request.application.clone()))
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
        .namespace(Some(request.allocation.namespace.clone()))
        .application(Some(request.allocation.application.clone()))
        .invocation(Some(request.invocation_id.clone()))
        .change_type(ChangeType::AllocationOutputsIngested(Box::new(
            AllocationOutputIngestedEvent {
                namespace: request.allocation.namespace.clone(),
                application: request.allocation.application.clone(),
                compute_fn: request.allocation.function.clone(),
                invocation_id: request.invocation_id.clone(),
                function_call_id: request.allocation.function_call_id.clone(),
                data_payload: request.data_payload.clone(),
                graph_updates: request
                    .graph_updates
                    .as_ref()
                    .map(|graph_updates| GraphUpdates {
                        graph_updates: graph_updates.graph_updates.clone(),
                        output_function_call_id: graph_updates.output_function_call_id.clone(),
                    }),
                allocation_key: request.allocation.key(),
                request_exception: request.request_exception.clone(),
            },
        )))
        .created_at(get_epoch_time_in_ms())
        .object_id(request.allocation.function_call_id.to_string())
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
        .application(None)
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
    executor_id: &ExecutorId,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_state_change_id.fetch_add(1, atomic::Ordering::Relaxed);

    let state_change = StateChangeBuilder::default()
        .change_type(ChangeType::ExecutorUpserted(ExecutorUpsertedEvent {
            executor_id: executor_id.clone(),
        }))
        .created_at(get_epoch_time_in_ms())
        .object_id(executor_id.to_string())
        .id(StateChangeId::new(last_change_id))
        .processed_at(None)
        .namespace(None)
        .application(None)
        .invocation(None)
        .build()?;

    Ok(vec![state_change])
}
