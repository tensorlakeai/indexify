use std::{
    sync::atomic::{self, AtomicU64},
    vec,
};

use anyhow::Result;

use crate::{
    data_model::{
        AllocationOutputIngestedEvent,
        ChangeType,
        ExecutorId,
        ExecutorRemovedEvent,
        ExecutorUpsertedEvent,
        FunctionCallEvent,
        FunctionRunFailureReason,
        FunctionRunOutcome,
        GraphUpdates,
        InvokeApplicationEvent,
        StateChange,
        StateChangeBuilder,
        StateChangeId,
        TombstoneApplicationEvent,
        TombstoneRequestEvent,
    },
    state_store::requests::{
        AllocationOutput,
        DeleteApplicationRequest,
        DeleteRequestRequest,
        DeregisterExecutorRequest,
        FunctionCallRequest,
        InvokeApplicationRequest,
    },
    utils::get_epoch_time_in_ms,
};

pub fn invoke_application(
    last_change_id: &AtomicU64,
    request: &InvokeApplicationRequest,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .namespace(Some(request.namespace.clone()))
        .application(Some(request.application_name.clone()))
        .request(Some(request.ctx.request_id.clone()))
        .change_type(ChangeType::InvokeApplication(InvokeApplicationEvent {
            namespace: request.namespace.clone(),
            request_id: request.ctx.request_id.clone(),
            application: request.application_name.clone(),
        }))
        .created_at(get_epoch_time_in_ms())
        .object_id(request.ctx.request_id.clone())
        .id(StateChangeId::new(last_change_id))
        .processed_at(None)
        .build()?;
    Ok(vec![state_change])
}

pub fn create_function_call(
    last_change_id: &AtomicU64,
    request: &FunctionCallRequest,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let graph_updates = GraphUpdates {
        graph_updates: request.updates.clone(),
        output_function_call_id: request.output_function_call_id.clone(),
    };
    let state_change = StateChangeBuilder::default()
        .namespace(Some(request.namespace.clone()))
        .application(Some(request.application_name.clone()))
        .request(Some(request.request_id.clone()))
        .change_type(ChangeType::CreateFunctionCall(FunctionCallEvent {
            namespace: request.namespace.clone(),
            request_id: request.request_id.clone(),
            application: request.application_name.clone(),
            source_function_call_id: request.source_function_call_id.clone(),
            graph_updates,
        }))
        .created_at(get_epoch_time_in_ms())
        .object_id(request.request_id.clone())
        .id(StateChangeId::new(last_change_id))
        .processed_at(None)
        .build()?;
    Ok(vec![state_change])
}

pub fn tombstone_application(
    last_change_id: &AtomicU64,
    request: &DeleteApplicationRequest,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .id(StateChangeId::new(last_change_id))
        .change_type(ChangeType::TombstoneApplication(
            TombstoneApplicationEvent {
                namespace: request.namespace.clone(),
                application: request.name.clone(),
            },
        ))
        .namespace(Some(request.namespace.clone()))
        .application(Some(request.name.clone()))
        .created_at(get_epoch_time_in_ms())
        .object_id(request.name.clone())
        .processed_at(None)
        .request(None)
        .build()?;
    Ok(vec![state_change])
}

pub fn tombstone_request(
    last_change_id: &AtomicU64,
    request: &DeleteRequestRequest,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .id(StateChangeId::new(last_change_id))
        .change_type(ChangeType::TombstoneRequest(TombstoneRequestEvent {
            namespace: request.namespace.clone(),
            application: request.application.clone(),
            request_id: request.request_id.clone(),
        }))
        .namespace(Some(request.namespace.clone()))
        .application(Some(request.application.clone()))
        .request(Some(request.request_id.clone()))
        .created_at(get_epoch_time_in_ms())
        .object_id(request.request_id.clone())
        .processed_at(None)
        .build()?;
    Ok(vec![state_change])
}

pub fn task_outputs_ingested(
    last_change_id: &AtomicU64,
    request: &AllocationOutput,
) -> Result<Vec<StateChange>> {
    // If the allocation is cancelled, we don't need to trigger the scheduler for
    // it.
    if let FunctionRunOutcome::Failure(FunctionRunFailureReason::FunctionRunCancelled) =
        request.allocation.outcome
    {
        return Ok(vec![]);
    }
    let last_change_id = last_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .namespace(Some(request.allocation.namespace.clone()))
        .application(Some(request.allocation.application.clone()))
        .request(Some(request.request_id.clone()))
        .change_type(ChangeType::AllocationOutputsIngested(Box::new(
            AllocationOutputIngestedEvent {
                namespace: request.allocation.namespace.clone(),
                application: request.allocation.application.clone(),
                function: request.allocation.function.clone(),
                request_id: request.request_id.clone(),
                function_call_id: request.allocation.function_call_id.clone(),
                data_payload: request.data_payload.clone(),
                graph_updates: request
                    .graph_updates
                    .as_ref()
                    .map(|graph_updates| GraphUpdates {
                        graph_updates: graph_updates.request_updates.clone(),
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
        .request(None)
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
        .request(None)
        .build()?;

    Ok(vec![state_change])
}
