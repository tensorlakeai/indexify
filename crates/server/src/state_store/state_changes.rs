use std::{
    sync::atomic::{self, AtomicU64},
    vec,
};

use anyhow::Result;

use crate::{
    data_model::{
        AllocationOutputIngestedEvent,
        ChangeType,
        CreateContainerPoolEvent,
        CreateSandboxEvent,
        DeleteContainerPoolEvent,
        ExecutorId,
        ExecutorUpsertedEvent,
        FunctionCallEvent,
        GraphUpdates,
        InvokeApplicationEvent,
        SnapshotSandboxEvent,
        StateChange,
        StateChangeBuilder,
        StateChangeId,
        TerminateSandboxEvent,
        TombstoneApplicationEvent,
        TombstoneRequestEvent,
        UpdateContainerPoolEvent,
    },
    state_store::requests::{
        AllocationOutput,
        CreateContainerPoolRequest,
        CreateOrUpdateApplicationRequest,
        CreateSandboxRequest,
        DataplaneResultsRequest,
        DeleteApplicationRequest,
        DeleteContainerPoolRequest,
        DeleteRequestRequest,
        FunctionCallRequest,
        InvokeApplicationRequest,
        SnapshotSandboxRequest,
        TerminateSandboxRequest,
        UpdateContainerPoolRequest,
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
    let state_change = StateChangeBuilder::default()
        .namespace(Some(request.namespace.clone()))
        .application(Some(request.application_name.clone()))
        .change_type(ChangeType::CreateFunctionCall(FunctionCallEvent {
            namespace: request.namespace.clone(),
            request_id: request.request_id.clone(),
            application: request.application_name.clone(),
            source_function_call_id: request.source_function_call_id.clone(),
            graph_updates: GraphUpdates {
                graph_updates: request.graph_updates.request_updates.clone(),
                output_function_call_id: request.graph_updates.output_function_call_id.clone(),
            },
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
    let last_change_id = last_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .namespace(Some(request.allocation.namespace.clone()))
        .application(Some(request.allocation.application.clone()))
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
                request_exception: request.request_exception.clone(),
                allocation_id: request.allocation.id.clone(),
                allocation_target: request.allocation.target.clone(),
                allocation_outcome: request.allocation.outcome.clone(),
                execution_duration_ms: request.allocation.execution_duration_ms,
            },
        )))
        .created_at(get_epoch_time_in_ms())
        .object_id(request.allocation.function_call_id.to_string())
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
        .build()?;

    Ok(vec![state_change])
}

pub fn create_sandbox(
    last_change_id: &AtomicU64,
    request: &CreateSandboxRequest,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .namespace(Some(request.sandbox.namespace.clone()))
        .application(None)
        .change_type(ChangeType::CreateSandbox(CreateSandboxEvent {
            namespace: request.sandbox.namespace.clone(),
            sandbox_id: request.sandbox.id.clone(),
        }))
        .created_at(get_epoch_time_in_ms())
        .object_id(request.sandbox.id.to_string())
        .id(StateChangeId::new(last_change_id))
        .processed_at(None)
        .build()?;
    Ok(vec![state_change])
}

pub fn terminate_sandbox(
    last_change_id: &AtomicU64,
    request: &TerminateSandboxRequest,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .namespace(Some(request.namespace.clone()))
        .application(None)
        .change_type(ChangeType::TerminateSandbox(TerminateSandboxEvent {
            namespace: request.namespace.clone(),
            sandbox_id: request.sandbox_id.clone(),
        }))
        .created_at(get_epoch_time_in_ms())
        .object_id(request.sandbox_id.to_string())
        .id(StateChangeId::new(last_change_id))
        .processed_at(None)
        .build()?;
    Ok(vec![state_change])
}

pub fn create_container_pool(
    last_change_id: &AtomicU64,
    request: &CreateContainerPoolRequest,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .namespace(Some(request.pool.namespace.clone()))
        .application(None)
        .change_type(ChangeType::CreateContainerPool(CreateContainerPoolEvent {
            namespace: request.pool.namespace.clone(),
            pool_id: request.pool.id.clone(),
        }))
        .created_at(get_epoch_time_in_ms())
        .object_id(request.pool.id.to_string())
        .id(StateChangeId::new(last_change_id))
        .processed_at(None)
        .build()?;
    Ok(vec![state_change])
}

pub fn update_container_pool(
    last_change_id: &AtomicU64,
    request: &UpdateContainerPoolRequest,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .namespace(Some(request.pool.namespace.clone()))
        .application(None)
        .change_type(ChangeType::UpdateContainerPool(UpdateContainerPoolEvent {
            namespace: request.pool.namespace.clone(),
            pool_id: request.pool.id.clone(),
        }))
        .created_at(get_epoch_time_in_ms())
        .object_id(request.pool.id.to_string())
        .id(StateChangeId::new(last_change_id))
        .processed_at(None)
        .build()?;
    Ok(vec![state_change])
}

pub fn delete_container_pool(
    last_change_id: &AtomicU64,
    request: &DeleteContainerPoolRequest,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .namespace(Some(request.namespace.clone()))
        .application(None)
        .change_type(ChangeType::DeleteContainerPool(DeleteContainerPoolEvent {
            namespace: request.namespace.clone(),
            pool_id: request.pool_id.clone(),
        }))
        .created_at(get_epoch_time_in_ms())
        .object_id(request.pool_id.to_string())
        .id(StateChangeId::new(last_change_id))
        .processed_at(None)
        .build()?;
    Ok(vec![state_change])
}

/// Generate CreateContainerPool state changes for each pool in a
/// CreateOrUpdateApplication request. Without these, the application
/// processor is never notified and the buffer reconciler never runs
/// for newly deployed applications.
pub fn create_or_update_application_pools(
    last_change_id: &AtomicU64,
    request: &CreateOrUpdateApplicationRequest,
) -> Result<Vec<StateChange>> {
    let mut changes = Vec::with_capacity(request.container_pools.len());
    for pool in &request.container_pools {
        let last_id = last_change_id.fetch_add(1, atomic::Ordering::Relaxed);
        let state_change = StateChangeBuilder::default()
            .namespace(Some(request.namespace.clone()))
            .application(Some(request.application.name.clone()))
            .change_type(ChangeType::CreateContainerPool(CreateContainerPoolEvent {
                namespace: request.namespace.clone(),
                pool_id: pool.id.clone(),
            }))
            .created_at(get_epoch_time_in_ms())
            .object_id(pool.id.to_string())
            .id(StateChangeId::new(last_id))
            .processed_at(None)
            .build()?;
        changes.push(state_change);
    }
    Ok(changes)
}

pub fn snapshot_sandbox(
    last_change_id: &AtomicU64,
    request: &SnapshotSandboxRequest,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .namespace(Some(request.snapshot.namespace.clone()))
        .application(None)
        .change_type(ChangeType::SnapshotSandbox(SnapshotSandboxEvent {
            namespace: request.snapshot.namespace.clone(),
            sandbox_id: request.snapshot.sandbox_id.clone(),
            snapshot_id: request.snapshot.id.clone(),
            upload_uri: request.upload_uri.clone(),
        }))
        .created_at(get_epoch_time_in_ms())
        .object_id(request.snapshot.id.to_string())
        .id(StateChangeId::new(last_change_id))
        .processed_at(None)
        .build()?;
    Ok(vec![state_change])
}

pub fn dataplane_results_ingested(
    last_change_id: &AtomicU64,
    request: &DataplaneResultsRequest,
) -> Result<Vec<StateChange>> {
    let last_change_id = last_change_id.fetch_add(1, atomic::Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .namespace(None)
        .application(None)
        .change_type(ChangeType::DataplaneResultsIngested(request.event.clone()))
        .created_at(get_epoch_time_in_ms())
        .object_id(request.event.executor_id.to_string())
        .id(StateChangeId::new(last_change_id))
        .processed_at(None)
        .build()?;
    Ok(vec![state_change])
}
