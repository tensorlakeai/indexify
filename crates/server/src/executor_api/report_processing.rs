use super::*;
use crate::{
    data_model::FunctionCallId,
    proto_convert::{
        prepare_data_payload,
        proto_container_termination_to_internal,
        proto_failure_reason_to_internal,
        proto_failure_reason_to_termination_reason,
        to_internal_compute_op,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocationIngestDisposition {
    Applied,
    SkippedNoop,
}

fn is_malformed_command_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("missing ") ||
        message.contains(" is empty") ||
        message.contains("invalid") ||
        message.contains("malformed")
}

/// Process command responses from a dataplane executor.
///
/// Converts proto `CommandResponse` messages into a single
/// `DataplaneResultsIngestedEvent` and writes it to the state machine via
/// `RequestPayload::DataplaneResults`. This is the shared logic used by both
/// the `report_command_responses` RPC handler and tests.
pub async fn process_command_responses(
    indexify_state: &Arc<IndexifyState>,
    executor_id: &ExecutorId,
    responses: Vec<executor_api_pb::CommandResponse>,
) -> Result<usize> {
    use data_model::ContainerStateUpdateInfo;

    let mut failed_items = 0usize;
    let mut container_state_updates = Vec::new();
    let mut container_started_ids = Vec::new();

    for resp in responses {
        let Some(response) = resp.response else {
            warn!(
                executor_id = executor_id.get(),
                request_id = "",
                "fn" = "",
                namespace = "",
                app = "",
                version = "",
                allocation_id = "",
                command_seq = ?resp.command_seq,
                "CommandResponse with no response oneof, skipping malformed item"
            );
            continue;
        };

        match response {
            executor_api_pb::command_response::Response::AllocationScheduled(scheduled) => {
                info!(
                    executor_id = executor_id.get(),
                    allocation_id = %scheduled.allocation_id,
                    command_seq = ?resp.command_seq,
                    "AllocationScheduled ack received"
                );
                // State tracking for allocation scheduling acks can be added
                // later. For now, we just log the ack.
            }
            executor_api_pb::command_response::Response::ContainerTerminated(terminated) => {
                let reason = proto_container_termination_to_internal(terminated.reason());
                info!(
                    executor_id = executor_id.get(),
                    container_id = %terminated.container_id,
                    reason = ?reason,
                    "ContainerTerminated ingested"
                );
                container_state_updates.push(ContainerStateUpdateInfo {
                    container_id: data_model::ContainerId::new(terminated.container_id),
                    termination_reason: Some(reason),
                });
            }
            executor_api_pb::command_response::Response::ContainerStarted(started) => {
                info!(
                    executor_id = executor_id.get(),
                    container_id = started.container_id,
                    "ContainerStarted -- will promote sandbox if pending"
                );
                container_started_ids.push(data_model::ContainerId::new(started.container_id));
            }
            executor_api_pb::command_response::Response::SnapshotCompleted(completed) => {
                info!(
                    executor_id = executor_id.get(),
                    container_id = %completed.container_id,
                    snapshot_id = %completed.snapshot_id,
                    snapshot_uri = %completed.snapshot_uri,
                    size_bytes = completed.size_bytes,
                    "SnapshotCompleted received"
                );
                if let Err(err) = handle_snapshot_completed(indexify_state, &completed).await {
                    if is_malformed_command_error(&err) {
                        warn!(
                            executor_id = executor_id.get(),
                            request_id = "",
                            "fn" = "",
                            namespace = "",
                            app = "",
                            version = "",
                            allocation_id = "",
                            command_seq = ?resp.command_seq,
                            container_id = %completed.container_id,
                            snapshot_id = %completed.snapshot_id,
                            error = %err,
                            "heartbeat: malformed snapshot_completed command response; skipping"
                        );
                        continue;
                    }
                    warn!(
                        executor_id = executor_id.get(),
                        request_id = "",
                        "fn" = "",
                        namespace = "",
                        app = "",
                        version = "",
                        allocation_id = "",
                        command_seq = ?resp.command_seq,
                        container_id = %completed.container_id,
                        snapshot_id = %completed.snapshot_id,
                        error = %err,
                        "SnapshotCompleted ingest failed"
                    );
                    failed_items = failed_items.saturating_add(1);
                }
            }
            executor_api_pb::command_response::Response::SnapshotFailed(failed) => {
                warn!(
                    executor_id = executor_id.get(),
                    container_id = %failed.container_id,
                    snapshot_id = %failed.snapshot_id,
                    error = %failed.error_message,
                    "SnapshotFailed received"
                );
                if let Err(err) = handle_snapshot_failed(indexify_state, &failed).await {
                    if is_malformed_command_error(&err) {
                        warn!(
                            executor_id = executor_id.get(),
                            request_id = "",
                            "fn" = "",
                            namespace = "",
                            app = "",
                            version = "",
                            allocation_id = "",
                            command_seq = ?resp.command_seq,
                            container_id = %failed.container_id,
                            snapshot_id = %failed.snapshot_id,
                            error = %err,
                            "heartbeat: malformed snapshot_failed command response; skipping"
                        );
                        continue;
                    }
                    warn!(
                        executor_id = executor_id.get(),
                        request_id = "",
                        "fn" = "",
                        namespace = "",
                        app = "",
                        version = "",
                        allocation_id = "",
                        command_seq = ?resp.command_seq,
                        container_id = %failed.container_id,
                        snapshot_id = %failed.snapshot_id,
                        error = %err,
                        "SnapshotFailed ingest failed"
                    );
                    failed_items = failed_items.saturating_add(1);
                }
            }
        }
    }

    if container_state_updates.is_empty() && container_started_ids.is_empty() {
        return Ok(failed_items);
    }

    write_dataplane_results(
        indexify_state,
        executor_id,
        vec![],
        container_state_updates,
        container_started_ids,
    )
    .await?;

    Ok(failed_items)
}

/// Process a single AllocationCompleted message.
pub async fn process_allocation_completed(
    indexify_state: &Arc<IndexifyState>,
    blob_storage_registry: &Arc<BlobStorageRegistry>,
    executor_id: &ExecutorId,
    completed: executor_api_pb::AllocationCompleted,
) -> Result<AllocationIngestDisposition> {
    use data_model::{AllocationOutputIngestedEvent, FunctionRunOutcome, GraphUpdates};

    let function = completed
        .function
        .ok_or_else(|| anyhow::anyhow!("AllocationCompleted missing function"))?;
    let namespace = function
        .namespace
        .ok_or_else(|| anyhow::anyhow!("AllocationCompleted missing namespace"))?;
    let application = function
        .application_name
        .ok_or_else(|| anyhow::anyhow!("AllocationCompleted missing application_name"))?;
    let fn_name = function
        .function_name
        .ok_or_else(|| anyhow::anyhow!("AllocationCompleted missing function_name"))?;
    let request_id = completed
        .request_id
        .ok_or_else(|| anyhow::anyhow!("AllocationCompleted missing request_id"))?;
    let function_call_id = completed
        .function_call_id
        .ok_or_else(|| anyhow::anyhow!("AllocationCompleted missing function_call_id"))?;
    let allocation_id = completed.allocation_id;

    let allocation_key =
        data_model::Allocation::key_from(&namespace, &application, &request_id, &allocation_id);
    let Some(allocation) = indexify_state
        .reader()
        .get_allocation(&allocation_key)
        .await?
    else {
        // Duplicate/out-of-order outcome. The allocation may already be
        // terminal and removed from active allocation storage.
        info!(
            executor_id = executor_id.get(),
            allocation_id = %allocation_id,
            request_id = %request_id,
            namespace = %namespace,
            app = %application,
            "fn" = %fn_name,
            function_call_id = %function_call_id,
            "AllocationCompleted: allocation not found, treating as idempotent no-op"
        );
        return Ok(AllocationIngestDisposition::SkippedNoop);
    };

    let (data_payload, graph_updates) = match completed.return_value {
        Some(executor_api_pb::allocation_completed::ReturnValue::Value(dp)) => {
            info!(
                executor_id = executor_id.get(),
                allocation_id = %allocation_id,
                request_id = %request_id,
                namespace = %namespace,
                app = %application,
                "fn" = %fn_name,
                function_call_id = %function_call_id,
                "AllocationCompleted: ReturnValue::Value"
            );
            let blob_store_url_scheme = blob_storage_registry
                .get_blob_store(&namespace)
                .get_url_scheme();
            let blob_store_url = blob_storage_registry.get_blob_store(&namespace).get_url();
            let payload = prepare_data_payload(dp, &blob_store_url_scheme, &blob_store_url)?;
            (Some(payload), None)
        }
        Some(executor_api_pb::allocation_completed::ReturnValue::Updates(updates)) => {
            let num_updates = updates.updates.len();
            let root_fc_id_str = updates.root_function_call_id.as_deref().unwrap_or("none");
            info!(
                executor_id = executor_id.get(),
                allocation_id = %allocation_id,
                request_id = %request_id,
                namespace = %namespace,
                app = %application,
                "fn" = %fn_name,
                function_call_id = %function_call_id,
                num_updates,
                root_function_call_id = %root_fc_id_str,
                "AllocationCompleted: ReturnValue::Updates (tail call)"
            );
            let root_function_call_id = updates
                .root_function_call_id
                .map(FunctionCallId::from)
                .unwrap_or_else(|| FunctionCallId::from(nanoid::nanoid!()));
            let mut compute_ops = Vec::new();
            for update in updates.updates {
                compute_ops.push(to_internal_compute_op(
                    update,
                    blob_storage_registry,
                    Some(function_call_id.clone()),
                )?);
            }
            (
                None,
                Some(GraphUpdates {
                    graph_updates: compute_ops,
                    output_function_call_id: root_function_call_id,
                }),
            )
        }
        None => {
            info!(
                executor_id = executor_id.get(),
                allocation_id = %allocation_id,
                request_id = %request_id,
                namespace = %namespace,
                app = %application,
                "fn" = %fn_name,
                function_call_id = %function_call_id,
                "AllocationCompleted: ReturnValue::None"
            );
            (None, None)
        }
    };

    info!(
        executor_id = executor_id.get(),
        allocation_id = %allocation_id,
        request_id = %request_id,
        namespace = %namespace,
        app = %application,
        "fn" = %fn_name,
        function_call_id = %function_call_id,
        "AllocationCompleted ingested"
    );

    let event = AllocationOutputIngestedEvent {
        namespace,
        application,
        function: fn_name,
        request_id,
        function_call_id: FunctionCallId::from(function_call_id),
        data_payload,
        graph_updates,
        request_exception: None,
        allocation_id: allocation.id,
        allocation_target: allocation.target,
        allocation_outcome: FunctionRunOutcome::Success,
        execution_duration_ms: completed.execution_duration_ms,
    };

    write_dataplane_results(indexify_state, executor_id, vec![event], vec![], vec![]).await?;
    Ok(AllocationIngestDisposition::Applied)
}

/// Process a single AllocationFailed message.
pub async fn process_allocation_failed(
    indexify_state: &Arc<IndexifyState>,
    blob_storage_registry: &Arc<BlobStorageRegistry>,
    executor_id: &ExecutorId,
    failed: executor_api_pb::AllocationFailed,
) -> Result<AllocationIngestDisposition> {
    use data_model::{AllocationOutputIngestedEvent, FunctionRunOutcome};

    let proto_reason = failed.reason();
    let failure_reason = proto_failure_reason_to_internal(proto_reason);
    let function = failed
        .function
        .ok_or_else(|| anyhow::anyhow!("AllocationFailed missing function"))?;
    let namespace = function
        .namespace
        .ok_or_else(|| anyhow::anyhow!("AllocationFailed missing namespace"))?;
    let application = function
        .application_name
        .ok_or_else(|| anyhow::anyhow!("AllocationFailed missing application_name"))?;
    let fn_name = function
        .function_name
        .ok_or_else(|| anyhow::anyhow!("AllocationFailed missing function_name"))?;
    let request_id = failed
        .request_id
        .ok_or_else(|| anyhow::anyhow!("AllocationFailed missing request_id"))?;
    let function_call_id = failed
        .function_call_id
        .ok_or_else(|| anyhow::anyhow!("AllocationFailed missing function_call_id"))?;
    let allocation_id = failed.allocation_id;

    let allocation_key =
        data_model::Allocation::key_from(&namespace, &application, &request_id, &allocation_id);
    let Some(allocation) = indexify_state
        .reader()
        .get_allocation(&allocation_key)
        .await?
    else {
        // Duplicate/out-of-order outcome. The allocation may already be
        // terminal and removed from active allocation storage.
        info!(
            executor_id = executor_id.get(),
            allocation_id = %allocation_id,
            request_id = %request_id,
            namespace = %namespace,
            app = %application,
            "fn" = %fn_name,
            failure_reason = ?proto_reason,
            "AllocationFailed: allocation not found, treating as idempotent no-op"
        );
        return Ok(AllocationIngestDisposition::SkippedNoop);
    };

    let request_exception = if let Some(dp) = failed.request_error {
        let blob_store_url_scheme = blob_storage_registry
            .get_blob_store(&namespace)
            .get_url_scheme();
        let blob_store_url = blob_storage_registry.get_blob_store(&namespace).get_url();
        Some(prepare_data_payload(
            dp,
            &blob_store_url_scheme,
            &blob_store_url,
        )?)
    } else {
        None
    };

    // If the dataplane included a container_id, include it as a container state
    // update so the scheduler marks it terminated before rescheduling. This
    // prevents retries from landing on the same dead container when
    // ContainerTerminated hasn't arrived yet via the separate channel.
    let container_state_updates = if let Some(cid) = &failed.container_id {
        let termination_reason = proto_failure_reason_to_termination_reason(proto_reason);
        info!(
            executor_id = executor_id.get(),
            allocation_id = %allocation_id,
            request_id = %request_id,
            namespace = %namespace,
            app = %application,
            "fn" = %fn_name,
            failure_reason = ?proto_reason,
            container_id = %cid,
            "AllocationFailed ingested (with container_id)"
        );
        vec![data_model::ContainerStateUpdateInfo {
            container_id: data_model::ContainerId::new(cid.clone()),
            termination_reason: Some(termination_reason),
        }]
    } else {
        info!(
            executor_id = executor_id.get(),
            allocation_id = %allocation_id,
            request_id = %request_id,
            namespace = %namespace,
            app = %application,
            "fn" = %fn_name,
            failure_reason = ?proto_reason,
            "AllocationFailed ingested"
        );
        vec![]
    };

    let event = AllocationOutputIngestedEvent {
        namespace,
        application,
        function: fn_name,
        request_id,
        function_call_id: FunctionCallId::from(function_call_id),
        data_payload: None,
        graph_updates: None,
        request_exception,
        allocation_id: allocation.id,
        allocation_target: allocation.target,
        allocation_outcome: FunctionRunOutcome::Failure(failure_reason),
        execution_duration_ms: failed.execution_duration_ms,
    };

    write_dataplane_results(
        indexify_state,
        executor_id,
        vec![event],
        container_state_updates,
        vec![],
    )
    .await?;

    Ok(AllocationIngestDisposition::Applied)
}

/// Handle a snapshot completed response from the dataplane.
async fn handle_snapshot_completed(
    indexify_state: &Arc<IndexifyState>,
    completed: &executor_api_pb::SnapshotCompleted,
) -> Result<()> {
    use crate::state_store::requests::CompleteSnapshotRequest;

    if completed.snapshot_id.is_empty() {
        anyhow::bail!("SnapshotCompleted: snapshot_id is empty");
    }
    if completed.snapshot_uri.is_empty() {
        anyhow::bail!("SnapshotCompleted: snapshot_uri is empty");
    }

    let request = StateMachineUpdateRequest {
        payload: RequestPayload::CompleteSnapshot(CompleteSnapshotRequest {
            snapshot_id: data_model::SnapshotId::new(completed.snapshot_id.clone()),
            snapshot_uri: completed.snapshot_uri.clone(),
            size_bytes: completed.size_bytes,
        }),
    };
    indexify_state.write(request).await
}

/// Handle a snapshot failed response from the dataplane.
async fn handle_snapshot_failed(
    indexify_state: &Arc<IndexifyState>,
    failed: &executor_api_pb::SnapshotFailed,
) -> Result<()> {
    use crate::state_store::requests::FailSnapshotRequest;

    if failed.snapshot_id.is_empty() {
        anyhow::bail!("SnapshotFailed: snapshot_id is empty");
    }

    let request = StateMachineUpdateRequest {
        payload: RequestPayload::FailSnapshot(FailSnapshotRequest {
            snapshot_id: data_model::SnapshotId::new(failed.snapshot_id.clone()),
            error: failed.error_message.clone(),
        }),
    };
    indexify_state.write(request).await
}

/// Write a `DataplaneResultsIngestedEvent` to the state machine.
///
/// Shared by `process_command_responses` (container events only) and
/// `process_allocation_activities` (allocation events only).
async fn write_dataplane_results(
    indexify_state: &Arc<IndexifyState>,
    executor_id: &ExecutorId,
    allocation_events: Vec<data_model::AllocationOutputIngestedEvent>,
    container_state_updates: Vec<data_model::ContainerStateUpdateInfo>,
    container_started_ids: Vec<data_model::ContainerId>,
) -> Result<()> {
    let event = data_model::DataplaneResultsIngestedEvent {
        executor_id: executor_id.clone(),
        allocation_events,
        container_state_updates,
        container_started_ids,
    };

    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::DataplaneResults(
                crate::state_store::requests::DataplaneResultsRequest { event },
            ),
        })
        .await?;

    Ok(())
}
