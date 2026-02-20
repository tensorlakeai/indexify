//! Proto conversion helpers shared across allocation_runner and controller.
//!
//! Centralizes the repetitive field-by-field conversions between
//! `function_executor_pb` (FE-side) and `executor_api_pb` (server-side)
//! protobuf types.

use proto_api::{
    executor_api_pb,
    executor_api_pb::{AllocationStreamRequest, CommandResponse},
    function_executor_pb,
};

/// Map enum variants between two protobuf types with identical variant names.
macro_rules! convert_enum {
    ($val:expr, $from_mod:ident :: $from_type:ident => $to_mod:ident :: $to_type:ident { $($variant:ident),+ $(,)? }) => {
        match $val {
            $( $from_mod :: $from_type :: $variant => $to_mod :: $to_type :: $variant, )+
        }
    };
}

/// Convert an FE `SerializedObjectInsideBlob` to a server `DataPayload`.
///
/// Used when mapping FE result fields (request_error, return value, function
/// args) back to the server format.
pub fn serialized_object_to_data_payload(
    so: &function_executor_pb::SerializedObjectInsideBlob,
    uri: Option<String>,
) -> executor_api_pb::DataPayload {
    let manifest = so.manifest.as_ref();
    executor_api_pb::DataPayload {
        uri,
        encoding: manifest.and_then(|m| m.encoding),
        encoding_version: manifest.and_then(|m| m.encoding_version),
        content_type: manifest.and_then(|m| m.content_type.clone()),
        metadata_size: manifest.and_then(|m| m.metadata_size),
        offset: so.offset,
        size: manifest.and_then(|m| m.size),
        sha256_hash: manifest.and_then(|m| m.sha256_hash.clone()),
        source_function_call_id: manifest.and_then(|m| m.source_function_call_id.clone()),
        id: None,
    }
}

/// Convert a server `DataPayload` to an FE `SerializedObjectInsideBlob`.
///
/// Used when delivering function call results from the server to the FE.
pub fn data_payload_to_serialized_object_inside_blob(
    data_payload: &executor_api_pb::DataPayload,
) -> function_executor_pb::SerializedObjectInsideBlob {
    let manifest = function_executor_pb::SerializedObjectManifest {
        encoding: data_payload.encoding,
        encoding_version: data_payload.encoding_version,
        size: data_payload.size,
        metadata_size: data_payload.metadata_size,
        sha256_hash: data_payload.sha256_hash.clone(),
        content_type: data_payload.content_type.clone(),
        source_function_call_id: data_payload.source_function_call_id.clone(),
    };

    function_executor_pb::SerializedObjectInsideBlob {
        manifest: Some(manifest),
        offset: data_payload.offset,
    }
}

/// Convert an FE `FunctionRef` to a server `FunctionRef`.
pub fn convert_function_ref(
    fe_ref: &function_executor_pb::FunctionRef,
) -> executor_api_pb::FunctionRef {
    executor_api_pb::FunctionRef {
        namespace: fe_ref.namespace.clone(),
        application_name: fe_ref.application_name.clone(),
        function_name: fe_ref.function_name.clone(),
        application_version: fe_ref.application_version.clone(),
    }
}

/// Convert an FE outcome code to the server equivalent.
pub fn convert_outcome_code_fe_to_server(
    code: function_executor_pb::AllocationOutcomeCode,
) -> executor_api_pb::AllocationOutcomeCode {
    convert_enum!(code, function_executor_pb::AllocationOutcomeCode => executor_api_pb::AllocationOutcomeCode {
        Success, Failure, Unknown,
    })
}

/// Convert a server outcome code to the FE equivalent.
pub fn convert_outcome_code_server_to_fe(
    code: executor_api_pb::AllocationOutcomeCode,
) -> function_executor_pb::AllocationOutcomeCode {
    convert_enum!(code, executor_api_pb::AllocationOutcomeCode => function_executor_pb::AllocationOutcomeCode {
        Success, Failure, Unknown,
    })
}

/// Convert an FE failure reason to the server equivalent.
///
/// Returns `None` for `Unknown` (no failure reason to report).
pub fn convert_failure_reason_fe_to_server(
    reason: function_executor_pb::AllocationFailureReason,
) -> Option<executor_api_pb::AllocationFailureReason> {
    match reason {
        function_executor_pb::AllocationFailureReason::InternalError => {
            Some(executor_api_pb::AllocationFailureReason::InternalError)
        }
        function_executor_pb::AllocationFailureReason::FunctionError => {
            Some(executor_api_pb::AllocationFailureReason::FunctionError)
        }
        function_executor_pb::AllocationFailureReason::RequestError => {
            Some(executor_api_pb::AllocationFailureReason::RequestError)
        }
        function_executor_pb::AllocationFailureReason::Unknown => None,
    }
}

/// Map a `FunctionExecutorTerminationReason` to an `AllocationFailureReason`.
///
/// Handles both startup reasons (used when rejecting allocations on a
/// terminated FE) and runtime reasons (used when cancelling running
/// allocations after the FE dies).
pub fn termination_to_failure_reason(
    reason: executor_api_pb::FunctionExecutorTerminationReason,
) -> executor_api_pb::AllocationFailureReason {
    match reason {
        // Startup reasons
        executor_api_pb::FunctionExecutorTerminationReason::StartupFailedFunctionTimeout => {
            executor_api_pb::AllocationFailureReason::StartupFailedFunctionTimeout
        }
        executor_api_pb::FunctionExecutorTerminationReason::StartupFailedInternalError => {
            executor_api_pb::AllocationFailureReason::StartupFailedInternalError
        }
        executor_api_pb::FunctionExecutorTerminationReason::StartupFailedFunctionError => {
            executor_api_pb::AllocationFailureReason::StartupFailedFunctionError
        }
        executor_api_pb::FunctionExecutorTerminationReason::StartupFailedBadImage => {
            executor_api_pb::AllocationFailureReason::StartupFailedBadImage
        }
        // Runtime reasons
        executor_api_pb::FunctionExecutorTerminationReason::Unhealthy => {
            executor_api_pb::AllocationFailureReason::FunctionError
        }
        executor_api_pb::FunctionExecutorTerminationReason::InternalError => {
            executor_api_pb::AllocationFailureReason::InternalError
        }
        executor_api_pb::FunctionExecutorTerminationReason::FunctionTimeout => {
            executor_api_pb::AllocationFailureReason::FunctionTimeout
        }
        executor_api_pb::FunctionExecutorTerminationReason::Oom => {
            executor_api_pb::AllocationFailureReason::Oom
        }
        executor_api_pb::FunctionExecutorTerminationReason::ProcessCrash => {
            executor_api_pb::AllocationFailureReason::FunctionError
        }
        _ => executor_api_pb::AllocationFailureReason::FunctionExecutorTerminated,
    }
}

/// Build a failure `AllocationResult` for the given allocation and reason.
pub fn make_failure_result(
    allocation: &executor_api_pb::Allocation,
    failure_reason: executor_api_pb::AllocationFailureReason,
) -> executor_api_pb::AllocationResult {
    executor_api_pb::AllocationResult {
        function: allocation.function.clone(),
        allocation_id: allocation.allocation_id.clone(),
        function_call_id: allocation.function_call_id.clone(),
        request_id: allocation.request_id.clone(),
        outcome_code: Some(executor_api_pb::AllocationOutcomeCode::Failure.into()),
        failure_reason: Some(failure_reason.into()),
        return_value: None,
        request_error: None,
        execution_duration_ms: None,
    }
}

// ---------------------------------------------------------------------------
// CommandResponse builders (v2 protocol)
// ---------------------------------------------------------------------------

/// Build a `CommandResponse` wrapping a `ContainerStarted`.
///
/// Sent as an unsolicited event (command_seq = 0) when a container
/// transitions to Running state.
pub fn make_container_started_response(container_id: &str) -> CommandResponse {
    CommandResponse {
        command_seq: None, // unsolicited
        response: Some(
            executor_api_pb::command_response::Response::ContainerStarted(
                executor_api_pb::ContainerStarted {
                    container_id: container_id.to_string(),
                },
            ),
        ),
    }
}

/// Build a `CommandResponse` wrapping a `ContainerTerminated`.
pub fn make_container_terminated_response(
    container_id: &str,
    reason: executor_api_pb::FunctionExecutorTerminationReason,
) -> CommandResponse {
    CommandResponse {
        command_seq: None, // unsolicited
        response: Some(
            executor_api_pb::command_response::Response::ContainerTerminated(
                executor_api_pb::ContainerTerminated {
                    container_id: container_id.to_string(),
                    reason: termination_reason_to_container_termination_reason(reason).into(),
                },
            ),
        ),
    }
}

/// Map a `FunctionExecutorTerminationReason` to a `ContainerTerminationReason`.
pub fn termination_reason_to_container_termination_reason(
    reason: executor_api_pb::FunctionExecutorTerminationReason,
) -> executor_api_pb::ContainerTerminationReason {
    match reason {
        executor_api_pb::FunctionExecutorTerminationReason::StartupFailedInternalError => {
            executor_api_pb::ContainerTerminationReason::StartupFailedInternalError
        }
        executor_api_pb::FunctionExecutorTerminationReason::StartupFailedFunctionError => {
            executor_api_pb::ContainerTerminationReason::StartupFailedFunctionError
        }
        executor_api_pb::FunctionExecutorTerminationReason::StartupFailedFunctionTimeout => {
            executor_api_pb::ContainerTerminationReason::StartupFailedFunctionTimeout
        }
        executor_api_pb::FunctionExecutorTerminationReason::Unhealthy => {
            executor_api_pb::ContainerTerminationReason::Unhealthy
        }
        executor_api_pb::FunctionExecutorTerminationReason::InternalError => {
            executor_api_pb::ContainerTerminationReason::InternalError
        }
        executor_api_pb::FunctionExecutorTerminationReason::FunctionTimeout => {
            executor_api_pb::ContainerTerminationReason::FunctionTimeout
        }
        executor_api_pb::FunctionExecutorTerminationReason::FunctionCancelled => {
            executor_api_pb::ContainerTerminationReason::FunctionCancelled
        }
        executor_api_pb::FunctionExecutorTerminationReason::Oom => {
            executor_api_pb::ContainerTerminationReason::Oom
        }
        executor_api_pb::FunctionExecutorTerminationReason::ProcessCrash => {
            executor_api_pb::ContainerTerminationReason::ProcessCrash
        }
        executor_api_pb::FunctionExecutorTerminationReason::StartupFailedBadImage => {
            executor_api_pb::ContainerTerminationReason::StartupFailedBadImage
        }
        _ => executor_api_pb::ContainerTerminationReason::Unknown,
    }
}

// ---------------------------------------------------------------------------
// AllocationScheduled builder (v2 fast ack)
// ---------------------------------------------------------------------------

/// Build a `CommandResponse` wrapping an `AllocationScheduled` ack.
///
/// `command_seq` ties the ack back to the original `RunAllocation` command.
pub fn make_allocation_scheduled_response(
    allocation_id: &str,
    command_seq: u64,
) -> CommandResponse {
    CommandResponse {
        command_seq: Some(command_seq),
        response: Some(
            executor_api_pb::command_response::Response::AllocationScheduled(
                executor_api_pb::AllocationScheduled {
                    allocation_id: allocation_id.to_string(),
                },
            ),
        ),
    }
}

// ---------------------------------------------------------------------------
// AllocationStreamRequest builders (outcomes via allocation stream)
// ---------------------------------------------------------------------------

/// Build an `AllocationStreamRequest` wrapping an `AllocationFailed`.
pub fn make_allocation_failed_stream_request(
    allocation: &executor_api_pb::Allocation,
    failure_reason: executor_api_pb::AllocationFailureReason,
    request_error: Option<executor_api_pb::DataPayload>,
    execution_duration_ms: Option<u64>,
    container_id: Option<String>,
) -> AllocationStreamRequest {
    AllocationStreamRequest {
        executor_id: String::new(),
        message: Some(executor_api_pb::allocation_stream_request::Message::Failed(
            executor_api_pb::AllocationFailed {
                allocation_id: allocation.allocation_id.clone().unwrap_or_default(),
                reason: failure_reason.into(),
                function: allocation.function.clone(),
                function_call_id: allocation.function_call_id.clone(),
                request_id: allocation.request_id.clone(),
                request_error,
                execution_duration_ms,
                container_id,
            },
        )),
    }
}

/// Convert a server `AllocationResult` into an `AllocationStreamRequest`.
///
/// Success results become `AllocationCompleted`; failures become
/// `AllocationFailed`.
pub fn allocation_result_to_stream_request(
    result: &executor_api_pb::AllocationResult,
    container_id: Option<String>,
) -> AllocationStreamRequest {
    let outcome_code = result.outcome_code.unwrap_or(0);
    if outcome_code == executor_api_pb::AllocationOutcomeCode::Success as i32 {
        AllocationStreamRequest {
            executor_id: String::new(),
            message: Some(
                executor_api_pb::allocation_stream_request::Message::Completed(
                    executor_api_pb::AllocationCompleted {
                        allocation_id: result.allocation_id.clone().unwrap_or_default(),
                        function: result.function.clone(),
                        function_call_id: result.function_call_id.clone(),
                        request_id: result.request_id.clone(),
                        return_value: result.return_value.as_ref().map(|rv| match rv {
                            executor_api_pb::allocation_result::ReturnValue::Value(dp) => {
                                executor_api_pb::allocation_completed::ReturnValue::Value(
                                    dp.clone(),
                                )
                            }
                            executor_api_pb::allocation_result::ReturnValue::Updates(u) => {
                                executor_api_pb::allocation_completed::ReturnValue::Updates(
                                    u.clone(),
                                )
                            }
                        }),
                        execution_duration_ms: result.execution_duration_ms,
                    },
                ),
            ),
        }
    } else {
        // Build a minimal Allocation to reuse make_allocation_failed_stream_request.
        let alloc = executor_api_pb::Allocation {
            allocation_id: result.allocation_id.clone(),
            function: result.function.clone(),
            function_call_id: result.function_call_id.clone(),
            request_id: result.request_id.clone(),
            ..Default::default()
        };
        let failure_reason = executor_api_pb::AllocationFailureReason::try_from(
            result
                .failure_reason
                .unwrap_or(executor_api_pb::AllocationFailureReason::InternalError as i32),
        )
        .unwrap_or(executor_api_pb::AllocationFailureReason::InternalError);
        make_allocation_failed_stream_request(
            &alloc,
            failure_reason,
            result.request_error.clone(),
            result.execution_duration_ms,
            container_id,
        )
    }
}

// ---------------------------------------------------------------------------
// Metrics for AllocationStreamRequest
// ---------------------------------------------------------------------------

/// Record allocation outcome metrics from an `AllocationStreamRequest`.
pub fn record_activity_metrics(
    activity: &AllocationStreamRequest,
    counters: &crate::metrics::DataplaneCounters,
) {
    use executor_api_pb::allocation_stream_request::Message;

    match &activity.message {
        Some(Message::Completed(c)) => {
            counters.record_allocation_completed("success", None, c.execution_duration_ms);
        }
        Some(Message::Failed(f)) => {
            let failure_reason = executor_api_pb::AllocationFailureReason::try_from(f.reason)
                .ok()
                .map(|reason| format!("{:?}", reason));
            counters.record_allocation_completed(
                "failure",
                failure_reason.as_deref(),
                f.execution_duration_ms,
            );
        }
        _ => {}
    }
}
