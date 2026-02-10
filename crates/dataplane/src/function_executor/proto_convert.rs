//! Proto conversion helpers shared across allocation_runner and controller.
//!
//! Centralizes the repetitive field-by-field conversions between
//! `function_executor_pb` (FE-side) and `executor_api_pb` (server-side)
//! protobuf types.

use proto_api::{executor_api_pb, function_executor_pb};

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
