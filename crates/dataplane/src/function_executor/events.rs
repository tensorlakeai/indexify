//! Event types for function executor allocation lifecycle.

use proto_api::executor_api_pb::AllocationResult as ServerAllocationResult;

use crate::blob_ops::MultipartUploadHandle;

/// Prepared allocation ready for execution on the FE.
pub struct PreparedAllocation {
    /// FE-format function inputs (with presigned URLs, blob handles).
    pub inputs: proto_api::function_executor_pb::FunctionInputs,
    /// Handle for the request error blob multipart upload (if created).
    pub request_error_blob_handle: Option<MultipartUploadHandle>,
}

/// Outcome of executing an allocation.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum AllocationOutcome {
    /// Allocation completed (success or function error).
    Completed {
        result: ServerAllocationResult,
        /// FE result containing uploaded blob info for finalization.
        fe_result: Option<proto_api::function_executor_pb::AllocationResult>,
        /// Output blob handles accumulated during execution.
        output_blob_handles: Vec<MultipartUploadHandle>,
    },
    /// Allocation was cancelled before/during execution.
    Cancelled {
        /// Output blob handles accumulated before cancellation.
        output_blob_handles: Vec<MultipartUploadHandle>,
    },
    /// Allocation failed due to internal/platform error.
    Failed {
        reason: proto_api::executor_api_pb::AllocationFailureReason,
        error_message: String,
        /// Output blob handles accumulated before failure.
        output_blob_handles: Vec<MultipartUploadHandle>,
        /// True if the failure was likely caused by the FE process crashing
        /// (gRPC transport error, stream closure, etc). The controller uses
        /// this to trigger immediate FE termination instead of waiting for
        /// the health checker.
        likely_fe_crash: bool,
        /// When `likely_fe_crash` is true, the termination reason determined
        /// by checking the process exit status (OOM vs crash). The controller
        /// uses this for the `ContainerTerminated` event so the health checker
        /// doesn't need to race.
        termination_reason: Option<proto_api::executor_api_pb::FunctionExecutorTerminationReason>,
    },
}

/// Data accumulated across prep and execution phases for finalization.
#[derive(Default)]
pub struct FinalizationContext {
    /// Handle for the request error blob multipart upload (if created).
    pub request_error_blob_handle: Option<MultipartUploadHandle>,
    /// Output blob handles accumulated during execution.
    pub output_blob_handles: Vec<MultipartUploadHandle>,
    /// FE result containing uploaded blob info for finalization.
    pub fe_result: Option<proto_api::function_executor_pb::AllocationResult>,
}
