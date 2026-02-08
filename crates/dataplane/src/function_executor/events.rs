//! Event types for the FunctionExecutorController event-driven state machine.

use proto_api::executor_api_pb::{
    Allocation as ServerAllocation,
    AllocationResult as ServerAllocationResult,
    FunctionExecutorTerminationReason,
};

use super::fe_client::FunctionExecutorGrpcClient;
use crate::blob_ops::MultipartUploadHandle;

/// Events from background tasks → FEController event loop.
pub enum FEEvent {
    /// FE subprocess was created and client is connected (or creation failed).
    FunctionExecutorCreated(anyhow::Result<FunctionExecutorGrpcClient>),
    /// FE subprocess terminated (health check failure, process died, etc.).
    FunctionExecutorTerminated {
        fe_id: String,
        reason: FunctionExecutorTerminationReason,
    },
    /// Allocation preparation completed (inputs downloaded, blobs created).
    AllocationPreparationFinished {
        allocation_id: String,
        result: anyhow::Result<PreparedAllocation>,
    },
    /// Signal to schedule the next runnable allocation.
    ScheduleAllocationExecution,
    /// Allocation execution completed on the FE.
    AllocationExecutionFinished {
        allocation_id: String,
        result: AllocationOutcome,
    },
    /// Post-execution blob finalization completed.
    AllocationFinalizationFinished { allocation_id: String },
}

/// Commands from StateReconciler → FEController.
pub enum FECommand {
    /// Add a new allocation to this FE.
    AddAllocation(ServerAllocation),
    /// Remove an allocation (cancelled by server).
    RemoveAllocation(String),
    /// Shut down this FE gracefully.
    Shutdown,
}

/// Prepared allocation ready for execution on the FE.
pub struct PreparedAllocation {
    /// The original server allocation.
    pub allocation: ServerAllocation,
    /// FE-format function inputs (with presigned URLs, blob handles).
    pub inputs: proto_api::function_executor_pb::FunctionInputs,
    /// Handle for the request error blob multipart upload (if created).
    pub request_error_blob_handle: Option<MultipartUploadHandle>,
}

/// Outcome of executing an allocation.
#[derive(Debug)]
pub enum AllocationOutcome {
    /// Allocation completed (success or function error).
    Completed {
        result: ServerAllocationResult,
        execution_duration_ms: u64,
        /// FE result containing uploaded blob info for finalization.
        fe_result: Option<proto_api::function_executor_pb::AllocationResult>,
    },
    /// Allocation was cancelled before/during execution.
    Cancelled,
    /// Allocation failed due to internal/platform error.
    Failed {
        reason: proto_api::executor_api_pb::AllocationFailureReason,
        error_message: String,
    },
}

/// Completed allocation result ready to be reported to the server.
#[derive(Debug)]
pub struct CompletedAllocation {
    pub result: ServerAllocationResult,
}
