//! Commands and events for the AllocationController.

use proto_api::executor_api_pb::{
    Allocation as ServerAllocation,
    FunctionExecutorDescription,
    FunctionExecutorTerminationReason,
};

use crate::{
    driver::ProcessHandle,
    function_executor::{
        events::{AllocationOutcome, PreparedAllocation},
        fe_client::FunctionExecutorGrpcClient,
    },
};

/// Commands sent TO the controller (from service.rs / state_reconciler).
#[allow(clippy::large_enum_variant)]
pub enum ACCommand {
    /// Atomic reconciliation: desired FEs + new allocations together.
    Reconcile {
        desired_fes: Vec<FunctionExecutorDescription>,
        /// (fe_id, allocation) pairs.
        new_allocations: Vec<(String, ServerAllocation)>,
    },
    /// Graceful shutdown of the entire controller.
    Shutdown,
}

/// Events sent TO the controller (from background tokio tasks).
#[allow(clippy::large_enum_variant)]
pub(super) enum ACEvent {
    // -- Container lifecycle events --
    ContainerStartupComplete {
        fe_id: String,
        result: anyhow::Result<(ProcessHandle, FunctionExecutorGrpcClient)>,
    },
    ContainerTerminated {
        fe_id: String,
        reason: FunctionExecutorTerminationReason,
    },

    // -- Allocation lifecycle events --
    AllocationPrepared {
        allocation_id: String,
        result: anyhow::Result<PreparedAllocation>,
    },
    AllocationExecutionFinished {
        allocation_id: String,
        result: AllocationOutcome,
    },
    AllocationFinalizationFinished {
        allocation_id: String,
        is_success: bool,
    },
}
