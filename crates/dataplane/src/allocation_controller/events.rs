//! Commands and events for the AllocationController.

use proto_api::executor_api_pb::{
    Allocation as ServerAllocation,
    ContainerDescription,
    ContainerTerminationReason,
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
    /// Delta reconciliation: add/update containers, remove containers, route
    /// new allocations. Unlike the old full-set Reconcile, containers not
    /// mentioned are left untouched.
    Reconcile {
        /// Containers to create or update.
        added_or_updated_fes: Vec<ContainerDescription>,
        /// Container IDs to destroy.
        removed_fe_ids: Vec<String>,
        /// (fe_id, allocation, command_seq) tuples to route.
        new_allocations: Vec<(String, ServerAllocation, u64)>,
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
        reason: ContainerTerminationReason,
        /// Allocation ID that triggered the termination (e.g. the allocation
        /// whose gRPC stream broke, signalling a process crash). Ensures this
        /// allocation is blamed even if it already transitioned out of Running.
        blamed_allocation_id: Option<String>,
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
