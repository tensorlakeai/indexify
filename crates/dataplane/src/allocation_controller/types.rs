//! State machine types for the AllocationController.
//!
//! Defines the container and allocation state machines that drive the
//! unified function execution architecture.

use std::{fmt, time::Instant};

use proto_api::executor_api_pb::{
    Allocation as ServerAllocation,
    AllocationResult as ServerAllocationResult,
    FunctionExecutorDescription,
    FunctionExecutorTerminationReason,
};
use tokio_util::sync::CancellationToken;

use crate::{
    driver::ProcessHandle,
    function_executor::{events::FinalizationContext, fe_client::FunctionExecutorGrpcClient},
};

// ---------------------------------------------------------------------------
// Container state machine
// ---------------------------------------------------------------------------

/// Lifecycle state of a function executor container.
#[allow(dead_code)]
pub(super) enum ContainerState {
    /// Container startup task is running.
    Starting,
    /// Container is running, ready to accept allocations.
    Running {
        handle: ProcessHandle,
        client: Box<FunctionExecutorGrpcClient>,
        health_checker_cancel: CancellationToken,
    },
    /// Container has terminated (crash, shutdown, OOM, etc.)
    Terminated {
        reason: FunctionExecutorTerminationReason,
    },
}

/// A function executor managed by the AllocationController.
#[allow(dead_code)]
pub(super) struct ManagedFE {
    pub description: FunctionExecutorDescription,
    pub state: ContainerState,
    pub max_concurrency: u32,
    pub allocated_gpu_uuids: Vec<String>,
    pub created_at: Instant,
}

// ---------------------------------------------------------------------------
// Allocation state machine
// ---------------------------------------------------------------------------

/// Lifecycle state of an allocation.
#[allow(dead_code)]
pub(super) enum AllocationState {
    /// Downloading/preparing inputs (tokio task running).
    Preparing { cancel_token: CancellationToken },
    /// Inputs ready, waiting for target FE container to become Running.
    WaitingForContainer,
    /// Inputs ready, container running, but concurrency limit reached.
    WaitingForSlot,
    /// Executing on an FE (tokio task running).
    Running {
        cancel_token: CancellationToken,
        finalization_ctx: FinalizationContext,
    },
    /// Finalizing output blobs (tokio task running).
    Finalizing { result: ServerAllocationResult },
    /// Terminal.
    Done,
}

/// An allocation managed by the AllocationController.
#[allow(dead_code)]
pub(super) struct ManagedAllocation {
    pub allocation: ServerAllocation,
    /// The FE this allocation targets (set at creation, never changes).
    pub fe_id: String,
    pub state: AllocationState,
    pub created_at: Instant,
}

// ---------------------------------------------------------------------------
// Display impls for log readability
// ---------------------------------------------------------------------------

impl fmt::Display for ContainerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContainerState::Starting => write!(f, "Starting"),
            ContainerState::Running { .. } => write!(f, "Running"),
            ContainerState::Terminated { reason, .. } => write!(f, "Terminated({:?})", reason),
        }
    }
}

impl fmt::Display for AllocationState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AllocationState::Preparing { .. } => write!(f, "Preparing"),
            AllocationState::WaitingForContainer => write!(f, "WaitingForContainer"),
            AllocationState::WaitingForSlot => write!(f, "WaitingForSlot"),
            AllocationState::Running { .. } => write!(f, "Running"),
            AllocationState::Finalizing { .. } => write!(f, "Finalizing"),
            AllocationState::Done => write!(f, "Done"),
        }
    }
}

// ---------------------------------------------------------------------------
// Helper to extract logging context from FE descriptions and allocations
// ---------------------------------------------------------------------------

/// Logging context extracted from a FunctionExecutorDescription.
pub(super) struct FELogCtx {
    pub namespace: String,
    pub app: String,
    pub version: String,
    pub fn_name: String,
    pub container_id: String,
}

impl FELogCtx {
    pub fn from_description(desc: &FunctionExecutorDescription) -> Self {
        let func_ref = desc.function.as_ref();
        Self {
            namespace: func_ref
                .and_then(|f| f.namespace.as_deref())
                .unwrap_or("")
                .to_string(),
            app: func_ref
                .and_then(|f| f.application_name.as_deref())
                .unwrap_or("")
                .to_string(),
            version: func_ref
                .and_then(|f| f.application_version.as_deref())
                .unwrap_or("")
                .to_string(),
            fn_name: func_ref
                .and_then(|f| f.function_name.as_deref())
                .unwrap_or("")
                .to_string(),
            container_id: desc.id.clone().unwrap_or_default(),
        }
    }
}

/// Logging context extracted from a ServerAllocation.
pub(super) struct AllocLogCtx {
    pub namespace: String,
    pub app: String,
    pub version: String,
    pub fn_name: String,
    pub request_id: String,
}

impl AllocLogCtx {
    pub fn from_allocation(alloc: &ServerAllocation) -> Self {
        let func_ref = alloc.function.as_ref();
        Self {
            namespace: func_ref
                .and_then(|f| f.namespace.as_deref())
                .unwrap_or("")
                .to_string(),
            app: func_ref
                .and_then(|f| f.application_name.as_deref())
                .unwrap_or("")
                .to_string(),
            version: func_ref
                .and_then(|f| f.application_version.as_deref())
                .unwrap_or("")
                .to_string(),
            fn_name: func_ref
                .and_then(|f| f.function_name.as_deref())
                .unwrap_or("")
                .to_string(),
            request_id: alloc.request_id.clone().unwrap_or_default(),
        }
    }
}
