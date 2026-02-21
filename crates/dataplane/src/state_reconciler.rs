//! Unified state reconciler — the bridge between the server's desired state
//! and the dataplane's two execution paths.
//!
//! The dataplane supports two distinct APIs:
//!
//! **Functions** (`FunctionExecutorType::Function`)
//!   Users invoke functions; the dataplane runs them inside subprocess-based
//!   function executors. All Function FEs are managed by a single
//!   [`AllocationController`] that handles container lifecycle, allocation
//!   scheduling, execution, and result reporting.
//!
//! **Sandboxes** (`FunctionExecutorType::Sandbox`)
//!   Users interact with containers directly (e.g. for interactive sessions).
//!   The dataplane makes Docker containers available and routes HTTP traffic
//!   to them. Containers are managed by [`FunctionContainerManager`], which
//!   handles Docker lifecycle, warm-pool pre-creation, health checks, and
//!   timeout enforcement.
//!
//! This reconciler partitions the server's desired FE descriptions by
//! `container_type` and routes each group to the appropriate handler.

use std::sync::Arc;

use proto_api::executor_api_pb::{
    Allocation,
    FunctionCallResult as ServerFunctionCallResult,
    FunctionCallWatch,
    FunctionExecutorDescription,
    FunctionExecutorState,
    FunctionExecutorType,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    allocation_controller::{AllocationController, AllocationControllerHandle, events::ACCommand},
    function_container_manager::FunctionContainerManager,
    function_executor::controller::FESpawnConfig,
};

/// Routes desired state to the AllocationController and sandbox containers.
///
/// See [module docs](self) for the two execution paths.
pub struct StateReconciler {
    /// Single controller for the **Function** execution path.
    allocation_controller: AllocationControllerHandle,
    /// Docker-based manager for the **Sandbox** execution path.
    container_manager: Arc<FunctionContainerManager>,
    /// Notify for state changes (added/removed FEs) to trigger immediate
    /// heartbeats.
    state_change_notify: Arc<Notify>,
}

impl StateReconciler {
    pub fn new(
        container_manager: Arc<FunctionContainerManager>,
        spawn_config: FESpawnConfig,
        cancel_token: CancellationToken,
        state_change_notify: Arc<Notify>,
    ) -> Self {
        let ac_handle =
            AllocationController::spawn(spawn_config, cancel_token, state_change_notify.clone());
        Self {
            allocation_controller: ac_handle,
            container_manager,
            state_change_notify,
        }
    }

    /// Reconcile desired state by partitioning FEs into the two execution
    /// paths and bundling allocations with function FEs for atomic delivery:
    /// - **Function** FEs + allocations → AllocationController
    ///   (`ACCommand::Reconcile`)
    /// - **Sandbox** FEs → Docker container manager (`sync`)
    pub async fn reconcile(
        &mut self,
        function_executors: Vec<FunctionExecutorDescription>,
        allocations: Vec<(String, Allocation)>,
    ) {
        let mut sandbox_fes = Vec::new();
        let mut function_fes = Vec::new();

        for fe in function_executors {
            match fe.container_type() {
                FunctionExecutorType::Function => function_fes.push(fe),
                FunctionExecutorType::Sandbox | FunctionExecutorType::Unknown => {
                    sandbox_fes.push(fe);
                }
            }
        }

        let sandbox_fe_count = sandbox_fes.len();
        self.container_manager.sync(sandbox_fes).await;

        let function_fe_count = function_fes.len();
        let allocation_count = allocations.len();
        let changed = function_fe_count > 0 || allocation_count > 0;

        let _ = self
            .allocation_controller
            .command_tx
            .send(ACCommand::Reconcile {
                desired_fes: function_fes,
                new_allocations: allocations,
            });

        if changed {
            info!(
                function_fe_count = function_fe_count,
                sandbox_fe_count = sandbox_fe_count,
                allocation_count = allocation_count,
                "Sent Reconcile command to AllocationController"
            );
        }
    }

    /// Route function call results from the server to registered watchers.
    pub async fn deliver_function_call_results(&self, results: &[ServerFunctionCallResult]) {
        self.allocation_controller
            .watcher_registry
            .deliver_results(results)
            .await;
    }

    /// Get active function call watches for inclusion in heartbeats.
    pub async fn get_function_call_watches(&self) -> Vec<FunctionCallWatch> {
        self.allocation_controller
            .watcher_registry
            .get_function_call_watches()
            .await
    }

    /// Get the watcher notify for waking up the heartbeat loop when watches
    /// change.
    pub fn watcher_notify(&self) -> Arc<Notify> {
        self.allocation_controller.watcher_registry.watcher_notify()
    }

    /// Get the notify for waking up the heartbeat loop when state changes (FEs
    /// added/removed).
    pub fn state_change_notify(&self) -> Arc<Notify> {
        self.state_change_notify.clone()
    }

    /// Get the driver from the container manager for snapshot operations.
    pub fn get_driver(&self) -> Arc<dyn crate::driver::ProcessDriver> {
        self.container_manager.get_driver()
    }

    /// Get all FE states for heartbeat reporting.
    ///
    /// Returns states from both the AllocationController and Docker containers.
    pub async fn get_all_fe_states(&self) -> Vec<FunctionExecutorState> {
        let mut states = self.container_manager.get_states().await;
        states.extend(self.allocation_controller.state_rx.borrow().clone());
        states
    }

    /// Shut down the AllocationController gracefully.
    pub async fn shutdown(&mut self) {
        let _ = self
            .allocation_controller
            .command_tx
            .send(ACCommand::Shutdown);
    }
}
