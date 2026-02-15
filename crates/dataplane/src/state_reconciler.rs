//! Unified state reconciler — the bridge between the server's desired state
//! and the dataplane's two execution paths.
//!
//! The dataplane supports two distinct APIs:
//!
//! **Functions** (`FunctionExecutorType::Function`)
//!   Users invoke functions; the dataplane runs them inside subprocess-based
//!   function executors. Each FE is managed by a [`FunctionExecutorController`]
//!   that handles process lifecycle, allocation execution (via
//!   [`AllocationRunner`]), health checking, and result reporting.
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

use std::{collections::HashMap, sync::Arc};

use proto_api::executor_api_pb::{
    FunctionCallResult as ServerFunctionCallResult,
    FunctionCallWatch,
    FunctionExecutorDescription,
    FunctionExecutorState,
    FunctionExecutorStatus,
    FunctionExecutorType,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    function_container_manager::FunctionContainerManager,
    function_executor::{
        controller::{FEControllerHandle, FESpawnConfig, FunctionExecutorController},
        events::FECommand,
        watcher_registry::WatcherRegistry,
    },
};

/// Routes desired state to function controllers and sandbox containers.
///
/// See [module docs](self) for the two execution paths.
pub struct StateReconciler {
    /// Subprocess-based controllers for the **Function** execution path.
    fe_controllers: HashMap<String, FEControllerHandle>,
    /// Docker-based manager for the **Sandbox** execution path.
    container_manager: Arc<FunctionContainerManager>,
    /// Shared spawn configuration for FE controllers.
    spawn_config: FESpawnConfig,
    /// Cancellation token for all controllers.
    cancel_token: CancellationToken,
    /// Shared watcher registry for function call result routing.
    watcher_registry: WatcherRegistry,
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
        Self {
            fe_controllers: HashMap::new(),
            container_manager,
            spawn_config,
            cancel_token,
            watcher_registry: WatcherRegistry::new(),
            state_change_notify,
        }
    }

    /// Reconcile desired state by partitioning FEs into the two execution
    /// paths:
    /// - **Function** FEs → subprocess controllers (create/remove as needed)
    /// - **Sandbox** FEs → Docker container manager (`sync`)
    pub async fn reconcile(&mut self, function_executors: Vec<FunctionExecutorDescription>) {
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

        self.container_manager.sync(sandbox_fes).await;
        self.reconcile_function_fes(function_fes).await;
    }

    /// Reconcile function-type FEs with FE controllers.
    ///
    /// Removals are fully awaited (container killed, resources freed) before
    /// new controllers are created. This guarantees resources like GPUs are
    /// returned to the pool before new FEs attempt to allocate them.
    async fn reconcile_function_fes(&mut self, desired: Vec<FunctionExecutorDescription>) {
        let desired_ids: HashMap<String, FunctionExecutorDescription> = desired
            .into_iter()
            .filter_map(|fe| {
                let id = fe.id.clone()?;
                Some((id, fe))
            })
            .collect();

        let mut changed = false;

        // Remove controllers for FEs no longer in desired state.
        // We send Shutdown and then wait for each controller to reach
        // Terminated so that resources (GPUs, container) are fully released
        // before we create any new controllers.
        let current_ids: Vec<String> = self.fe_controllers.keys().cloned().collect();
        let mut removed_handles: Vec<(String, FEControllerHandle)> = Vec::new();
        for id in current_ids {
            if !desired_ids.contains_key(&id) {
                info!(fe_id = %id, "Removing function executor controller");
                if let Some(handle) = self.fe_controllers.remove(&id) {
                    let _ = handle.command_tx.send(FECommand::Shutdown);
                    removed_handles.push((id, handle));
                    changed = true;
                }
            }
        }

        // Wait for all removed controllers to fully terminate (container
        // killed, GPUs deallocated) before creating new ones.
        for (id, mut handle) in removed_handles {
            let wait_result = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                wait_for_terminated(&mut handle.state_rx),
            )
            .await;
            match wait_result {
                Ok(()) => {
                    debug!(fe_id = %id, "Function executor fully terminated");
                }
                Err(_) => {
                    tracing::warn!(
                        fe_id = %id,
                        "Timed out waiting for function executor to terminate"
                    );
                }
            }
        }

        // Create controllers for new FEs
        for (id, description) in &desired_ids {
            if !self.fe_controllers.contains_key(id) {
                info!(fe_id = %id, "Creating function executor controller");
                let handle = FunctionExecutorController::spawn(
                    description.clone(),
                    self.spawn_config.clone(),
                    self.cancel_token.child_token(),
                    self.watcher_registry.clone(),
                );
                self.fe_controllers.insert(id.clone(), handle);
                changed = true;
            }
        }

        if changed {
            self.state_change_notify.notify_one();
        }
    }

    /// Route a new allocation to a **Function** FE controller.
    ///
    /// Sandbox FEs do not receive allocations — they serve user traffic
    /// directly via HTTP proxy.
    ///
    /// If the controller's command channel is closed (e.g. the controller
    /// task exited after a startup failure), the allocation is reported as
    /// failed immediately so the server doesn't wait forever for a result.
    pub fn add_allocation(&self, fe_id: &str, allocation: proto_api::executor_api_pb::Allocation) {
        if let Some(handle) = self.fe_controllers.get(fe_id) &&
            handle
                .command_tx
                .send(FECommand::AddAllocation(allocation.clone()))
                .is_err()
        {
            tracing::warn!(
                fe_id = %fe_id,
                allocation_id = ?allocation.allocation_id,
                "FE controller channel closed, reporting allocation failure"
            );
            let result = crate::function_executor::proto_convert::make_failure_result(
                &allocation,
                proto_api::executor_api_pb::AllocationFailureReason::FunctionExecutorTerminated,
            );
            let _ = self.spawn_config.result_tx.send(result);
        }
    }

    /// Route function call results from the server to registered watchers.
    pub async fn deliver_function_call_results(&self, results: &[ServerFunctionCallResult]) {
        self.watcher_registry.deliver_results(results).await;
    }

    /// Get active function call watches for inclusion in heartbeats.
    pub async fn get_function_call_watches(&self) -> Vec<FunctionCallWatch> {
        self.watcher_registry.get_function_call_watches().await
    }

    /// Get the watcher notify for waking up the heartbeat loop when watches
    /// change.
    pub fn watcher_notify(&self) -> Arc<Notify> {
        self.watcher_registry.watcher_notify()
    }

    /// Get the notify for waking up the heartbeat loop when state changes (FEs
    /// added/removed).
    pub fn state_change_notify(&self) -> Arc<Notify> {
        self.state_change_notify.clone()
    }

    /// Get all FE states for heartbeat reporting.
    ///
    /// Returns states from both FE controllers and Docker containers.
    pub async fn get_all_fe_states(&self) -> Vec<FunctionExecutorState> {
        let mut states = self.container_manager.get_states().await;

        for handle in self.fe_controllers.values() {
            let state = handle.state_rx.borrow().clone();
            states.push(state);
        }

        states
    }

    /// Shut down all FE controllers gracefully.
    pub async fn shutdown(&mut self) {
        for (id, handle) in self.fe_controllers.drain() {
            debug!(fe_id = %id, "Shutting down FE controller");
            let _ = handle.command_tx.send(FECommand::Shutdown);
        }
    }
}

/// Wait for an FE controller's state to reach Terminated.
async fn wait_for_terminated(state_rx: &mut tokio::sync::watch::Receiver<FunctionExecutorState>) {
    loop {
        if let Some(status) = state_rx.borrow().status &&
            status == FunctionExecutorStatus::Terminated as i32
        {
            return;
        }
        // Wait for the next state change
        if state_rx.changed().await.is_err() {
            // Sender dropped — controller task exited, resources are freed
            return;
        }
    }
}
