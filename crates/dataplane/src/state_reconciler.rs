//! Unified state reconciler that manages both FE controllers (function-type)
//! and Docker containers (sandbox-type).
//!
//! Routes function executors from the server's DesiredExecutorState to either
//! the FE controller path (subprocess-based) or the container manager path
//! (Docker-based) based on `container_type`.

use std::{collections::HashMap, sync::Arc};

use proto_api::executor_api_pb::{
    FunctionCallResult as ServerFunctionCallResult,
    FunctionCallWatch,
    FunctionExecutorDescription,
    FunctionExecutorState,
    FunctionExecutorType,
};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::{debug, info};

use crate::{
    blob_ops::BlobStore,
    code_cache::CodeCache,
    driver::ProcessDriver,
    function_container_manager::{FunctionContainerManager, ImageResolver},
    function_executor::{
        controller::{FEControllerHandle, FunctionExecutorController},
        events::{CompletedAllocation, FECommand},
        watcher_registry::WatcherRegistry,
    },
};

/// Manages both FE controllers and Docker containers.
pub struct StateReconciler {
    /// Function executor controllers (subprocess-based, for Function type FEs).
    fe_controllers: HashMap<String, FEControllerHandle>,
    /// Container manager (Docker-based, for Sandbox type FEs).
    container_manager: Arc<FunctionContainerManager>,
    /// Process driver for spawning FE subprocesses.
    driver: Arc<dyn ProcessDriver>,
    /// Image resolver for resolving container images.
    image_resolver: Arc<dyn ImageResolver>,
    /// Channel for collecting allocation results from all FE controllers.
    result_tx: mpsc::UnboundedSender<CompletedAllocation>,
    /// Cancellation token for all controllers.
    cancel_token: CancellationToken,
    /// Server gRPC channel for call_function RPC.
    server_channel: Channel,
    /// Blob store for allocation operations.
    blob_store: Arc<BlobStore>,
    /// Code cache for application code download.
    code_cache: Arc<CodeCache>,
    /// Executor ID.
    executor_id: String,
    /// Path to the function-executor binary.
    fe_binary_path: String,
    /// Shared watcher registry for function call result routing.
    watcher_registry: WatcherRegistry,
    /// Notify for state changes (added/removed FEs) to trigger immediate
    /// heartbeats.
    state_change_notify: Arc<Notify>,
}

impl StateReconciler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        container_manager: Arc<FunctionContainerManager>,
        driver: Arc<dyn ProcessDriver>,
        image_resolver: Arc<dyn ImageResolver>,
        result_tx: mpsc::UnboundedSender<CompletedAllocation>,
        cancel_token: CancellationToken,
        server_channel: Channel,
        blob_store: Arc<BlobStore>,
        code_cache: Arc<CodeCache>,
        executor_id: String,
        fe_binary_path: String,
        state_change_notify: Arc<Notify>,
    ) -> Self {
        Self {
            fe_controllers: HashMap::new(),
            container_manager,
            driver,
            image_resolver,
            result_tx,
            cancel_token,
            server_channel,
            blob_store,
            code_cache,
            executor_id,
            fe_binary_path,
            watcher_registry: WatcherRegistry::new(),
            state_change_notify,
        }
    }

    /// Reconcile desired state: route each FE to the appropriate handler.
    pub async fn reconcile(&mut self, function_executors: Vec<FunctionExecutorDescription>) {
        let mut sandbox_fes = Vec::new();
        let mut function_fes = Vec::new();

        // Partition by container_type
        for fe in function_executors {
            let container_type = fe.container_type();
            match container_type {
                FunctionExecutorType::Function => {
                    function_fes.push(fe);
                }
                FunctionExecutorType::Sandbox | FunctionExecutorType::Unknown => {
                    sandbox_fes.push(fe);
                }
            }
        }

        // Route sandbox FEs to existing container manager
        self.container_manager.sync(sandbox_fes).await;

        // Route function FEs to FE controllers
        self.reconcile_function_fes(function_fes).await;
    }

    /// Reconcile function-type FEs with FE controllers.
    async fn reconcile_function_fes(&mut self, desired: Vec<FunctionExecutorDescription>) {
        let desired_ids: HashMap<String, FunctionExecutorDescription> = desired
            .into_iter()
            .filter_map(|fe| {
                let id = fe.id.clone()?;
                Some((id, fe))
            })
            .collect();

        let mut changed = false;

        // Remove controllers for FEs no longer in desired state
        let current_ids: Vec<String> = self.fe_controllers.keys().cloned().collect();
        for id in current_ids {
            if !desired_ids.contains_key(&id) {
                info!(fe_id = %id, "Removing function executor controller");
                if let Some(handle) = self.fe_controllers.remove(&id) {
                    let _ = handle.command_tx.send(FECommand::Shutdown);
                    changed = true;
                }
            }
        }

        // Create controllers for new FEs
        for (id, description) in &desired_ids {
            if !self.fe_controllers.contains_key(id) {
                info!(fe_id = %id, "Creating function executor controller");
                let handle = FunctionExecutorController::spawn(
                    description.clone(),
                    self.driver.clone(),
                    self.image_resolver.clone(),
                    self.result_tx.clone(),
                    self.cancel_token.child_token(),
                    self.server_channel.clone(),
                    self.blob_store.clone(),
                    self.code_cache.clone(),
                    self.executor_id.clone(),
                    self.fe_binary_path.clone(),
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

    /// Route a new allocation to the appropriate FE controller.
    pub fn add_allocation(&self, fe_id: &str, allocation: proto_api::executor_api_pb::Allocation) {
        if let Some(handle) = self.fe_controllers.get(fe_id) {
            let _ = handle.command_tx.send(FECommand::AddAllocation(allocation));
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
