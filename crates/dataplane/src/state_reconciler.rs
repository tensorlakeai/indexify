//! Unified state reconciler — the bridge between the server's desired state
//! and the dataplane's two execution paths.
//!
//! The dataplane supports two distinct APIs:
//!
//! **Functions** (`ContainerType::Function`)
//!   Users invoke functions; the dataplane runs them inside subprocess-based
//!   function executors. All Function FEs are managed by a single
//!   [`AllocationController`] that handles container lifecycle, allocation
//!   scheduling, execution, and result reporting.
//!
//! **Sandboxes** (`ContainerType::Sandbox`)
//!   Users interact with containers directly (e.g. for interactive sessions).
//!   The dataplane makes Docker containers available and routes HTTP traffic
//!   to them. Containers are managed by [`FunctionContainerManager`], which
//!   handles Docker lifecycle, warm-pool pre-creation, health checks, and
//!   timeout enforcement.
//!
//! This reconciler partitions the server's desired FE descriptions by
//! `container_type` and routes each group to the appropriate handler.
//!
//! ## Delta vs full-state semantics
//!
//! The command stream delivers individual `AddContainer` / `RemoveContainer`
//! commands one-by-one.
//!
//! - **Functions** use the AllocationController, which has native delta
//!   semantics (add/remove by ID). No accumulation needed.
//! - **Sandboxes** use `FunctionContainerManager::add_or_update_container()`
//!   and `remove_container_by_id()` for delta operations. This avoids the
//!   problem where calling `sync()` (full-state) with a partial set during
//!   reconnection would incorrectly stop containers that haven't been
//!   re-announced yet.

use std::{collections::HashSet, sync::Arc};

use proto_api::executor_api_pb::{Allocation, ContainerDescription, ContainerState, ContainerType};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    allocation_controller::{AllocationController, AllocationControllerHandle, events::ACCommand},
    allocation_result_dispatcher::AllocationResultDispatcher,
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
        allocation_result_dispatcher: Arc<AllocationResultDispatcher>,
    ) -> Self {
        let ac_handle = AllocationController::spawn(
            spawn_config,
            cancel_token,
            state_change_notify.clone(),
            allocation_result_dispatcher,
        );
        Self {
            allocation_controller: ac_handle,
            container_manager,
            state_change_notify,
        }
    }

    /// Reconcile container commands: add/remove containers.
    ///
    /// Partitions FEs by type:
    /// - **Function** FEs → AllocationController (`ACCommand::Reconcile`)
    /// - **Sandbox** FEs → `FunctionContainerManager` delta methods
    ///   (`add_or_update_container` / `remove_container_by_id`)
    ///
    /// Removal IDs are applied to both paths: forwarded to the
    /// AllocationController and to `FunctionContainerManager`.
    pub async fn reconcile_containers(
        &mut self,
        added_or_updated: Vec<ContainerDescription>,
        removed_container_ids: Vec<String>,
    ) {
        let mut function_fes = Vec::new();

        for fe in added_or_updated {
            match fe.container_type() {
                ContainerType::Function => function_fes.push(fe),
                ContainerType::Sandbox | ContainerType::Unknown => {
                    self.container_manager.add_or_update_container(fe).await;
                }
            }
        }

        // Apply removals to both paths. We don't know the type of removed IDs
        // upfront, so try both — the AllocationController ignores IDs it
        // doesn't know about, and FunctionContainerManager ignores unknown IDs.
        for id in &removed_container_ids {
            self.container_manager.remove_container_by_id(id).await;
        }

        let function_fe_count = function_fes.len();
        let removed_count = removed_container_ids.len();
        let changed = function_fe_count > 0 || removed_count > 0;

        let _ = self
            .allocation_controller
            .command_tx
            .send(ACCommand::Reconcile {
                added_or_updated_fes: function_fes,
                removed_fe_ids: removed_container_ids,
                new_allocations: vec![],
            });

        if changed {
            info!(
                function_fe_count,
                removed_count, "Sent container Reconcile to AllocationController"
            );
        }
    }

    /// Apply a targeted description update to an existing container.
    ///
    /// Unlike `reconcile_containers`, this does NOT create or remove
    /// containers. It patches specific fields on a container that already
    /// exists. Currently supports updating `sandbox_metadata` (warm pool
    /// claim).
    pub async fn update_container_description(
        &mut self,
        update: proto_api::executor_api_pb::UpdateContainerDescription,
    ) {
        self.container_manager
            .update_container_description(update)
            .await;
    }

    /// Reconcile allocation stream update: route allocations.
    ///
    /// Each allocation tuple is `(fe_id, allocation, command_seq)`.
    pub async fn reconcile_allocations(&mut self, allocations: Vec<(String, Allocation, u64)>) {
        if !allocations.is_empty() {
            let allocation_count = allocations.len();
            let _ = self
                .allocation_controller
                .command_tx
                .send(ACCommand::Reconcile {
                    added_or_updated_fes: vec![],
                    removed_fe_ids: vec![],
                    new_allocations: allocations,
                });
            info!(
                allocation_count,
                "Sent allocation Reconcile to AllocationController"
            );
        }
    }

    /// Snapshot a container's filesystem.
    ///
    /// Delegates to the container manager which handles stopping the container,
    /// exporting the filesystem, compressing, and uploading to the blob store.
    pub async fn snapshot_container(
        &mut self,
        container_id: &str,
        snapshot_id: &str,
        upload_uri: &str,
    ) {
        self.container_manager
            .snapshot_container(container_id, snapshot_id, upload_uri)
            .await;
    }

    /// Access the underlying container manager (e.g. to spawn snapshot work
    /// in a separate task).
    pub fn container_manager(&self) -> &Arc<FunctionContainerManager> {
        &self.container_manager
    }

    /// Get the notify for waking up the heartbeat loop when state changes (FEs
    /// added/removed).
    pub fn state_change_notify(&self) -> Arc<Notify> {
        self.state_change_notify.clone()
    }

    /// Get all FE states for heartbeat reporting.
    ///
    /// Returns states from both the AllocationController and Docker containers.
    pub async fn get_all_fe_states(&self) -> Vec<ContainerState> {
        let mut states = self.container_manager.get_states().await;
        states.extend(self.allocation_controller.state_rx.borrow().clone());
        states
    }

    /// Recover AC containers from the state file.
    ///
    /// Sends a Recover command to the AllocationController and waits for
    /// it to process. Returns the set of recovered handle IDs.
    pub async fn recover_ac_containers(&self) -> HashSet<String> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let _ = self
            .allocation_controller
            .command_tx
            .send(ACCommand::Recover { reply: reply_tx });
        reply_rx.await.unwrap_or_default()
    }

    /// Clean up orphaned containers from all execution paths.
    ///
    /// `ac_known_handles` contains handle IDs recovered by the
    /// AllocationController, so the FCM orphan cleanup won't kill them.
    pub async fn cleanup_orphans(&self, ac_known_handles: &HashSet<String>) -> usize {
        self.container_manager
            .cleanup_orphans(ac_known_handles)
            .await
    }

    /// Shut down the AllocationController gracefully.
    pub async fn shutdown(&mut self) {
        let _ = self
            .allocation_controller
            .command_tx
            .send(ACCommand::Shutdown);
    }
}
