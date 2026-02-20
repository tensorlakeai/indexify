//! Health check and timeout management for managed containers.

use std::time::Duration;

use proto_api::executor_api_pb::ContainerTerminationReason;
use tokio_util::sync::CancellationToken;

use super::{
    FunctionContainerManager,
    types::{ContainerState, ContainerStore, termination_reason_from_exit_status},
};
use crate::{daemon_client::DaemonClient, driver::ProcessHandle, network_rules};

const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);

impl FunctionContainerManager {
    /// Run the health check loop. Call this from a spawned task.
    pub async fn run_health_checks(&self, cancel_token: CancellationToken) {
        let mut interval = tokio::time::interval(HEALTH_CHECK_INTERVAL);

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!("Health check loop cancelled");
                    return;
                }
                _ = interval.tick() => {
                    self.check_timeouts().await;
                    self.check_all_containers().await;
                }
            }
        }
    }

    /// Check for sandbox containers that have exceeded their timeout.
    pub(super) async fn check_timeouts(&self) {
        let mut containers = self.containers.write().await;
        self.stop_timed_out_containers(&mut containers).await;
    }

    /// Check all running containers to see if they're still alive.
    async fn check_all_containers(&self) {
        let mut containers = self.containers.write().await;
        let ids: Vec<String> = containers.keys().cloned().collect();

        for id in ids {
            let Some(container) = containers.get_mut(&id) else {
                continue;
            };

            let check_data = match &container.state {
                ContainerState::Running {
                    handle,
                    daemon_client,
                } => Some((handle.clone(), Some(daemon_client.clone()), None)),
                ContainerState::Stopping { handle, reason, .. } => {
                    Some((handle.clone(), None, Some(*reason)))
                }
                _ => None,
            };

            if let Some((handle, daemon_client, stopping_reason)) = check_data {
                let span = container.info().tracing_span();
                if let Some(reason) = stopping_reason {
                    self.check_stopping_container(container, &handle, reason, &span)
                        .await;
                } else {
                    self.check_running_container(container, &handle, daemon_client, &span)
                        .await;
                }
            }
        }
    }

    /// Check if a stopping container has died yet and transition to Terminated.
    async fn check_stopping_container(
        &self,
        container: &mut super::types::ManagedContainer,
        handle: &ProcessHandle,
        reason: ContainerTerminationReason,
        span: &tracing::Span,
    ) {
        if let Ok(false) = self.driver.alive(handle).await {
            tracing::info!(parent: span, reason = ?reason, "Container stopped");
            if container.transition_to_terminated(reason).is_ok() {
                let id = container.description.id.as_deref().unwrap_or("");
                Self::send_container_terminated(&self.container_state_tx, id, reason);
            }
        }
    }

    /// Check a running container's liveness and daemon health.
    async fn check_running_container(
        &self,
        container: &mut super::types::ManagedContainer,
        handle: &ProcessHandle,
        daemon_client: Option<DaemonClient>,
        span: &tracing::Span,
    ) {
        match self.driver.alive(handle).await {
            Ok(true) => {
                // Container is alive, check daemon health.
                // Note: We check daemon health, not individual process status.
                // User-started processes (via HTTP API) may complete independently
                // without affecting the container's lifecycle.
                if let Some(mut client) = daemon_client {
                    match client.health().await {
                        Ok(healthy) if !healthy => {
                            tracing::info!(
                                parent: span,
                                "Daemon is unhealthy, terminating container"
                            );
                            let _ = network_rules::remove_rules(&handle.id, &handle.container_ip);
                            let _ = self.driver.kill(handle).await;
                            let reason = ContainerTerminationReason::Unhealthy;
                            if container.transition_to_terminated(reason).is_ok() {
                                let id = container.description.id.as_deref().unwrap_or("");
                                Self::send_container_terminated(
                                    &self.container_state_tx,
                                    id,
                                    reason,
                                );
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                parent: span,
                                error = %e,
                                "Failed to check daemon health"
                            );
                        }
                        _ => {} // healthy
                    }
                }
            }
            Ok(false) => {
                let exit_status = self.driver.get_exit_status(handle).await.ok().flatten();
                let reason = termination_reason_from_exit_status(exit_status.clone());
                tracing::info!(
                    parent: span,
                    exit_code = ?exit_status.as_ref().and_then(|s| s.exit_code),
                    oom_killed = ?exit_status.as_ref().map(|s| s.oom_killed),
                    reason = ?reason,
                    "Container is no longer alive"
                );
                if container.transition_to_terminated(reason).is_ok() {
                    let id = container.description.id.as_deref().unwrap_or("");
                    Self::send_container_terminated(&self.container_state_tx, id, reason);
                }
            }
            Err(e) => {
                tracing::warn!(
                    parent: span,
                    error = %e,
                    "Failed to check container status"
                );
            }
        }
    }

    /// Stop all sandbox containers that have exceeded their timeout.
    pub(super) async fn stop_timed_out_containers(&self, containers: &mut ContainerStore) {
        let timed_out_ids: Vec<String> = containers
            .iter()
            .filter(|(_, container)| container.is_timed_out())
            .map(|(id, _)| id.clone())
            .collect();

        for id in timed_out_ids {
            let Some(container) = containers.get_mut(&id) else {
                continue;
            };
            let Some(timeout_secs) = container
                .description
                .sandbox_metadata
                .as_ref()
                .and_then(|m| m.timeout_secs)
            else {
                continue;
            };
            let info = container.info();
            let elapsed = container
                .sandbox_claimed_at
                .map(|s| s.elapsed().as_secs())
                .unwrap_or(0);
            tracing::warn!(
                parent: &info.tracing_span(),
                timeout_secs = timeout_secs,
                elapsed_secs = elapsed,
                "Sandbox container timed out, terminating"
            );
            self.initiate_stop(
                container,
                ContainerTerminationReason::FunctionTimeout,
            )
            .await;
        }
    }
}
