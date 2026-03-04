//! Health check and timeout management for managed containers.

use std::time::{Duration, Instant};

use proto_api::executor_api_pb::ContainerTerminationReason;
use tokio_util::sync::CancellationToken;

use super::{
    FunctionContainerManager,
    types::{ContainerState, ContainerStore, termination_reason_from_exit_status},
};
use crate::{daemon_client::DaemonClient, driver::ProcessHandle, network_rules};

const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);

enum HealthCheck {
    Running {
        id: String,
        handle: ProcessHandle,
        daemon_client: DaemonClient,
        span: tracing::Span,
    },
    Stopping {
        id: String,
        handle: ProcessHandle,
        reason: ContainerTerminationReason,
        span: tracing::Span,
    },
}

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
    pub(super) async fn check_all_containers(&self) {
        let checks = {
            let containers = self.containers.read().await;
            containers
                .iter()
                .filter_map(|(id, container)| {
                    let span = container.info().tracing_span();
                    match &container.state {
                        ContainerState::Running {
                            handle,
                            daemon_client,
                        } => Some(HealthCheck::Running {
                            id: id.clone(),
                            handle: handle.clone(),
                            daemon_client: daemon_client.clone(),
                            span,
                        }),
                        ContainerState::Stopping { handle, reason, .. } => {
                            Some(HealthCheck::Stopping {
                                id: id.clone(),
                                handle: handle.clone(),
                                reason: *reason,
                                span,
                            })
                        }
                        _ => None,
                    }
                })
                .collect::<Vec<_>>()
        };

        for check in checks {
            match check {
                HealthCheck::Running {
                    id,
                    handle,
                    daemon_client,
                    span,
                } => {
                    self.check_running_container(&id, &handle, daemon_client, &span)
                        .await;
                }
                HealthCheck::Stopping {
                    id,
                    handle,
                    reason,
                    span,
                } => {
                    self.check_stopping_container(&id, &handle, reason, &span)
                        .await;
                }
            }
        }
    }

    /// Check if a stopping container has died yet and transition to Terminated.
    async fn check_stopping_container(
        &self,
        id: &str,
        handle: &ProcessHandle,
        reason: ContainerTerminationReason,
        span: &tracing::Span,
    ) {
        if let Ok(false) = self.driver.alive(handle).await {
            // Even if the process is already dead, run driver cleanup so VM
            // network/LV resources are released before reporting termination.
            if let Err(e) = network_rules::remove_rules(&handle.id, &handle.container_ip) {
                tracing::warn!(
                    parent: span,
                    error = ?e,
                    "Failed to remove network rules"
                );
            }
            if let Err(e) = self.driver.kill(handle).await {
                tracing::warn!(
                    parent: span,
                    error = ?e,
                    "Failed to cleanup stopped container"
                );
            }

            tracing::info!(parent: span, ?reason, "Container stopped and cleaned up");
            self.transition_stopping_to_terminated(id, handle, reason, span)
                .await;
        }
    }

    /// Check a running container's liveness and daemon health.
    async fn check_running_container(
        &self,
        id: &str,
        handle: &ProcessHandle,
        mut daemon_client: DaemonClient,
        span: &tracing::Span,
    ) {
        match self.driver.alive(handle).await {
            Ok(true) => {
                // Container is alive, check daemon health.
                // Note: We check daemon health, not individual process status.
                // User-started processes (via HTTP API) may complete independently
                // without affecting the container's lifecycle.
                let health_start = Instant::now();
                let health_result = daemon_client.health().await;
                self.metrics
                    .histograms
                    .sandbox_health_check_latency_seconds
                    .record(health_start.elapsed().as_secs_f64(), &[]);
                match health_result {
                    Ok(healthy) if !healthy => {
                        self.metrics
                            .counters
                            .sandbox_failed_health_checks
                            .add(1, &[]);
                        tracing::info!(parent: span, "Daemon is unhealthy, terminating container");
                        let _ = network_rules::remove_rules(&handle.id, &handle.container_ip);
                        let _ = self.driver.kill(handle).await;
                        let reason = ContainerTerminationReason::Unhealthy;
                        self.transition_running_to_terminated(id, handle, reason, span)
                            .await;
                    }
                    Err(e) => {
                        self.metrics
                            .counters
                            .sandbox_failed_health_checks
                            .add(1, &[]);
                        tracing::warn!(
                            parent: span,
                            error = ?e,
                            "Failed to check daemon health"
                        );
                    }
                    _ => {} // healthy
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
                if let Err(e) = network_rules::remove_rules(&handle.id, &handle.container_ip) {
                    tracing::warn!(
                        parent: span,
                        error = ?e,
                        "Failed to remove network rules"
                    );
                }
                if let Err(e) = self.driver.kill(handle).await {
                    tracing::warn!(
                        parent: span,
                        error = ?e,
                        "Failed to cleanup dead container"
                    );
                }
                self.transition_running_to_terminated(id, handle, reason, span)
                    .await;
            }
            Err(e) => {
                tracing::warn!(
                    parent: span,
                    error = ?e,
                    "Failed to check container status"
                );
            }
        }
    }

    async fn transition_stopping_to_terminated(
        &self,
        id: &str,
        expected_handle: &ProcessHandle,
        expected_reason: ContainerTerminationReason,
        span: &tracing::Span,
    ) {
        let mut containers = self.containers.write().await;
        let Some(container) = containers.get_mut(id) else {
            return;
        };
        let is_expected_state = matches!(
            &container.state,
            ContainerState::Stopping { handle, reason, .. }
                if handle.id == expected_handle.id && *reason == expected_reason
        );
        if !is_expected_state {
            return;
        }
        if container
            .transition_to_terminated(expected_reason)
            .inspect_err(|error| tracing::warn!(parent: span, ?error, "Invalid state transition"))
            .is_ok()
        {
            Self::send_container_terminated(&self.container_state_tx, id, expected_reason);
        }
    }

    async fn transition_running_to_terminated(
        &self,
        id: &str,
        expected_handle: &ProcessHandle,
        reason: ContainerTerminationReason,
        span: &tracing::Span,
    ) {
        let mut containers = self.containers.write().await;
        let Some(container) = containers.get_mut(id) else {
            return;
        };
        let is_expected_state = matches!(
            &container.state,
            ContainerState::Running { handle, .. } if handle.id == expected_handle.id
        );
        if !is_expected_state {
            return;
        }
        if container
            .transition_to_terminated(reason)
            .inspect_err(|error| tracing::warn!(parent: span, ?error, "Invalid state transition"))
            .is_ok()
        {
            Self::send_container_terminated(&self.container_state_tx, id, reason);
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
            self.initiate_stop(container, ContainerTerminationReason::FunctionTimeout)
                .await;
        }
    }
}
