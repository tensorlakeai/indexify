//! Periodic health checker for function executor subprocesses.
//!
//! Uses both gRPC health checks and container-level liveness checks
//! (via the process driver) to detect dead function executors quickly.
//! This is critical for OOM kills where the TCP connection may be
//! half-open and gRPC calls hang.

use std::{sync::Arc, time::Duration};

use proto_api::executor_api_pb::FunctionExecutorTerminationReason;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::{events::FEEvent, fe_client::FunctionExecutorGrpcClient};
use crate::driver::{ProcessDriver, ProcessHandle};

const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);
const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(5);

/// Runs periodic health checks against a function executor subprocess.
/// Sends a `FunctionExecutorTerminated` event if health check fails.
///
/// Uses two checks:
/// 1. gRPC health check (with timeout) — detects FE-level issues
/// 2. Container liveness via process driver — detects OOM kills, crashes
pub async fn run_health_checker(
    mut client: FunctionExecutorGrpcClient,
    driver: Arc<dyn ProcessDriver>,
    process_handle: ProcessHandle,
    event_tx: mpsc::UnboundedSender<FEEvent>,
    cancel_token: CancellationToken,
    fe_id: String,
) {
    // Apply gRPC-level timeout so health checks don't hang on half-open
    // TCP connections (e.g. after OOM kills).
    client.set_timeout(HEALTH_CHECK_TIMEOUT);

    let mut interval = tokio::time::interval(HEALTH_CHECK_INTERVAL);
    let mut consecutive_failures = 0u32;
    const MAX_FAILURES: u32 = 3;

    // Reset the interval so the first tick happens after the duration
    interval.reset();

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                info!(fe_id = %fe_id, "Health checker cancelled");
                return;
            }
            _ = interval.tick() => {
                // First: check if the container/process is still alive.
                // This catches OOM kills immediately without waiting for
                // gRPC timeouts.
                match driver.alive(&process_handle).await {
                    Ok(false) => {
                        let exit_status = driver.get_exit_status(&process_handle).await.ok().flatten();
                        let is_oom = exit_status.as_ref().is_some_and(|s| s.oom_killed);
                        let reason = if is_oom {
                            FunctionExecutorTerminationReason::Oom
                        } else {
                            FunctionExecutorTerminationReason::Unhealthy
                        };
                        warn!(
                            fe_id = %fe_id,
                            exit_status = ?exit_status,
                            is_oom = is_oom,
                            "Container is no longer running"
                        );
                        let _ = event_tx.send(FEEvent::FunctionExecutorTerminated {
                            fe_id: fe_id.clone(),
                            reason,
                        });
                        return;
                    }
                    Err(e) => {
                        warn!(
                            fe_id = %fe_id,
                            error = %e,
                            "Failed to check container liveness"
                        );
                        // Fall through to gRPC check
                    }
                    Ok(true) => {
                        // Container is alive, proceed to gRPC check
                    }
                }

                // Second: gRPC health check (timeout is set on the client)
                match client.check_health().await {
                    Ok(response) => {
                        if response.healthy.unwrap_or(false) {
                            consecutive_failures = 0;
                        } else {
                            consecutive_failures += 1;
                            warn!(
                                fe_id = %fe_id,
                                consecutive_failures = consecutive_failures,
                                status_message = ?response.status_message,
                                "FE health check returned unhealthy"
                            );
                        }
                    }
                    Err(e) => {
                        consecutive_failures += 1;
                        warn!(
                            fe_id = %fe_id,
                            consecutive_failures = consecutive_failures,
                            error = %e,
                            "FE health check failed"
                        );
                    }
                }

                if consecutive_failures >= MAX_FAILURES {
                    warn!(
                        fe_id = %fe_id,
                        consecutive_failures = consecutive_failures,
                        "FE failed too many health checks, terminating"
                    );
                    let _ = event_tx.send(FEEvent::FunctionExecutorTerminated {
                        fe_id: fe_id.clone(),
                        reason: FunctionExecutorTerminationReason::Unhealthy,
                    });
                    return;
                }
            }
        }
    }
}
