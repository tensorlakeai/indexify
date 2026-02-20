//! Periodic health checker for function executor subprocesses.
//!
//! Uses both gRPC health checks and container-level liveness checks
//! (via the process driver) to detect dead function executors quickly.
//! This is critical for OOM kills where the TCP connection may be
//! half-open and gRPC calls hang.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use proto_api::executor_api_pb::ContainerTerminationReason;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::fe_client::FunctionExecutorGrpcClient;
use crate::{
    driver::{ProcessDriver, ProcessHandle},
    metrics::DataplaneMetrics,
};

const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(5);

/// Core health check loop. Returns the termination reason when the FE dies.
/// Returns `None` if cancelled.
pub async fn run_health_check_loop(
    mut client: FunctionExecutorGrpcClient,
    driver: Arc<dyn ProcessDriver>,
    process_handle: ProcessHandle,
    cancel_token: CancellationToken,
    fe_id: &str,
    metrics: Arc<DataplaneMetrics>,
) -> Option<ContainerTerminationReason> {
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
                return None;
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
                            ContainerTerminationReason::Oom
                        } else {
                            ContainerTerminationReason::ProcessCrash
                        };
                        warn!(
                            fe_id = %fe_id,
                            exit_status = ?exit_status,
                            is_oom = is_oom,
                            "Container is no longer running"
                        );
                        return Some(reason);
                    }
                    Err(e) => {
                        warn!(
                            fe_id = %fe_id,
                            error = ?e,
                            "Failed to check container liveness"
                        );
                        // Fall through to gRPC check
                    }
                    Ok(true) => {
                        // Container is alive, proceed to gRPC check
                    }
                }

                // Second: gRPC health check (timeout is set on the client)
                let check_start = Instant::now();
                match client.check_health().await {
                    Ok(response) => {
                        metrics
                            .histograms
                            .function_executor_health_check_latency_seconds
                            .record(check_start.elapsed().as_secs_f64(), &[]);
                        if response.healthy.unwrap_or(false) {
                            consecutive_failures = 0;
                        } else {
                            consecutive_failures += 1;
                            metrics
                                .counters
                                .function_executor_failed_health_checks
                                .add(1, &[]);
                            warn!(
                                fe_id = %fe_id,
                                consecutive_failures = consecutive_failures,
                                status_message = ?response.status_message,
                                "FE health check returned unhealthy"
                            );
                        }
                    }
                    Err(e) => {
                        metrics
                            .histograms
                            .function_executor_health_check_latency_seconds
                            .record(check_start.elapsed().as_secs_f64(), &[]);
                        consecutive_failures += 1;
                        metrics
                            .counters
                            .function_executor_failed_health_checks
                            .add(1, &[]);
                        warn!(
                            fe_id = %fe_id,
                            consecutive_failures = consecutive_failures,
                            error = ?e,
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
                    return Some(ContainerTerminationReason::Unhealthy);
                }
            }
        }
    }
}
