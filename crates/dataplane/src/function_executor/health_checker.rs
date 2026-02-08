//! Periodic health checker for function executor subprocesses.

use std::time::Duration;

use proto_api::executor_api_pb::FunctionExecutorTerminationReason;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::{events::FEEvent, fe_client::FunctionExecutorGrpcClient};

const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// Runs periodic health checks against a function executor subprocess.
/// Sends a `FunctionExecutorTerminated` event if health check fails.
pub async fn run_health_checker(
    mut client: FunctionExecutorGrpcClient,
    event_tx: mpsc::UnboundedSender<FEEvent>,
    cancel_token: CancellationToken,
    fe_id: String,
) {
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
