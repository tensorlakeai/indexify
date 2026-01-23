//! PID1 responsibilities: signal handling and zombie process reaping.
//!
//! When running as PID1 (init) inside a container, we must:
//! 1. Handle SIGTERM/SIGINT for graceful shutdown
//! 2. Reap zombie processes (orphaned children become our children)
//! 3. Forward signals to child processes

use anyhow::Result;
#[cfg(unix)]
use nix::sys::wait::{WaitPidFlag, WaitStatus, waitpid};
#[cfg(unix)]
use nix::unistd::Pid;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Set up signal handlers for SIGTERM and SIGINT.
/// These signals trigger graceful shutdown.
pub fn setup_signal_handlers(
    cancel_token: CancellationToken,
    shutdown_tx: mpsc::Sender<()>,
) -> Result<()> {
    // Spawn a task to handle signals
    tokio::spawn(async move {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to set up SIGTERM handler");
        let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .expect("Failed to set up SIGINT handler");

        tokio::select! {
            _ = sigterm.recv() => {
                info!("Received SIGTERM");
            }
            _ = sigint.recv() => {
                info!("Received SIGINT");
            }
            _ = cancel_token.cancelled() => {
                return;
            }
        }

        // Signal shutdown
        let _ = shutdown_tx.send(()).await;
    });

    Ok(())
}

/// Run the zombie reaper loop.
/// As PID1, we adopt orphaned processes and must reap them to prevent zombies.
pub async fn run_zombie_reaper(cancel_token: CancellationToken) {
    info!("Starting zombie reaper");

    // Check for zombies periodically
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                info!("Zombie reaper shutting down");
                // Final reap before exit
                reap_zombies();
                return;
            }
            _ = interval.tick() => {
                reap_zombies();
            }
        }
    }
}

/// Reap any zombie processes.
/// Called periodically and on SIGCHLD.
#[cfg(unix)]
fn reap_zombies() {
    loop {
        // Wait for any child process without blocking (WNOHANG)
        match waitpid(Pid::from_raw(-1), Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::Exited(pid, status)) => {
                debug!(
                    pid = pid.as_raw(),
                    exit_code = status,
                    "Reaped zombie (exited)"
                );
            }
            Ok(WaitStatus::Signaled(pid, signal, _)) => {
                debug!(
                    pid = pid.as_raw(),
                    signal = signal as i32,
                    "Reaped zombie (signaled)"
                );
            }
            Ok(WaitStatus::StillAlive) => {
                // No more zombies to reap
                break;
            }
            Ok(status) => {
                debug!(?status, "waitpid returned unexpected status");
            }
            Err(nix::errno::Errno::ECHILD) => {
                // No children to wait for
                break;
            }
            Err(e) => {
                warn!(error = %e, "waitpid error");
                break;
            }
        }
    }
}

#[cfg(not(unix))]
fn reap_zombies() {
    // No-op on non-Unix systems
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_signal_handlers_setup() {
        let cancel_token = CancellationToken::new();
        let (tx, _rx) = mpsc::channel(1);

        // Should not panic
        let result = setup_signal_handlers(cancel_token.clone(), tx);
        assert!(result.is_ok());

        // Cleanup
        cancel_token.cancel();
    }
}
