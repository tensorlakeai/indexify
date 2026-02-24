//! Indexify Container Daemon
//!
//! A PID1 daemon that runs inside function executor containers, managing the
//! function executor process lifecycle and providing a gRPC API for the
//! dataplane agent to communicate with.
//!
//! The daemon runs two servers:
//! - gRPC server (port 9500): Internal API for dataplane communication
//! - HTTP server (port 9501): User-facing Sandbox API for interactive process
//!   execution

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use indexify_container_daemon::{
    file_manager::FileManager,
    grpc_server,
    http_models,
    http_server,
    pid1,
    process_manager,
    pty_manager,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// Indexify Container Daemon - PID1 process manager for function executor
/// containers
#[derive(Parser, Debug)]
#[command(name = "indexify-container-daemon")]
#[command(about = "PID1 daemon for Indexify function executor containers")]
struct Args {
    /// TCP port for gRPC server (internal API)
    #[arg(long, default_value = "9500")]
    port: u16,

    /// TCP port for HTTP server (user-facing Sandbox API)
    #[arg(long, default_value = "9501")]
    http_port: u16,

    /// Log directory for process stdout/stderr
    #[arg(long, default_value = "/var/log/indexify")]
    log_dir: PathBuf,

    /// Graceful shutdown timeout in seconds
    #[arg(long, default_value = "30")]
    shutdown_timeout_secs: u64,

    /// Command to run (passed after --)
    #[arg(last = true)]
    command: Vec<String>,
}

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    let args = Args::parse();

    info!(version = VERSION, "Starting indexify-container-daemon");

    // Create log directory
    if !args.log_dir.exists() {
        std::fs::create_dir_all(&args.log_dir).context("Failed to create log directory")?;
    }

    // Initialize PID1 signal handling (zombie reaping, signal forwarding)
    let cancel_token = CancellationToken::new();
    let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);

    // Set up signal handlers
    pid1::setup_signal_handlers(cancel_token.clone(), shutdown_tx)?;

    // Create the process manager
    let process_manager = process_manager::ProcessManager::new(args.log_dir.clone());

    // Create the file manager
    let file_manager = FileManager::new();

    // Create the PTY manager
    let pty_manager = pty_manager::PtyManager::new(cancel_token.clone());

    // Start the gRPC server
    let grpc_server_handle = {
        let process_manager = process_manager.clone();
        let port = args.port;
        let cancel_token = cancel_token.clone();

        tokio::spawn(async move {
            if let Err(e) = grpc_server::run_server(port, process_manager, cancel_token).await {
                error!(error = %e, "gRPC server error");
            }
        })
    };

    // Start the HTTP server (Sandbox API)
    let http_server_handle = {
        let process_manager = process_manager.clone();
        let file_manager = file_manager.clone();
        let pty_manager = pty_manager.clone();
        let port = args.http_port;
        let cancel_token = cancel_token.clone();

        tokio::spawn(async move {
            if let Err(e) = http_server::run_http_server(
                port,
                process_manager,
                file_manager,
                pty_manager,
                cancel_token,
            )
            .await
            {
                error!(error = %e, "HTTP server error");
            }
        })
    };

    // Start zombie reaper task
    let reaper_handle = {
        let cancel_token = cancel_token.clone();
        tokio::spawn(async move {
            pid1::run_zombie_reaper(cancel_token).await;
        })
    };

    // If entrypoint command was provided, start it as a child process
    if !args.command.is_empty() {
        let command = args.command[0].clone();
        let cmd_args: Vec<String> = args.command.iter().skip(1).cloned().collect();

        info!(
            command = %command,
            args = ?cmd_args,
            "Starting entrypoint process"
        );

        let req = http_models::StartProcessRequest {
            command,
            args: cmd_args,
            env: Default::default(),
            working_dir: None,
            stdin_mode: http_models::StdinMode::Pipe,
            stdout_mode: http_models::OutputMode::Capture,
            stderr_mode: http_models::OutputMode::Capture,
        };

        match process_manager.start_process(req).await {
            Ok(info) => {
                info!(pid = info.pid, "Entrypoint process started");
            }
            Err(e) => {
                error!(error = %e, "Failed to start entrypoint process");
            }
        }
    } else {
        info!("No entrypoint command provided, waiting for HTTP API commands");
    }

    // Wait for shutdown signal
    info!(
        grpc_port = args.port,
        http_port = args.http_port,
        "Daemon ready"
    );
    wait_for_shutdown(shutdown_rx, cancel_token.clone()).await;

    // Graceful shutdown
    info!(
        timeout_secs = args.shutdown_timeout_secs,
        "Initiating graceful shutdown"
    );

    // Stop all PTY sessions
    pty_manager.stop_all_sessions().await;

    // Signal child processes to stop
    if let Err(e) = process_manager
        .stop_process(args.shutdown_timeout_secs)
        .await
    {
        error!(error = %e, "Error stopping child processes");
    }

    // Cancel all tasks
    cancel_token.cancel();

    // Wait for tasks to finish
    let _ = tokio::join!(grpc_server_handle, http_server_handle, reaper_handle);

    info!("Daemon shutdown complete");
    Ok(())
}

async fn wait_for_shutdown(mut shutdown_rx: mpsc::Receiver<()>, cancel_token: CancellationToken) {
    tokio::select! {
        _ = shutdown_rx.recv() => {
            info!("Received shutdown signal");
        }
        _ = cancel_token.cancelled() => {
            info!("Cancellation token triggered");
        }
    }
}
