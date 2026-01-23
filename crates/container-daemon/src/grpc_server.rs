//! gRPC server implementation over TCP.
//!
//! Implements the ContainerDaemon service for communication with the dataplane
//! agent.

use std::{net::SocketAddr, time::Instant};

use anyhow::{Context, Result};
use proto_api::container_daemon_pb::{
    HealthRequest,
    HealthResponse,
    SendSignalRequest,
    SendSignalResponse,
    container_daemon_server::{ContainerDaemon, ContainerDaemonServer},
};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::{error, info};

use crate::process_manager::ProcessManager;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// gRPC service implementation
pub struct ContainerDaemonService {
    process_manager: ProcessManager,
    start_time: Instant,
}

impl ContainerDaemonService {
    pub fn new(process_manager: ProcessManager) -> Self {
        Self {
            process_manager,
            start_time: Instant::now(),
        }
    }
}

#[tonic::async_trait]
impl ContainerDaemon for ContainerDaemonService {
    async fn send_signal(
        &self,
        request: Request<SendSignalRequest>,
    ) -> Result<Response<SendSignalResponse>, Status> {
        let req = request.into_inner();
        let signal = req.signal.unwrap_or(15); // Default to SIGTERM

        match self.process_manager.send_signal_legacy(signal).await {
            Ok(()) => {
                info!(signal = signal, "Signal sent successfully");
                Ok(Response::new(SendSignalResponse {
                    success: Some(true),
                    error: None,
                }))
            }
            Err(e) => {
                error!(error = %e, signal = signal, "Failed to send signal");
                Ok(Response::new(SendSignalResponse {
                    success: Some(false),
                    error: Some(e.to_string()),
                }))
            }
        }
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let uptime_secs = self.start_time.elapsed().as_secs();

        Ok(Response::new(HealthResponse {
            healthy: Some(true),
            version: Some(VERSION.to_string()),
            uptime_secs: Some(uptime_secs),
        }))
    }
}

/// Run the gRPC server over TCP.
pub async fn run_server(
    port: u16,
    process_manager: ProcessManager,
    cancel_token: CancellationToken,
) -> Result<()> {
    // Bind to all interfaces so the port is accessible from outside the container
    let addr: SocketAddr = ([0, 0, 0, 0], port).into();

    info!(port = port, addr = %addr, "gRPC server listening");

    // Create service
    let service = ContainerDaemonService::new(process_manager);
    let server = ContainerDaemonServer::new(service);

    // Run server with graceful shutdown
    tonic::transport::Server::builder()
        .add_service(server)
        .serve_with_shutdown(addr, async move {
            cancel_token.cancelled().await;
            info!("gRPC server shutting down");
        })
        .await
        .context("gRPC server error")?;

    Ok(())
}
