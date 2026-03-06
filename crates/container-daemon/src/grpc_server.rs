//! gRPC server implementation over TCP.
//!
//! Implements the ContainerDaemon service for communication with the dataplane
//! agent.

use std::{net::SocketAddr, time::Instant};

use anyhow::{Context, Result};
use proto_api::container_daemon_pb::{
    HealthRequest,
    HealthResponse,
    PrepareRequest,
    PrepareResponse,
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

    /// Mount a user image block device, optionally with overlayfs.
    async fn mount_user_image(
        &self,
        mount: &proto_api::container_daemon_pb::MountConfig,
    ) -> anyhow::Result<()> {
        // Idempotency: if the device mount point already has something mounted,
        // skip the entire mount sequence.
        if is_mountpoint(&mount.device_mount_point).await {
            info!(
                mount_point = %mount.device_mount_point,
                "Device already mounted (idempotent Prepare), skipping"
            );
            return Ok(());
        }

        // Create mount point directories
        tokio::fs::create_dir_all(&mount.device_mount_point).await?;

        // Mount device read-only
        let output = tokio::process::Command::new("mount")
            .args(["-o", "ro", &mount.device, &mount.device_mount_point])
            .output()
            .await?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!(
                "mount {} -> {} failed with exit code {:?}: {}",
                mount.device,
                mount.device_mount_point,
                output.status.code(),
                stderr.trim()
            );
        }

        if mount.use_overlay {
            // Upper/work dirs must be OUTSIDE the overlay mount point to avoid
            // a self-referential mount. Use a tmpfs scratch area.
            let scratch = "/mnt/overlay-work";
            let upper = format!("{}/upper", scratch);
            let work = format!("{}/work", scratch);
            tokio::fs::create_dir_all(&upper).await?;
            tokio::fs::create_dir_all(&work).await?;
            tokio::fs::create_dir_all(&mount.overlay_mount_point).await?;

            // Mount overlayfs
            let overlay_opts = format!(
                "lowerdir={},upperdir={},workdir={}",
                mount.device_mount_point, upper, work
            );
            let output = tokio::process::Command::new("mount")
                .args([
                    "-t",
                    "overlay",
                    "overlay",
                    "-o",
                    &overlay_opts,
                    &mount.overlay_mount_point,
                ])
                .output()
                .await?;
            if !output.status.success() {
                // Clean up the device mount before returning the error.
                let _ = tokio::process::Command::new("umount")
                    .arg(&mount.device_mount_point)
                    .status()
                    .await;
                let stderr = String::from_utf8_lossy(&output.stderr);
                anyhow::bail!(
                    "overlayfs mount failed with exit code {:?}: {}",
                    output.status.code(),
                    stderr.trim()
                );
            }
        }

        Ok(())
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

    async fn prepare(
        &self,
        request: Request<PrepareRequest>,
    ) -> Result<Response<PrepareResponse>, Status> {
        let req = request.into_inner();

        // Store env vars as defaults for future process spawns
        let env_map: std::collections::HashMap<String, String> =
            req.env_vars.into_iter().map(|e| (e.key, e.value)).collect();

        if !env_map.is_empty() {
            info!(
                count = env_map.len(),
                "Setting default environment variables"
            );
            self.process_manager.set_default_env(env_map).await;
        }

        // Handle mount configuration
        if let Some(mount) = req.mount {
            info!(
                device = %mount.device,
                mount_point = %mount.device_mount_point,
                overlay = %mount.overlay_mount_point,
                use_overlay = mount.use_overlay,
                "Mounting user image"
            );

            if let Err(e) = self.mount_user_image(&mount).await {
                error!(error = %e, "Failed to mount user image");
                return Ok(Response::new(PrepareResponse {
                    success: false,
                    error: Some(e.to_string()),
                }));
            }

            // Set chroot path so future processes run inside the mounted image
            let chroot_path = if mount.use_overlay {
                mount.overlay_mount_point.clone()
            } else {
                mount.device_mount_point.clone()
            };
            self.process_manager
                .set_chroot_path(std::path::PathBuf::from(chroot_path))
                .await;
        }

        // Set working directory for future processes
        if let Some(working_dir) = req.working_dir {
            self.process_manager
                .set_default_working_dir(std::path::PathBuf::from(working_dir))
                .await;
        }

        Ok(Response::new(PrepareResponse {
            success: true,
            error: None,
        }))
    }
}

/// Check if a path is currently a mountpoint.
async fn is_mountpoint(path: &str) -> bool {
    tokio::process::Command::new("mountpoint")
        .arg("-q")
        .arg(path)
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
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
