use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::process::Command;
use tracing::info;

use super::{ProcessConfig, ProcessDriver, ProcessHandle};
use crate::daemon_binary;

/// Container path for the daemon binary.
const CONTAINER_DAEMON_PATH: &str = "/indexify-daemon";

/// Container port for the daemon gRPC server (internal API).
const DAEMON_GRPC_PORT: u16 = 9500;

/// Container port for the daemon HTTP server (user-facing Sandbox API).
const DAEMON_HTTP_PORT: u16 = 9501;

pub struct DockerDriver {
    /// Docker socket path (e.g., /var/run/docker.sock)
    docker_socket_path: Option<String>,
}

impl DockerDriver {
    pub fn new() -> Self {
        Self {
            docker_socket_path: None,
        }
    }

    pub fn with_socket(docker_socket_path: String) -> Self {
        Self {
            docker_socket_path: Some(docker_socket_path),
        }
    }

    fn docker_cmd(&self) -> Command {
        let mut cmd = Command::new("docker");
        if let Some(socket) = &self.docker_socket_path {
            cmd.arg("-H").arg(format!("unix://{}", socket));
        }
        cmd
    }

    /// Get the host port mapped to a container port.
    async fn get_mapped_port(&self, container_name: &str, container_port: u16) -> Result<u16> {
        let mut cmd = self.docker_cmd();
        cmd.arg("port");
        cmd.arg(container_name);
        cmd.arg(container_port.to_string());

        let output = cmd.output().await.context("Failed to get container port")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("Failed to get mapped port: {}", stderr);
        }

        // Output format: "0.0.0.0:32768" or ":::32768"
        let output_str = String::from_utf8_lossy(&output.stdout);
        let port_str = output_str
            .trim()
            .rsplit(':')
            .next()
            .context("Invalid port output format")?;

        port_str.parse().context("Invalid port number")
    }
}

impl Default for DockerDriver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ProcessDriver for DockerDriver {
    async fn start(&self, config: ProcessConfig) -> Result<ProcessHandle> {
        let image = config
            .image
            .as_ref()
            .context("Docker driver requires an image")?;

        let container_name = format!("indexify-{}", uuid::Uuid::new_v4());

        // Get daemon binary path
        let daemon_binary_path =
            daemon_binary::get_daemon_path().context("Daemon binary not available")?;

        info!(
            container = %container_name,
            daemon_path = %daemon_binary_path.display(),
            grpc_port = DAEMON_GRPC_PORT,
            http_port = DAEMON_HTTP_PORT,
            "Starting container with daemon injection"
        );

        let mut cmd = self.docker_cmd();
        cmd.arg("run");
        cmd.arg("-d");
        cmd.arg("--name").arg(&container_name);

        // Publish daemon ports (let Docker choose host ports)
        cmd.arg("-p").arg(format!("0:{}", DAEMON_GRPC_PORT));
        cmd.arg("-p").arg(format!("0:{}", DAEMON_HTTP_PORT));

        // Bind mount the daemon binary (read-only)
        cmd.arg("-v").arg(format!(
            "{}:{}:ro",
            daemon_binary_path.display(),
            CONTAINER_DAEMON_PATH
        ));

        // Set environment variables
        for (key, value) in &config.env {
            cmd.arg("-e").arg(format!("{}={}", key, value));
        }

        // Set working directory if specified
        if let Some(dir) = &config.working_dir {
            cmd.arg("-w").arg(dir);
        }

        // Apply resource limits
        if let Some(resources) = &config.resources {
            if let Some(memory_mb) = resources.memory_mb {
                cmd.arg("--memory").arg(format!("{}m", memory_mb));
            }
            if let Some(cpu_millicores) = resources.cpu_millicores {
                // Docker uses --cpus which takes a decimal (1.5 = 1.5 cores)
                // We convert millicores (1500 = 1.5 cores) to this format
                let cpus = cpu_millicores as f64 / 1000.0;
                cmd.arg("--cpus").arg(format!("{:.3}", cpus));
            }
        }

        // Override entrypoint to the daemon
        cmd.arg("--entrypoint").arg(CONTAINER_DAEMON_PATH);

        // Add the image
        cmd.arg(image);

        // Pass daemon arguments (ports, log dir) then original command after --
        cmd.arg("--port").arg(DAEMON_GRPC_PORT.to_string());
        cmd.arg("--http-port").arg(DAEMON_HTTP_PORT.to_string());
        cmd.arg("--log-dir").arg("/var/log/indexify");

        // Pass original command as arguments to the daemon (after --)
        // The daemon will receive these and can use them when StartProcess is called
        cmd.arg("--");
        cmd.arg(&config.command);
        cmd.args(&config.args);

        let output = cmd.output().await.context("Failed to execute docker run")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("Docker run failed: {}", stderr);
        }

        // Get the mapped host ports
        let grpc_host_port = self
            .get_mapped_port(&container_name, DAEMON_GRPC_PORT)
            .await?;
        let http_host_port = self
            .get_mapped_port(&container_name, DAEMON_HTTP_PORT)
            .await?;

        let daemon_addr = format!("127.0.0.1:{}", grpc_host_port);
        let http_addr = format!("127.0.0.1:{}", http_host_port);

        info!(
            container = %container_name,
            daemon_addr = %daemon_addr,
            http_addr = %http_addr,
            grpc_port = grpc_host_port,
            http_port = http_host_port,
            "Container started, daemon available on localhost"
        );

        Ok(ProcessHandle {
            id: container_name,
            daemon_addr: Some(daemon_addr),
            http_addr: Some(http_addr),
        })
    }

    async fn send_sig(&self, handle: &ProcessHandle, signal: i32) -> Result<()> {
        let mut cmd = self.docker_cmd();
        cmd.arg("kill");
        cmd.arg("--signal").arg(signal.to_string());
        cmd.arg(&handle.id);

        let output = cmd
            .output()
            .await
            .context("Failed to execute docker kill")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("Docker kill failed: {}", stderr);
        }

        Ok(())
    }

    async fn kill(&self, handle: &ProcessHandle) -> Result<()> {
        // First kill the container
        let mut kill_cmd = self.docker_cmd();
        kill_cmd.arg("kill").arg(&handle.id);
        let _ = kill_cmd.output().await;

        // Then remove it
        let mut rm_cmd = self.docker_cmd();
        rm_cmd.arg("rm").arg("-f").arg(&handle.id);

        let output = rm_cmd
            .output()
            .await
            .context("Failed to execute docker rm")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("Docker rm failed: {}", stderr);
        }

        Ok(())
    }

    async fn alive(&self, handle: &ProcessHandle) -> Result<bool> {
        let mut cmd = self.docker_cmd();
        cmd.arg("inspect");
        cmd.arg("-f").arg("{{.State.Running}}");
        cmd.arg(&handle.id);

        let output = cmd
            .output()
            .await
            .context("Failed to execute docker inspect")?;

        if !output.status.success() {
            return Ok(false);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        Ok(stdout.trim() == "true")
    }
}
