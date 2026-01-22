use std::{collections::HashMap, time::Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bollard::{
    Docker,
    models::{
        ContainerCreateBody,
        ContainerStateStatusEnum,
        HostConfig,
        PortBinding,
        ResourcesUlimits,
    },
    query_parameters::{
        CreateContainerOptions,
        CreateImageOptions,
        InspectContainerOptions,
        KillContainerOptions,
        RemoveContainerOptions,
        StartContainerOptions,
    },
};
use futures_util::StreamExt;
use tracing::info;

use super::{ExitStatus, ProcessConfig, ProcessDriver, ProcessHandle};
use crate::daemon_binary;

/// Container path for the daemon binary.
const CONTAINER_DAEMON_PATH: &str = "/indexify-daemon";

/// Container port for the daemon gRPC server (internal API).
const DAEMON_GRPC_PORT: u16 = 9500;

/// Container port for the daemon HTTP server (user-facing Sandbox API).
const DAEMON_HTTP_PORT: u16 = 9501;

pub struct DockerDriver {
    docker: Docker,
}

impl DockerDriver {
    /// Create a new DockerDriver connecting to the default Docker socket.
    pub fn new() -> Result<Self> {
        let docker =
            Docker::connect_with_local_defaults().context("Failed to connect to Docker daemon")?;
        Ok(Self { docker })
    }

    /// Create a DockerDriver connecting to a specific Docker address.
    ///
    /// Supported address formats:
    /// - Unix socket: `unix:///var/run/docker.sock` or `/var/run/docker.sock`
    /// - HTTP: `http://localhost:2375` or `tcp://localhost:2375`
    /// - HTTPS: `https://localhost:2376` (requires TLS setup)
    pub fn with_address(address: &str) -> Result<Self> {
        let docker = if address.starts_with("http://") || address.starts_with("tcp://") {
            // HTTP connection
            let addr = address
                .trim_start_matches("http://")
                .trim_start_matches("tcp://");
            Docker::connect_with_http(
                &format!("http://{}", addr),
                120,
                bollard::API_DEFAULT_VERSION,
            )
            .context("Failed to connect to Docker daemon via HTTP")?
        } else if address.starts_with("https://") {
            // HTTPS connection - use defaults which picks up DOCKER_HOST, DOCKER_CERT_PATH
            // env vars
            Docker::connect_with_defaults()
                .context("Failed to connect to Docker daemon via HTTPS")?
        } else {
            // Assume Unix socket (with or without unix:// prefix)
            let socket_path = address.trim_start_matches("unix://");
            Docker::connect_with_socket(socket_path, 120, bollard::API_DEFAULT_VERSION)
                .context("Failed to connect to Docker daemon via Unix socket")?
        };
        Ok(Self { docker })
    }

    /// Check if an image exists locally.
    async fn image_exists(&self, image: &str) -> Result<bool> {
        match self.docker.inspect_image(image).await {
            Ok(_) => Ok(true),
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => Ok(false),
            Err(e) => Err(e).context("Failed to inspect image"),
        }
    }

    /// Ensure an image is available locally, pulling it if necessary.
    async fn ensure_image(&self, image: &str) -> Result<()> {
        if self.image_exists(image).await? {
            info!(image = %image, "Image already exists locally");
            return Ok(());
        }

        info!(image = %image, event = "image_pull_started", "Pulling Docker image");
        let start = Instant::now();

        let options = CreateImageOptions {
            from_image: Some(image.to_string()),
            ..Default::default()
        };

        let mut stream = self.docker.create_image(Some(options), None, None);

        while let Some(result) = stream.next().await {
            match result {
                Ok(info) => {
                    if let Some(status) = info.status {
                        tracing::debug!(image = %image, status = %status, "Pull progress");
                    }
                }
                Err(e) => {
                    let duration_ms = start.elapsed().as_millis();
                    tracing::error!(
                        image = %image,
                        duration_ms = %duration_ms,
                        error = %e,
                        event = "image_pull_failed",
                        "Failed to pull Docker image"
                    );
                    return Err(e).context(format!("Failed to pull image {}", image));
                }
            }
        }

        let duration_ms = start.elapsed().as_millis();
        info!(
            image = %image,
            duration_ms = %duration_ms,
            event = "image_pull_completed",
            "Docker image pull completed"
        );

        Ok(())
    }

    /// Get the host port mapped to a container port.
    async fn get_mapped_port(&self, container_name: &str, container_port: u16) -> Result<u16> {
        let inspect = self
            .docker
            .inspect_container(container_name, None::<InspectContainerOptions>)
            .await
            .context("Failed to inspect container for port mapping")?;

        let network_settings = inspect
            .network_settings
            .context("No network settings found")?;

        let ports = network_settings.ports.context("No ports found")?;

        let port_key = format!("{}/tcp", container_port);
        let bindings = ports
            .get(&port_key)
            .context(format!("Port {} not found in container", container_port))?
            .as_ref()
            .context("No bindings for port")?;

        let binding = bindings.first().context("No port binding found")?;

        binding
            .host_port
            .as_ref()
            .context("No host port in binding")?
            .parse()
            .context("Invalid port number")
    }
}

impl Default for DockerDriver {
    fn default() -> Self {
        Self::new().expect("Failed to create default DockerDriver")
    }
}

#[async_trait]
impl ProcessDriver for DockerDriver {
    async fn start(&self, config: ProcessConfig) -> Result<ProcessHandle> {
        let image = config
            .image
            .as_ref()
            .context("Docker driver requires an image")?;

        // Ensure image is available locally (pull if needed)
        self.ensure_image(image).await?;

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

        // Build port bindings - expose container ports to random host ports
        let mut port_bindings: HashMap<String, Option<Vec<PortBinding>>> = HashMap::new();
        port_bindings.insert(
            format!("{}/tcp", DAEMON_GRPC_PORT),
            Some(vec![PortBinding {
                host_ip: Some("0.0.0.0".to_string()),
                host_port: None, // Let Docker choose
            }]),
        );
        port_bindings.insert(
            format!("{}/tcp", DAEMON_HTTP_PORT),
            Some(vec![PortBinding {
                host_ip: Some("0.0.0.0".to_string()),
                host_port: None, // Let Docker choose
            }]),
        );

        // Build bind mount for daemon binary
        let binds = vec![format!(
            "{}:{}:ro",
            daemon_binary_path.display(),
            CONTAINER_DAEMON_PATH
        )];

        // Build resource limits
        let mut memory_limit = None;
        let mut nano_cpus = None;

        if let Some(resources) = &config.resources {
            if let Some(memory_mb) = resources.memory_mb {
                memory_limit = Some((memory_mb * 1024 * 1024) as i64);
            }
            if let Some(cpu_millicores) = resources.cpu_millicores {
                // Docker uses nano CPUs (1 CPU = 1e9 nano CPUs)
                // millicores: 1000 = 1 CPU, so nano = millicores * 1e6
                nano_cpus = Some((cpu_millicores as i64) * 1_000_000);
            }
        }

        // Build environment variables
        let env: Vec<String> = config
            .env
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();

        // Build labels
        let labels: HashMap<String, String> = config.labels.iter().cloned().collect();

        // Build command arguments for daemon
        let mut cmd: Vec<String> = vec![
            "--port".to_string(),
            DAEMON_GRPC_PORT.to_string(),
            "--http-port".to_string(),
            DAEMON_HTTP_PORT.to_string(),
            "--log-dir".to_string(),
            "/var/log/indexify".to_string(),
            "--".to_string(),
            config.command.clone(),
        ];
        cmd.extend(config.args.clone());

        // Exposed ports for container
        let exposed_ports = vec![
            format!("{}/tcp", DAEMON_GRPC_PORT),
            format!("{}/tcp", DAEMON_HTTP_PORT),
        ];

        let host_config = HostConfig {
            binds: Some(binds),
            port_bindings: Some(port_bindings),
            memory: memory_limit,
            nano_cpus,
            // Set reasonable ulimits
            ulimits: Some(vec![ResourcesUlimits {
                name: Some("nofile".to_string()),
                soft: Some(65536),
                hard: Some(65536),
            }]),
            ..Default::default()
        };

        let container_config = ContainerCreateBody {
            image: Some(image.clone()),
            entrypoint: Some(vec![CONTAINER_DAEMON_PATH.to_string()]),
            cmd: Some(cmd),
            env: Some(env),
            labels: Some(labels),
            working_dir: config.working_dir.clone(),
            exposed_ports: Some(exposed_ports),
            host_config: Some(host_config),
            ..Default::default()
        };

        let create_options = CreateContainerOptions {
            name: Some(container_name.clone()),
            platform: String::new(),
        };

        // Create container
        self.docker
            .create_container(Some(create_options), container_config)
            .await
            .context("Failed to create container")?;

        // Start container
        self.docker
            .start_container(&container_name, None::<StartContainerOptions>)
            .await
            .context("Failed to start container")?;

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
        // Convert signal number to string (e.g., "SIGTERM", "SIGKILL")
        let signal_str = match signal {
            9 => "SIGKILL".to_string(),
            15 => "SIGTERM".to_string(),
            _ => return Err(anyhow::anyhow!("Unsupported signal: {}", signal)),
        };

        self.docker
            .kill_container(
                &handle.id,
                Some(KillContainerOptions { signal: signal_str }),
            )
            .await
            .context("Failed to send signal to container")?;

        Ok(())
    }

    async fn kill(&self, handle: &ProcessHandle) -> Result<()> {
        // First try to kill the container
        let _ = self
            .docker
            .kill_container(
                &handle.id,
                Some(KillContainerOptions {
                    signal: "SIGKILL".to_string(),
                }),
            )
            .await;

        // Then remove it forcefully
        self.docker
            .remove_container(
                &handle.id,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await
            .context("Failed to remove container")?;

        Ok(())
    }

    async fn alive(&self, handle: &ProcessHandle) -> Result<bool> {
        match self
            .docker
            .inspect_container(&handle.id, None::<InspectContainerOptions>)
            .await
        {
            Ok(inspect) => {
                let running = inspect.state.and_then(|s| s.running).unwrap_or(false);
                Ok(running)
            }
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => Ok(false),
            Err(e) => Err(e).context("Failed to inspect container"),
        }
    }

    async fn get_exit_status(&self, handle: &ProcessHandle) -> Result<Option<ExitStatus>> {
        match self
            .docker
            .inspect_container(&handle.id, None::<InspectContainerOptions>)
            .await
        {
            Ok(inspect) => {
                let Some(state) = inspect.state else {
                    return Ok(None);
                };

                // If container is still running, no exit status yet
                if state.status == Some(ContainerStateStatusEnum::RUNNING) {
                    return Ok(None);
                }

                let exit_code = state.exit_code;
                let oom_killed = state.oom_killed.unwrap_or(false);

                Ok(Some(ExitStatus {
                    exit_code,
                    oom_killed,
                }))
            }
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => Ok(None),
            Err(e) => Err(e).context("Failed to inspect container for exit status"),
        }
    }
}
