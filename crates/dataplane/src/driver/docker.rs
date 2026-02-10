use std::{collections::HashMap, time::Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bollard::{
    Docker,
    models::{ContainerCreateBody, ContainerStateStatusEnum, HostConfig, ResourcesUlimits},
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

use super::{
    DAEMON_GRPC_PORT,
    DAEMON_HTTP_PORT,
    ExitStatus,
    ProcessConfig,
    ProcessDriver,
    ProcessHandle,
    ProcessType,
};
use crate::daemon_binary;

/// Container path for the daemon binary.
const CONTAINER_DAEMON_PATH: &str = "/indexify-daemon";

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

    /// Get a container's IP address using bollard inspect.
    async fn get_container_ip(&self, container_name: &str) -> Result<String> {
        let inspect = self
            .docker
            .inspect_container(container_name, None::<InspectContainerOptions>)
            .await
            .context("Failed to inspect container")?;

        // Extract IP from network settings
        let networks = inspect
            .network_settings
            .and_then(|ns| ns.networks)
            .context("Container has no network settings")?;

        // Get the first network's IP address (usually "bridge" network)
        for (_network_name, endpoint) in networks {
            if let Some(ip) = endpoint.ip_address &&
                !ip.is_empty()
            {
                return Ok(ip);
            }
        }

        anyhow::bail!("Container {} has no IP address", container_name)
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
}

impl Default for DockerDriver {
    fn default() -> Self {
        Self::new().expect("Failed to create default DockerDriver")
    }
}

const CPU_PERIOD_MICROSEC: i64 = 20_000;

const SHMEM_SIZE: i64 = 1024 * 1024 * 1024; // 1 GB

const ONE_GB: i64 = 1024 * 1024 * 1024;
const ONE_MILLION: i64 = 1_000_000;

fn build_ulimits() -> Vec<ResourcesUlimits> {
    vec![
        // Core files are useless in ephemeral container filesystem.
        ResourcesUlimits {
            name: Some("core".to_string()),
            soft: Some(0),
            hard: Some(0),
        },
        ResourcesUlimits {
            name: Some("memlock".to_string()),
            soft: Some(ONE_GB),
            hard: Some(ONE_GB),
        },
        ResourcesUlimits {
            name: Some("stack".to_string()),
            soft: Some(ONE_GB),
            hard: Some(ONE_GB),
        },
        ResourcesUlimits {
            name: Some("msgqueue".to_string()),
            soft: Some(ONE_GB),
            hard: Some(ONE_GB),
        },
        ResourcesUlimits {
            name: Some("nofile".to_string()),
            soft: Some(ONE_MILLION),
            hard: Some(ONE_MILLION),
        },
    ]
}

fn build_host_config_resources(resources: &Option<super::ResourceLimits>) -> HostConfig {
    let Some(resources) = resources else {
        return HostConfig {
            shm_size: Some(SHMEM_SIZE),
            ulimits: Some(build_ulimits()),
            ..Default::default()
        };
    };

    let memory = resources.memory_bytes.map(|v| v as i64);

    let (cpu_period, cpu_quota) = if let Some(cpu_millicores) = resources.cpu_millicores {
        let cpu_fraction = cpu_millicores as f64 / 1000.0;
        let quota = (cpu_fraction * CPU_PERIOD_MICROSEC as f64).ceil() as i64;
        (Some(CPU_PERIOD_MICROSEC), Some(quota))
    } else {
        (None, None)
    };

    HostConfig {
        memory,
        cpu_period,
        cpu_quota,
        shm_size: Some(SHMEM_SIZE),
        ulimits: Some(build_ulimits()),
        ..Default::default()
    }
}

/// Internal specification for creating a Docker container.
struct ContainerSpec {
    container_name: String,
    image: String,
    entrypoint: Option<Vec<String>>,
    cmd: Vec<String>,
    env: Vec<String>,
    labels: HashMap<String, String>,
    working_dir: Option<String>,
    host_config: HostConfig,
}

impl DockerDriver {
    /// Create and start a Docker container from the given spec.
    /// Returns `(container_name, container_ip)`.
    async fn create_and_start_container(&self, spec: ContainerSpec) -> Result<(String, String)> {
        let container_config = ContainerCreateBody {
            image: Some(spec.image),
            entrypoint: spec.entrypoint,
            cmd: Some(spec.cmd),
            env: Some(spec.env),
            labels: Some(spec.labels),
            working_dir: spec.working_dir,
            host_config: Some(spec.host_config),
            ..Default::default()
        };

        let create_options = CreateContainerOptions {
            name: Some(spec.container_name.clone()),
            platform: String::new(),
        };

        self.docker
            .create_container(Some(create_options), container_config)
            .await
            .context("Failed to create container")?;

        self.docker
            .start_container(&spec.container_name, None::<StartContainerOptions>)
            .await
            .context("Failed to start container")?;

        let container_ip = self
            .get_container_ip(&spec.container_name)
            .await
            .context("Failed to get container IP address")?;

        Ok((spec.container_name, container_ip))
    }

    /// Build a ContainerSpec for a function executor container.
    fn build_function_spec(&self, config: &ProcessConfig, image: &str) -> ContainerSpec {
        let container_name = format!("indexify-{}", config.id);
        let fe_grpc_port: u16 = 9600;

        let env: Vec<String> = config
            .env
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();
        let labels: HashMap<String, String> = config.labels.iter().cloned().collect();

        let mut cmd: Vec<String> = config.args.clone();
        cmd.push("--address".to_string());
        cmd.push(format!("0.0.0.0:{}", fe_grpc_port));

        let entrypoint = if config.command.is_empty() {
            None
        } else {
            Some(vec![config.command.clone()])
        };

        ContainerSpec {
            container_name,
            image: image.to_string(),
            entrypoint,
            cmd,
            env,
            labels,
            working_dir: config.working_dir.clone(),
            host_config: build_host_config_resources(&config.resources),
        }
    }

    /// Build a ContainerSpec for a sandbox container with daemon injection.
    fn build_sandbox_spec(&self, config: &ProcessConfig, image: &str) -> Result<ContainerSpec> {
        let container_name = format!("indexify-{}", config.id);

        let daemon_binary_path =
            daemon_binary::get_daemon_path().context("Daemon binary not available")?;

        let binds = vec![format!(
            "{}:{}:ro",
            daemon_binary_path.display(),
            CONTAINER_DAEMON_PATH
        )];

        let env: Vec<String> = config
            .env
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();
        let labels: HashMap<String, String> = config.labels.iter().cloned().collect();

        let mut cmd: Vec<String> = vec![
            "--port".to_string(),
            DAEMON_GRPC_PORT.to_string(),
            "--http-port".to_string(),
            DAEMON_HTTP_PORT.to_string(),
            "--log-dir".to_string(),
            "/var/log/indexify".to_string(),
        ];

        if !config.command.is_empty() {
            cmd.push("--".to_string());
            cmd.push(config.command.clone());
            cmd.extend(config.args.clone());
        }

        let mut host_config = build_host_config_resources(&config.resources);
        host_config.binds = Some(binds);

        Ok(ContainerSpec {
            container_name,
            image: image.to_string(),
            entrypoint: Some(vec![CONTAINER_DAEMON_PATH.to_string()]),
            cmd,
            env,
            labels,
            working_dir: config.working_dir.clone(),
            host_config,
        })
    }
}

#[async_trait]
impl ProcessDriver for DockerDriver {
    async fn start(&self, config: ProcessConfig) -> Result<ProcessHandle> {
        let image = config
            .image
            .as_ref()
            .context("Docker driver requires an image")?;

        self.ensure_image(image).await?;

        let (spec, grpc_port, http_port): (ContainerSpec, u16, Option<u16>) =
            match config.process_type {
                ProcessType::Function => {
                    let spec = self.build_function_spec(&config, image);
                    info!(
                        container = %spec.container_name,
                        image = %image,
                        grpc_port = 9600u16,
                        "Starting function-executor container"
                    );
                    (spec, 9600, None)
                }
                ProcessType::Sandbox => {
                    let spec = self.build_sandbox_spec(&config, image)?;
                    info!(
                        container = %spec.container_name,
                        grpc_port = DAEMON_GRPC_PORT,
                        http_port = DAEMON_HTTP_PORT,
                        "Starting container with daemon injection"
                    );
                    (spec, DAEMON_GRPC_PORT, Some(DAEMON_HTTP_PORT))
                }
            };

        let (container_name, container_ip) = self.create_and_start_container(spec).await?;

        let daemon_addr = format!("{}:{}", container_ip, grpc_port);
        let http_addr = http_port.map(|p| format!("{}:{}", container_ip, p));

        info!(
            container = %container_name,
            container_ip = %container_ip,
            daemon_addr = %daemon_addr,
            http_addr = ?http_addr,
            "Container started"
        );

        Ok(ProcessHandle {
            id: container_name,
            daemon_addr: Some(daemon_addr),
            http_addr,
            container_ip,
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

    async fn list_containers(&self) -> Result<Vec<String>> {
        use std::collections::HashMap;

        use bollard::query_parameters::ListContainersOptions;

        // Filter by label instead of name prefix for more reliable reconciliation
        let mut filters: HashMap<String, Vec<String>> = HashMap::new();
        filters.insert(
            "label".to_string(),
            vec!["indexify.managed=true".to_string()],
        );

        let options = ListContainersOptions {
            all: true,
            filters: Some(filters),
            ..Default::default()
        };

        let containers = self
            .docker
            .list_containers(Some(options))
            .await
            .context("Failed to list containers")?;

        let names: Vec<String> = containers
            .into_iter()
            .filter_map(|c| {
                c.names
                    .and_then(|names| names.first().cloned())
                    .map(|name| name.trim_start_matches('/').to_string())
            })
            .collect();

        Ok(names)
    }
}
