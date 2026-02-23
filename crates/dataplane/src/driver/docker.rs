use std::{collections::HashMap, path::PathBuf, time::Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bollard::{
    Docker,
    auth::DockerCredentials,
    models::{
        ContainerCreateBody, ContainerStateStatusEnum, DeviceRequest, HostConfig,
        HostConfigLogConfig, ResourcesUlimits,
    },
    query_parameters::{
        CreateContainerOptions, CreateImageOptions, InspectContainerOptions, KillContainerOptions,
        RemoveContainerOptions, StartContainerOptions, StopContainerOptionsBuilder,
    },
};
use futures_util::StreamExt;
use tracing::info;

use super::{
    DAEMON_GRPC_PORT, DAEMON_HTTP_PORT, ExitStatus, ProcessConfig, ProcessDriver, ProcessHandle,
    ProcessType,
};
use crate::daemon_binary;

/// Container path for the daemon binary.
const CONTAINER_DAEMON_PATH: &str = "/indexify-daemon";

/// Sentinel error for image-related failures (missing image, pull failure, bad
/// name). Used to classify startup failures as non-retriable.
#[derive(Debug)]
pub struct ImageError(pub String);

impl std::fmt::Display for ImageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Image error: {}", self.0)
    }
}

impl std::error::Error for ImageError {}

pub struct DockerDriver {
    docker: Docker,
    /// OCI runtime to use for containers (e.g., "runsc" for gVisor).
    runtime: Option<String>,
    /// Docker network mode for containers.
    network: Option<String>,
    /// Volume bind mounts for function executor containers.
    binds: Vec<String>,
}

impl DockerDriver {
    /// Create a new DockerDriver connecting to the default Docker socket.
    pub fn new(
        runtime: Option<String>,
        network: Option<String>,
        binds: Vec<String>,
    ) -> Result<Self> {
        let docker =
            Docker::connect_with_local_defaults().context("Failed to connect to Docker daemon")?;
        Ok(Self {
            docker,
            runtime,
            network,
            binds,
        })
    }

    /// Create a DockerDriver connecting to a specific Docker address.
    ///
    /// Supported address formats:
    /// - Unix socket: `unix:///var/run/docker.sock` or `/var/run/docker.sock`
    /// - HTTP: `http://localhost:2375` or `tcp://localhost:2375`
    /// - HTTPS: `https://localhost:2376` (requires TLS setup)
    pub fn with_address(
        address: &str,
        runtime: Option<String>,
        network: Option<String>,
        binds: Vec<String>,
    ) -> Result<Self> {
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
        Ok(Self {
            docker,
            runtime,
            network,
            binds,
        })
    }

    /// Get a reference to the underlying Docker client.
    /// Used by DockerSnapshotter for export/import operations.
    pub fn docker_client(&self) -> &Docker {
        &self.docker
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
            if let Some(ip) = endpoint.ip_address
                && !ip.is_empty()
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

        let auth = get_docker_credentials(image);
        let mut stream = self.docker.create_image(Some(options), None, auth);

        while let Some(result) = stream.next().await {
            match result {
                Ok(info) => {
                    if let Some(status) = info.status {
                        tracing::debug!(image = %image, status = %status, "Pull progress");
                    }
                }
                Err(e) => {
                    let duration_ms = start.elapsed().as_millis();
                    // Distinguish image-not-found (not retriable) from other
                    // pull errors like network/auth failures (retriable as
                    // internal error).
                    let is_not_found = matches!(
                        &e,
                        bollard::errors::Error::DockerResponseServerError {
                            status_code: 404,
                            ..
                        }
                    );
                    tracing::error!(
                        image = %image,
                        duration_ms = %duration_ms,
                        error = ?e,
                        is_not_found = is_not_found,
                        event = "image_pull_failed",
                        "Failed to pull Docker image"
                    );
                    if is_not_found {
                        return Err(ImageError(format!("Image not found {}: {}", image, e)).into());
                    }
                    return Err(anyhow::anyhow!("Failed to pull image {}: {}", image, e));
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

// ---------------------------------------------------------------------------
// Docker registry credential resolution
// ---------------------------------------------------------------------------

/// Parsed representation of `~/.docker/config.json`.
#[derive(Default)]
struct DockerConfig {
    /// Default credential store (e.g. "ecr-login", "desktop").
    creds_store: Option<String>,
    /// Per-registry credential helpers.
    cred_helpers: HashMap<String, String>,
}

/// Return the path to the Docker config file, respecting `$DOCKER_CONFIG`.
fn docker_config_path() -> Option<PathBuf> {
    if let Ok(dir) = std::env::var("DOCKER_CONFIG") {
        return Some(PathBuf::from(dir).join("config.json"));
    }
    let home = std::env::var("HOME").ok()?;
    Some(PathBuf::from(home).join(".docker").join("config.json"))
}

/// Read and parse the Docker config file.
fn read_docker_config() -> Option<DockerConfig> {
    let path = docker_config_path()?;
    let data = std::fs::read_to_string(&path).ok()?;
    let json: serde_json::Value = serde_json::from_str(&data).ok()?;

    let creds_store = json
        .get("credsStore")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let cred_helpers = json
        .get("credHelpers")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    Some(DockerConfig {
        creds_store,
        cred_helpers,
    })
}

/// Extract the registry hostname from a Docker image reference.
fn extract_registry(image: &str) -> Option<String> {
    // Strip tag or digest
    let without_tag = image.split('@').next().unwrap_or(image);
    let without_tag = without_tag.split(':').next().unwrap_or(without_tag);

    // The first path component is the registry if it contains a '.' or ':'
    let first_component = without_tag.split('/').next()?;
    if first_component.contains('.') || first_component.contains(':') {
        Some(first_component.to_string())
    } else {
        None
    }
}

/// Invoke `docker-credential-<helper> get` with the server URL on stdin and
/// parse the JSON response.
fn call_credential_helper(helper: &str, registry: &str) -> Option<DockerCredentials> {
    let binary = format!("docker-credential-{}", helper);
    // Credential helpers expect the full URL with scheme for most registries.
    let server_url = if registry.starts_with("https://") || registry.starts_with("http://") {
        registry.to_string()
    } else {
        format!("https://{}", registry)
    };

    let output = std::process::Command::new(&binary)
        .arg("get")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            if let Some(ref mut stdin) = child.stdin {
                stdin.write_all(server_url.as_bytes())?;
            }
            child.wait_with_output()
        });

    let output = match output {
        Ok(o) if o.status.success() => o,
        Ok(o) => {
            tracing::error!(
                helper = %binary,
                registry = %registry,
                stderr = %String::from_utf8_lossy(&o.stderr),
                "Credential helper returned non-zero exit code"
            );
            return None;
        }
        Err(e) => {
            tracing::error!(
                helper = %binary,
                registry = %registry,
                error = %e,
                "Failed to run credential helper"
            );
            return None;
        }
    };

    let json: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    let username = json
        .get("Username")
        .and_then(|v| v.as_str())
        .map(String::from);
    let secret = json
        .get("Secret")
        .and_then(|v| v.as_str())
        .map(String::from);

    // Some helpers return a token instead of username/password
    if username.as_deref() == Some("<token>") {
        return Some(DockerCredentials {
            identitytoken: secret,
            ..Default::default()
        });
    }

    Some(DockerCredentials {
        username,
        password: secret,
        ..Default::default()
    })
}

/// Resolve Docker credentials for the given image reference by reading
/// `~/.docker/config.json` and invoking credential helpers as needed.
fn get_docker_credentials(image: &str) -> Option<DockerCredentials> {
    let registry = extract_registry(image)?;
    let config = read_docker_config()?;

    // 1. Check credHelpers for a registry-specific helper
    if let Some(helper) = config.cred_helpers.get(&registry) {
        return call_credential_helper(helper, &registry);
    }

    // 2. Fall back to the default credsStore
    if let Some(ref store) = config.creds_store {
        return call_credential_helper(store, &registry);
    }

    None
}

impl Default for DockerDriver {
    fn default() -> Self {
        Self::new(None, None, Vec::new()).expect("Failed to create default DockerDriver")
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

fn build_log_config() -> HostConfigLogConfig {
    let mut config = HashMap::new();
    config.insert("max-size".to_string(), "10m".to_string());
    config.insert("max-file".to_string(), "3".to_string());
    config.insert("compress".to_string(), "true".to_string());
    config.insert("mode".to_string(), "non-blocking".to_string());
    HostConfigLogConfig {
        typ: Some("local".to_string()),
        config: Some(config),
    }
}

fn build_device_requests(device_ids: &[String]) -> Vec<DeviceRequest> {
    vec![DeviceRequest {
        driver: Some("nvidia".to_string()),
        device_ids: Some(device_ids.to_vec()),
        capabilities: Some(vec![vec!["gpu".to_string()]]),
        ..Default::default()
    }]
}

fn build_host_config_resources(resources: &Option<super::ResourceLimits>) -> HostConfig {
    let log_config = Some(build_log_config());

    let Some(resources) = resources else {
        return HostConfig {
            shm_size: Some(SHMEM_SIZE),
            ulimits: Some(build_ulimits()),
            log_config,
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

    let device_requests = resources
        .gpu_device_ids
        .as_ref()
        .filter(|ids| !ids.is_empty())
        .map(|ids| build_device_requests(ids));

    // memory_swap == memory means zero swap (Docker's memory_swap is RAM+swap
    // total).
    let memory_swap = memory;

    HostConfig {
        memory,
        memory_swap,
        cpu_period,
        cpu_quota,
        shm_size: Some(SHMEM_SIZE),
        ulimits: Some(build_ulimits()),
        log_config,
        device_requests,
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
            .with_context(|| format!("Failed to create container {}", spec.container_name))?;

        if let Err(e) = self
            .docker
            .start_container(&spec.container_name, None::<StartContainerOptions>)
            .await
        {
            // Container was created but failed to start — try to get logs
            let logs = self
                .get_container_logs_by_name(&spec.container_name, 50)
                .await;
            let log_context = if logs.is_empty() {
                String::new()
            } else {
                format!("\nContainer logs:\n{logs}")
            };

            // Clean up the failed container
            let _ = self
                .docker
                .remove_container(
                    &spec.container_name,
                    Some(RemoveContainerOptions {
                        force: true,
                        ..Default::default()
                    }),
                )
                .await;

            return Err(e).with_context(|| {
                format!(
                    "Failed to start container {}{}",
                    spec.container_name, log_context
                )
            });
        }

        let container_ip = self
            .get_container_ip(&spec.container_name)
            .await
            .context("Failed to get container IP address")?;

        Ok((spec.container_name, container_ip))
    }

    /// Build a base HostConfig with resource limits and driver-level settings
    /// (runtime, network, log rotation, binds).
    fn build_host_config(&self, resources: &Option<super::ResourceLimits>) -> HostConfig {
        let mut host_config = build_host_config_resources(resources);
        host_config.runtime = self.runtime.clone();
        host_config.network_mode = self.network.clone();
        if !self.binds.is_empty() {
            let existing = host_config.binds.get_or_insert_with(Vec::new);
            existing.extend(self.binds.clone());
        }
        host_config
    }

    /// Get container logs by name (for error diagnostics when we don't have a
    /// ProcessHandle yet).
    async fn get_container_logs_by_name(&self, container_name: &str, tail: u32) -> String {
        use bollard::query_parameters::LogsOptions;

        let options = LogsOptions {
            stdout: true,
            stderr: true,
            tail: tail.to_string(),
            ..Default::default()
        };

        let mut stream = self.docker.logs(container_name, Some(options));
        let mut output = String::new();
        const MAX_LOG_BYTES: usize = 4096;

        while let Some(result) = stream.next().await {
            match result {
                Ok(log_output) => {
                    let line = log_output.to_string();
                    if output.len() + line.len() > MAX_LOG_BYTES {
                        output.push_str(&line[..MAX_LOG_BYTES.saturating_sub(output.len())]);
                        output.push_str("\n... (truncated)");
                        break;
                    }
                    output.push_str(&line);
                }
                _ => break,
            }
        }

        output
    }

    /// Build a ContainerSpec for a function executor container.
    fn build_function_spec(&self, config: &ProcessConfig, image: &str) -> ContainerSpec {
        let fe_grpc_port: u16 = 9600;

        let mut cmd: Vec<String> = config.args.clone();
        cmd.push("--address".to_string());
        cmd.push(format!("0.0.0.0:{}", fe_grpc_port));

        let entrypoint = if config.command.is_empty() {
            None
        } else {
            Some(vec![config.command.clone()])
        };

        ContainerSpec {
            container_name: format!("indexify-function-{}", config.id),
            image: image.to_string(),
            entrypoint,
            cmd,
            env: format_env(&config.env),
            labels: config.labels.iter().cloned().collect(),
            working_dir: config.working_dir.clone(),
            host_config: self.build_host_config(&config.resources),
        }
    }

    /// Build a ContainerSpec for a sandbox container with daemon injection.
    fn build_sandbox_spec(&self, config: &ProcessConfig, image: &str) -> Result<ContainerSpec> {
        let daemon_binary_path =
            daemon_binary::get_daemon_path().context("Daemon binary not available")?;

        let mut host_config = self.build_host_config(&config.resources);
        host_config.binds = Some(vec![format!(
            "{}:{}:ro",
            daemon_binary_path.display(),
            CONTAINER_DAEMON_PATH
        )]);

        // When restoring a gVisor snapshot, apply the rootfs overlay via the
        // `dev.gvisor.tar.rootfs.upper` annotation. gVisor reads this at
        // container start and overlays the tar contents on top of the base image.
        if let Some(ref overlay_path) = config.rootfs_overlay {
            let mut annotations = HashMap::new();
            annotations.insert(
                "dev.gvisor.tar.rootfs.upper".to_string(),
                overlay_path.clone(),
            );
            host_config.annotations = Some(annotations);
        }

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

        Ok(ContainerSpec {
            container_name: format!("indexify-sandbox-{}", config.id),
            image: image.to_string(),
            entrypoint: Some(vec![CONTAINER_DAEMON_PATH.to_string()]),
            cmd,
            env: format_env(&config.env),
            labels: config.labels.iter().cloned().collect(),
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
            .ok_or_else(|| ImageError("Docker driver requires an image".to_string()))?;

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

    async fn stop(&self, handle: &ProcessHandle, timeout_secs: u64) -> Result<()> {
        let options = StopContainerOptionsBuilder::default()
            .t(timeout_secs as i32)
            .build();
        self.docker
            .stop_container(&handle.id, Some(options))
            .await
            .context("Failed to stop container")?;
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

    async fn get_logs(&self, handle: &ProcessHandle, tail: u32) -> Result<String> {
        use bollard::query_parameters::LogsOptions;

        let options = LogsOptions {
            stdout: true,
            stderr: true,
            tail: tail.to_string(),
            ..Default::default()
        };

        let mut stream = self.docker.logs(&handle.id, Some(options));
        let mut output = String::new();
        const MAX_LOG_BYTES: usize = 4096;

        while let Some(result) = stream.next().await {
            match result {
                Ok(log_output) => {
                    let line = log_output.to_string();
                    if output.len() + line.len() > MAX_LOG_BYTES {
                        output.push_str(&line[..MAX_LOG_BYTES.saturating_sub(output.len())]);
                        output.push_str("\n... (truncated)");
                        break;
                    }
                    output.push_str(&line);
                }
                Err(bollard::errors::Error::DockerResponseServerError {
                    status_code: 404, ..
                }) => break,
                Err(e) => return Err(e).context("Failed to fetch container logs"),
            }
        }

        Ok(output)
    }
}

fn format_env(env: &[(String, String)]) -> Vec<String> {
    env.iter().map(|(k, v)| format!("{}={}", k, v)).collect()
}
