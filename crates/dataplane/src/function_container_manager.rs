use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use proto_api::executor_api_pb::{
    FunctionExecutorDescription,
    FunctionExecutorState,
    FunctionExecutorStatus,
    FunctionExecutorTerminationReason,
    FunctionExecutorType,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{
    daemon_client::DaemonClient,
    driver::{ProcessConfig, ProcessDriver, ProcessHandle},
};

const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);
const KILL_GRACE_PERIOD: Duration = Duration::from_secs(10);
const DAEMON_READY_TIMEOUT: Duration = Duration::from_secs(60);

/// Helper struct for structured logging of function executor info.
struct FunctionInfo<'a> {
    fe_id: &'a str,
    namespace: &'a str,
    app: &'a str,
    fn_name: &'a str,
    app_version: &'a str,
}

impl<'a> FunctionInfo<'a> {
    fn from_description(desc: &'a FunctionExecutorDescription) -> Self {
        let fe_id = desc.id.as_deref().unwrap_or("");
        let (namespace, app, fn_name, app_version) = desc
            .function
            .as_ref()
            .map(|f| {
                (
                    f.namespace.as_deref().unwrap_or(""),
                    f.application_name.as_deref().unwrap_or(""),
                    f.function_name.as_deref().unwrap_or(""),
                    f.application_version.as_deref().unwrap_or(""),
                )
            })
            .unwrap_or(("", "", "", ""));

        Self {
            fe_id,
            namespace,
            app,
            fn_name,
            app_version,
        }
    }
}

/// Internal state of a managed container.
enum ContainerState {
    /// Container is starting up (daemon not yet ready).
    Pending,
    /// Container is running with daemon connected.
    Running {
        handle: ProcessHandle,
        daemon_client: DaemonClient,
    },
    /// Container was signaled to stop, waiting for graceful shutdown.
    Stopping {
        handle: ProcessHandle,
        #[allow(dead_code)] // Reserved for future graceful shutdown via daemon
        daemon_client: Option<DaemonClient>,
    },
    /// Container has terminated.
    Terminated {
        reason: FunctionExecutorTerminationReason,
    },
}

/// A managed function executor container.
struct ManagedContainer {
    description: FunctionExecutorDescription,
    state: ContainerState,
    /// When the container started running (set when state becomes Running)
    started_at: Option<std::time::Instant>,
    /// HTTP address of the daemon (for sandbox containers).
    /// Set when the container becomes Running.
    daemon_http_address: Option<String>,
}

impl ManagedContainer {
    fn to_proto_state(&self) -> FunctionExecutorState {
        let (status, termination_reason) = match &self.state {
            ContainerState::Pending => (FunctionExecutorStatus::Pending, None),
            ContainerState::Running { .. } => (FunctionExecutorStatus::Running, None),
            ContainerState::Stopping { .. } => (FunctionExecutorStatus::Running, None),
            ContainerState::Terminated { reason } => {
                (FunctionExecutorStatus::Terminated, Some(*reason))
            }
        };

        FunctionExecutorState {
            description: Some(self.description.clone()),
            status: Some(status.into()),
            termination_reason: termination_reason.map(|r| r.into()),
            allocation_ids_caused_termination: vec![],
            daemon_http_address: self.daemon_http_address.clone(),
            container_type: self.description.container_type,
        }
    }

    fn info(&self) -> FunctionInfo<'_> {
        FunctionInfo::from_description(&self.description)
    }

    /// Check if this sandbox container has exceeded its timeout.
    /// Returns true if the container should be terminated due to timeout.
    fn is_timed_out(&self) -> bool {
        // Only check timeout for running containers with a timeout configured
        let timeout_secs = self.description.sandbox_timeout_secs.unwrap_or(0);
        if timeout_secs == 0 {
            return false; // No timeout configured
        }

        if !matches!(self.state, ContainerState::Running { .. }) {
            return false; // Not running, can't timeout
        }

        if let Some(started_at) = self.started_at {
            let elapsed = started_at.elapsed();
            elapsed.as_secs() >= timeout_secs
        } else {
            false
        }
    }
}

/// Resolves container images for function executors.
pub trait ImageResolver: Send + Sync {
    fn resolve_image(&self, description: &FunctionExecutorDescription) -> String;
}

/// Default image resolver that uses the image from FunctionExecutorDescription
/// if present, otherwise falls back to a default image.
pub struct DefaultImageResolver;

impl ImageResolver for DefaultImageResolver {
    fn resolve_image(&self, description: &FunctionExecutorDescription) -> String {
        // Use image from description if provided (e.g., for sandboxes)
        if let Some(ref image) = description.image {
            return image.clone();
        }
        // Fall back to default Python image for functions
        "python:3.11-slim".to_string()
    }
}

/// Manages function executor containers.
pub struct FunctionContainerManager {
    driver: Arc<dyn ProcessDriver>,
    image_resolver: Arc<dyn ImageResolver>,
    containers: Arc<Mutex<HashMap<String, ManagedContainer>>>,
}

impl FunctionContainerManager {
    pub fn new(driver: Arc<dyn ProcessDriver>, image_resolver: Arc<dyn ImageResolver>) -> Self {
        Self {
            driver,
            image_resolver,
            containers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Sync the containers with the desired state from the server.
    /// Creates new containers, and marks removed ones for termination.
    pub async fn sync(&self, desired: Vec<FunctionExecutorDescription>) {
        let desired_ids: HashSet<String> = desired.iter().filter_map(|d| d.id.clone()).collect();

        let mut containers = self.containers.lock().await;

        // Check for sandbox timeouts and stop expired containers
        let timed_out_ids: Vec<String> = containers
            .iter()
            .filter(|(_, container)| container.is_timed_out())
            .map(|(id, _)| id.clone())
            .collect();

        for id in timed_out_ids {
            if let Some(container) = containers.get_mut(&id) {
                let info = container.info();
                let timeout_secs = container.description.sandbox_timeout_secs.unwrap_or(0);
                tracing::warn!(
                    fe_id = %info.fe_id,
                    namespace = %info.namespace,
                    app = %info.app,
                    fn_name = %info.fn_name,
                    timeout_secs = timeout_secs,
                    "Sandbox container timed out, terminating"
                );
                self.initiate_stop(container).await;
            }
        }

        // Find containers to remove (not in desired state)
        let current_ids: Vec<String> = containers.keys().cloned().collect();
        for id in current_ids {
            if !desired_ids.contains(&id) &&
                let Some(container) = containers.get_mut(&id)
            {
                let info = container.info();
                match &container.state {
                    ContainerState::Terminated { .. } => {
                        // Server no longer wants this FE, remove from memory
                        tracing::info!(
                            fe_id = %info.fe_id,
                            namespace = %info.namespace,
                            app = %info.app,
                            fn_name = %info.fn_name,
                            app_version = %info.app_version,
                            "Removed terminated container from memory"
                        );
                        containers.remove(&id);
                    }
                    ContainerState::Stopping { .. } => {
                        // Already stopping, let it continue
                    }
                    ContainerState::Pending | ContainerState::Running { .. } => {
                        // Need to stop this container
                        self.initiate_stop(container).await;
                    }
                }
            }
        }

        // Create new containers
        for desc in desired {
            let id = match &desc.id {
                Some(id) => id.clone(),
                None => continue,
            };

            if !containers.contains_key(&id) {
                let info = FunctionInfo::from_description(&desc);
                tracing::info!(
                    fe_id = %info.fe_id,
                    namespace = %info.namespace,
                    app = %info.app,
                    fn_name = %info.fn_name,
                    app_version = %info.app_version,
                    "Creating new container"
                );

                let container = ManagedContainer {
                    description: desc.clone(),
                    state: ContainerState::Pending,
                    started_at: None,
                    daemon_http_address: None,
                };
                containers.insert(id.clone(), container);

                // Spawn container creation with daemon integration
                let driver = self.driver.clone();
                let image_resolver = self.image_resolver.clone();
                let containers_ref = self.containers.clone();
                let desc_clone = desc.clone();

                tokio::spawn(async move {
                    let result =
                        start_container_with_daemon(&driver, &image_resolver, &desc_clone).await;

                    let mut containers = containers_ref.lock().await;
                    if let Some(container) = containers.get_mut(&id) {
                        match result {
                            Ok((handle, daemon_client)) => {
                                let info = container.info();
                                tracing::info!(
                                    fe_id = %info.fe_id,
                                    namespace = %info.namespace,
                                    app = %info.app,
                                    fn_name = %info.fn_name,
                                    app_version = %info.app_version,
                                    container_id = %handle.id,
                                    http_addr = ?handle.http_addr,
                                    "Container started with daemon"
                                );
                                container.daemon_http_address = handle.http_addr.clone();
                                container.state = ContainerState::Running {
                                    handle,
                                    daemon_client,
                                };
                                container.started_at = Some(std::time::Instant::now());
                            }
                            Err(e) => {
                                let info = container.info();
                                tracing::error!(
                                    fe_id = %info.fe_id,
                                    namespace = %info.namespace,
                                    app = %info.app,
                                    fn_name = %info.fn_name,
                                    app_version = %info.app_version,
                                    error = %e,
                                    "Failed to start container"
                                );
                                container.state = ContainerState::Terminated {
                                    reason:
                                        FunctionExecutorTerminationReason::StartupFailedInternalError,
                                };
                            }
                        }
                    }
                });
            }
        }
    }

    /// Initiate stopping a container (signal first, then kill after grace
    /// period).
    async fn initiate_stop(&self, container: &mut ManagedContainer) {
        let id = container.description.id.clone().unwrap_or_default();

        // Extract what we need from the current state
        let (handle, daemon_client) = match &container.state {
            ContainerState::Running {
                handle,
                daemon_client,
            } => (handle.clone(), Some(daemon_client.clone())),
            ContainerState::Pending => {
                container.state = ContainerState::Terminated {
                    reason: FunctionExecutorTerminationReason::Unknown,
                };
                return;
            }
            _ => return,
        };

        let info = container.info();
        tracing::info!(
            fe_id = %info.fe_id,
            namespace = %info.namespace,
            app = %info.app,
            fn_name = %info.fn_name,
            app_version = %info.app_version,
            "Signaling container to stop via daemon"
        );

        // Try to signal via daemon first (SIGTERM to the function executor)
        if let Some(mut client) = daemon_client.clone() {
            if let Err(e) = client.send_signal(15).await {
                tracing::warn!(
                    fe_id = %info.fe_id,
                    error = %e,
                    "Failed to send signal via daemon, falling back to container signal"
                );
                // Fall back to docker signal
                if let Err(e) = self.driver.send_sig(&handle, 15).await {
                    tracing::warn!(
                        fe_id = %info.fe_id,
                        error = %e,
                        "Failed to send signal to container"
                    );
                }
            }
        } else if let Err(e) = self.driver.send_sig(&handle, 15).await {
            tracing::warn!(
                fe_id = %info.fe_id,
                error = %e,
                "Failed to send signal to container"
            );
        }

        // Move to stopping state
        container.state = ContainerState::Stopping {
            handle: handle.clone(),
            daemon_client,
        };

        // Schedule kill after grace period
        let driver = self.driver.clone();
        let containers_ref = self.containers.clone();
        let container_id = id.clone();

        tokio::spawn(async move {
            tokio::time::sleep(KILL_GRACE_PERIOD).await;

            let mut containers = containers_ref.lock().await;
            if let Some(container) = containers.get_mut(&container_id) &&
                let ContainerState::Stopping { handle, .. } = &container.state
            {
                let handle = handle.clone();
                let info = container.info();
                tracing::info!(
                    fe_id = %info.fe_id,
                    namespace = %info.namespace,
                    app = %info.app,
                    fn_name = %info.fn_name,
                    app_version = %info.app_version,
                    "Killing container after grace period"
                );
                if let Err(e) = driver.kill(&handle).await {
                    tracing::warn!(
                        fe_id = %info.fe_id,
                        namespace = %info.namespace,
                        app = %info.app,
                        fn_name = %info.fn_name,
                        app_version = %info.app_version,
                        error = %e,
                        "Failed to kill container"
                    );
                }
                container.state = ContainerState::Terminated {
                    reason: FunctionExecutorTerminationReason::Unknown,
                };
            }
        });
    }

    /// Run the health check loop. Call this from a spawned task.
    pub async fn run_health_checks(&self, cancel_token: CancellationToken) {
        let mut interval = tokio::time::interval(HEALTH_CHECK_INTERVAL);

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!("Health check loop cancelled");
                    return;
                }
                _ = interval.tick() => {
                    self.check_timeouts().await;
                    self.check_all_containers().await;
                }
            }
        }
    }

    /// Check for sandbox containers that have exceeded their timeout.
    async fn check_timeouts(&self) {
        let mut containers = self.containers.lock().await;

        // Find timed out containers
        let timed_out_ids: Vec<String> = containers
            .iter()
            .filter(|(_, container)| container.is_timed_out())
            .map(|(id, _)| id.clone())
            .collect();

        // Initiate stop for timed out containers
        for id in timed_out_ids {
            if let Some(container) = containers.get_mut(&id) {
                let info = container.info();
                let timeout_secs = container.description.sandbox_timeout_secs.unwrap_or(0);
                let elapsed = container
                    .started_at
                    .map(|s| s.elapsed().as_secs())
                    .unwrap_or(0);
                tracing::warn!(
                    fe_id = %info.fe_id,
                    namespace = %info.namespace,
                    app = %info.app,
                    fn_name = %info.fn_name,
                    timeout_secs = timeout_secs,
                    elapsed_secs = elapsed,
                    "Sandbox container timed out, terminating"
                );
                self.initiate_stop(container).await;
            }
        }
    }

    /// Check all running containers to see if they're still alive.
    async fn check_all_containers(&self) {
        let mut containers = self.containers.lock().await;
        let ids: Vec<String> = containers.keys().cloned().collect();

        for id in ids {
            if let Some(container) = containers.get_mut(&id) {
                // Extract needed data from state first
                let check_result = match &container.state {
                    ContainerState::Running {
                        handle,
                        daemon_client,
                    } => {
                        let handle = handle.clone();
                        let client = daemon_client.clone();
                        Some((handle, Some(client), false))
                    }
                    ContainerState::Stopping { handle, .. } => {
                        let handle = handle.clone();
                        Some((handle, None, true))
                    }
                    _ => None,
                };

                if let Some((handle, daemon_client, is_stopping)) = check_result {
                    let info = container.info();

                    if is_stopping {
                        // Just check if container is still alive
                        if let Ok(false) = self.driver.alive(&handle).await {
                            tracing::info!(
                                fe_id = %info.fe_id,
                                namespace = %info.namespace,
                                app = %info.app,
                                fn_name = %info.fn_name,
                                app_version = %info.app_version,
                                "Container stopped"
                            );
                            container.state = ContainerState::Terminated {
                                reason: FunctionExecutorTerminationReason::Unknown,
                            };
                        }
                    } else {
                        // Running state - check container and daemon health
                        match self.driver.alive(&handle).await {
                            Ok(true) => {
                                // Container is alive, check daemon health
                                // Note: We check daemon health, not individual process status.
                                // User-started processes (via HTTP API) may complete independently
                                // without affecting the container's lifecycle.
                                if let Some(mut client) = daemon_client {
                                    match client.health().await {
                                        Ok(healthy) => {
                                            if !healthy {
                                                tracing::info!(
                                                    fe_id = %info.fe_id,
                                                    namespace = %info.namespace,
                                                    app = %info.app,
                                                    fn_name = %info.fn_name,
                                                    app_version = %info.app_version,
                                                    "Daemon is unhealthy, terminating container"
                                                );
                                                let _ = self.driver.kill(&handle).await;
                                                container.state = ContainerState::Terminated {
                                                    reason:
                                                        FunctionExecutorTerminationReason::Unknown,
                                                };
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                fe_id = %info.fe_id,
                                                error = %e,
                                                "Failed to check daemon health"
                                            );
                                        }
                                    }
                                }
                            }
                            Ok(false) => {
                                tracing::info!(
                                    fe_id = %info.fe_id,
                                    namespace = %info.namespace,
                                    app = %info.app,
                                    fn_name = %info.fn_name,
                                    app_version = %info.app_version,
                                    "Container is no longer alive"
                                );
                                container.state = ContainerState::Terminated {
                                    reason: FunctionExecutorTerminationReason::Unknown,
                                };
                            }
                            Err(e) => {
                                tracing::warn!(
                                    fe_id = %info.fe_id,
                                    namespace = %info.namespace,
                                    app = %info.app,
                                    fn_name = %info.fn_name,
                                    app_version = %info.app_version,
                                    error = %e,
                                    "Failed to check container status"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    /// Get the current state of all containers for reporting to the server.
    pub async fn get_states(&self) -> Vec<FunctionExecutorState> {
        let containers = self.containers.lock().await;
        containers.values().map(|c| c.to_proto_state()).collect()
    }
}

/// Start a container with the daemon and wait for it to be ready.
async fn start_container_with_daemon(
    driver: &Arc<dyn ProcessDriver>,
    image_resolver: &Arc<dyn ImageResolver>,
    desc: &FunctionExecutorDescription,
) -> anyhow::Result<(ProcessHandle, DaemonClient)> {
    let info = FunctionInfo::from_description(desc);
    let image = image_resolver.resolve_image(desc);

    // Extract resource limits from the function executor description
    let resources = desc.resources.as_ref().map(|r| {
        crate::driver::ResourceLimits {
            // cpu_ms_per_sec is equivalent to millicores (1000 = 1 CPU)
            cpu_millicores: r.cpu_ms_per_sec.map(|v| v as u64),
            // Convert bytes to megabytes
            memory_mb: r.memory_bytes.map(|v| v / (1024 * 1024)),
        }
    });

    // Start the container with the daemon as PID 1.
    // If entrypoint is provided, pass it to the daemon to start as a child process.
    // Otherwise, daemon just waits for commands via its HTTP API.
    let (command, args) = if !desc.entrypoint.is_empty() {
        let cmd = desc.entrypoint[0].clone();
        let args: Vec<String> = desc.entrypoint.iter().skip(1).cloned().collect();
        (cmd, args)
    } else {
        (String::new(), vec![])
    };

    // Build labels for container identification
    let container_type = match desc.container_type() {
        FunctionExecutorType::Unknown => "unknown",
        FunctionExecutorType::Function => "function",
        FunctionExecutorType::Sandbox => "sandbox",
    };
    let labels = vec![
        ("indexify.managed".to_string(), "true".to_string()),
        ("indexify.type".to_string(), container_type.to_string()),
        ("indexify.namespace".to_string(), info.namespace.to_string()),
        ("indexify.application".to_string(), info.app.to_string()),
        ("indexify.function".to_string(), info.fn_name.to_string()),
        ("indexify.version".to_string(), info.app_version.to_string()),
        ("indexify.fe_id".to_string(), info.fe_id.to_string()),
    ];

    let config = ProcessConfig {
        image: Some(image),
        command,
        args,
        env: vec![],
        working_dir: None,
        resources,
        labels,
    };

    // Start the container (daemon will be PID 1)
    let handle = driver.start(config).await?;

    // Get the daemon address from the handle
    let daemon_addr = handle
        .daemon_addr
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No daemon address available for container"))?;

    tracing::info!(
        fe_id = %info.fe_id,
        container_id = %handle.id,
        daemon_addr = %daemon_addr,
        "Container started, connecting to daemon"
    );

    // Connect to the daemon with retry (container may take a moment to start)
    let mut daemon_client =
        DaemonClient::connect_with_retry(daemon_addr, DAEMON_READY_TIMEOUT).await?;

    // Wait for daemon to be ready
    daemon_client.wait_for_ready(DAEMON_READY_TIMEOUT).await?;

    tracing::info!(
        fe_id = %info.fe_id,
        container_id = %handle.id,
        "Daemon ready, waiting for HTTP API commands"
    );

    Ok((handle, daemon_client))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use async_trait::async_trait;
    use proto_api::executor_api_pb::FunctionRef;

    use super::*;

    /// Mock process driver for testing
    struct MockProcessDriver {
        start_count: AtomicUsize,
        alive_result: AtomicBool,
        should_fail: AtomicBool,
    }

    impl MockProcessDriver {
        fn new() -> Self {
            Self {
                start_count: AtomicUsize::new(0),
                alive_result: AtomicBool::new(true),
                should_fail: AtomicBool::new(false),
            }
        }

        #[allow(dead_code)] // Test helper for future tests
        fn set_alive(&self, alive: bool) {
            self.alive_result.store(alive, Ordering::SeqCst);
        }

        #[allow(dead_code)] // Test helper for future tests
        fn set_should_fail(&self, fail: bool) {
            self.should_fail.store(fail, Ordering::SeqCst);
        }

        fn start_count(&self) -> usize {
            self.start_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ProcessDriver for MockProcessDriver {
        async fn start(&self, _config: ProcessConfig) -> anyhow::Result<ProcessHandle> {
            if self.should_fail.load(Ordering::SeqCst) {
                anyhow::bail!("Mock start failure");
            }
            self.start_count.fetch_add(1, Ordering::SeqCst);
            Ok(ProcessHandle {
                id: format!("mock-container-{}", self.start_count.load(Ordering::SeqCst)),
                daemon_addr: None, // No daemon address means daemon connection will fail
                http_addr: None,
            })
        }

        async fn send_sig(&self, _handle: &ProcessHandle, _signal: i32) -> anyhow::Result<()> {
            Ok(())
        }

        async fn kill(&self, _handle: &ProcessHandle) -> anyhow::Result<()> {
            Ok(())
        }

        async fn alive(&self, _handle: &ProcessHandle) -> anyhow::Result<bool> {
            Ok(self.alive_result.load(Ordering::SeqCst))
        }
    }

    fn create_test_fe_description(id: &str) -> FunctionExecutorDescription {
        FunctionExecutorDescription {
            id: Some(id.to_string()),
            function: Some(FunctionRef {
                namespace: Some("test-ns".to_string()),
                application_name: Some("test-app".to_string()),
                function_name: Some("test-fn".to_string()),
                application_version: Some("v1".to_string()),
            }),
            secret_names: vec![],
            initialization_timeout_ms: None,
            application: None,
            resources: None,
            max_concurrency: None,
            allocation_timeout_ms: None,
            image: None,
            sandbox_timeout_secs: None,
            entrypoint: vec![],
            container_type: None,
        }
    }

    #[test]
    fn test_function_info_from_description() {
        let desc = create_test_fe_description("fe-123");
        let info = FunctionInfo::from_description(&desc);

        assert_eq!(info.fe_id, "fe-123");
        assert_eq!(info.namespace, "test-ns");
        assert_eq!(info.app, "test-app");
        assert_eq!(info.fn_name, "test-fn");
        assert_eq!(info.app_version, "v1");
    }

    #[test]
    fn test_function_info_empty_description() {
        let desc = FunctionExecutorDescription {
            id: None,
            function: None,
            secret_names: vec![],
            initialization_timeout_ms: None,
            application: None,
            resources: None,
            max_concurrency: None,
            allocation_timeout_ms: None,
            image: None,
            sandbox_timeout_secs: None,
            entrypoint: vec![],
            container_type: None,
        };
        let info = FunctionInfo::from_description(&desc);

        assert_eq!(info.fe_id, "");
        assert_eq!(info.namespace, "");
        assert_eq!(info.app, "");
        assert_eq!(info.fn_name, "");
        assert_eq!(info.app_version, "");
    }

    #[test]
    fn test_default_image_resolver() {
        let resolver = DefaultImageResolver;
        let desc = create_test_fe_description("fe-123");
        let image = resolver.resolve_image(&desc);

        assert_eq!(image, "python:3.11-slim");
    }

    #[test]
    fn test_image_resolver_with_custom_image() {
        let resolver = DefaultImageResolver;
        let mut desc = create_test_fe_description("fe-123");
        desc.image = Some("custom-sandbox:latest".to_string());
        let image = resolver.resolve_image(&desc);

        assert_eq!(image, "custom-sandbox:latest");
    }

    #[test]
    fn test_container_state_to_proto_pending() {
        let container = ManagedContainer {
            description: create_test_fe_description("fe-123"),
            state: ContainerState::Pending,
            started_at: None,
            daemon_http_address: None,
        };

        let proto_state = container.to_proto_state();
        assert_eq!(
            proto_state.status,
            Some(FunctionExecutorStatus::Pending.into())
        );
        assert!(proto_state.termination_reason.is_none());
        assert!(proto_state.daemon_http_address.is_none());
    }

    #[test]
    fn test_container_state_to_proto_terminated() {
        let container = ManagedContainer {
            description: create_test_fe_description("fe-123"),
            state: ContainerState::Terminated {
                reason: FunctionExecutorTerminationReason::StartupFailedInternalError,
            },
            started_at: None,
            daemon_http_address: None,
        };

        let proto_state = container.to_proto_state();
        assert_eq!(
            proto_state.status,
            Some(FunctionExecutorStatus::Terminated.into())
        );
        assert_eq!(
            proto_state.termination_reason,
            Some(FunctionExecutorTerminationReason::StartupFailedInternalError.into())
        );
    }

    #[tokio::test]
    async fn test_manager_new() {
        let driver = Arc::new(MockProcessDriver::new());
        let resolver = Arc::new(DefaultImageResolver);

        let manager = FunctionContainerManager::new(driver, resolver);
        let states = manager.get_states().await;

        assert!(states.is_empty());
    }

    #[tokio::test]
    async fn test_sync_creates_containers() {
        let driver = Arc::new(MockProcessDriver::new());
        let resolver = Arc::new(DefaultImageResolver);
        let manager = FunctionContainerManager::new(driver.clone(), resolver);

        // Sync with one desired FE
        let desired = vec![create_test_fe_description("fe-123")];
        manager.sync(desired).await;

        // Should have one container in pending state
        let states = manager.get_states().await;
        assert_eq!(states.len(), 1);
        assert_eq!(
            states[0].status,
            Some(FunctionExecutorStatus::Pending.into())
        );

        // Wait a bit for the spawn task to run
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // The container creation will fail because socket_path is None,
        // so it should transition to Terminated
        let states = manager.get_states().await;
        assert_eq!(states.len(), 1);
        assert_eq!(
            states[0].status,
            Some(FunctionExecutorStatus::Terminated.into())
        );
    }

    #[tokio::test]
    async fn test_sync_removes_containers_not_in_desired() {
        let driver = Arc::new(MockProcessDriver::new());
        let resolver = Arc::new(DefaultImageResolver);
        let manager = FunctionContainerManager::new(driver.clone(), resolver);

        // First sync with one FE
        let desired = vec![create_test_fe_description("fe-123")];
        manager.sync(desired).await;

        // Wait for container creation to complete (will fail and terminate)
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Now sync with empty desired state
        manager.sync(vec![]).await;

        // Container should be removed since it was terminated
        let states = manager.get_states().await;
        assert!(states.is_empty());
    }

    #[tokio::test]
    async fn test_sync_ignores_already_tracked_containers() {
        let driver = Arc::new(MockProcessDriver::new());
        let resolver = Arc::new(DefaultImageResolver);
        let manager = FunctionContainerManager::new(driver.clone(), resolver);

        // Sync with one FE
        let desired = vec![create_test_fe_description("fe-123")];
        manager.sync(desired.clone()).await;

        let start_count_1 = driver.start_count();

        // Sync again with same FE
        manager.sync(desired).await;

        // Should not start another container
        assert_eq!(driver.start_count(), start_count_1);
    }

    #[tokio::test]
    async fn test_sync_skips_fe_without_id() {
        let driver = Arc::new(MockProcessDriver::new());
        let resolver = Arc::new(DefaultImageResolver);
        let manager = FunctionContainerManager::new(driver.clone(), resolver);

        // Sync with FE that has no ID
        let mut desc = create_test_fe_description("fe-123");
        desc.id = None;
        manager.sync(vec![desc]).await;

        // Should not create any containers
        let states = manager.get_states().await;
        assert!(states.is_empty());
        assert_eq!(driver.start_count(), 0);
    }

    fn create_test_fe_description_with_timeout(
        id: &str,
        timeout_secs: u64,
    ) -> FunctionExecutorDescription {
        let mut desc = create_test_fe_description(id);
        desc.sandbox_timeout_secs = Some(timeout_secs);
        desc
    }

    /// Create a mock DaemonClient for testing (connects to invalid address but
    /// that's ok for state tests)
    fn create_mock_daemon_client() -> DaemonClient {
        DaemonClient::new_for_testing()
    }

    fn create_mock_handle(id: &str) -> crate::driver::ProcessHandle {
        crate::driver::ProcessHandle {
            id: id.to_string(),
            daemon_addr: None,
            http_addr: None,
        }
    }

    #[test]
    fn test_is_timed_out_no_timeout_configured() {
        // Container with no timeout (timeout_secs = 0) should never time out
        let container = ManagedContainer {
            description: create_test_fe_description("fe-123"),
            state: ContainerState::Pending,
            started_at: Some(std::time::Instant::now() - std::time::Duration::from_secs(1000)),
            daemon_http_address: None,
        };

        assert!(!container.is_timed_out());
    }

    #[test]
    fn test_is_timed_out_not_running() {
        // Container that's not running should not report as timed out
        let container = ManagedContainer {
            description: create_test_fe_description_with_timeout("fe-123", 10),
            state: ContainerState::Pending,
            started_at: Some(std::time::Instant::now() - std::time::Duration::from_secs(100)),
            daemon_http_address: None,
        };

        assert!(!container.is_timed_out());
    }

    #[test]
    fn test_is_timed_out_no_started_at() {
        // Container without started_at should not report as timed out
        let container = ManagedContainer {
            description: create_test_fe_description_with_timeout("fe-123", 10),
            state: ContainerState::Terminated {
                reason: FunctionExecutorTerminationReason::Unknown,
            },
            started_at: None,
            daemon_http_address: None,
        };

        assert!(!container.is_timed_out());
    }

    #[tokio::test]
    async fn test_is_timed_out_within_timeout() {
        let container = ManagedContainer {
            description: create_test_fe_description_with_timeout("fe-123", 600), // 10 min timeout
            state: ContainerState::Running {
                handle: create_mock_handle("test-container"),
                daemon_client: create_mock_daemon_client(),
            },
            started_at: Some(std::time::Instant::now()), // Just started
            daemon_http_address: None,
        };

        assert!(!container.is_timed_out());
    }

    #[tokio::test]
    async fn test_is_timed_out_exceeded() {
        let container = ManagedContainer {
            description: create_test_fe_description_with_timeout("fe-123", 10), // 10 sec timeout
            state: ContainerState::Running {
                handle: create_mock_handle("test-container"),
                daemon_client: create_mock_daemon_client(),
            },
            // Started 15 seconds ago, so 10 second timeout is exceeded
            started_at: Some(std::time::Instant::now() - std::time::Duration::from_secs(15)),
            daemon_http_address: None,
        };

        assert!(container.is_timed_out());
    }

    #[tokio::test]
    async fn test_is_timed_out_exactly_at_boundary() {
        let container = ManagedContainer {
            description: create_test_fe_description_with_timeout("fe-123", 10),
            state: ContainerState::Running {
                handle: create_mock_handle("test-container"),
                daemon_client: create_mock_daemon_client(),
            },
            // Started exactly 10 seconds ago - should be timed out (>= comparison)
            started_at: Some(std::time::Instant::now() - std::time::Duration::from_secs(10)),
            daemon_http_address: None,
        };

        assert!(container.is_timed_out());
    }

    #[tokio::test]
    async fn test_check_timeouts_terminates_expired_container() {
        // This test verifies that check_timeouts() terminates containers that have
        // exceeded their timeout
        let driver = Arc::new(MockProcessDriver::new());
        let resolver = Arc::new(DefaultImageResolver);
        let manager = FunctionContainerManager::new(driver.clone(), resolver);

        // Insert a container that started 10 seconds ago with a 5 second timeout
        // (already timed out)
        {
            let mut containers = manager.containers.lock().await;
            let container = ManagedContainer {
                description: create_test_fe_description_with_timeout("fe-timeout-test", 5),
                state: ContainerState::Running {
                    handle: create_mock_handle("test-container"),
                    daemon_client: create_mock_daemon_client(),
                },
                // Set started_at to 10 seconds ago - well past the 5 second timeout
                started_at: Some(std::time::Instant::now() - std::time::Duration::from_secs(10)),
                daemon_http_address: None,
            };
            containers.insert("fe-timeout-test".to_string(), container);
        }

        // Verify container is timed out
        {
            let containers = manager.containers.lock().await;
            let container = containers.get("fe-timeout-test").unwrap();
            assert!(container.is_timed_out(), "Container should be timed out");
        }

        // Run check_timeouts - this should initiate stop for the timed out container
        manager.check_timeouts().await;

        // Container should now be in Stopping state (initiate_stop was called)
        {
            let containers = manager.containers.lock().await;
            let container = containers.get("fe-timeout-test").unwrap();
            assert!(
                matches!(container.state, ContainerState::Stopping { .. }),
                "Expected Stopping state after timeout, got {:?}",
                match &container.state {
                    ContainerState::Pending => "Pending",
                    ContainerState::Running { .. } => "Running",
                    ContainerState::Stopping { .. } => "Stopping",
                    ContainerState::Terminated { .. } => "Terminated",
                }
            );
        }
    }

    #[tokio::test]
    async fn test_check_timeouts_does_not_stop_container_within_timeout() {
        // Test that check_timeouts() does NOT stop containers still within timeout
        let driver = Arc::new(MockProcessDriver::new());
        let resolver = Arc::new(DefaultImageResolver);
        let manager = FunctionContainerManager::new(driver.clone(), resolver);

        // Insert a container that just started with a 600 second timeout
        {
            let mut containers = manager.containers.lock().await;
            let container = ManagedContainer {
                description: create_test_fe_description_with_timeout("fe-not-expired", 600),
                state: ContainerState::Running {
                    handle: create_mock_handle("test-container"),
                    daemon_client: create_mock_daemon_client(),
                },
                started_at: Some(std::time::Instant::now()), // Just started
                daemon_http_address: None,
            };
            containers.insert("fe-not-expired".to_string(), container);
        }

        // Run check_timeouts
        manager.check_timeouts().await;

        // Container should still be running
        {
            let containers = manager.containers.lock().await;
            let container = containers.get("fe-not-expired").unwrap();
            assert!(
                matches!(container.state, ContainerState::Running { .. }),
                "Container within timeout should still be running"
            );
        }
    }

    #[tokio::test]
    async fn test_check_timeouts_does_not_affect_no_timeout_containers() {
        let driver = Arc::new(MockProcessDriver::new());
        let resolver = Arc::new(DefaultImageResolver);
        let manager = FunctionContainerManager::new(driver.clone(), resolver);

        // Insert a container with no timeout that started a long time ago
        {
            let mut containers = manager.containers.lock().await;
            let container = ManagedContainer {
                description: create_test_fe_description("fe-no-timeout"), // No timeout (None)
                state: ContainerState::Running {
                    handle: create_mock_handle("test-container"),
                    daemon_client: create_mock_daemon_client(),
                },
                // Started 1 hour ago, but has no timeout so shouldn't be stopped
                started_at: Some(std::time::Instant::now() - std::time::Duration::from_secs(3600)),
                daemon_http_address: None,
            };
            containers.insert("fe-no-timeout".to_string(), container);
        }

        // Run check_timeouts
        manager.check_timeouts().await;

        // Container should still be running (no timeout configured)
        {
            let containers = manager.containers.lock().await;
            let container = containers.get("fe-no-timeout").unwrap();
            assert!(
                matches!(container.state, ContainerState::Running { .. }),
                "Container with no timeout should still be running"
            );
        }
    }
}
