use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use proto_api::executor_api_pb::{
    FunctionExecutorDescription,
    FunctionExecutorState,
    FunctionExecutorStatus,
    FunctionExecutorTerminationReason,
    FunctionExecutorType,
};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::{
    daemon_client::DaemonClient,
    driver::{ExitStatus, ProcessConfig, ProcessDriver, ProcessHandle},
    metrics::{ContainerCounts, DataplaneMetrics},
    network_rules,
    state_file::{PersistedContainer, StateFile},
};

const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);
const KILL_GRACE_PERIOD: Duration = Duration::from_secs(10);
const DAEMON_READY_TIMEOUT: Duration = Duration::from_secs(60);

fn termination_reason_from_exit_status(
    exit_status: Option<ExitStatus>,
) -> FunctionExecutorTerminationReason {
    match exit_status {
        Some(status) if status.oom_killed => FunctionExecutorTerminationReason::Oom,
        Some(status) => match status.exit_code {
            Some(0) => FunctionExecutorTerminationReason::FunctionTimeout,
            Some(137) => FunctionExecutorTerminationReason::Oom,
            Some(143) => FunctionExecutorTerminationReason::FunctionCancelled,
            _ => FunctionExecutorTerminationReason::Unknown,
        },
        None => FunctionExecutorTerminationReason::Unknown,
    }
}

/// Helper struct for structured logging of container info.
struct ContainerInfo<'a> {
    container_id: &'a str,
    namespace: &'a str,
    app: &'a str,
    fn_name: &'a str,
    app_version: &'a str,
    sandbox_id: Option<&'a str>,
}

impl<'a> ContainerInfo<'a> {
    fn from_description(desc: &'a FunctionExecutorDescription) -> Self {
        let container_id = desc.id.as_deref().unwrap_or("");
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
        let sandbox_id = desc
            .sandbox_metadata
            .as_ref()
            .and_then(|m| m.sandbox_id.as_deref());

        Self {
            container_id,
            namespace,
            app,
            fn_name,
            app_version,
            sandbox_id,
        }
    }
}

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
        /// The reason for stopping (used when container terminates)
        reason: FunctionExecutorTerminationReason,
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
    /// When the container was created (for latency tracking)
    created_at: Instant,
    /// When the container started running (set when state becomes Running)
    started_at: Option<Instant>,
    /// When a sandbox claimed this container
    sandbox_claimed_at: Option<Instant>,
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
        }
    }

    fn info(&self) -> ContainerInfo<'_> {
        ContainerInfo::from_description(&self.description)
    }

    /// Check if this sandbox container has exceeded its timeout.
    /// Returns true if the container should be terminated due to timeout.
    fn is_timed_out(&self) -> bool {
        // Only check timeout for running containers with a timeout configured
        let Some(timeout_secs) = self
            .description
            .sandbox_metadata
            .as_ref()
            .and_then(|m| m.timeout_secs)
        else {
            return false; // No timeout configured
        };

        if !matches!(self.state, ContainerState::Running { .. }) {
            return false; // Not running, can't timeout
        }

        // Timeout only applies once a sandbox has claimed this container.
        // Warm pool containers (sandbox_claimed_at = None) never time out.
        if let Some(claimed_at) = self.sandbox_claimed_at {
            claimed_at.elapsed().as_secs() >= timeout_secs
        } else {
            false
        }
    }
}

/// Resolves container images for function executors.
pub trait ImageResolver: Send + Sync {
    fn sandbox_image_for_pool(&self, namespace: &str, pool_id: &str) -> anyhow::Result<String>;
    fn sandbox_image(&self, namespace: &str, sandbox_id: &str) -> anyhow::Result<String>;
    fn function_image(
        &self,
        namespace: &str,
        app: &str,
        function: &str,
        version: &str,
    ) -> anyhow::Result<String>;
}

/// Default image resolver that returns errors for all methods.
///
/// The calling code in `FunctionContainerManager` first checks
/// `sandbox_metadata.image` from the server's description (primary source for
/// sandbox containers), and only falls back to the resolver if not set.
/// Custom main functions can inject their own `ImageResolver` with real logic.
pub struct DefaultImageResolver;

impl DefaultImageResolver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DefaultImageResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl ImageResolver for DefaultImageResolver {
    fn sandbox_image_for_pool(&self, _namespace: &str, _pool_id: &str) -> anyhow::Result<String> {
        anyhow::bail!(
            "No image configured — override ImageResolver or set sandbox_metadata.image on the description"
        )
    }

    fn sandbox_image(&self, _namespace: &str, _sandbox_id: &str) -> anyhow::Result<String> {
        anyhow::bail!("No image configured — override ImageResolver")
    }

    fn function_image(
        &self,
        _namespace: &str,
        _app: &str,
        _function: &str,
        _version: &str,
    ) -> anyhow::Result<String> {
        anyhow::bail!("No image configured — override ImageResolver")
    }
}

/// Container storage with a secondary index for O(1) sandbox_id lookups.
///
/// The primary map is keyed by container_id (nanoid). Pool containers have a
/// container_id that differs from the sandbox_id they serve, so a secondary
/// index maps sandbox_id -> container_id for fast proxy routing.
struct ContainerStore {
    map: HashMap<String, ManagedContainer>,
    /// sandbox_id -> container_id for containers that have been claimed by a
    /// sandbox.
    sandbox_index: HashMap<String, String>,
}

impl ContainerStore {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            sandbox_index: HashMap::new(),
        }
    }

    /// Look up a container by sandbox_id (O(1) via secondary index).
    fn get_by_sandbox_id(&self, sandbox_id: &str) -> Option<&ManagedContainer> {
        self.sandbox_index
            .get(sandbox_id)
            .and_then(|cid| self.map.get(cid))
    }

    /// Update the sandbox index when a container gains a sandbox_id.
    fn index_sandbox(&mut self, sandbox_id: String, container_id: String) {
        self.sandbox_index.insert(sandbox_id, container_id);
    }

    /// Remove a sandbox_id from the index.
    fn unindex_sandbox(&mut self, sandbox_id: &str) {
        self.sandbox_index.remove(sandbox_id);
    }
}

impl std::ops::Deref for ContainerStore {
    type Target = HashMap<String, ManagedContainer>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl std::ops::DerefMut for ContainerStore {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

/// Manages function executor containers.
pub struct FunctionContainerManager {
    driver: Arc<dyn ProcessDriver>,
    image_resolver: Arc<dyn ImageResolver>,
    containers: Arc<RwLock<ContainerStore>>,
    metrics: Arc<DataplaneMetrics>,
    state_file: Arc<StateFile>,
    executor_id: String,
}

impl FunctionContainerManager {
    pub fn new(
        driver: Arc<dyn ProcessDriver>,
        image_resolver: Arc<dyn ImageResolver>,
        metrics: Arc<DataplaneMetrics>,
        state_file: Arc<StateFile>,
        executor_id: String,
    ) -> Self {
        Self {
            driver,
            image_resolver,
            containers: Arc::new(RwLock::new(ContainerStore::new())),
            metrics,
            state_file,
            executor_id,
        }
    }

    /// Recover containers from the state file.
    ///
    /// This should be called on startup to reconnect to any containers that
    /// were running before the dataplane restarted.
    pub async fn recover(&self) -> usize {
        let persisted = self.state_file.get_all().await;
        let mut recovered = 0;

        for entry in persisted {
            // Check if the container is still alive
            let handle = ProcessHandle {
                id: entry.handle_id.clone(),
                daemon_addr: Some(entry.daemon_addr.clone()),
                http_addr: Some(entry.http_addr.clone()),
                container_ip: entry.container_ip.clone(),
            };

            match self.driver.alive(&handle).await {
                Ok(true) => {
                    // Container is still alive, try to reconnect to daemon
                    match DaemonClient::connect_with_retry(
                        &entry.daemon_addr,
                        std::time::Duration::from_secs(5),
                    )
                    .await
                    {
                        Ok(daemon_client) => {
                            // Decode the full description from the state file
                            let description = match entry.decode_description() {
                                Some(desc) => desc,
                                None => {
                                    tracing::warn!(
                                        container_id = %entry.container_id,
                                        "No description stored in state file, skipping recovery"
                                    );
                                    let _ = self.state_file.remove(&entry.container_id).await;
                                    continue;
                                }
                            };

                            let recovered_info = ContainerInfo::from_description(&description);
                            tracing::info!(
                                container_id = %entry.container_id,
                                handle_id = %entry.handle_id,
                                daemon_addr = %entry.daemon_addr,
                                sandbox_id = ?recovered_info.sandbox_id,
                                "Recovered container from state file"
                            );

                            let sandbox_id_for_index = description
                                .sandbox_metadata
                                .as_ref()
                                .and_then(|m| m.sandbox_id.clone());
                            let sandbox_claimed_at =
                                sandbox_id_for_index.as_ref().map(|_| Instant::now());
                            let container = ManagedContainer {
                                description,
                                state: ContainerState::Running {
                                    handle,
                                    daemon_client,
                                },
                                created_at: Instant::now(),
                                started_at: Some(Instant::now()),
                                sandbox_claimed_at,
                            };

                            let mut containers = self.containers.write().await;
                            containers.insert(entry.container_id.clone(), container);
                            if let Some(sid) = sandbox_id_for_index {
                                containers.index_sandbox(sid, entry.container_id.clone());
                            }
                            recovered += 1;
                        }
                        Err(e) => {
                            tracing::warn!(
                                container_id = %entry.container_id,
                                handle_id = %entry.handle_id,
                                error = %e,
                                "Failed to reconnect to daemon, removing from state"
                            );
                            let _ = self.state_file.remove(&entry.container_id).await;
                        }
                    }
                }
                Ok(false) => {
                    tracing::info!(
                        container_id = %entry.container_id,
                        handle_id = %entry.handle_id,
                        "Container no longer alive, removing from state"
                    );
                    let _ = self.state_file.remove(&entry.container_id).await;
                }
                Err(e) => {
                    tracing::warn!(
                        container_id = %entry.container_id,
                        handle_id = %entry.handle_id,
                        error = %e,
                        "Failed to check container status, removing from state"
                    );
                    let _ = self.state_file.remove(&entry.container_id).await;
                }
            }
        }

        recovered
    }

    /// Clean up orphaned containers that exist in Docker but not in the state
    /// file.
    ///
    /// This handles containers that were created but the dataplane crashed
    /// before saving them to the state file, or containers left behind when
    /// the server terminated a sandbox while the dataplane was down.
    pub async fn cleanup_orphans(&self) -> usize {
        let known_handles: HashSet<String> = {
            let containers = self.containers.read().await;
            containers
                .values()
                .filter_map(|c| match &c.state {
                    ContainerState::Running { handle, .. } |
                    ContainerState::Stopping { handle, .. } => Some(handle.id.clone()),
                    _ => None,
                })
                .collect()
        };

        let all_containers = match self.driver.list_containers().await {
            Ok(containers) => containers,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to list containers for orphan cleanup");
                return 0;
            }
        };

        let mut cleaned = 0;
        for container_id in all_containers {
            if !known_handles.contains(&container_id) {
                tracing::info!(
                    container_id = %container_id,
                    "Removing orphaned container"
                );

                let handle = ProcessHandle {
                    id: container_id.clone(),
                    daemon_addr: None,
                    http_addr: None,
                    container_ip: String::new(), // Unknown for orphans, not needed for kill
                };

                if let Err(e) = self.driver.kill(&handle).await {
                    tracing::warn!(
                        container_id = %container_id,
                        error = %e,
                        "Failed to remove orphaned container"
                    );
                } else {
                    cleaned += 1;
                }
            }
        }

        cleaned
    }

    /// Sync the containers with the desired state from the server.
    /// Creates new containers, and marks removed ones for termination.
    pub async fn sync(&self, desired: Vec<FunctionExecutorDescription>) {
        let desired_ids: HashSet<String> = desired.iter().filter_map(|d| d.id.clone()).collect();

        let mut containers = self.containers.write().await;

        // Check for sandbox timeouts and stop expired containers
        let timed_out_ids: Vec<String> = containers
            .iter()
            .filter(|(_, container)| container.is_timed_out())
            .map(|(id, _)| id.clone())
            .collect();

        for id in timed_out_ids {
            let Some(container) = containers.get_mut(&id) else {
                continue;
            };
            let Some(timeout_secs) = container
                .description
                .sandbox_metadata
                .as_ref()
                .and_then(|m| m.timeout_secs)
            else {
                continue;
            };
            let info = container.info();
            let elapsed = container
                .sandbox_claimed_at
                .map(|s| s.elapsed().as_secs())
                .unwrap_or(0);
            tracing::warn!(
                container_id = %info.container_id,
                namespace = %info.namespace,
                app = %info.app,
                fn_name = %info.fn_name,
                sandbox_id = ?info.sandbox_id,
                timeout_secs = timeout_secs,
                elapsed_secs = elapsed,
                "Sandbox container timed out, terminating"
            );
            self.initiate_stop(
                container,
                FunctionExecutorTerminationReason::FunctionTimeout,
            )
            .await;
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
                        // Server no longer wants this FE, remove from memory and state file
                        tracing::info!(
                            container_id = %info.container_id,
                            namespace = %info.namespace,
                            app = %info.app,
                            fn_name = %info.fn_name,
                            app_version = %info.app_version,
                            sandbox_id = ?info.sandbox_id,
                            "Removed terminated container from memory"
                        );
                        if let Err(e) = self.state_file.remove(&id).await {
                            tracing::warn!(
                                container_id = %info.container_id,
                                error = %e,
                                "Failed to remove container from state file"
                            );
                        }
                        if let Some(removed) = containers.remove(&id) &&
                            let Some(sid) = removed
                                .description
                                .sandbox_metadata
                                .as_ref()
                                .and_then(|m| m.sandbox_id.as_ref())
                        {
                            containers.unindex_sandbox(sid);
                        }
                    }
                    ContainerState::Stopping { .. } => {
                        // Already stopping, let it continue
                    }
                    ContainerState::Pending | ContainerState::Running { .. } => {
                        // Need to stop this container (removed from desired state)
                        self.initiate_stop(
                            container,
                            FunctionExecutorTerminationReason::FunctionCancelled,
                        )
                        .await;
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
                let info = ContainerInfo::from_description(&desc);
                let container_type = container_type_str(&desc);

                tracing::info!(
                    container_id = %info.container_id,
                    namespace = %info.namespace,
                    app = %info.app,
                    fn_name = %info.fn_name,
                    app_version = %info.app_version,
                    sandbox_id = ?info.sandbox_id,
                    container_type = %container_type,
                    event = "container_creating",
                    "Creating new container"
                );

                // If the container already has a sandbox_id in its initial
                // description, it was created specifically for a sandbox (not
                // claimed from a warm pool). Start the timeout countdown now.
                let sandbox_claimed_at = desc
                    .sandbox_metadata
                    .as_ref()
                    .and_then(|m| m.sandbox_id.as_ref())
                    .map(|_| Instant::now());

                let container = ManagedContainer {
                    description: desc.clone(),
                    state: ContainerState::Pending,
                    created_at: Instant::now(),
                    started_at: None,
                    sandbox_claimed_at,
                };
                containers.insert(id.clone(), container);
                if let Some(sid) = desc
                    .sandbox_metadata
                    .as_ref()
                    .and_then(|m| m.sandbox_id.clone())
                {
                    containers.index_sandbox(sid, id.clone());
                }

                // Spawn container creation with daemon integration
                let driver = self.driver.clone();
                let image_resolver = self.image_resolver.clone();
                let containers_ref = self.containers.clone();
                let metrics = self.metrics.clone();
                let state_file = self.state_file.clone();
                let desc_clone = desc.clone();
                let executor_id = self.executor_id.clone();

                tokio::spawn(async move {
                    let result =
                        start_container_with_daemon(&driver, &image_resolver, &desc_clone).await;

                    let mut containers = containers_ref.write().await;
                    if let Some(container) = containers.get_mut(&id) {
                        let startup_duration_ms = container.created_at.elapsed().as_millis();
                        let info = container.info();
                        let container_type = container_type_str(&container.description);

                        match result {
                            Ok((handle, daemon_client)) => {
                                // Record container started metric
                                metrics.counters.record_container_started(container_type);

                                // Structured log with latency
                                tracing::info!(
                                    container_id = %info.container_id,
                                    namespace = %info.namespace,
                                    app = %info.app,
                                    fn_name = %info.fn_name,
                                    app_version = %info.app_version,
                                    sandbox_id = ?info.sandbox_id,
                                    container_id = %handle.id,
                                    http_addr = ?handle.http_addr,
                                    container_type = %container_type,
                                    startup_duration_ms = %startup_duration_ms,
                                    event = "container_started",
                                    "Container started with daemon"
                                );

                                // Persist to state file for recovery after restart
                                if let (Some(daemon_addr), Some(http_addr)) =
                                    (&handle.daemon_addr, &handle.http_addr)
                                {
                                    let persisted = PersistedContainer {
                                        container_id: id.clone(),
                                        handle_id: handle.id.clone(),
                                        daemon_addr: daemon_addr.clone(),
                                        http_addr: http_addr.clone(),
                                        container_ip: handle.container_ip.clone(),
                                        started_at: std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .map(|d| d.as_millis() as u64)
                                            .unwrap_or(0),
                                        description_proto: Some(
                                            PersistedContainer::encode_description(&desc_clone),
                                        ),
                                    };
                                    if let Err(e) = state_file.upsert(persisted).await {
                                        tracing::warn!(
                                            container_id = %info.container_id,
                                            error = %e,
                                            "Failed to persist container state"
                                        );
                                    }
                                }

                                container.state = ContainerState::Running {
                                    handle,
                                    daemon_client,
                                };
                                container.started_at = Some(Instant::now());

                                // Update container counts
                                update_container_counts(&containers, &metrics).await;
                            }
                            Err(e) => {
                                // Record container terminated (startup failure)
                                metrics
                                    .counters
                                    .record_container_terminated(container_type, "startup_failed");

                                tracing::error!(
                                    container_id = %info.container_id,
                                    namespace = %info.namespace,
                                    app = %info.app,
                                    fn_name = %info.fn_name,
                                    app_version = %info.app_version,
                                    sandbox_id = ?info.sandbox_id,
                                    container_type = %container_type,
                                    startup_duration_ms = %startup_duration_ms,
                                    error = %e,
                                    event = "container_startup_failed",
                                    "Failed to start container"
                                );
                                container.state = ContainerState::Terminated {
                                    reason:
                                        FunctionExecutorTerminationReason::StartupFailedInternalError,
                                };

                                // Update container counts
                                update_container_counts(&containers, &metrics).await;
                            }
                        }
                    }
                }.instrument(tracing::info_span!("container_lifecycle", %executor_id)));
            } else {
                // Container already exists — check if sandbox_id changed (warm → claimed)
                let old_sandbox_id = containers
                    .get(&id)
                    .and_then(|c| c.description.sandbox_metadata.as_ref())
                    .and_then(|m| m.sandbox_id.clone());
                let new_sandbox_id = desc
                    .sandbox_metadata
                    .as_ref()
                    .and_then(|m| m.sandbox_id.clone());

                if let Some(container) = containers.get_mut(&id) {
                    if old_sandbox_id.is_none() && new_sandbox_id.is_some() {
                        let info = ContainerInfo::from_description(&desc);
                        tracing::info!(
                            container_id = %info.container_id,
                            sandbox_id = ?new_sandbox_id,
                            executor_id = %self.executor_id,
                            "Warm container claimed by sandbox, starting timeout"
                        );
                        container.sandbox_claimed_at = Some(Instant::now());
                    }
                    // Always update the description to reflect server's desired state
                    container.description = desc;
                }

                // Update sandbox index
                if old_sandbox_id != new_sandbox_id {
                    if let Some(ref old_sid) = old_sandbox_id {
                        containers.unindex_sandbox(old_sid);
                    }
                    if let Some(new_sid) = new_sandbox_id {
                        containers.index_sandbox(new_sid, id);
                    }
                }
            }
        }

        // Update container counts after sync
        update_container_counts(&containers, &self.metrics).await;
    }

    /// Initiate stopping a container (signal first, then kill after grace
    /// period).
    async fn initiate_stop(
        &self,
        container: &mut ManagedContainer,
        reason: FunctionExecutorTerminationReason,
    ) {
        let id = container.description.id.clone().unwrap_or_default();
        let container_type = container_type_str(&container.description);

        // Extract what we need from the current state
        let (handle, daemon_client) = match &container.state {
            ContainerState::Running {
                handle,
                daemon_client,
            } => (handle.clone(), Some(daemon_client.clone())),
            ContainerState::Pending => {
                self.metrics
                    .counters
                    .record_container_terminated(container_type, "cancelled_pending");
                container.state = ContainerState::Terminated { reason };
                return;
            }
            _ => return,
        };

        // Extract container_id for logging before modifying container state
        let container_id_for_log = container.description.id.clone().unwrap_or_default();
        {
            let info = container.info();
            tracing::info!(
                container_id = %info.container_id,
                namespace = %info.namespace,
                app = %info.app,
                fn_name = %info.fn_name,
                app_version = %info.app_version,
                sandbox_id = ?info.sandbox_id,
                container_type = %container_type,
                event = "container_stopping",
                "Signaling container to stop via daemon"
            );

            // Try to signal via daemon first (SIGTERM to the function executor)
            if let Some(mut client) = daemon_client.clone() {
                if let Err(e) = client.send_signal(15).await {
                    tracing::info!(
                        container_id = %info.container_id,
                        error = %e,
                        "No process to signal via daemon, falling back to container signal"
                    );
                    // Fall back to docker signal
                    if let Err(e) = self.driver.send_sig(&handle, 15).await {
                        tracing::warn!(
                            container_id = %info.container_id,
                            error = %e,
                            "Failed to send signal to container"
                        );
                    }
                }
            } else if let Err(e) = self.driver.send_sig(&handle, 15).await {
                tracing::warn!(
                    container_id = %info.container_id,
                    error = %e,
                    "Failed to send signal to container"
                );
            }
        }

        // Move to stopping state with the reason
        container.state = ContainerState::Stopping {
            handle: handle.clone(),
            daemon_client,
            reason,
        };

        // Remove from state file since container is no longer running
        if let Err(e) = self.state_file.remove(&id).await {
            tracing::warn!(
                container_id = %container_id_for_log,
                error = %e,
                "Failed to remove container from state file"
            );
        }

        // Schedule kill after grace period
        let driver = self.driver.clone();
        let containers_ref = self.containers.clone();
        let metrics = self.metrics.clone();
        let container_id = id.clone();
        let container_type_owned = container_type.to_string();
        let executor_id = self.executor_id.clone();

        tokio::spawn(
            async move {
                tokio::time::sleep(KILL_GRACE_PERIOD).await;

                let mut containers = containers_ref.write().await;
                if let Some(container) = containers.get_mut(&container_id) &&
                    let ContainerState::Stopping { handle, reason, .. } = &container.state
                {
                    let handle = handle.clone();
                    let termination_reason = *reason;
                    let info = container.info();
                    let run_duration_ms = container
                        .started_at
                        .map(|s| s.elapsed().as_millis())
                        .unwrap_or(0);

                    tracing::info!(
                        container_id = %info.container_id,
                        namespace = %info.namespace,
                        app = %info.app,
                        fn_name = %info.fn_name,
                        app_version = %info.app_version,
                        sandbox_id = ?info.sandbox_id,
                        container_type = %container_type_owned,
                        run_duration_ms = %run_duration_ms,
                        event = "container_killing",
                        "Killing container after grace period"
                    );

                    // Clean up network rules before killing container
                    if let Err(e) = network_rules::remove_rules(&handle.id, &handle.container_ip) {
                        tracing::warn!(
                            container_id = %info.container_id,
                            error = %e,
                            "Failed to remove network rules"
                        );
                    }

                    if let Err(e) = driver.kill(&handle).await {
                        tracing::warn!(
                            container_id = %info.container_id,
                            namespace = %info.namespace,
                            app = %info.app,
                            fn_name = %info.fn_name,
                            app_version = %info.app_version,
                            sandbox_id = ?info.sandbox_id,
                            error = %e,
                            "Failed to kill container"
                        );
                    }

                    metrics
                        .counters
                        .record_container_terminated(&container_type_owned, "grace_period_kill");

                    tracing::info!(
                        container_id = %info.container_id,
                        namespace = %info.namespace,
                        app = %info.app,
                        fn_name = %info.fn_name,
                        app_version = %info.app_version,
                        sandbox_id = ?info.sandbox_id,
                        container_type = %container_type_owned,
                        run_duration_ms = %run_duration_ms,
                        event = "container_terminated",
                        "Container terminated"
                    );

                    container.state = ContainerState::Terminated {
                        reason: termination_reason,
                    };

                    update_container_counts(&containers, &metrics).await;
                }
            }
            .instrument(tracing::info_span!("container_stop", %executor_id)),
        );
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
        let mut containers = self.containers.write().await;

        // Find timed out containers
        let timed_out_ids: Vec<String> = containers
            .iter()
            .filter(|(_, container)| container.is_timed_out())
            .map(|(id, _)| id.clone())
            .collect();

        // Initiate stop for timed out containers
        for id in timed_out_ids {
            let Some(container) = containers.get_mut(&id) else {
                continue;
            };
            let Some(timeout_secs) = container
                .description
                .sandbox_metadata
                .as_ref()
                .and_then(|m| m.timeout_secs)
            else {
                continue;
            };
            let info = container.info();
            let elapsed = container
                .sandbox_claimed_at
                .map(|s| s.elapsed().as_secs())
                .unwrap_or(0);
            tracing::warn!(
                container_id = %info.container_id,
                namespace = %info.namespace,
                app = %info.app,
                fn_name = %info.fn_name,
                sandbox_id = ?info.sandbox_id,
                timeout_secs = timeout_secs,
                elapsed_secs = elapsed,
                "Sandbox container timed out, terminating"
            );
            self.initiate_stop(
                container,
                FunctionExecutorTerminationReason::FunctionTimeout,
            )
            .await;
        }
    }

    /// Check all running containers to see if they're still alive.
    async fn check_all_containers(&self) {
        let mut containers = self.containers.write().await;
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
                        Some((handle, Some(client), None)) // None = running, no predetermined reason
                    }
                    ContainerState::Stopping { handle, reason, .. } => {
                        let handle = handle.clone();
                        Some((handle, None, Some(*reason))) // Reason from initiate_stop
                    }
                    _ => None,
                };

                if let Some((handle, daemon_client, stopping_reason)) = check_result {
                    let info = container.info();

                    if let Some(reason) = stopping_reason {
                        // Container is stopping - check if it's dead yet
                        if let Ok(false) = self.driver.alive(&handle).await {
                            // Use the reason from initiate_stop (not exit code)
                            tracing::info!(
                                container_id = %info.container_id,
                                namespace = %info.namespace,
                                app = %info.app,
                                fn_name = %info.fn_name,
                                app_version = %info.app_version,
                                sandbox_id = ?info.sandbox_id,
                                reason = ?reason,
                                "Container stopped"
                            );
                            container.state = ContainerState::Terminated { reason };
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
                                                    container_id = %info.container_id,
                                                    namespace = %info.namespace,
                                                    app = %info.app,
                                                    fn_name = %info.fn_name,
                                                    app_version = %info.app_version,
                                                    sandbox_id = ?info.sandbox_id,
                                                    "Daemon is unhealthy, terminating container"
                                                );
                                                // Clean up network rules before killing
                                                let _ = network_rules::remove_rules(
                                                    &handle.id,
                                                    &handle.container_ip,
                                                );
                                                let _ = self.driver.kill(&handle).await;
                                                container.state = ContainerState::Terminated {
                                                    reason:
                                                        FunctionExecutorTerminationReason::Unhealthy,
                                                };
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                container_id = %info.container_id,
                                                error = %e,
                                                "Failed to check daemon health"
                                            );
                                        }
                                    }
                                }
                            }
                            Ok(false) => {
                                // Get exit status to determine termination reason
                                let exit_status =
                                    self.driver.get_exit_status(&handle).await.ok().flatten();
                                let reason =
                                    termination_reason_from_exit_status(exit_status.clone());
                                tracing::info!(
                                    container_id = %info.container_id,
                                    namespace = %info.namespace,
                                    app = %info.app,
                                    fn_name = %info.fn_name,
                                    app_version = %info.app_version,
                                    sandbox_id = ?info.sandbox_id,
                                    exit_code = ?exit_status.as_ref().and_then(|s| s.exit_code),
                                    oom_killed = ?exit_status.as_ref().map(|s| s.oom_killed),
                                    reason = ?reason,
                                    "Container is no longer alive"
                                );
                                container.state = ContainerState::Terminated { reason };
                            }
                            Err(e) => {
                                tracing::warn!(
                                    container_id = %info.container_id,
                                    namespace = %info.namespace,
                                    app = %info.app,
                                    fn_name = %info.fn_name,
                                    app_version = %info.app_version,
                                    sandbox_id = ?info.sandbox_id,
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
        let containers = self.containers.read().await;
        containers.values().map(|c| c.to_proto_state()).collect()
    }

    /// Get detailed status of a sandbox lookup.
    ///
    /// Returns:
    /// - `SandboxLookupResult::Running(addr)` - Sandbox is running, returns
    ///   address
    /// - `SandboxLookupResult::NotFound` - Sandbox ID not known to this
    ///   dataplane
    /// - `SandboxLookupResult::NotRunning(state)` - Sandbox exists but not
    ///   running
    pub async fn lookup_sandbox(&self, sandbox_id: &str, port: u16) -> SandboxLookupResult {
        let containers = self.containers.read().await;

        // Direct lookup by container_id (works for direct sandbox containers
        // where container_id == sandbox_id), then O(1) index lookup for pool
        // containers whose container_id is a nanoid different from sandbox_id.
        let container = containers
            .get(sandbox_id)
            .or_else(|| containers.get_by_sandbox_id(sandbox_id));

        let Some(container) = container else {
            return SandboxLookupResult::NotFound;
        };

        match &container.state {
            ContainerState::Running { handle, .. } => {
                let addr = format!("{}:{}", handle.container_ip, port);
                SandboxLookupResult::Running(addr)
            }
            ContainerState::Pending => SandboxLookupResult::NotRunning("pending"),
            ContainerState::Stopping { .. } => SandboxLookupResult::NotRunning("stopping"),
            ContainerState::Terminated { .. } => SandboxLookupResult::NotRunning("terminated"),
        }
    }
}

/// Result of looking up a sandbox for proxying.
#[derive(Debug, Clone)]
pub enum SandboxLookupResult {
    /// Sandbox is running, contains the address to connect to
    Running(String),
    /// Sandbox ID is not known to this dataplane (404)
    NotFound,
    /// Sandbox exists but is not in running state (503)
    NotRunning(&'static str),
}

/// Start a container with the daemon and wait for it to be ready.
async fn start_container_with_daemon(
    driver: &Arc<dyn ProcessDriver>,
    image_resolver: &Arc<dyn ImageResolver>,
    desc: &FunctionExecutorDescription,
) -> anyhow::Result<(ProcessHandle, DaemonClient)> {
    let info = ContainerInfo::from_description(desc);

    // Prefer image from sandbox_metadata (server-provided)
    let image = if let Some(ref meta) = desc.sandbox_metadata &&
        let Some(ref img) = meta.image
    {
        img.clone()
    } else if let Some(ref pool_id) = desc.pool_id {
        image_resolver.sandbox_image_for_pool(info.namespace, pool_id)?
    } else if let Some(sid) = info.sandbox_id {
        image_resolver.sandbox_image(info.namespace, sid)?
    } else {
        anyhow::bail!("Cannot determine image: no sandbox_metadata.image, pool_id, or sandbox_id")
    };

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
    // If entrypoint is provided in sandbox_metadata, pass it to the daemon to start
    // as a child process. Otherwise, daemon just waits for commands via its
    // HTTP API.
    let entrypoint = desc
        .sandbox_metadata
        .as_ref()
        .map(|m| m.entrypoint.clone())
        .unwrap_or_default();
    let (command, args) = if !entrypoint.is_empty() {
        let cmd = entrypoint[0].clone();
        let args: Vec<String> = entrypoint.iter().skip(1).cloned().collect();
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
        (
            "indexify.container_id".to_string(),
            info.container_id.to_string(),
        ),
    ];

    let config = ProcessConfig {
        id: info.container_id.to_string(),
        process_type: crate::driver::ProcessType::Sandbox,
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

    // Apply network firewall rules BEFORE daemon connection.
    // Container has IP now (cached in handle), but hasn't done any network requests
    // yet. This ensures network policy is enforced before any user code runs.
    if let Some(policy) = desc
        .sandbox_metadata
        .as_ref()
        .and_then(|m| m.network_policy.as_ref()) &&
        let Err(e) = network_rules::apply_rules(&handle.id, &handle.container_ip, policy)
    {
        tracing::warn!(
            container_id = %info.container_id,
            container_id = %handle.id,
            error = %e,
            "Failed to apply network rules (continuing anyway)"
        );
        // Continue anyway - rules are defense-in-depth
    }

    // Get the daemon address from the handle
    let daemon_addr = handle
        .daemon_addr
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No daemon address available for container"))?;

    tracing::info!(
        container_id = %info.container_id,
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
        container_id = %info.container_id,
        container_id = %handle.id,
        "Daemon ready, waiting for HTTP API commands"
    );

    Ok((handle, daemon_client))
}

/// Get the container type as a string for metrics/logging.
fn container_type_str(desc: &FunctionExecutorDescription) -> &'static str {
    match desc.container_type() {
        FunctionExecutorType::Unknown => "unknown",
        FunctionExecutorType::Function => "function",
        FunctionExecutorType::Sandbox => "sandbox",
    }
}

/// Update container counts in the metrics state.
async fn update_container_counts(
    containers: &HashMap<String, ManagedContainer>,
    metrics: &DataplaneMetrics,
) {
    let mut counts = ContainerCounts::default();

    for container in containers.values() {
        let is_sandbox = matches!(
            container.description.container_type(),
            FunctionExecutorType::Sandbox
        );

        match &container.state {
            ContainerState::Pending => {
                if is_sandbox {
                    counts.pending_sandboxes += 1;
                } else {
                    counts.pending_functions += 1;
                }
            }
            ContainerState::Running { .. } => {
                if is_sandbox {
                    counts.running_sandboxes += 1;
                } else {
                    counts.running_functions += 1;
                }
            }
            ContainerState::Stopping { .. } => {
                // Count stopping as still running for metrics purposes
                if is_sandbox {
                    counts.running_sandboxes += 1;
                } else {
                    counts.running_functions += 1;
                }
            }
            ContainerState::Terminated { .. } => {
                // Don't count terminated containers
            }
        }
    }

    metrics.update_container_counts(counts).await;
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use async_trait::async_trait;
    use proto_api::executor_api_pb::{FunctionRef, SandboxMetadata};
    use tempfile::tempdir;

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
                container_ip: "127.0.0.1".to_string(),
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

        async fn get_exit_status(
            &self,
            _handle: &ProcessHandle,
        ) -> anyhow::Result<Option<ExitStatus>> {
            Ok(Some(ExitStatus {
                exit_code: Some(0),
                oom_killed: false,
            }))
        }

        async fn list_containers(&self) -> anyhow::Result<Vec<String>> {
            Ok(vec![])
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
            sandbox_metadata: None,
            container_type: None,
            pool_id: None,
        }
    }

    #[test]
    fn test_function_info_from_description() {
        let desc = create_test_fe_description("fe-123");
        let info = ContainerInfo::from_description(&desc);

        assert_eq!(info.container_id, "fe-123");
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
            sandbox_metadata: None,
            container_type: None,
            pool_id: None,
        };
        let info = ContainerInfo::from_description(&desc);

        assert_eq!(info.container_id, "");
        assert_eq!(info.namespace, "");
        assert_eq!(info.app, "");
        assert_eq!(info.fn_name, "");
        assert_eq!(info.app_version, "");
    }

    #[test]
    fn test_default_image_resolver_no_image() {
        let resolver = DefaultImageResolver::new();
        let result = resolver.sandbox_image_for_pool("ns", "pool-1");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("No image configured")
        );

        let result = resolver.sandbox_image("ns", "sb-1");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("No image configured")
        );

        let result = resolver.function_image("ns", "app", "fn", "v1");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("No image configured")
        );
    }

    #[test]
    fn test_container_state_to_proto_pending() {
        let container = ManagedContainer {
            description: create_test_fe_description("fe-123"),
            state: ContainerState::Pending,
            created_at: Instant::now(),
            started_at: None,
            sandbox_claimed_at: None,
        };

        let proto_state = container.to_proto_state();
        assert_eq!(
            proto_state.status,
            Some(FunctionExecutorStatus::Pending.into())
        );
        assert!(proto_state.termination_reason.is_none());
    }

    #[test]
    fn test_container_state_to_proto_terminated() {
        let container = ManagedContainer {
            description: create_test_fe_description("fe-123"),
            state: ContainerState::Terminated {
                reason: FunctionExecutorTerminationReason::StartupFailedInternalError,
            },
            created_at: Instant::now(),
            started_at: None,
            sandbox_claimed_at: None,
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

    fn create_test_metrics() -> Arc<DataplaneMetrics> {
        Arc::new(DataplaneMetrics::new())
    }

    async fn create_test_state_file() -> Arc<StateFile> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");
        // Leak the tempdir so it doesn't get cleaned up during the test
        std::mem::forget(dir);
        Arc::new(StateFile::new(&path).await.unwrap())
    }

    #[tokio::test]
    async fn test_manager_new() {
        let driver = Arc::new(MockProcessDriver::new());
        let resolver = Arc::new(DefaultImageResolver::new());
        let metrics = create_test_metrics();
        let state_file = create_test_state_file().await;

        let manager = FunctionContainerManager::new(
            driver,
            resolver,
            metrics,
            state_file,
            "test-executor".to_string(),
        );
        let states = manager.get_states().await;

        assert!(states.is_empty());
    }

    #[tokio::test]
    async fn test_sync_creates_containers() {
        let driver = Arc::new(MockProcessDriver::new());
        let resolver = Arc::new(DefaultImageResolver::new());
        let metrics = create_test_metrics();
        let state_file = create_test_state_file().await;
        let manager = FunctionContainerManager::new(
            driver.clone(),
            resolver,
            metrics,
            state_file,
            "test-executor".to_string(),
        );

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
        let resolver = Arc::new(DefaultImageResolver::new());
        let metrics = create_test_metrics();
        let state_file = create_test_state_file().await;
        let manager = FunctionContainerManager::new(
            driver.clone(),
            resolver,
            metrics,
            state_file,
            "test-executor".to_string(),
        );

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
        let resolver = Arc::new(DefaultImageResolver::new());
        let metrics = create_test_metrics();
        let state_file = create_test_state_file().await;
        let manager = FunctionContainerManager::new(
            driver.clone(),
            resolver,
            metrics,
            state_file,
            "test-executor".to_string(),
        );

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
        let resolver = Arc::new(DefaultImageResolver::new());
        let metrics = create_test_metrics();
        let state_file = create_test_state_file().await;
        let manager = FunctionContainerManager::new(
            driver.clone(),
            resolver,
            metrics,
            state_file,
            "test-executor".to_string(),
        );

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
        desc.sandbox_metadata = Some(SandboxMetadata {
            image: None,
            timeout_secs: Some(timeout_secs),
            entrypoint: vec![],
            network_policy: None,
            sandbox_id: None,
        });
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
            container_ip: "172.17.0.2".to_string(),
        }
    }

    #[test]
    fn test_is_timed_out_no_timeout_configured() {
        // Container with no timeout (timeout_secs = 0) should never time out
        let container = ManagedContainer {
            description: create_test_fe_description("fe-123"),
            state: ContainerState::Pending,
            created_at: Instant::now(),
            started_at: Some(Instant::now() - Duration::from_secs(1000)),
            sandbox_claimed_at: Some(Instant::now() - Duration::from_secs(1000)),
        };

        assert!(!container.is_timed_out());
    }

    #[test]
    fn test_is_timed_out_not_running() {
        // Container that's not running should not report as timed out
        let container = ManagedContainer {
            description: create_test_fe_description_with_timeout("fe-123", 10),
            state: ContainerState::Pending,
            created_at: Instant::now(),
            started_at: Some(Instant::now() - Duration::from_secs(100)),
            sandbox_claimed_at: Some(Instant::now() - Duration::from_secs(100)),
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
            created_at: Instant::now(),
            started_at: None,
            sandbox_claimed_at: None,
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
            created_at: Instant::now(),
            started_at: Some(Instant::now()),         // Just started
            sandbox_claimed_at: Some(Instant::now()), // Just claimed
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
            created_at: Instant::now(),
            started_at: Some(Instant::now() - Duration::from_secs(20)),
            // Claimed 15 seconds ago, so 10 second timeout is exceeded
            sandbox_claimed_at: Some(Instant::now() - Duration::from_secs(15)),
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
            created_at: Instant::now(),
            started_at: Some(Instant::now() - Duration::from_secs(15)),
            // Claimed exactly 10 seconds ago - should be timed out (>= comparison)
            sandbox_claimed_at: Some(Instant::now() - Duration::from_secs(10)),
        };

        assert!(container.is_timed_out());
    }

    #[tokio::test]
    async fn test_check_timeouts_terminates_expired_container() {
        // This test verifies that check_timeouts() terminates containers that have
        // exceeded their timeout
        let driver = Arc::new(MockProcessDriver::new());
        let resolver = Arc::new(DefaultImageResolver::new());
        let metrics = create_test_metrics();
        let state_file = create_test_state_file().await;
        let manager = FunctionContainerManager::new(
            driver.clone(),
            resolver,
            metrics,
            state_file,
            "test-executor".to_string(),
        );

        // Insert a container that was claimed 10 seconds ago with a 5 second timeout
        // (already timed out)
        {
            let mut containers = manager.containers.write().await;
            let container = ManagedContainer {
                description: create_test_fe_description_with_timeout("fe-timeout-test", 5),
                state: ContainerState::Running {
                    handle: create_mock_handle("test-container"),
                    daemon_client: create_mock_daemon_client(),
                },
                created_at: Instant::now(),
                started_at: Some(Instant::now() - Duration::from_secs(15)),
                // Set sandbox_claimed_at to 10 seconds ago - well past the 5 second timeout
                sandbox_claimed_at: Some(Instant::now() - Duration::from_secs(10)),
            };
            containers.insert("fe-timeout-test".to_string(), container);
        }

        // Verify container is timed out
        {
            let containers = manager.containers.write().await;
            let container = containers.get("fe-timeout-test").unwrap();
            assert!(container.is_timed_out(), "Container should be timed out");
        }

        // Run check_timeouts - this should initiate stop for the timed out container
        manager.check_timeouts().await;

        // Container should now be in Stopping state (initiate_stop was called)
        {
            let containers = manager.containers.write().await;
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
        let resolver = Arc::new(DefaultImageResolver::new());
        let metrics = create_test_metrics();
        let state_file = create_test_state_file().await;
        let manager = FunctionContainerManager::new(
            driver.clone(),
            resolver,
            metrics,
            state_file,
            "test-executor".to_string(),
        );

        // Insert a container that just started with a 600 second timeout
        {
            let mut containers = manager.containers.write().await;
            let container = ManagedContainer {
                description: create_test_fe_description_with_timeout("fe-not-expired", 600),
                state: ContainerState::Running {
                    handle: create_mock_handle("test-container"),
                    daemon_client: create_mock_daemon_client(),
                },
                created_at: Instant::now(),
                started_at: Some(Instant::now()), // Just started
                sandbox_claimed_at: Some(Instant::now()), // Just claimed
            };
            containers.insert("fe-not-expired".to_string(), container);
        }

        // Run check_timeouts
        manager.check_timeouts().await;

        // Container should still be running
        {
            let containers = manager.containers.write().await;
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
        let resolver = Arc::new(DefaultImageResolver::new());
        let metrics = create_test_metrics();
        let state_file = create_test_state_file().await;
        let manager = FunctionContainerManager::new(
            driver.clone(),
            resolver,
            metrics,
            state_file,
            "test-executor".to_string(),
        );

        // Insert a container with no timeout that started a long time ago
        {
            let mut containers = manager.containers.write().await;
            let container = ManagedContainer {
                description: create_test_fe_description("fe-no-timeout"), // No timeout (None)
                state: ContainerState::Running {
                    handle: create_mock_handle("test-container"),
                    daemon_client: create_mock_daemon_client(),
                },
                created_at: Instant::now(),
                // Started 1 hour ago, but has no timeout so shouldn't be stopped
                started_at: Some(Instant::now() - Duration::from_secs(3600)),
                sandbox_claimed_at: None,
            };
            containers.insert("fe-no-timeout".to_string(), container);
        }

        // Run check_timeouts
        manager.check_timeouts().await;

        // Container should still be running (no timeout configured)
        {
            let containers = manager.containers.write().await;
            let container = containers.get("fe-no-timeout").unwrap();
            assert!(
                matches!(container.state, ContainerState::Running { .. }),
                "Container with no timeout should still be running"
            );
        }
    }

    #[tokio::test]
    async fn test_warm_container_does_not_time_out() {
        // A warm pool container (no sandbox_id, no sandbox_claimed_at)
        // should NOT time out even when started_at has elapsed past the timeout.
        let container = ManagedContainer {
            description: create_test_fe_description_with_timeout("fe-warm", 10), // 10 sec timeout
            state: ContainerState::Running {
                handle: create_mock_handle("test-container"),
                daemon_client: create_mock_daemon_client(),
            },
            created_at: Instant::now(),
            // Started 100 seconds ago — well past timeout
            started_at: Some(Instant::now() - Duration::from_secs(100)),
            // But never claimed by a sandbox
            sandbox_claimed_at: None,
        };

        assert!(
            !container.is_timed_out(),
            "Warm container without sandbox_claimed_at should not time out"
        );
    }

    #[tokio::test]
    async fn test_sync_sets_sandbox_claimed_at_on_claim() {
        // When sync() sees sandbox_id transition from None to Some on an
        // existing container, it should set sandbox_claimed_at.
        let driver = Arc::new(MockProcessDriver::new());
        let resolver = Arc::new(DefaultImageResolver::new());
        let metrics = create_test_metrics();
        let state_file = create_test_state_file().await;
        let manager = FunctionContainerManager::new(
            driver.clone(),
            resolver,
            metrics,
            state_file,
            "test-executor".to_string(),
        );

        // Insert a warm container (no sandbox_id)
        {
            let mut containers = manager.containers.write().await;
            let container = ManagedContainer {
                description: create_test_fe_description_with_timeout("fe-warm-claim", 60),
                state: ContainerState::Running {
                    handle: create_mock_handle("test-container"),
                    daemon_client: create_mock_daemon_client(),
                },
                created_at: Instant::now(),
                started_at: Some(Instant::now()),
                sandbox_claimed_at: None,
            };
            containers.insert("fe-warm-claim".to_string(), container);
        }

        // Verify sandbox_claimed_at is None
        {
            let containers = manager.containers.read().await;
            let container = containers.get("fe-warm-claim").unwrap();
            assert!(
                container.sandbox_claimed_at.is_none(),
                "sandbox_claimed_at should be None before claim"
            );
        }

        // Sync with a description that has sandbox_id set (simulates server claiming)
        let mut desc = create_test_fe_description_with_timeout("fe-warm-claim", 60);
        desc.sandbox_metadata.as_mut().unwrap().sandbox_id = Some("sandbox-abc".to_string());
        manager.sync(vec![desc]).await;

        // Verify sandbox_claimed_at is now set
        {
            let containers = manager.containers.read().await;
            let container = containers.get("fe-warm-claim").unwrap();
            assert!(
                container.sandbox_claimed_at.is_some(),
                "sandbox_claimed_at should be set after sandbox_id transition"
            );
            assert_eq!(
                container
                    .description
                    .sandbox_metadata
                    .as_ref()
                    .unwrap()
                    .sandbox_id,
                Some("sandbox-abc".to_string()),
                "Description should have updated sandbox_id"
            );
        }
    }

    #[tokio::test]
    async fn test_lookup_sandbox_pool_container() {
        // Pool containers have container_id (nanoid) != sandbox_id.
        // lookup_sandbox must find them via the sandbox_metadata fallback scan.
        let driver = Arc::new(MockProcessDriver::new());
        let resolver = Arc::new(DefaultImageResolver::new());
        let metrics = create_test_metrics();
        let state_file = create_test_state_file().await;
        let manager = FunctionContainerManager::new(
            driver.clone(),
            resolver,
            metrics,
            state_file,
            "test-executor".to_string(),
        );

        // Insert a running pool container with a nanoid container_id and a
        // sandbox_id that differs from the container_id.
        let mut desc = create_test_fe_description_with_timeout("pool-nanoid-123", 60);
        desc.sandbox_metadata.as_mut().unwrap().sandbox_id = Some("sandbox-xyz".to_string());

        {
            let mut containers = manager.containers.write().await;
            containers.insert(
                "pool-nanoid-123".to_string(),
                ManagedContainer {
                    description: desc,
                    state: ContainerState::Running {
                        handle: create_mock_handle("test-container"),
                        daemon_client: create_mock_daemon_client(),
                    },
                    created_at: Instant::now(),
                    started_at: Some(Instant::now()),
                    sandbox_claimed_at: Some(Instant::now()),
                },
            );
            containers.index_sandbox("sandbox-xyz".to_string(), "pool-nanoid-123".to_string());
        }

        // Direct lookup by container_id should work
        let result = manager.lookup_sandbox("pool-nanoid-123", 8080).await;
        assert!(
            matches!(result, SandboxLookupResult::Running(_)),
            "Direct lookup by container_id should find the container"
        );

        // Lookup by sandbox_id (different from container_id) should also work
        // via the secondary index
        let result = manager.lookup_sandbox("sandbox-xyz", 8080).await;
        assert!(
            matches!(result, SandboxLookupResult::Running(_)),
            "Index lookup by sandbox_id should find the pool container"
        );

        // Lookup with unknown sandbox_id should return NotFound
        let result = manager.lookup_sandbox("unknown-id", 8080).await;
        assert!(
            matches!(result, SandboxLookupResult::NotFound),
            "Unknown sandbox_id should return NotFound"
        );
    }
}
