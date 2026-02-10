mod health;
mod image_resolver;
mod lifecycle;
mod types;

use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

pub use image_resolver::{DefaultImageResolver, ImageResolver};
use proto_api::executor_api_pb::{
    FunctionExecutorDescription,
    FunctionExecutorState,
    FunctionExecutorTerminationReason,
};
use tokio::sync::RwLock;
use tracing::Instrument;
use types::container_type_str;

use self::types::{
    ContainerInfo,
    ContainerState,
    ContainerStore,
    ManagedContainer,
    update_container_counts,
};
use crate::{
    daemon_client::DaemonClient,
    driver::{ProcessDriver, ProcessHandle},
    metrics::DataplaneMetrics,
    network_rules,
    state_file::StateFile,
};

const KILL_GRACE_PERIOD: Duration = Duration::from_secs(10);

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
            if let Some((id, container, sandbox_id)) = self.recover_single_entry(&entry).await {
                let mut containers = self.containers.write().await;
                containers.insert(id.clone(), container);
                if let Some(sid) = sandbox_id {
                    containers.index_sandbox(sid, id);
                }
                recovered += 1;
            }
        }

        recovered
    }

    /// Try to recover a single container from a persisted state entry.
    ///
    /// Returns `(container_id, container, optional_sandbox_id)` on success,
    /// or `None` if the container is dead or unrecoverable (cleans up the
    /// state file entry on failure).
    async fn recover_single_entry(
        &self,
        entry: &crate::state_file::PersistedContainer,
    ) -> Option<(String, ManagedContainer, Option<String>)> {
        let handle = ProcessHandle {
            id: entry.handle_id.clone(),
            daemon_addr: Some(entry.daemon_addr.clone()),
            http_addr: Some(entry.http_addr.clone()),
            container_ip: entry.container_ip.clone(),
        };

        let alive = match self.driver.alive(&handle).await {
            Ok(true) => true,
            Ok(false) => {
                tracing::info!(
                    container_id = %entry.container_id,
                    handle_id = %entry.handle_id,
                    "Container no longer alive, removing from state"
                );
                let _ = self.state_file.remove(&entry.container_id).await;
                return None;
            }
            Err(e) => {
                tracing::warn!(
                    container_id = %entry.container_id,
                    handle_id = %entry.handle_id,
                    error = %e,
                    "Failed to check container status, removing from state"
                );
                let _ = self.state_file.remove(&entry.container_id).await;
                return None;
            }
        };
        debug_assert!(alive);

        let daemon_client = match DaemonClient::connect_with_retry(
            &entry.daemon_addr,
            std::time::Duration::from_secs(5),
        )
        .await
        {
            Ok(dc) => dc,
            Err(e) => {
                tracing::warn!(
                    container_id = %entry.container_id,
                    handle_id = %entry.handle_id,
                    error = %e,
                    "Failed to reconnect to daemon, removing from state"
                );
                let _ = self.state_file.remove(&entry.container_id).await;
                return None;
            }
        };

        let description = match entry.decode_description() {
            Some(desc) => desc,
            None => {
                tracing::warn!(
                    container_id = %entry.container_id,
                    "No description stored in state file, skipping recovery"
                );
                let _ = self.state_file.remove(&entry.container_id).await;
                return None;
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
        let sandbox_claimed_at = sandbox_id_for_index.as_ref().map(|_| Instant::now());
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

        Some((entry.container_id.clone(), container, sandbox_id_for_index))
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

        self.stop_timed_out_containers(&mut containers).await;
        self.remove_undesired_containers(&mut containers, &desired_ids)
            .await;

        for desc in desired {
            let Some(id) = desc.id.clone() else { continue };
            if !containers.contains_key(&id) {
                self.create_container(&mut containers, desc).await;
            } else {
                self.update_existing_container(&mut containers, &id, desc)
                    .await;
            }
        }

        update_container_counts(&containers, &self.metrics).await;
    }

    /// Remove containers that are no longer in the desired state.
    /// Terminated containers are removed from memory and state file.
    /// Running/pending containers are signaled to stop.
    async fn remove_undesired_containers(
        &self,
        containers: &mut ContainerStore,
        desired_ids: &HashSet<String>,
    ) {
        let current_ids: Vec<String> = containers.keys().cloned().collect();
        for id in current_ids {
            if !desired_ids.contains(&id) &&
                let Some(container) = containers.get_mut(&id)
            {
                let info = container.info();
                match &container.state {
                    ContainerState::Terminated { .. } => {
                        tracing::info!(
                            parent: &info.tracing_span(),
                            "Removed terminated container from memory"
                        );
                        if let Err(e) = self.state_file.remove(&id).await {
                            tracing::warn!(
                                container_id = %info.container_id,
                                error = %e,
                                "Failed to remove container from state file"
                            );
                        }
                        containers.remove(&id);
                    }
                    ContainerState::Stopping { .. } => {
                        // Already stopping, let it continue
                    }
                    ContainerState::Pending | ContainerState::Running { .. } => {
                        self.initiate_stop(
                            container,
                            FunctionExecutorTerminationReason::FunctionCancelled,
                        )
                        .await;
                    }
                }
            }
        }
    }

    /// Create a new container from a desired description.
    /// Inserts a Pending container, indexes sandbox, and spawns the lifecycle
    /// task.
    async fn create_container(
        &self,
        containers: &mut ContainerStore,
        desc: FunctionExecutorDescription,
    ) {
        let id = match &desc.id {
            Some(id) => id.clone(),
            None => return,
        };

        let info = ContainerInfo::from_description(&desc);
        let container_type = container_type_str(&desc);

        tracing::info!(
            parent: &info.tracing_span(),
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
        let executor_id = self.executor_id.clone();

        tokio::spawn(
            async move {
                let result =
                    lifecycle::start_container_with_daemon(&driver, &image_resolver, &desc).await;
                lifecycle::handle_container_startup_result(
                    id,
                    desc,
                    result,
                    containers_ref,
                    metrics,
                    state_file,
                )
                .await;
            }
            .instrument(tracing::info_span!("container_lifecycle", %executor_id)),
        );
    }

    /// Update an existing container's description.
    /// Handles warm→claimed sandbox transitions and description updates.
    async fn update_existing_container(
        &self,
        containers: &mut ContainerStore,
        id: &str,
        desc: FunctionExecutorDescription,
    ) {
        let old_sandbox_id = containers
            .get(id)
            .and_then(|c| c.description.sandbox_metadata.as_ref())
            .and_then(|m| m.sandbox_id.clone());
        let new_sandbox_id = desc
            .sandbox_metadata
            .as_ref()
            .and_then(|m| m.sandbox_id.clone());

        if let Some(container) = containers.get_mut(id) {
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
                containers.index_sandbox(new_sid, id.to_string());
            }
        }
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

        // Handle based on current state
        if matches!(container.state, ContainerState::Pending) {
            self.metrics
                .counters
                .record_container_terminated(container_type, "cancelled_pending");
            if let Err(e) = container.transition_to_terminated(reason) {
                tracing::warn!(container_id = %id, error = %e, "Invalid state transition");
            }
            return;
        }

        // Try Running → Stopping transition
        let (handle, daemon_client) = match container.transition_to_stopping(reason) {
            Ok((h, dc)) => (h, dc),
            Err(_) => return, // Not in Running state
        };

        let span = container.info().tracing_span();

        tracing::info!(
            parent: &span,
            container_type = %container_type,
            event = "container_stopping",
            "Signaling container to stop via daemon"
        );

        send_stop_signal(&*self.driver, &handle, daemon_client.clone(), &span).await;

        // Remove from state file since container is no longer running
        if let Err(e) = self.state_file.remove(&id).await {
            tracing::warn!(
                parent: &span,
                error = %e,
                "Failed to remove container from state file"
            );
        }

        spawn_grace_period_kill(
            id,
            container_type.to_string(),
            self.containers.clone(),
            self.driver.clone(),
            self.metrics.clone(),
            span,
        );
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

/// Send SIGTERM to a container via daemon first, falling back to docker signal.
async fn send_stop_signal(
    driver: &dyn ProcessDriver,
    handle: &ProcessHandle,
    daemon_client: Option<DaemonClient>,
    span: &tracing::Span,
) {
    if let Some(mut client) = daemon_client {
        if let Err(e) = client.send_signal(15).await {
            tracing::info!(
                parent: span,
                error = %e,
                "No process to signal via daemon, falling back to container signal"
            );
            if let Err(e) = driver.send_sig(handle, 15).await {
                tracing::warn!(
                    parent: span,
                    error = %e,
                    "Failed to send signal to container"
                );
            }
        }
    } else if let Err(e) = driver.send_sig(handle, 15).await {
        tracing::warn!(
            parent: span,
            error = %e,
            "Failed to send signal to container"
        );
    }
}

/// Spawn a task that kills a container after the grace period expires.
fn spawn_grace_period_kill(
    container_id: String,
    container_type: String,
    containers_ref: Arc<RwLock<ContainerStore>>,
    driver: Arc<dyn ProcessDriver>,
    metrics: Arc<DataplaneMetrics>,
    span: tracing::Span,
) {
    tokio::spawn(
        async move {
            tokio::time::sleep(KILL_GRACE_PERIOD).await;

            let mut containers = containers_ref.write().await;
            if let Some(container) = containers.get_mut(&container_id) &&
                let ContainerState::Stopping { handle, reason, .. } = &container.state
            {
                let handle = handle.clone();
                let termination_reason = *reason;
                let run_duration_ms = container
                    .started_at
                    .map(|s| s.elapsed().as_millis())
                    .unwrap_or(0);

                tracing::info!(
                    container_type = %container_type,
                    run_duration_ms = %run_duration_ms,
                    event = "container_killing",
                    "Killing container after grace period"
                );

                if let Err(e) = network_rules::remove_rules(&handle.id, &handle.container_ip) {
                    tracing::warn!(error = %e, "Failed to remove network rules");
                }

                if let Err(e) = driver.kill(&handle).await {
                    tracing::warn!(error = %e, "Failed to kill container");
                }

                metrics
                    .counters
                    .record_container_terminated(&container_type, "grace_period_kill");

                tracing::info!(
                    container_type = %container_type,
                    run_duration_ms = %run_duration_ms,
                    event = "container_terminated",
                    "Container terminated"
                );

                if let Err(e) = container.transition_to_terminated(termination_reason) {
                    tracing::warn!(error = %e, "Invalid state transition in grace period kill");
                }

                update_container_counts(&containers, &metrics).await;
            }
        }
        .instrument(span),
    );
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use async_trait::async_trait;
    use proto_api::executor_api_pb::{FunctionExecutorStatus, FunctionRef, SandboxMetadata};
    use tempfile::tempdir;

    use super::*;
    use crate::driver::ExitStatus;

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
        async fn start(
            &self,
            _config: crate::driver::ProcessConfig,
        ) -> anyhow::Result<ProcessHandle> {
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

    async fn create_test_manager() -> (Arc<MockProcessDriver>, FunctionContainerManager) {
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
        (driver, manager)
    }

    #[tokio::test]
    async fn test_manager_new() {
        let (_driver, manager) = create_test_manager().await;
        let states = manager.get_states().await;

        assert!(states.is_empty());
    }

    #[tokio::test]
    async fn test_sync_creates_containers() {
        let (driver, manager) = create_test_manager().await;

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
        let (_driver, manager) = create_test_manager().await;

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
        let (driver, manager) = create_test_manager().await;

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
        let (driver, manager) = create_test_manager().await;

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

    fn create_mock_handle(id: &str) -> ProcessHandle {
        ProcessHandle {
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
        let (_driver, manager) = create_test_manager().await;

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
        let (_driver, manager) = create_test_manager().await;

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
        let (_driver, manager) = create_test_manager().await;

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
        let (_driver, manager) = create_test_manager().await;

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
        let (_driver, manager) = create_test_manager().await;

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
