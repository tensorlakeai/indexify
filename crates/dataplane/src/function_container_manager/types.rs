//! Internal data types for the function container manager.

use std::{collections::HashMap, time::Instant};

use proto_api::executor_api_pb::{
    ContainerDescription,
    ContainerState as ProtoContainerState,
    ContainerStatus,
    ContainerTerminationReason,
    ContainerType,
};

use crate::{
    daemon_client::DaemonClient,
    driver::ProcessHandle,
    metrics::{ContainerCounts, DataplaneMetrics},
};

// ---------------------------------------------------------------------------
// ContainerState
// ---------------------------------------------------------------------------

pub(super) enum ContainerState {
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
        reason: ContainerTerminationReason,
    },
    /// Container has terminated.
    Terminated {
        reason: ContainerTerminationReason,
    },
}

// ---------------------------------------------------------------------------
// ContainerInfo
// ---------------------------------------------------------------------------

/// Helper struct for structured logging of container info.
pub(super) struct ContainerInfo<'a> {
    pub container_id: &'a str,
    pub executor_id: &'a str,
    pub namespace: &'a str,
    pub app: &'a str,
    pub fn_name: &'a str,
    pub app_version: &'a str,
    pub sandbox_id: Option<&'a str>,
    pub pool_id: Option<&'a str>,
}

impl<'a> ContainerInfo<'a> {
    pub fn from_description(desc: &'a ContainerDescription, executor_id: &'a str) -> Self {
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
        let pool_id = desc.pool_id.as_deref();

        Self {
            container_id,
            executor_id,
            namespace,
            app,
            fn_name,
            app_version,
            sandbox_id,
            pool_id,
        }
    }

    /// Create a tracing span with all container identity fields.
    ///
    /// Log calls within this span (via `parent: &span` or `.instrument(span)`)
    /// automatically inherit the container fields, eliminating repetition.
    pub fn tracing_span(&self) -> tracing::Span {
        tracing::info_span!(
            "container",
            container_id = %self.container_id,
            executor_id = %self.executor_id,
            namespace = %self.namespace,
            app = %self.app,
            fn = %self.fn_name,
            version = %self.app_version,
            sandbox_id = ?self.sandbox_id,
            pool_id = ?self.pool_id,
        )
    }
}

// ---------------------------------------------------------------------------
// ManagedContainer
// ---------------------------------------------------------------------------

/// A managed function executor container.
pub(super) struct ManagedContainer {
    pub description: ContainerDescription,
    pub executor_id: String,
    pub state: ContainerState,
    /// When the container was created (for latency tracking)
    pub created_at: Instant,
    /// When the container started running (set when state becomes Running)
    pub started_at: Option<Instant>,
    /// When a sandbox claimed this container
    pub sandbox_claimed_at: Option<Instant>,
}

impl ManagedContainer {
    pub fn to_proto_state(&self) -> ProtoContainerState {
        let (status, termination_reason) = match &self.state {
            ContainerState::Pending => (ContainerStatus::Pending, None),
            ContainerState::Running { .. } => (ContainerStatus::Running, None),
            ContainerState::Stopping { .. } => (ContainerStatus::Running, None),
            ContainerState::Terminated { reason } => {
                (ContainerStatus::Terminated, Some(*reason))
            }
        };

        ProtoContainerState {
            description: Some(self.description.clone()),
            status: Some(status.into()),
            termination_reason: termination_reason.map(|r| r.into()),
        }
    }

    pub fn info(&self) -> ContainerInfo<'_> {
        ContainerInfo::from_description(&self.description, &self.executor_id)
    }

    // -- State transition methods ---------------------------------------------
    // Valid transitions:
    //   Pending  → Running | Terminated
    //   Running  → Stopping | Terminated
    //   Stopping → Terminated

    /// Transition from Pending to Running. Sets `started_at`.
    /// Returns `Err` if the current state is not Pending.
    pub fn transition_to_running(
        &mut self,
        handle: ProcessHandle,
        daemon_client: DaemonClient,
    ) -> Result<(), String> {
        if !matches!(self.state, ContainerState::Pending) {
            return Err(format!(
                "Invalid transition to Running from {:?}",
                self.state_name()
            ));
        }
        self.state = ContainerState::Running {
            handle,
            daemon_client,
        };
        self.started_at = Some(Instant::now());
        Ok(())
    }

    /// Transition from Running to Stopping.
    /// Returns `Err` if the current state is not Running.
    pub fn transition_to_stopping(
        &mut self,
        reason: ContainerTerminationReason,
    ) -> Result<(ProcessHandle, Option<DaemonClient>), String> {
        if !matches!(self.state, ContainerState::Running { .. }) {
            return Err(format!(
                "Invalid transition to Stopping from {:?}",
                self.state_name()
            ));
        }
        // Safe: we just verified the state is Running above.
        let ContainerState::Running {
            handle,
            daemon_client,
        } = std::mem::replace(&mut self.state, ContainerState::Pending)
        else {
            unreachable!()
        };
        self.state = ContainerState::Stopping {
            handle: handle.clone(),
            daemon_client: Some(daemon_client.clone()),
            reason,
        };
        Ok((handle, Some(daemon_client)))
    }

    /// Transition to Terminated from Pending, Running, or Stopping.
    /// Returns `Err` if already Terminated.
    pub fn transition_to_terminated(
        &mut self,
        reason: ContainerTerminationReason,
    ) -> Result<(), String> {
        match &self.state {
            ContainerState::Terminated { .. } => Err("Already in Terminated state".to_string()),
            _ => {
                self.state = ContainerState::Terminated { reason };
                Ok(())
            }
        }
    }

    /// Human-readable name of the current state (for error messages).
    fn state_name(&self) -> &'static str {
        match &self.state {
            ContainerState::Pending => "Pending",
            ContainerState::Running { .. } => "Running",
            ContainerState::Stopping { .. } => "Stopping",
            ContainerState::Terminated { .. } => "Terminated",
        }
    }

    /// Check if this sandbox container has exceeded its timeout.
    /// Returns true if the container should be terminated due to timeout.
    pub fn is_timed_out(&self) -> bool {
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

// ---------------------------------------------------------------------------
// ContainerStore
// ---------------------------------------------------------------------------

/// Container storage with a secondary index for O(1) sandbox_id lookups.
///
/// The primary map is keyed by container_id (nanoid). Pool containers have a
/// container_id that differs from the sandbox_id they serve, so a secondary
/// index maps sandbox_id -> container_id for fast proxy routing.
pub(super) struct ContainerStore {
    map: HashMap<String, ManagedContainer>,
    /// sandbox_id -> container_id for containers that have been claimed by a
    /// sandbox.
    sandbox_index: HashMap<String, String>,
}

impl ContainerStore {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            sandbox_index: HashMap::new(),
        }
    }

    /// Look up a container by sandbox_id (O(1) via secondary index).
    pub fn get_by_sandbox_id(&self, sandbox_id: &str) -> Option<&ManagedContainer> {
        self.sandbox_index
            .get(sandbox_id)
            .and_then(|cid| self.map.get(cid))
    }

    /// Update the sandbox index when a container gains a sandbox_id.
    pub fn index_sandbox(&mut self, sandbox_id: String, container_id: String) {
        self.sandbox_index.insert(sandbox_id, container_id);
    }

    /// Remove a sandbox_id from the index.
    pub fn unindex_sandbox(&mut self, sandbox_id: &str) {
        self.sandbox_index.remove(sandbox_id);
    }

    pub fn get(&self, key: &str) -> Option<&ManagedContainer> {
        self.map.get(key)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut ManagedContainer> {
        self.map.get_mut(key)
    }

    /// Insert a container. Does NOT auto-index sandbox (caller controls that).
    pub fn insert(&mut self, key: String, value: ManagedContainer) {
        self.map.insert(key, value);
    }

    /// Remove a container. Automatically unindexes sandbox if the removed
    /// container had a sandbox_id.
    pub fn remove(&mut self, key: &str) -> Option<ManagedContainer> {
        let removed = self.map.remove(key);
        if let Some(ref container) = removed &&
            let Some(sid) = container
                .description
                .sandbox_metadata
                .as_ref()
                .and_then(|m| m.sandbox_id.as_ref())
        {
            self.sandbox_index.remove(sid.as_str());
        }
        removed
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.map.keys()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &ManagedContainer)> {
        self.map.iter()
    }

    pub fn values(&self) -> impl Iterator<Item = &ManagedContainer> {
        self.map.values()
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.map.len()
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

/// Map an exit status to a termination reason.
pub(super) fn termination_reason_from_exit_status(
    exit_status: Option<crate::driver::ExitStatus>,
) -> ContainerTerminationReason {
    match exit_status {
        Some(status) if status.oom_killed => ContainerTerminationReason::Oom,
        Some(status) => match status.exit_code {
            // The daemon exits with code 0 when its configured timeout elapses.
            Some(0) => ContainerTerminationReason::FunctionTimeout,
            Some(137) => ContainerTerminationReason::Oom,
            Some(143) => ContainerTerminationReason::FunctionCancelled,
            _ => ContainerTerminationReason::Unknown,
        },
        None => ContainerTerminationReason::Unknown,
    }
}

/// Get the container type as a string for metrics/logging.
pub(super) fn container_type_str(desc: &ContainerDescription) -> &'static str {
    match desc.container_type() {
        ContainerType::Unknown => "unknown",
        ContainerType::Function => "function",
        ContainerType::Sandbox => "sandbox",
    }
}

/// Update container counts in the metrics state.
pub(super) async fn update_container_counts(
    containers: &ContainerStore,
    metrics: &DataplaneMetrics,
) {
    let mut counts = ContainerCounts::default();

    for container in containers.values() {
        let is_sandbox = matches!(
            container.description.container_type(),
            ContainerType::Sandbox
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
