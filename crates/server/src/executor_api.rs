use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
    vec,
};

use anyhow::Result;
use executor_api_pb::executor_api_server::ExecutorApi;
pub use proto_api::executor_api_pb;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

mod heartbeat_helpers;
mod report_processing;
mod result_routing;

pub use report_processing::{
    process_allocation_completed,
    process_allocation_failed,
    process_command_responses,
};
pub use result_routing::FunctionCallResultRouter;
use result_routing::{handle_log_entry, try_route_failure, try_route_result};

use crate::{
    blob_store::registry::BlobStorageRegistry,
    data_model::{self, ExecutorId, ExecutorMetadataBuilder, FunctionAllowlist},
    executors::{ExecutorManager, ExecutorStateSnapshot},
    state_store::{
        IndexifyState,
        requests::{RequestPayload, StateMachineUpdateRequest, UpsertExecutorRequest},
    },
};

// Proto <-> internal conversion impls live in crate::proto_convert.

/// Pure, stateful diff engine that compares the current `ExecutorStateSnapshot`
/// against what it previously saw and produces typed `Command` messages.
///
/// On the first call, the tracking sets are empty so everything is "new" ---
/// producing AddContainer + RunAllocation for full state (equivalent to
/// initial sync).
///
/// `command_seq = 0` means the command is informational / unsolicited.
#[derive(Clone)]
pub struct CommandEmitter {
    next_seq: u64,
    /// Container descriptions sent via AddContainer, keyed by container ID.
    /// Tracked as full descriptions so we can detect changes and emit
    /// `UpdateContainerDescription` commands.
    pub(crate) known_containers: HashMap<String, executor_api_pb::ContainerDescription>,
    /// Allocation IDs sent via RunAllocation.
    pub(crate) known_allocations: HashSet<String>,
    /// Snapshot IDs sent via SnapshotContainer.
    pub(crate) known_snapshot_ids: HashSet<String>,
    /// Whether this emitter has completed at least one full sync
    /// (commit_snapshot). `false` on initial creation or after
    /// re-registration. `true` after the first successful
    /// `commit_snapshot`. Used by the command generator task to decide:
    /// - `has_synced == false` -> initial full sync needed
    /// - `has_synced == true` -> skip full sync, drain buffered events
    pub has_synced: bool,
}

impl CommandEmitter {
    pub fn new() -> Self {
        Self {
            next_seq: 1,
            known_containers: HashMap::new(),
            known_allocations: HashSet::new(),
            known_snapshot_ids: HashSet::new(),
            has_synced: false,
        }
    }

    pub(crate) fn next_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    /// Diff the current desired state against what was previously seen and
    /// produce a batch of `Command` messages for the delta.
    ///
    /// **Command ordering** matches what `emit_scheduler_events` guarantees:
    /// 1. REMOVALS first — free GPU/memory before new containers claim them
    /// 2. ADDITIONS — new containers
    /// 3. UPDATES — changed descriptions (sandbox_metadata)
    /// 4. ALLOCATIONS — must come after AddContainer so container exists
    /// 5. SNAPSHOTS — new snapshot commands
    ///
    /// **Important**: this does NOT update the emitter's tracking state.
    /// Call [`commit_snapshot`] after all commands have been successfully
    /// delivered to the client so that the tracking sets stay accurate if
    /// delivery fails partway through.
    pub fn emit_commands(
        &mut self,
        snapshot: &ExecutorStateSnapshot,
    ) -> Vec<executor_api_pb::Command> {
        let mut commands = Vec::new();

        let current_containers: HashMap<String, executor_api_pb::ContainerDescription> = snapshot
            .containers
            .iter()
            .filter_map(|fe| fe.id.clone().map(|id| (id, fe.clone())))
            .collect();

        // 1. REMOVALS first — free GPU/memory before new containers claim them
        let removed_containers: Vec<String> = self
            .known_containers
            .keys()
            .filter(|id| !current_containers.contains_key(*id))
            .cloned()
            .collect();
        for id in removed_containers {
            let seq = self.next_seq();
            commands.push(executor_api_pb::Command {
                seq,
                command: Some(executor_api_pb::command::Command::RemoveContainer(
                    executor_api_pb::RemoveContainer {
                        container_id: id,
                        reason: None,
                    },
                )),
            });
        }

        // 2. ADDITIONS — new containers
        for fe in &snapshot.containers {
            if let Some(id) = &fe.id &&
                !self.known_containers.contains_key(id)
            {
                let seq = self.next_seq();
                commands.push(executor_api_pb::Command {
                    seq,
                    command: Some(executor_api_pb::command::Command::AddContainer(
                        executor_api_pb::AddContainer {
                            container: Some(fe.clone()),
                        },
                    )),
                });
            }
        }

        // 3. UPDATES — changed descriptions (sandbox_metadata)
        for fe in &snapshot.containers {
            if let Some(id) = &fe.id &&
                let Some(known) = self.known_containers.get(id) &&
                known != fe
            {
                let mut update = executor_api_pb::UpdateContainerDescription {
                    container_id: id.clone(),
                    sandbox_metadata: None,
                };
                if known.sandbox_metadata != fe.sandbox_metadata {
                    update.sandbox_metadata = fe.sandbox_metadata.clone();
                }
                if update.sandbox_metadata.is_some() {
                    let seq = self.next_seq();
                    commands.push(executor_api_pb::Command {
                        seq,
                        command: Some(
                            executor_api_pb::command::Command::UpdateContainerDescription(update),
                        ),
                    });
                }
            }
        }

        // 4. ALLOCATIONS — must come after AddContainer so container exists
        for allocation in &snapshot.allocations {
            if let Some(id) = &allocation.allocation_id &&
                !self.known_allocations.contains(id)
            {
                let seq = self.next_seq();
                commands.push(executor_api_pb::Command {
                    seq,
                    command: Some(executor_api_pb::command::Command::RunAllocation(
                        executor_api_pb::RunAllocation {
                            allocation: Some(allocation.clone()),
                        },
                    )),
                });
            }
        }

        // 5. SNAPSHOTS — new snapshot commands
        for snap in &snapshot.pending_snapshots {
            if !self.known_snapshot_ids.contains(&snap.snapshot_id) {
                let seq = self.next_seq();
                commands.push(executor_api_pb::Command {
                    seq,
                    command: Some(executor_api_pb::command::Command::SnapshotContainer(
                        executor_api_pb::SnapshotContainer {
                            container_id: snap.container_id.clone(),
                            snapshot_id: snap.snapshot_id.clone(),
                            upload_uri: snap.upload_uri.clone(),
                        },
                    )),
                });
            }
        }

        commands
    }

    /// Commit the snapshot to the emitter's tracking state.
    ///
    /// Call this only after all commands from [`emit_commands`] have been
    /// successfully delivered to the client.  If delivery fails partway
    /// through, skipping this call ensures the next full sync re-emits the
    /// missing commands.
    pub fn commit_snapshot(&mut self, snapshot: &ExecutorStateSnapshot) {
        self.known_containers = snapshot
            .containers
            .iter()
            .filter_map(|fe| fe.id.clone().map(|id| (id, fe.clone())))
            .collect();

        // Allocations that disappear are completed, not killed. We just stop
        // tracking.
        self.known_allocations = snapshot
            .allocations
            .iter()
            .filter_map(|a| a.allocation_id.clone())
            .collect();

        self.known_snapshot_ids = snapshot
            .pending_snapshots
            .iter()
            .map(|s| s.snapshot_id.clone())
            .collect();

        self.has_synced = true;
    }
}

/// Register an executor's full state --- the shared business logic behind
/// both the v2 heartbeat RPC (with `full_state`) and the test
/// infrastructure.
///
/// Always writes an `UpsertExecutor` to the state machine. In the delta
/// model (v2), full state only arrives on first registration or after a
/// FullSync (e.g. post-deletion), so there is no benefit to hashing and
/// skipping writes.
///
/// Callers are responsible for calling `heartbeat_v2()` for liveness
/// before or after this function.
pub async fn sync_executor_full_state(
    executor_manager: &ExecutorManager,
    indexify_state: Arc<IndexifyState>,
    executor: data_model::ExecutorMetadata,
) -> Result<()> {
    // Register runtime data (marks executor as "known" for delta processing)
    executor_manager.register_executor(executor.clone()).await?;

    let upsert_request =
        UpsertExecutorRequest::build(executor, vec![], true, indexify_state.clone())?;

    let sm_req = StateMachineUpdateRequest {
        payload: RequestPayload::UpsertExecutor(upsert_request),
    };
    indexify_state.write(sm_req).await?;

    Ok(())
}

pub struct ExecutorAPIService {
    indexify_state: Arc<IndexifyState>,
    executor_manager: Arc<ExecutorManager>,
    blob_storage_registry: Arc<BlobStorageRegistry>,
    function_call_result_router: Arc<FunctionCallResultRouter>,
}

impl ExecutorAPIService {
    pub fn new(
        indexify_state: Arc<IndexifyState>,
        executor_manager: Arc<ExecutorManager>,
        blob_storage_registry: Arc<BlobStorageRegistry>,
    ) -> Self {
        let function_call_result_router = Arc::new(FunctionCallResultRouter::new());
        executor_manager.set_function_call_result_router(function_call_result_router.clone());

        Self {
            indexify_state,
            executor_manager,
            blob_storage_registry,
            function_call_result_router,
        }
    }

    async fn handle_full_state(
        &self,
        executor_id: &ExecutorId,
        full_state: executor_api_pb::DataplaneStateFullSync,
    ) -> Result<(), Status> {
        // --- Proto -> internal conversion ---

        let mut executor_metadata = ExecutorMetadataBuilder::default();
        executor_metadata.id(executor_id.clone());
        executor_metadata.addr(full_state.hostname.clone().unwrap_or_default());
        executor_metadata.executor_version(full_state.version.unwrap_or_default());
        if let Some(catalog_name) = full_state.catalog_entry_name {
            executor_metadata.catalog_name(Some(catalog_name));
        }
        if let Some(proxy_address) = full_state.proxy_address {
            executor_metadata.proxy_address(Some(proxy_address));
        }

        let host_resources = full_state
            .total_container_resources
            .map(data_model::HostResources::try_from)
            .transpose()
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?
            .unwrap_or_default();
        executor_metadata.host_resources(host_resources);

        let allowed_functions: Vec<FunctionAllowlist> = full_state
            .allowed_functions
            .into_iter()
            .filter_map(|f| FunctionAllowlist::try_from(f).ok())
            .collect();
        if allowed_functions.is_empty() {
            executor_metadata.function_allowlist(None);
        } else {
            executor_metadata.function_allowlist(Some(allowed_functions));
        }
        executor_metadata.labels(full_state.labels.into());
        executor_metadata.state(data_model::ExecutorState::Running);
        executor_metadata.state_hash(String::new());

        let mut containers = imbl::HashMap::new();
        for fe_state in full_state.container_states {
            match data_model::Container::try_from(fe_state) {
                Ok(container) => {
                    containers.insert(container.id.clone(), container);
                }
                Err(e) => {
                    warn!(
                        executor_id = executor_id.get(),
                        error = %e,
                        "skipping container in full state sync"
                    );
                }
            }
        }
        executor_metadata.containers(containers);

        let executor = executor_metadata
            .build()
            .map_err(|e| Status::internal(e.to_string()))?;

        // --- Shared registration logic ---
        sync_executor_full_state(
            &self.executor_manager,
            self.indexify_state.clone(),
            executor,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        // Ensure an ExecutorConnection exists so events can buffer even
        // before the command stream connects.
        self.indexify_state
            .register_executor_connection(executor_id)
            .await;

        // Emit an immediate full-sync command batch for this executor.
        self.executor_manager
            .emit_commands_for_executor(executor_id, true)
            .await;

        Ok(())
    }
}

/// Convert a `data_model::NetworkPolicy` to the proto `NetworkPolicy`.
pub(crate) fn network_policy_to_pb(
    np: &data_model::NetworkPolicy,
) -> executor_api_pb::NetworkPolicy {
    executor_api_pb::NetworkPolicy {
        allow_internet_access: Some(np.allow_internet_access),
        allow_out: np.allow_out.clone(),
        deny_out: np.deny_out.clone(),
    }
}

const LONG_POLL_TIMEOUT: Duration = Duration::from_secs(300);

/// Long-poll helper for the commands buffer.
///
/// Both `poll_commands` and `poll_allocation_results` share this structure:
/// 1. Drain acked items.
/// 2. Clone current items; return early if non-empty.
/// 3. Wait on notify or timeout.
/// 4. Re-clone and return.
///
/// `kind` selects which buffer (commands vs results) to operate on.
async fn long_poll_commands(
    indexify_state: &IndexifyState,
    executor_id: &ExecutorId,
    acked_seq: Option<u64>,
) -> Vec<executor_api_pb::Command> {
    let connections = indexify_state.executor_connections.read().await;
    let Some(conn) = connections.get(executor_id) else {
        return vec![];
    };

    if conn.observe_command_ack(acked_seq) {
        warn!(
            executor_id = executor_id.get(),
            observed_ack = ?acked_seq,
            "detected command ack regression; requesting full state sync"
        );
    }

    if let Some(seq) = acked_seq {
        if let Err(err) = indexify_state.ack_executor_commands(executor_id, seq).await {
            warn!(
                executor_id = executor_id.get(),
                acked_seq = seq,
                error = ?err,
                "failed to persist command ack"
            );
        }
        conn.drain_commands_up_to(seq).await;
    }

    let items = conn.clone_commands().await;
    if !items.is_empty() {
        return items;
    }

    let notify = conn.commands_notify();
    drop(connections);

    tokio::select! {
        _ = notify.notified() => {},
        _ = tokio::time::sleep(LONG_POLL_TIMEOUT) => {},
    }

    let connections = indexify_state.executor_connections.read().await;
    if let Some(conn) = connections.get(executor_id) {
        conn.clone_commands().await
    } else {
        vec![]
    }
}

async fn long_poll_results(
    indexify_state: &IndexifyState,
    executor_id: &ExecutorId,
    acked_seq: Option<u64>,
) -> Vec<executor_api_pb::SequencedAllocationResult> {
    let connections = indexify_state.executor_connections.read().await;
    let Some(conn) = connections.get(executor_id) else {
        return vec![];
    };

    if let Some(seq) = acked_seq {
        conn.drain_results_up_to(seq).await;
    }

    let items = conn.clone_results().await;
    if !items.is_empty() {
        return items;
    }

    let notify = conn.results_notify();
    drop(connections);

    tokio::select! {
        _ = notify.notified() => {},
        _ = tokio::time::sleep(LONG_POLL_TIMEOUT) => {},
    }

    let connections = indexify_state.executor_connections.read().await;
    if let Some(conn) = connections.get(executor_id) {
        conn.clone_results().await
    } else {
        vec![]
    }
}

#[tonic::async_trait]
impl ExecutorApi for ExecutorAPIService {
    async fn heartbeat(
        &self,
        request: Request<executor_api_pb::HeartbeatRequest>,
    ) -> Result<Response<executor_api_pb::HeartbeatResponse>, Status> {
        let executor_api_pb::HeartbeatRequest {
            executor_id: raw_executor_id,
            status,
            full_state,
            command_responses,
            allocation_outcomes,
            allocation_log_entries,
        } = request.into_inner();

        let executor_id: ExecutorId = raw_executor_id
            .ok_or(Status::invalid_argument("executor_id required"))?
            .into();
        let reported_status =
            status.and_then(|s| executor_api_pb::ExecutorStatus::try_from(s).ok());

        // Touch the executor liveness
        self.executor_manager
            .heartbeat_v2(&executor_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let executor_known = self
            .resolve_executor_known(&executor_id, full_state)
            .await?;
        self.process_heartbeat_reports(
            &executor_id,
            executor_known,
            command_responses,
            allocation_outcomes,
            allocation_log_entries,
        )
        .await?;

        if self
            .maybe_deregister_stopped_executor(&executor_id, reported_status)
            .await
        {
            // Executor is intentionally shutting down; do not request full
            // state re-registration.
            return Ok(Response::new(executor_api_pb::HeartbeatResponse {
                send_state: Some(false),
            }));
        }

        let mut send_state = !executor_known;
        if executor_known && !send_state {
            let connections = self.indexify_state.executor_connections.read().await;
            if let Some(conn) = connections.get(&executor_id) &&
                conn.take_full_state_request()
            {
                info!(
                    executor_id = executor_id.get(),
                    "requesting full state after command ack regression"
                );
                send_state = true;
            }
        }

        Ok(Response::new(executor_api_pb::HeartbeatResponse {
            send_state: Some(send_state),
        }))
    }

    async fn poll_commands(
        &self,
        request: Request<executor_api_pb::PollCommandsRequest>,
    ) -> Result<Response<executor_api_pb::PollCommandsResponse>, Status> {
        let req = request.into_inner();
        let executor_id = ExecutorId::new(req.executor_id);

        self.executor_manager
            .heartbeat_v2(&executor_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Verify the executor is registered before entering long-poll
        {
            let connections = self.indexify_state.executor_connections.read().await;
            if !connections.contains_key(&executor_id) {
                return Err(Status::not_found("executor not registered"));
            }
        }

        let commands =
            long_poll_commands(&self.indexify_state, &executor_id, req.acked_command_seq).await;

        Ok(Response::new(executor_api_pb::PollCommandsResponse {
            commands,
        }))
    }

    async fn poll_allocation_results(
        &self,
        request: Request<executor_api_pb::PollAllocationResultsRequest>,
    ) -> Result<Response<executor_api_pb::PollAllocationResultsResponse>, Status> {
        let req = request.into_inner();
        let executor_id = ExecutorId::new(req.executor_id);

        self.executor_manager
            .heartbeat_v2(&executor_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Verify the executor is registered before entering long-poll
        {
            let connections = self.indexify_state.executor_connections.read().await;
            if !connections.contains_key(&executor_id) {
                return Err(Status::not_found("executor not registered"));
            }
        }

        let results =
            long_poll_results(&self.indexify_state, &executor_id, req.acked_result_seq).await;

        Ok(Response::new(
            executor_api_pb::PollAllocationResultsResponse { results },
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use executor_api_pb::{
        ContainerDescription,
        ContainerResources,
        ContainerState,
        ContainerStatus,
        ContainerTerminationReason as TerminationReasonPb,
        ContainerType as ContainerTypePb,
        FunctionRef,
        SandboxMetadata,
    };
    use proto_api::executor_api_pb;
    use tonic::Request;

    use super::executor_api_pb::executor_api_server::ExecutorApi;
    use crate::{
        data_model::{self, ContainerTerminationReason, ContainerType},
        executors::{EXECUTOR_TIMEOUT, ExecutorStateSnapshot},
        testing::TestService,
    };

    /// Build a minimal sandbox ContainerState proto with the given
    /// sandbox_id and termination status, simulating what the dataplane reports
    /// when a sandbox container fails to start (e.g. image pull failure).
    fn sandbox_fe_state_proto(
        container_id: &str,
        sandbox_id: &str,
        status: ContainerStatus,
        termination_reason: Option<TerminationReasonPb>,
    ) -> ContainerState {
        ContainerState {
            description: Some(ContainerDescription {
                id: Some(container_id.to_string()),
                function: Some(FunctionRef {
                    namespace: Some("test-ns".to_string()),
                    application_name: Some("".to_string()),
                    function_name: Some(container_id.to_string()),
                    application_version: Some("".to_string()),
                }),
                resources: Some(ContainerResources {
                    cpu_ms_per_sec: Some(100),
                    memory_bytes: Some(256 * 1024 * 1024),
                    disk_bytes: Some(1024 * 1024 * 1024),
                    gpu: None,
                }),
                max_concurrency: Some(1),
                container_type: Some(ContainerTypePb::Sandbox.into()),
                sandbox_metadata: Some(SandboxMetadata {
                    image: Some("ubuntu".to_string()),
                    timeout_secs: Some(600),
                    entrypoint: vec![],
                    network_policy: None,
                    sandbox_id: Some(sandbox_id.to_string()),
                    snapshot_uri: None,
                }),
                secret_names: vec![],
                initialization_timeout_ms: None,
                application: None,
                allocation_timeout_ms: None,
                pool_id: None,
            }),
            status: Some(status.into()),
            termination_reason: termination_reason.map(|r| r.into()),
        }
    }

    #[test]
    fn test_terminated_sandbox_container_preserves_sandbox_id() {
        // Simulates the dataplane reporting a sandbox container that failed to
        // start (e.g. Docker image pull failure). The proto->Container conversion
        // must preserve sandbox_id so the reconciler can find and terminate the
        // associated sandbox.
        let fe_state = sandbox_fe_state_proto(
            "sb-container-123",
            "sb-container-123",
            ContainerStatus::Terminated,
            Some(TerminationReasonPb::StartupFailedInternalError),
        );

        let container = data_model::Container::try_from(fe_state).unwrap();

        assert_eq!(container.container_type, ContainerType::Sandbox);
        assert!(
            matches!(
                container.state,
                data_model::ContainerState::Terminated {
                    reason: ContainerTerminationReason::StartupFailedInternalError,
                    ..
                }
            ),
            "Container should be Terminated with StartupFailedInternalError, got: {:?}",
            container.state
        );
        assert_eq!(
            container.sandbox_id.as_ref().map(|s| s.get()),
            Some("sb-container-123"),
            "sandbox_id must be preserved through proto conversion"
        );
    }

    #[test]
    fn test_running_sandbox_container_preserves_sandbox_id() {
        let fe_state = sandbox_fe_state_proto(
            "sb-container-456",
            "sb-container-456",
            ContainerStatus::Running,
            None,
        );

        let container = data_model::Container::try_from(fe_state).unwrap();

        assert_eq!(
            container.sandbox_id.as_ref().map(|s| s.get()),
            Some("sb-container-456"),
        );
        assert_eq!(container.state, data_model::ContainerState::Running);
    }

    #[test]
    fn test_function_container_has_no_sandbox_id() {
        // Function containers don't have sandbox_metadata, so sandbox_id should
        // be None.
        let fe_state = ContainerState {
            description: Some(ContainerDescription {
                id: Some("fn-container-1".to_string()),
                function: Some(FunctionRef {
                    namespace: Some("test-ns".to_string()),
                    application_name: Some("app".to_string()),
                    function_name: Some("process".to_string()),
                    application_version: Some("v1".to_string()),
                }),
                resources: Some(ContainerResources {
                    cpu_ms_per_sec: Some(100),
                    memory_bytes: Some(256 * 1024 * 1024),
                    disk_bytes: Some(1024 * 1024 * 1024),
                    gpu: None,
                }),
                max_concurrency: Some(1),
                container_type: Some(ContainerTypePb::Function.into()),
                sandbox_metadata: None,
                secret_names: vec![],
                initialization_timeout_ms: None,
                application: None,
                allocation_timeout_ms: None,
                pool_id: None,
            }),
            status: Some(ContainerStatus::Running.into()),
            termination_reason: None,
        };

        let container = data_model::Container::try_from(fe_state).unwrap();

        assert_eq!(container.container_type, ContainerType::Function);
        assert!(
            container.sandbox_id.is_none(),
            "Function containers should not have sandbox_id"
        );
    }

    fn make_fe_description(id: &str) -> executor_api_pb::ContainerDescription {
        ContainerDescription {
            id: Some(id.to_string()),
            function: Some(FunctionRef {
                namespace: Some("ns".to_string()),
                application_name: Some("app".to_string()),
                function_name: Some("fn".to_string()),
                application_version: Some("v1".to_string()),
            }),
            resources: Some(ContainerResources {
                cpu_ms_per_sec: Some(100),
                memory_bytes: Some(256 * 1024 * 1024),
                disk_bytes: Some(1024 * 1024 * 1024),
                gpu: None,
            }),
            max_concurrency: Some(1),
            container_type: Some(ContainerTypePb::Function.into()),
            sandbox_metadata: None,
            secret_names: vec![],
            initialization_timeout_ms: None,
            application: None,
            allocation_timeout_ms: None,
            pool_id: None,
        }
    }

    fn make_allocation(id: &str) -> executor_api_pb::Allocation {
        executor_api_pb::Allocation {
            function: Some(FunctionRef {
                namespace: Some("ns".to_string()),
                application_name: Some("app".to_string()),
                function_name: Some("fn".to_string()),
                application_version: None,
            }),
            allocation_id: Some(id.to_string()),
            function_call_id: Some(format!("fc-{id}")),
            request_id: Some("req-1".to_string()),
            args: vec![],
            request_data_payload_uri_prefix: None,
            request_error_payload_uri_prefix: None,
            container_id: Some("c1".to_string()),
            function_call_metadata: None,
            replay_mode: None,
            last_event_clock: None,
        }
    }

    fn make_command(seq: u64) -> executor_api_pb::Command {
        executor_api_pb::Command { seq, command: None }
    }

    fn make_call_function_log_entry(
        parent_allocation_id: &str,
        child_function_call_id: &str,
    ) -> executor_api_pb::AllocationLogEntry {
        executor_api_pb::AllocationLogEntry {
            allocation_id: parent_allocation_id.to_string(),
            clock: 0,
            entry: Some(executor_api_pb::allocation_log_entry::Entry::CallFunction(
                executor_api_pb::FunctionCallRequest {
                    namespace: Some("ns".to_string()),
                    application: Some("app".to_string()),
                    request_id: Some("req-1".to_string()),
                    source_function_call_id: Some("source-fc".to_string()),
                    updates: Some(executor_api_pb::ExecutionPlanUpdates {
                        updates: vec![executor_api_pb::ExecutionPlanUpdate {
                            op: Some(executor_api_pb::execution_plan_update::Op::FunctionCall(
                                executor_api_pb::FunctionCall {
                                    id: Some(child_function_call_id.to_string()),
                                    target: Some(FunctionRef {
                                        namespace: Some("ns".to_string()),
                                        application_name: Some("app".to_string()),
                                        function_name: Some("child-fn".to_string()),
                                        application_version: Some("v1".to_string()),
                                    }),
                                    args: vec![],
                                    call_metadata: None,
                                },
                            )),
                        }],
                        root_function_call_id: Some(child_function_call_id.to_string()),
                        start_at: None,
                    }),
                },
            )),
        }
    }

    fn make_completed_outcome(
        child_function_call_id: &str,
        allocation_id: &str,
    ) -> executor_api_pb::AllocationOutcome {
        executor_api_pb::AllocationOutcome {
            outcome: Some(executor_api_pb::allocation_outcome::Outcome::Completed(
                executor_api_pb::AllocationCompleted {
                    allocation_id: allocation_id.to_string(),
                    function: Some(FunctionRef {
                        namespace: Some("ns".to_string()),
                        application_name: Some("app".to_string()),
                        function_name: Some("child-fn".to_string()),
                        application_version: Some("v1".to_string()),
                    }),
                    function_call_id: Some(child_function_call_id.to_string()),
                    request_id: Some("req-1".to_string()),
                    return_value: None,
                    execution_duration_ms: None,
                },
            )),
        }
    }

    #[test]
    fn test_command_emitter_first_call_emits_full_state() {
        let mut emitter = super::CommandEmitter::new();

        let desired = ExecutorStateSnapshot {
            containers: vec![make_fe_description("c1")],
            allocations: vec![make_allocation("a1")],
            pending_snapshots: vec![],
            clock: Some(1),
        };

        let commands = emitter.emit_commands(&desired);

        // First call: AddContainer + RunAllocation
        assert_eq!(commands.len(), 2, "expected 2 commands: {commands:?}");

        let add_container = commands.iter().find(|c| {
            matches!(
                &c.command,
                Some(executor_api_pb::command::Command::AddContainer(_))
            )
        });
        assert!(add_container.is_some(), "expected AddContainer command");

        let run_alloc = commands.iter().find(|c| {
            matches!(
                &c.command,
                Some(executor_api_pb::command::Command::RunAllocation(_))
            )
        });
        assert!(run_alloc.is_some(), "expected RunAllocation command");

        // Sequence numbers should be monotonically increasing
        let seqs: Vec<u64> = commands.iter().map(|c| c.seq).collect();
        assert_eq!(seqs, vec![1, 2]);
    }

    #[test]
    fn test_command_emitter_no_change_emits_nothing() {
        let mut emitter = super::CommandEmitter::new();

        let desired = ExecutorStateSnapshot {
            containers: vec![make_fe_description("c1")],
            allocations: vec![make_allocation("a1")],
            pending_snapshots: vec![],
            clock: Some(1),
        };

        // First call -- full sync
        let commands = emitter.emit_commands(&desired);
        assert_eq!(commands.len(), 2);
        emitter.commit_snapshot(&desired);

        // Second call -- same state -> no commands
        let commands = emitter.emit_commands(&desired);
        assert!(commands.is_empty(), "expected 0 commands: {commands:?}");
    }

    #[test]
    fn test_command_emitter_container_removal() {
        let mut emitter = super::CommandEmitter::new();

        // First: one container
        let desired1 = ExecutorStateSnapshot {
            containers: vec![make_fe_description("c1")],
            allocations: vec![],
            pending_snapshots: vec![],
            clock: Some(1),
        };
        emitter.emit_commands(&desired1);
        emitter.commit_snapshot(&desired1);

        // Second: container removed
        let desired2 = ExecutorStateSnapshot {
            containers: vec![],
            allocations: vec![],
            pending_snapshots: vec![],
            clock: Some(2),
        };
        let commands = emitter.emit_commands(&desired2);
        assert_eq!(commands.len(), 1, "{commands:?}");
        assert!(matches!(
            &commands[0].command,
            Some(executor_api_pb::command::Command::RemoveContainer(r))
            if r.container_id == "c1"
        ));
    }

    #[test]
    fn test_command_emitter_new_allocation_after_initial() {
        let mut emitter = super::CommandEmitter::new();

        // First: one container, one allocation
        let desired1 = ExecutorStateSnapshot {
            containers: vec![make_fe_description("c1")],
            allocations: vec![make_allocation("a1")],
            pending_snapshots: vec![],
            clock: Some(1),
        };
        emitter.emit_commands(&desired1);
        emitter.commit_snapshot(&desired1);

        // Second: same container, new allocation added
        let desired2 = ExecutorStateSnapshot {
            containers: vec![make_fe_description("c1")],
            allocations: vec![make_allocation("a1"), make_allocation("a2")],
            pending_snapshots: vec![],
            clock: Some(2),
        };
        let commands = emitter.emit_commands(&desired2);
        assert_eq!(commands.len(), 1, "{commands:?}");
        assert!(matches!(
            &commands[0].command,
            Some(executor_api_pb::command::Command::RunAllocation(r))
            if r.allocation.as_ref().unwrap().allocation_id.as_deref() == Some("a2")
        ));
    }

    #[test]
    fn test_command_emitter_allocation_completion_no_command() {
        let mut emitter = super::CommandEmitter::new();

        // First: one allocation
        let desired1 = ExecutorStateSnapshot {
            containers: vec![make_fe_description("c1")],
            allocations: vec![make_allocation("a1")],
            pending_snapshots: vec![],
            clock: Some(1),
        };
        emitter.emit_commands(&desired1);
        emitter.commit_snapshot(&desired1);

        // Second: allocation completed (removed from desired state)
        let desired2 = ExecutorStateSnapshot {
            containers: vec![make_fe_description("c1")],
            allocations: vec![],
            pending_snapshots: vec![],
            clock: Some(2),
        };
        let commands = emitter.emit_commands(&desired2);
        // Completed allocations disappear silently -- no KillAllocation
        assert!(commands.is_empty(), "expected 0 commands: {commands:?}");
    }

    #[test]
    fn test_command_emitter_seq_continuity() {
        let mut emitter = super::CommandEmitter::new();

        // First batch: 2 commands (seq 1, 2)
        let desired1 = ExecutorStateSnapshot {
            containers: vec![make_fe_description("c1")],
            allocations: vec![make_allocation("a1")],
            pending_snapshots: vec![],
            clock: Some(1),
        };
        let cmds1 = emitter.emit_commands(&desired1);
        assert_eq!(cmds1.last().unwrap().seq, 2);
        emitter.commit_snapshot(&desired1);

        // Second batch: 1 command (seq 3)
        let desired2 = ExecutorStateSnapshot {
            containers: vec![make_fe_description("c1")],
            allocations: vec![make_allocation("a1"), make_allocation("a2")],
            pending_snapshots: vec![],
            clock: Some(2),
        };
        let cmds2 = emitter.emit_commands(&desired2);
        assert_eq!(cmds2[0].seq, 3, "seq should continue from previous batch");
    }

    #[tokio::test]
    async fn test_reconnect_full_state_resets_command_outbox() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );
        let executor_id = crate::data_model::ExecutorId::from("executor-reconnect-reset");

        // Initial register.
        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        // Seed outbox with a pending command and observe non-zero ack
        // progression to establish a cursor.
        {
            let connections = test_service
                .service
                .indexify_state
                .executor_connections
                .read()
                .await;
            let conn = connections
                .get(&executor_id)
                .expect("executor connection should exist");
            conn.push_commands(vec![make_command(10)]).await;
        }
        let _ = ExecutorApi::poll_commands(
            &api,
            Request::new(executor_api_pb::PollCommandsRequest {
                executor_id: executor_id.get().to_string(),
                acked_command_seq: Some(7),
            }),
        )
        .await
        .unwrap();

        // Reconnect with full state (same executor_id): this should reset
        // command outbox/cursor before re-syncing.
        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        let connections = test_service
            .service
            .indexify_state
            .executor_connections
            .read()
            .await;
        let conn = connections
            .get(&executor_id)
            .expect("executor connection should exist");
        assert!(
            conn.clone_commands().await.is_empty(),
            "reconnect full state should clear stale pending commands"
        );
    }

    #[tokio::test]
    async fn test_long_poll_survives_reconnect_and_receives_new_commands() {
        let test_service = TestService::new().await.unwrap();
        let api = Arc::new(super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        ));
        let executor_id = crate::data_model::ExecutorId::from("executor-reconnect-long-poll");

        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        let poll_executor_id = executor_id.get().to_string();
        let poll_api = api.clone();
        let poll_task = tokio::spawn(async move {
            ExecutorApi::poll_commands(
                poll_api.as_ref(),
                Request::new(executor_api_pb::PollCommandsRequest {
                    executor_id: poll_executor_id,
                    acked_command_seq: None,
                }),
            )
            .await
            .unwrap()
            .into_inner()
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Reconnect with the same executor ID (full-state sync path).
        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        test_service
            .service
            .indexify_state
            .enqueue_executor_commands(&executor_id, vec![make_command(0)])
            .await
            .unwrap();

        let response = tokio::time::timeout(Duration::from_secs(2), poll_task)
            .await
            .expect("long poll should be released by new command")
            .unwrap();
        assert_eq!(
            response.commands.len(),
            1,
            "long poll should receive command after reconnect"
        );
    }

    #[tokio::test]
    async fn test_stopped_heartbeat_deregisters_executor_immediately() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );
        let executor_id = crate::data_model::ExecutorId::from("executor-stopped-heartbeat");

        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        {
            let connections = test_service
                .service
                .indexify_state
                .executor_connections
                .read()
                .await;
            assert!(connections.contains_key(&executor_id));
        }
        {
            let runtime_data = test_service
                .service
                .executor_manager
                .runtime_data_read()
                .await;
            assert!(runtime_data.contains_key(&executor_id));
        }

        let response = ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_id.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Stopped.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![],
                allocation_log_entries: vec![],
            }),
        )
        .await
        .unwrap()
        .into_inner();

        assert_eq!(response.send_state, Some(false));

        {
            let connections = test_service
                .service
                .indexify_state
                .executor_connections
                .read()
                .await;
            assert!(!connections.contains_key(&executor_id));
        }
        {
            let runtime_data = test_service
                .service
                .executor_manager
                .runtime_data_read()
                .await;
            assert!(!runtime_data.contains_key(&executor_id));
        }
    }

    #[tokio::test]
    async fn test_ack_regression_requests_full_state_on_next_heartbeat() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );
        let executor_id = crate::data_model::ExecutorId::from("executor-ack-regression");

        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        // Seed one pending command so poll_commands returns immediately.
        {
            let connections = test_service
                .service
                .indexify_state
                .executor_connections
                .read()
                .await;
            let conn = connections
                .get(&executor_id)
                .expect("executor connection should exist");
            conn.push_commands(vec![make_command(10)]).await;
        }

        // First poll reports a non-zero ack baseline.
        let _ = ExecutorApi::poll_commands(
            &api,
            Request::new(executor_api_pb::PollCommandsRequest {
                executor_id: executor_id.get().to_string(),
                acked_command_seq: Some(5),
            }),
        )
        .await
        .unwrap();

        let hb1 = ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_id.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![],
                allocation_log_entries: vec![],
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(hb1.send_state, Some(false));

        // Second poll regresses ack to None (0), which should trigger
        // a one-shot full-state request on the next heartbeat.
        let _ = ExecutorApi::poll_commands(
            &api,
            Request::new(executor_api_pb::PollCommandsRequest {
                executor_id: executor_id.get().to_string(),
                acked_command_seq: None,
            }),
        )
        .await
        .unwrap();

        let hb2 = ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_id.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![],
                allocation_log_entries: vec![],
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(
            hb2.send_state,
            Some(true),
            "ack regression should request full state on next heartbeat"
        );

        // One-shot: subsequent heartbeat should clear the flag.
        let hb3 = ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_id.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![],
                allocation_log_entries: vec![],
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(hb3.send_state, Some(false));
    }

    #[tokio::test]
    async fn test_result_routing_prefers_latest_parent_after_reschedule() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );

        let executor_a = crate::data_model::ExecutorId::from("executor-route-a");
        let executor_b = crate::data_model::ExecutorId::from("executor-route-b");
        let child_fc_id = "child-fc-stable";

        // Register both executors.
        api.handle_full_state(
            &executor_a,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();
        api.handle_full_state(
            &executor_b,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        // Attempt A registers a child call route first.
        ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_a.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![],
                allocation_log_entries: vec![make_call_function_log_entry(
                    "parent-alloc-a",
                    child_fc_id,
                )],
            }),
        )
        .await
        .unwrap();

        // Rescheduled attempt B reports the same function_call_id with a new
        // parent allocation.
        ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_b.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![],
                allocation_log_entries: vec![make_call_function_log_entry(
                    "parent-alloc-b",
                    child_fc_id,
                )],
            }),
        )
        .await
        .unwrap();

        // Seed the child allocation so outcome ingestion succeeds.
        let child_allocation = data_model::AllocationBuilder::default()
            .target(data_model::AllocationTarget::new(
                executor_b.clone(),
                data_model::ContainerId::new("child-container".to_string()),
            ))
            .function_call_id(data_model::FunctionCallId::from(child_fc_id))
            .namespace("ns".to_string())
            .application("app".to_string())
            .application_version("v1".to_string())
            .function("child-fn".to_string())
            .request_id("req-1".to_string())
            .outcome(data_model::FunctionRunOutcome::Unknown)
            .input_args(vec![])
            .call_metadata(bytes::Bytes::new())
            .build()
            .expect("child allocation should build");
        let child_allocation_id = child_allocation.id.to_string();
        let mut update = crate::state_store::requests::SchedulerUpdateRequest::default();
        update.new_allocations.push(child_allocation);
        test_service
            .service
            .indexify_state
            .write(crate::state_store::requests::StateMachineUpdateRequest {
                payload: crate::state_store::requests::RequestPayload::SchedulerUpdate(
                    crate::state_store::requests::SchedulerUpdatePayload::new(update),
                ),
            })
            .await
            .unwrap();

        // Child completion arrives; result must route to the latest parent
        // (executor B / parent-alloc-b), not stale attempt A.
        ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_b.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![make_completed_outcome(
                    child_fc_id,
                    &child_allocation_id,
                )],
                allocation_log_entries: vec![],
            }),
        )
        .await
        .unwrap();

        let (results_a, results_b) = {
            let connections = test_service
                .service
                .indexify_state
                .executor_connections
                .read()
                .await;
            let conn_a = connections
                .get(&executor_a)
                .expect("executor A connection should exist");
            let conn_b = connections
                .get(&executor_b)
                .expect("executor B connection should exist");
            (conn_a.clone_results().await, conn_b.clone_results().await)
        };

        assert!(
            results_a.is_empty(),
            "stale executor should not receive routed result"
        );
        assert_eq!(
            results_b.len(),
            1,
            "latest parent executor should receive exactly one routed result"
        );

        let routed = results_b[0]
            .entry
            .as_ref()
            .expect("sequenced result should include log entry");
        assert_eq!(routed.allocation_id, "parent-alloc-b");

        match routed
            .entry
            .as_ref()
            .expect("log entry payload should be present")
        {
            executor_api_pb::allocation_log_entry::Entry::FunctionCallResult(result) => {
                assert_eq!(
                    result.function_call_id.as_deref(),
                    Some(child_fc_id),
                    "routed result should preserve original function_call_id"
                );
                assert_eq!(
                    result.outcome_code,
                    Some(executor_api_pb::AllocationOutcomeCode::Success.into())
                );
            }
            _ => panic!("expected FunctionCallResult entry"),
        }
    }

    #[tokio::test]
    async fn test_lapsed_deregistration_purges_router_entries() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );

        let executor_id = crate::data_model::ExecutorId::from("executor-lapsed-router-purge");
        let child_fc_id = "child-fc-timeout-purge";

        // Deterministic lapsed-heartbeat timing.
        tokio::time::pause();

        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        // Register a pending router entry for this executor.
        ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_id.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![],
                allocation_log_entries: vec![make_call_function_log_entry(
                    "parent-timeout-alloc",
                    child_fc_id,
                )],
            }),
        )
        .await
        .unwrap();

        assert_eq!(
            api.function_call_result_router.pending_len().await,
            1,
            "expected one pending route before timeout deregistration"
        );

        // Let heartbeat deadline lapse and run timeout cleanup.
        tokio::time::advance(EXECUTOR_TIMEOUT + Duration::from_secs(1)).await;
        api.executor_manager
            .process_lapsed_executors()
            .await
            .unwrap();

        assert_eq!(
            api.function_call_result_router.pending_len().await,
            0,
            "lapsed deregistration should purge router entries"
        );
    }

    #[tokio::test]
    async fn test_lapsed_deregistration_purges_router_entries_under_load() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );

        let executor_id = crate::data_model::ExecutorId::from("executor-lapsed-router-purge-load");
        tokio::time::pause();

        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        let mut entries = Vec::new();
        for idx in 0..64 {
            entries.push(make_call_function_log_entry(
                &format!("parent-timeout-alloc-{idx}"),
                &format!("child-fc-timeout-purge-{idx}"),
            ));
        }

        ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_id.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![],
                allocation_log_entries: entries,
            }),
        )
        .await
        .unwrap();

        assert_eq!(
            api.function_call_result_router.pending_len().await,
            64,
            "expected all router entries to be registered before timeout"
        );

        tokio::time::advance(EXECUTOR_TIMEOUT + Duration::from_secs(1)).await;
        api.executor_manager
            .process_lapsed_executors()
            .await
            .unwrap();

        assert_eq!(
            api.function_call_result_router.pending_len().await,
            0,
            "lapsed deregistration should purge all stale router entries"
        );
    }

    #[tokio::test]
    async fn test_outcome_ingest_failure_keeps_router_entry_for_retry() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );

        let executor_id = crate::data_model::ExecutorId::from("executor-route-retry-on-error");
        let child_fc_id = "child-fc-retry-on-error";

        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        // Register route entry first.
        ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_id.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![],
                allocation_log_entries: vec![make_call_function_log_entry(
                    "parent-retry-alloc",
                    child_fc_id,
                )],
            }),
        )
        .await
        .unwrap();

        assert_eq!(
            api.function_call_result_router.pending_len().await,
            1,
            "expected one pending route before ingest failure"
        );

        // Send a completion that references a missing allocation. Heartbeat
        // must fail so dataplane retries, and router entry must be retained.
        let err = ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_id.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![make_completed_outcome(
                    child_fc_id,
                    "missing-allocation",
                )],
                allocation_log_entries: vec![],
            }),
        )
        .await
        .expect_err("heartbeat must fail when outcome ingestion fails");
        assert_eq!(err.code(), tonic::Code::Internal);

        assert_eq!(
            api.function_call_result_router.pending_len().await,
            1,
            "router entry should remain so retried outcome can still route"
        );
    }

    #[tokio::test]
    async fn test_scheduler_update_emits_commands_without_wake_loop() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );
        let executor_id = crate::data_model::ExecutorId::from("executor-update-driven-emission");

        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        // Process queued UpsertExecutor so scheduler has executor metadata.
        test_service.process_all_state_changes().await.unwrap();

        let container = crate::data_model::ContainerBuilder::default()
            .id(crate::data_model::ContainerId::new(
                "container-update-driven".to_string(),
            ))
            .namespace("ns".to_string())
            .application_name("app".to_string())
            .function_name("fn".to_string())
            .version("v1".to_string())
            .state(crate::data_model::ContainerState::Running)
            .resources(crate::data_model::ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .max_concurrency(1)
            .container_type(crate::data_model::ContainerType::Function)
            .build()
            .unwrap();
        let mut executor_state = crate::data_model::ExecutorServerMetadata {
            executor_id: executor_id.clone(),
            function_container_ids: imbl::HashSet::new(),
            free_resources: crate::data_model::HostResources {
                cpu_ms_per_sec: 8_000,
                memory_bytes: 16 * 1024 * 1024 * 1024,
                disk_bytes: 100 * 1024 * 1024 * 1024,
                gpu: None,
            },
            resource_claims: imbl::HashMap::new(),
        };
        executor_state
            .add_container(&container)
            .expect("container should fit executor resources");

        let container_meta = crate::data_model::ContainerServerMetadata::new(
            executor_id.clone(),
            container.clone(),
            crate::data_model::ContainerState::Running,
        );

        let mut update = crate::state_store::requests::SchedulerUpdateRequest::default();
        update
            .updated_executor_states
            .insert(executor_id.clone(), Box::new(executor_state));
        update
            .containers
            .insert(container.id.clone(), Box::new(container_meta));

        // Mirror scheduler publish path (ArcSwap update), then emit commands
        // directly from the scheduler update.
        let current = test_service.service.indexify_state.app_state.load_full();
        let mut indexes = current.indexes.clone();
        let mut scheduler = current.scheduler.clone();
        let clock = indexes.clock;
        indexes
            .apply_scheduler_update(clock, &update, "test_scheduler_update")
            .unwrap();
        scheduler.apply_container_update(&update);
        test_service
            .service
            .indexify_state
            .app_state
            .store(std::sync::Arc::new(crate::state_store::AppState {
                indexes,
                scheduler,
            }));

        test_service
            .service
            .executor_manager
            .emit_commands_from_scheduler_update(&update)
            .await;

        let response = ExecutorApi::poll_commands(
            &api,
            Request::new(executor_api_pb::PollCommandsRequest {
                executor_id: executor_id.get().to_string(),
                acked_command_seq: None,
            }),
        )
        .await
        .unwrap()
        .into_inner();

        let has_add = response.commands.iter().any(|c| {
            matches!(
                c.command,
                Some(executor_api_pb::command::Command::AddContainer(_))
            )
        });
        assert!(
            has_add,
            "expected AddContainer command after scheduler update"
        );
    }
}
