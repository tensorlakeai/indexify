use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
    vec,
};

use anyhow::Result;
use executor_api_pb::executor_api_server::ExecutorApi;
pub use proto_api::executor_api_pb;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::{
    blob_store::registry::BlobStorageRegistry,
    data_model::{
        self,
        ContainerId,
        ExecutorId,
        ExecutorMetadataBuilder,
        FunctionAllowlist,
        FunctionCallId,
    },
    executors::{ExecutorManager, ExecutorStateSnapshot},
    pb_helpers::blob_store_path_to_url,
    proto_convert::{
        prepare_data_payload,
        proto_container_termination_to_internal,
        proto_failure_reason_to_internal,
        proto_failure_reason_to_termination_reason,
        to_internal_compute_op,
    },
    state_store::{
        ExecutorEvent,
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
pub struct CommandEmitter {
    next_seq: u64,
    /// Container descriptions sent via AddContainer, keyed by container ID.
    /// Tracked as full descriptions so we can detect changes and emit
    /// `UpdateContainerDescription` commands.
    pub(crate) known_containers: HashMap<String, executor_api_pb::ContainerDescription>,
    /// Allocation IDs sent via RunAllocation.
    pub(crate) known_allocations: HashSet<String>,
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
            has_synced: false,
        }
    }

    /// Track an allocation ID so FullSync won't re-emit it.
    pub fn track_allocation(&mut self, id: String) {
        self.known_allocations.insert(id);
    }

    /// Track a container so FullSync won't re-emit it.
    pub fn track_container(
        &mut self,
        id: String,
        description: executor_api_pb::ContainerDescription,
    ) {
        self.known_containers.insert(id, description);
    }

    /// Untrack a container ID (removed).
    pub fn untrack_container(&mut self, id: &str) {
        self.known_containers.remove(id);
    }

    pub(crate) fn next_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    /// Diff the current desired state against what was previously seen and
    /// produce a batch of `Command` messages for the delta.
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

        // --- Containers ---
        let current_containers: HashMap<String, executor_api_pb::ContainerDescription> = snapshot
            .containers
            .iter()
            .filter_map(|fe| fe.id.clone().map(|id| (id, fe.clone())))
            .collect();

        for fe in &snapshot.containers {
            if let Some(id) = &fe.id {
                if let Some(known) = self.known_containers.get(id) {
                    // Known container -- check if description changed
                    if known != fe {
                        // Build a targeted update with only changed fields
                        let mut update = executor_api_pb::UpdateContainerDescription {
                            container_id: id.clone(),
                            sandbox_metadata: None,
                        };
                        if known.sandbox_metadata != fe.sandbox_metadata {
                            update.sandbox_metadata = fe.sandbox_metadata.clone();
                        }
                        // Only emit if there are actual changes to send
                        if update.sandbox_metadata.is_some() {
                            let seq = self.next_seq();
                            commands.push(executor_api_pb::Command {
                                seq,
                                command: Some(
                                    executor_api_pb::command::Command::UpdateContainerDescription(
                                        update,
                                    ),
                                ),
                            });
                        }
                    }
                } else {
                    // New container -> AddContainer
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
        }

        // Removed containers -> RemoveContainer
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
                        reason: None, // Reason comes from server state machine, not snapshot diff
                    },
                )),
            });
        }

        // --- Allocations ---
        // New allocations -> RunAllocation
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

        // Function call results are delivered via the AllocationEvent log
        // (get_allocation_events RPC), not via Commands.

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

/// Process command responses from a dataplane executor.
///
/// Converts proto `CommandResponse` messages into a single
/// `DataplaneResultsIngestedEvent` and writes it to the state machine via
/// `RequestPayload::DataplaneResults`. This is the shared logic used by both
/// the `report_command_responses` RPC handler and tests.
pub async fn process_command_responses(
    indexify_state: &Arc<IndexifyState>,
    executor_id: &ExecutorId,
    responses: Vec<executor_api_pb::CommandResponse>,
) -> Result<()> {
    use data_model::ContainerStateUpdateInfo;

    let mut container_state_updates = Vec::new();
    let mut container_started_ids = Vec::new();

    for resp in responses {
        let Some(response) = resp.response else {
            warn!("CommandResponse with no response oneof, skipping");
            continue;
        };

        match response {
            executor_api_pb::command_response::Response::AllocationScheduled(scheduled) => {
                info!(
                    executor_id = executor_id.get(),
                    allocation_id = %scheduled.allocation_id,
                    command_seq = ?resp.command_seq,
                    "AllocationScheduled ack received"
                );
                // State tracking for allocation scheduling acks can be added
                // later. For now, we just log the ack.
            }
            executor_api_pb::command_response::Response::ContainerTerminated(terminated) => {
                let reason = proto_container_termination_to_internal(terminated.reason());
                info!(
                    executor_id = executor_id.get(),
                    container_id = %terminated.container_id,
                    reason = ?reason,
                    "ContainerTerminated ingested"
                );
                container_state_updates.push(ContainerStateUpdateInfo {
                    container_id: data_model::ContainerId::new(terminated.container_id),
                    termination_reason: Some(reason),
                });
            }
            executor_api_pb::command_response::Response::ContainerStarted(started) => {
                info!(
                    executor_id = executor_id.get(),
                    container_id = started.container_id,
                    "ContainerStarted -- will promote sandbox if pending"
                );
                container_started_ids.push(data_model::ContainerId::new(started.container_id));
            }
            executor_api_pb::command_response::Response::SnapshotCompleted(completed) => {
                info!(
                    executor_id = executor_id.get(),
                    container_id = %completed.container_id,
                    snapshot_id = %completed.snapshot_id,
                    snapshot_uri = %completed.snapshot_uri,
                    size_bytes = completed.size_bytes,
                    "SnapshotCompleted received"
                );
                handle_snapshot_completed(indexify_state, &completed).await?;
            }
            executor_api_pb::command_response::Response::SnapshotFailed(failed) => {
                warn!(
                    executor_id = executor_id.get(),
                    container_id = %failed.container_id,
                    snapshot_id = %failed.snapshot_id,
                    error = %failed.error_message,
                    "SnapshotFailed received"
                );
                handle_snapshot_failed(indexify_state, &failed).await?;
            }
        }
    }

    if container_state_updates.is_empty() && container_started_ids.is_empty() {
        return Ok(());
    }

    write_dataplane_results(
        indexify_state,
        executor_id,
        vec![],
        container_state_updates,
        container_started_ids,
    )
    .await
}

/// Handle a snapshot completed response from the dataplane.
async fn handle_snapshot_completed(
    indexify_state: &Arc<IndexifyState>,
    completed: &executor_api_pb::SnapshotCompleted,
) -> Result<()> {
    use crate::state_store::requests::CompleteSnapshotRequest;

    if completed.snapshot_id.is_empty() {
        anyhow::bail!("SnapshotCompleted: snapshot_id is empty");
    }
    if completed.snapshot_uri.is_empty() {
        anyhow::bail!("SnapshotCompleted: snapshot_uri is empty");
    }

    let request = StateMachineUpdateRequest {
        payload: RequestPayload::CompleteSnapshot(CompleteSnapshotRequest {
            snapshot_id: data_model::SnapshotId::new(completed.snapshot_id.clone()),
            snapshot_uri: completed.snapshot_uri.clone(),
            size_bytes: completed.size_bytes,
        }),
    };
    indexify_state.write(request).await
}

/// Handle a snapshot failed response from the dataplane.
async fn handle_snapshot_failed(
    indexify_state: &Arc<IndexifyState>,
    failed: &executor_api_pb::SnapshotFailed,
) -> Result<()> {
    use crate::state_store::requests::FailSnapshotRequest;

    if failed.snapshot_id.is_empty() {
        anyhow::bail!("SnapshotFailed: snapshot_id is empty");
    }

    let request = StateMachineUpdateRequest {
        payload: RequestPayload::FailSnapshot(FailSnapshotRequest {
            snapshot_id: data_model::SnapshotId::new(failed.snapshot_id.clone()),
            error: failed.error_message.clone(),
        }),
    };
    indexify_state.write(request).await
}

/// Process a single AllocationCompleted message.
pub async fn process_allocation_completed(
    indexify_state: &Arc<IndexifyState>,
    blob_storage_registry: &Arc<BlobStorageRegistry>,
    executor_id: &ExecutorId,
    completed: executor_api_pb::AllocationCompleted,
) -> Result<()> {
    use data_model::{AllocationOutputIngestedEvent, FunctionRunOutcome, GraphUpdates};

    let function = completed
        .function
        .ok_or_else(|| anyhow::anyhow!("AllocationCompleted missing function"))?;
    let namespace = function
        .namespace
        .ok_or_else(|| anyhow::anyhow!("AllocationCompleted missing namespace"))?;
    let application = function
        .application_name
        .ok_or_else(|| anyhow::anyhow!("AllocationCompleted missing application_name"))?;
    let fn_name = function
        .function_name
        .ok_or_else(|| anyhow::anyhow!("AllocationCompleted missing function_name"))?;
    let request_id = completed
        .request_id
        .ok_or_else(|| anyhow::anyhow!("AllocationCompleted missing request_id"))?;
    let function_call_id = completed
        .function_call_id
        .ok_or_else(|| anyhow::anyhow!("AllocationCompleted missing function_call_id"))?;
    let allocation_id = completed.allocation_id;

    let allocation_key =
        data_model::Allocation::key_from(&namespace, &application, &request_id, &allocation_id);
    let allocation = indexify_state
        .reader()
        .get_allocation(&allocation_key)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "AllocationCompleted: allocation not found: {}",
                allocation_key
            )
        })?;

    let (data_payload, graph_updates) = match completed.return_value {
        Some(executor_api_pb::allocation_completed::ReturnValue::Value(dp)) => {
            info!(
                executor_id = executor_id.get(),
                allocation_id = %allocation_id,
                request_id = %request_id,
                namespace = %namespace,
                app = %application,
                "fn" = %fn_name,
                function_call_id = %function_call_id,
                "AllocationCompleted: ReturnValue::Value"
            );
            let blob_store_url_scheme = blob_storage_registry
                .get_blob_store(&namespace)
                .get_url_scheme();
            let blob_store_url = blob_storage_registry.get_blob_store(&namespace).get_url();
            let payload = prepare_data_payload(dp, &blob_store_url_scheme, &blob_store_url)?;
            (Some(payload), None)
        }
        Some(executor_api_pb::allocation_completed::ReturnValue::Updates(updates)) => {
            let num_updates = updates.updates.len();
            let root_fc_id_str = updates.root_function_call_id.as_deref().unwrap_or("none");
            info!(
                executor_id = executor_id.get(),
                allocation_id = %allocation_id,
                request_id = %request_id,
                namespace = %namespace,
                app = %application,
                "fn" = %fn_name,
                function_call_id = %function_call_id,
                num_updates,
                root_function_call_id = %root_fc_id_str,
                "AllocationCompleted: ReturnValue::Updates (tail call)"
            );
            let root_function_call_id = updates
                .root_function_call_id
                .map(FunctionCallId::from)
                .unwrap_or_else(|| FunctionCallId::from(nanoid::nanoid!()));
            let mut compute_ops = Vec::new();
            for update in updates.updates {
                compute_ops.push(to_internal_compute_op(
                    update,
                    blob_storage_registry,
                    Some(function_call_id.clone()),
                )?);
            }
            (
                None,
                Some(GraphUpdates {
                    graph_updates: compute_ops,
                    output_function_call_id: root_function_call_id,
                }),
            )
        }
        None => {
            info!(
                executor_id = executor_id.get(),
                allocation_id = %allocation_id,
                request_id = %request_id,
                namespace = %namespace,
                app = %application,
                "fn" = %fn_name,
                function_call_id = %function_call_id,
                "AllocationCompleted: ReturnValue::None"
            );
            (None, None)
        }
    };

    info!(
        executor_id = executor_id.get(),
        allocation_id = %allocation_id,
        request_id = %request_id,
        namespace = %namespace,
        app = %application,
        "fn" = %fn_name,
        function_call_id = %function_call_id,
        "AllocationCompleted ingested"
    );

    let event = AllocationOutputIngestedEvent {
        namespace,
        application,
        function: fn_name,
        request_id,
        function_call_id: FunctionCallId::from(function_call_id),
        data_payload,
        graph_updates,
        request_exception: None,
        allocation_id: allocation.id,
        allocation_target: allocation.target,
        allocation_outcome: FunctionRunOutcome::Success,
        execution_duration_ms: completed.execution_duration_ms,
    };

    write_dataplane_results(indexify_state, executor_id, vec![event], vec![], vec![]).await
}

/// Process a single AllocationFailed message.
pub async fn process_allocation_failed(
    indexify_state: &Arc<IndexifyState>,
    blob_storage_registry: &Arc<BlobStorageRegistry>,
    executor_id: &ExecutorId,
    failed: executor_api_pb::AllocationFailed,
) -> Result<()> {
    use data_model::{AllocationOutputIngestedEvent, FunctionRunOutcome};

    let proto_reason = failed.reason();
    let failure_reason = proto_failure_reason_to_internal(proto_reason);
    let function = failed
        .function
        .ok_or_else(|| anyhow::anyhow!("AllocationFailed missing function"))?;
    let namespace = function
        .namespace
        .ok_or_else(|| anyhow::anyhow!("AllocationFailed missing namespace"))?;
    let application = function
        .application_name
        .ok_or_else(|| anyhow::anyhow!("AllocationFailed missing application_name"))?;
    let fn_name = function
        .function_name
        .ok_or_else(|| anyhow::anyhow!("AllocationFailed missing function_name"))?;
    let request_id = failed
        .request_id
        .ok_or_else(|| anyhow::anyhow!("AllocationFailed missing request_id"))?;
    let function_call_id = failed
        .function_call_id
        .ok_or_else(|| anyhow::anyhow!("AllocationFailed missing function_call_id"))?;
    let allocation_id = failed.allocation_id;

    let allocation_key =
        data_model::Allocation::key_from(&namespace, &application, &request_id, &allocation_id);
    let allocation = indexify_state
        .reader()
        .get_allocation(&allocation_key)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("AllocationFailed: allocation not found: {}", allocation_key)
        })?;

    let request_exception = if let Some(dp) = failed.request_error {
        let blob_store_url_scheme = blob_storage_registry
            .get_blob_store(&namespace)
            .get_url_scheme();
        let blob_store_url = blob_storage_registry.get_blob_store(&namespace).get_url();
        Some(prepare_data_payload(
            dp,
            &blob_store_url_scheme,
            &blob_store_url,
        )?)
    } else {
        None
    };

    // If the dataplane included a container_id, include it as a container state
    // update so the scheduler marks it terminated before rescheduling. This
    // prevents retries from landing on the same dead container when
    // ContainerTerminated hasn't arrived yet via the separate channel.
    let container_state_updates = if let Some(cid) = &failed.container_id {
        let termination_reason = proto_failure_reason_to_termination_reason(proto_reason);
        info!(
            executor_id = executor_id.get(),
            allocation_id = %allocation_id,
            request_id = %request_id,
            namespace = %namespace,
            app = %application,
            "fn" = %fn_name,
            failure_reason = ?proto_reason,
            container_id = %cid,
            "AllocationFailed ingested (with container_id)"
        );
        vec![data_model::ContainerStateUpdateInfo {
            container_id: data_model::ContainerId::new(cid.clone()),
            termination_reason: Some(termination_reason),
        }]
    } else {
        info!(
            executor_id = executor_id.get(),
            allocation_id = %allocation_id,
            request_id = %request_id,
            namespace = %namespace,
            app = %application,
            "fn" = %fn_name,
            failure_reason = ?proto_reason,
            "AllocationFailed ingested"
        );
        vec![]
    };

    let event = AllocationOutputIngestedEvent {
        namespace,
        application,
        function: fn_name,
        request_id,
        function_call_id: FunctionCallId::from(function_call_id),
        data_payload: None,
        graph_updates: None,
        request_exception,
        allocation_id: allocation.id,
        allocation_target: allocation.target,
        allocation_outcome: FunctionRunOutcome::Failure(failure_reason),
        execution_duration_ms: failed.execution_duration_ms,
    };

    write_dataplane_results(
        indexify_state,
        executor_id,
        vec![event],
        container_state_updates,
        vec![],
    )
    .await
}

/// Write a `DataplaneResultsIngestedEvent` to the state machine.
///
/// Shared by `process_command_responses` (container events only) and
/// `process_allocation_activities` (allocation events only).
async fn write_dataplane_results(
    indexify_state: &Arc<IndexifyState>,
    executor_id: &ExecutorId,
    allocation_events: Vec<data_model::AllocationOutputIngestedEvent>,
    container_state_updates: Vec<data_model::ContainerStateUpdateInfo>,
    container_started_ids: Vec<data_model::ContainerId>,
) -> Result<()> {
    let event = data_model::DataplaneResultsIngestedEvent {
        executor_id: executor_id.clone(),
        allocation_events,
        container_state_updates,
        container_started_ids,
    };

    indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::DataplaneResults(
                crate::state_store::requests::DataplaneResultsRequest { event },
            ),
        })
        .await?;

    Ok(())
}

struct PendingFunctionCall {
    parent_allocation_id: String,
    /// The function_call_id from the original CallFunction that the parent FE
    /// registered its watcher under. Preserved through re-registrations so
    /// the final result is delivered with the ID the FE expects.
    original_function_call_id: String,
    executor_id: ExecutorId,
}

/// Routes function call results from child allocation completions
/// back to the parent executor's pending_results buffer via ExecutorConnection.
pub struct FunctionCallResultRouter {
    pending: RwLock<HashMap<String, PendingFunctionCall>>,
}

impl FunctionCallResultRouter {
    pub fn new() -> Self {
        Self {
            pending: RwLock::new(HashMap::new()),
        }
    }

    pub async fn register(
        &self,
        function_call_id: String,
        parent_allocation_id: String,
        original_function_call_id: String,
        executor_id: ExecutorId,
    ) {
        self.pending.write().await.insert(
            function_call_id,
            PendingFunctionCall {
                parent_allocation_id,
                original_function_call_id,
                executor_id,
            },
        );
    }

    /// Register only if no entry exists for this function_call_id.
    ///
    /// Returns `true` if inserted, `false` if an entry already exists.
    /// This prevents `handle_log_entry` from overwriting a re-registration
    /// made by `try_route_result` for tail-call chains.
    pub async fn register_if_absent(
        &self,
        function_call_id: String,
        parent_allocation_id: String,
        original_function_call_id: String,
        executor_id: ExecutorId,
    ) -> bool {
        use std::collections::hash_map::Entry;
        let mut pending = self.pending.write().await;
        match pending.entry(function_call_id) {
            Entry::Occupied(_) => false,
            Entry::Vacant(v) => {
                v.insert(PendingFunctionCall {
                    parent_allocation_id,
                    original_function_call_id,
                    executor_id,
                });
                true
            }
        }
    }

    async fn take(&self, function_call_id: &str) -> Option<PendingFunctionCall> {
        self.pending.write().await.remove(function_call_id)
    }
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
        Self {
            indexify_state,
            executor_manager,
            blob_storage_registry,
            function_call_result_router: Arc::new(FunctionCallResultRouter::new()),
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

        // Spawn the background command generator for this executor.
        spawn_command_generator(
            executor_id.clone(),
            self.executor_manager.clone(),
            self.blob_storage_registry.clone(),
            self.indexify_state.clone(),
        )
        .await;

        Ok(())
    }
}

/// Build a RunAllocation command from an internal `Allocation` model.
fn build_run_allocation_command(
    emitter: &mut CommandEmitter,
    allocation: &data_model::Allocation,
    blob_storage_registry: &BlobStorageRegistry,
) -> executor_api_pb::Command {
    let blob_store_url_scheme = blob_storage_registry
        .get_blob_store(&allocation.namespace)
        .get_url_scheme();
    let blob_store_url = blob_storage_registry
        .get_blob_store(&allocation.namespace)
        .get_url();

    let mut args = vec![];
    for input_arg in &allocation.input_args {
        args.push(executor_api_pb::DataPayload {
            id: Some(input_arg.data_payload.id.clone()),
            uri: Some(blob_store_path_to_url(
                &input_arg.data_payload.path,
                &blob_store_url_scheme,
                &blob_store_url,
            )),
            size: Some(input_arg.data_payload.size),
            sha256_hash: Some(input_arg.data_payload.sha256_hash.clone()),
            encoding: Some(
                crate::pb_helpers::string_to_data_payload_encoding(
                    &input_arg.data_payload.encoding,
                )
                .into(),
            ),
            encoding_version: Some(0),
            offset: Some(input_arg.data_payload.offset),
            metadata_size: Some(input_arg.data_payload.metadata_size),
            source_function_call_id: input_arg.function_call_id.as_ref().map(|id| id.to_string()),
            content_type: Some(input_arg.data_payload.encoding.clone()),
        });
    }

    let request_data_payload_uri_prefix = format!(
        "{}/{}",
        blob_store_url,
        data_model::DataPayload::request_key_prefix(
            &allocation.namespace,
            &allocation.application,
            &allocation.request_id,
        ),
    );

    let allocation_pb = executor_api_pb::Allocation {
        function: Some(executor_api_pb::FunctionRef {
            namespace: Some(allocation.namespace.clone()),
            application_name: Some(allocation.application.clone()),
            function_name: Some(allocation.function.clone()),
            application_version: None,
        }),
        container_id: Some(allocation.target.container_id.get().to_string()),
        allocation_id: Some(allocation.id.to_string()),
        function_call_id: Some(allocation.function_call_id.to_string()),
        request_id: Some(allocation.request_id.to_string()),
        args,
        request_data_payload_uri_prefix: Some(request_data_payload_uri_prefix.clone()),
        request_error_payload_uri_prefix: Some(request_data_payload_uri_prefix),
        function_call_metadata: Some(allocation.call_metadata.clone().into()),
        replay_mode: None,
        last_event_clock: None,
    };

    let seq = emitter.next_seq();
    executor_api_pb::Command {
        seq,
        command: Some(executor_api_pb::command::Command::RunAllocation(
            executor_api_pb::RunAllocation {
                allocation: Some(allocation_pb),
            },
        )),
    }
}

/// Build an AddContainer command by looking up container data from state.
async fn build_add_container_command(
    emitter: &mut CommandEmitter,
    container_id: &ContainerId,
    blob_storage_registry: &BlobStorageRegistry,
    indexify_state: &IndexifyState,
) -> Option<executor_api_pb::Command> {
    let state = indexify_state.app_state.load();
    let fc = state.scheduler.function_containers.get(container_id)?;

    // Skip terminated containers
    if matches!(
        fc.desired_state,
        data_model::ContainerState::Terminated { .. }
    ) {
        return None;
    }

    let fe = &fc.function_container;

    let cg_version = state
        .indexes
        .application_versions
        .get(&data_model::ApplicationVersion::key_from(
            &fe.namespace,
            &fe.application_name,
            &fe.version,
        ))
        .cloned();

    let cg_node = cg_version
        .as_ref()
        .and_then(|v| v.functions.get(&fe.function_name).cloned());

    let code_payload_pb = cg_version.and_then(|v| v.code).map(|code| {
        let blob_store_url_scheme = blob_storage_registry
            .get_blob_store(&fe.namespace)
            .get_url_scheme();
        let blob_store_url = blob_storage_registry
            .get_blob_store(&fe.namespace)
            .get_url();
        executor_api_pb::DataPayload {
            id: Some(code.id.clone()),
            uri: Some(blob_store_path_to_url(
                &code.path,
                &blob_store_url_scheme,
                &blob_store_url,
            )),
            size: Some(code.size),
            sha256_hash: Some(code.sha256_hash.clone()),
            encoding: Some(executor_api_pb::DataPayloadEncoding::BinaryZip.into()),
            encoding_version: Some(0),
            offset: Some(0),
            metadata_size: Some(0),
            source_function_call_id: None,
            content_type: Some("application/zip".to_string()),
        }
    });

    let fe_type_pb = match fe.container_type {
        data_model::ContainerType::Function => executor_api_pb::ContainerType::Function,
        data_model::ContainerType::Sandbox => executor_api_pb::ContainerType::Sandbox,
    };

    let sandbox_metadata = sandbox_metadata_to_pb(fe);

    let resources_pb: Option<executor_api_pb::ContainerResources> =
        fe.resources.clone().try_into().ok();

    let initialization_timeout_ms = cg_node
        .as_ref()
        .map(|n| n.initialization_timeout.0)
        .unwrap_or_else(|| {
            fe.timeout_secs
                .saturating_mul(1000)
                .try_into()
                .unwrap_or(u32::MAX)
        });

    let allocation_timeout_ms = cg_node.map(|n| n.timeout.0).unwrap_or_else(|| {
        fe.timeout_secs
            .saturating_mul(1000)
            .try_into()
            .unwrap_or(u32::MAX)
    });

    let fe_description_pb = executor_api_pb::ContainerDescription {
        id: Some(fe.id.get().to_string()),
        function: Some(executor_api_pb::FunctionRef {
            namespace: Some(fe.namespace.clone()),
            application_name: Some(fe.application_name.clone()),
            function_name: Some(fe.function_name.clone()),
            application_version: Some(fe.version.to_string()),
        }),
        secret_names: cg_node_secret_names(&state.indexes, fe),
        initialization_timeout_ms: Some(initialization_timeout_ms),
        application: code_payload_pb,
        allocation_timeout_ms: Some(allocation_timeout_ms),
        resources: resources_pb,
        max_concurrency: Some(fe.max_concurrency),
        sandbox_metadata,
        container_type: Some(fe_type_pb.into()),
        pool_id: fe.pool_id.as_ref().map(|p| p.get().to_string()),
    };

    drop(state);

    let seq = emitter.next_seq();
    Some(executor_api_pb::Command {
        seq,
        command: Some(executor_api_pb::command::Command::AddContainer(
            executor_api_pb::AddContainer {
                container: Some(fe_description_pb),
            },
        )),
    })
}

/// Build an UpdateContainerDescription command for a pre-existing container
/// whose description has changed (e.g. sandbox_id set on warm-pool claim).
///
/// Compares the current state with what the CommandEmitter already sent and
/// emits only the changed fields.  Updates `emitter.known_containers` so
/// subsequent comparisons stay correct.
async fn build_update_container_description_command(
    emitter: &mut CommandEmitter,
    container_id: &ContainerId,
    indexify_state: &IndexifyState,
) -> Option<executor_api_pb::Command> {
    let state = indexify_state.app_state.load();
    let fc = state.scheduler.function_containers.get(container_id)?;
    let fe = &fc.function_container;

    // Skip non-sandbox containers -- only sandbox descriptions change today.
    if fe.container_type != data_model::ContainerType::Sandbox {
        return None;
    }

    let cid = container_id.get().to_string();

    // Build current sandbox_metadata from state.
    let current_sandbox_metadata = sandbox_metadata_to_pb(fe);

    // Compare with what was previously sent.
    let known = emitter.known_containers.get(&cid);
    let known_sandbox_metadata = known.and_then(|k| k.sandbox_metadata.clone());
    if known_sandbox_metadata == current_sandbox_metadata {
        return None; // No change
    }

    let update = executor_api_pb::UpdateContainerDescription {
        container_id: cid.clone(),
        sandbox_metadata: current_sandbox_metadata.clone(),
    };

    // Update the known state so subsequent comparisons are correct.
    if let Some(known_mut) = emitter.known_containers.get_mut(&cid) {
        known_mut.sandbox_metadata = current_sandbox_metadata;
    }

    let seq = emitter.next_seq();
    Some(executor_api_pb::Command {
        seq,
        command: Some(executor_api_pb::command::Command::UpdateContainerDescription(update)),
    })
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

/// Convert a `data_model::Container` to `Option<SandboxMetadata>` proto.
///
/// Returns `None` for non-sandbox containers.
fn sandbox_metadata_to_pb(fe: &data_model::Container) -> Option<executor_api_pb::SandboxMetadata> {
    if fe.container_type != data_model::ContainerType::Sandbox {
        return None;
    }
    Some(executor_api_pb::SandboxMetadata {
        image: fe.image.clone(),
        timeout_secs: if fe.timeout_secs > 0 {
            Some(fe.timeout_secs)
        } else {
            None
        },
        entrypoint: fe.entrypoint.clone(),
        network_policy: fe.network_policy.as_ref().map(network_policy_to_pb),
        sandbox_id: fe.sandbox_id.as_ref().map(|s| s.get().to_string()),
        snapshot_uri: fe.snapshot_uri.clone(),
    })
}

/// Helper to get secret_names from in-memory state for a container.
fn cg_node_secret_names(
    indexes: &crate::state_store::in_memory_state::InMemoryState,
    fe: &data_model::Container,
) -> Vec<String> {
    indexes
        .application_versions
        .get(&data_model::ApplicationVersion::key_from(
            &fe.namespace,
            &fe.application_name,
            &fe.version,
        ))
        .and_then(|v| v.functions.get(&fe.function_name))
        .and_then(|n| n.secret_names.clone())
        .unwrap_or_default()
}

/// Perform a full sync: fetch complete executor state, diff via emitter,
/// and push all resulting commands into the connection's pending_commands
/// buffer.
async fn do_full_sync_buffered(
    executor_id: &ExecutorId,
    executor_manager: &ExecutorManager,
    emitter: &mut CommandEmitter,
    indexify_state: &IndexifyState,
) {
    let Some(snapshot) = executor_manager.get_executor_state(executor_id).await else {
        warn!(
            executor_id = executor_id.get(),
            "command_generator: executor state not available for full sync"
        );
        return;
    };

    let commands = emitter.emit_commands(&snapshot);
    if !commands.is_empty() {
        info!(
            executor_id = executor_id.get(),
            num_commands = commands.len(),
            "command_generator: full sync emitting commands"
        );
        let connections = indexify_state.executor_connections.read().await;
        if let Some(conn) = connections.get(executor_id) {
            conn.push_commands(commands).await;
        }
    }
    // All commands buffered -- update tracking state
    emitter.commit_snapshot(&snapshot);
}

/// Spawn a background task that consumes `ExecutorEvent`s from the
/// connection's event channel and pushes generated `Command` messages into
/// the connection's pending_commands buffer for long-poll delivery.
///
/// The task runs until the event channel closes (connection deregistered).
/// It replaces the old `command_stream_loop` which pushed directly to a gRPC
/// stream.
async fn spawn_command_generator(
    executor_id: ExecutorId,
    executor_manager: Arc<ExecutorManager>,
    blob_storage_registry: Arc<BlobStorageRegistry>,
    indexify_state: Arc<IndexifyState>,
) {
    // Get handles from the connection
    let (event_rx, emitter) = {
        let connections = indexify_state.executor_connections.read().await;
        let Some(conn) = connections.get(&executor_id) else {
            warn!(
                executor_id = executor_id.get(),
                "spawn_command_generator: connection not found"
            );
            return;
        };
        (conn.event_rx.clone(), conn.emitter.clone())
    };

    let eid = executor_id.clone();
    let indexify_state_for_handle = indexify_state.clone();
    let handle = tokio::spawn(async move {
        // Initial full sync if emitter hasn't synced yet
        {
            let mut emitter_guard = emitter.lock().await;
            if !emitter_guard.has_synced {
                do_full_sync_buffered(&eid, &executor_manager, &mut emitter_guard, &indexify_state)
                    .await;
            }
        }

        loop {
            let event = {
                let mut rx_guard = event_rx.lock().await;
                match rx_guard.recv().await {
                    Some(e) => e,
                    None => {
                        info!(
                            executor_id = eid.get(),
                            "command_generator: event channel closed"
                        );
                        return;
                    }
                }
            };

            let mut emitter_guard = emitter.lock().await;
            let mut commands = Vec::new();

            match event {
                ExecutorEvent::AllocationCreated(allocation) => {
                    let alloc_id = allocation.id.to_string();
                    if emitter_guard.known_allocations.contains(&alloc_id) {
                        continue;
                    }
                    let cmd = build_run_allocation_command(
                        &mut emitter_guard,
                        &allocation,
                        &blob_storage_registry,
                    );
                    emitter_guard.track_allocation(alloc_id);
                    info!(
                        executor_id = eid.get(),
                        allocation_id = %allocation.id,
                        request_id = %allocation.request_id,
                        namespace = %allocation.namespace,
                        app = %allocation.application,
                        "fn" = %allocation.function,
                        "command_generator: emitting RunAllocation"
                    );
                    commands.push(cmd);
                }
                ExecutorEvent::ContainerAdded(container_id) => {
                    let cid = container_id.get().to_string();
                    if emitter_guard.known_containers.contains_key(&cid) {
                        continue;
                    }
                    if let Some(cmd) = build_add_container_command(
                        &mut emitter_guard,
                        &container_id,
                        &blob_storage_registry,
                        &indexify_state,
                    )
                    .await
                    {
                        let desc = match &cmd.command {
                            Some(executor_api_pb::command::Command::AddContainer(add)) => {
                                add.container.clone().unwrap_or_default()
                            }
                            _ => Default::default(),
                        };
                        emitter_guard.track_container(cid, desc.clone());
                        info!(
                            executor_id = eid.get(),
                            container_id = container_id.get(),
                            namespace = ?desc.function.as_ref().and_then(|f| f.namespace.as_deref()),
                            app = ?desc.function.as_ref().and_then(|f| f.application_name.as_deref()),
                            "fn" = ?desc.function.as_ref().and_then(|f| f.function_name.as_deref()),
                            sandbox_id = ?desc.sandbox_metadata.as_ref().and_then(|m| m.sandbox_id.as_deref()),
                            "command_generator: emitting AddContainer"
                        );
                        commands.push(cmd);
                    }
                }
                ExecutorEvent::ContainerRemoved(container_id) => {
                    let cid = container_id.get().to_string();
                    let tracked_desc = emitter_guard.known_containers.get(&cid).cloned();
                    emitter_guard.untrack_container(&cid);
                    let seq = emitter_guard.next_seq();
                    let cmd = executor_api_pb::Command {
                        seq,
                        command: Some(executor_api_pb::command::Command::RemoveContainer(
                            executor_api_pb::RemoveContainer {
                                container_id: cid.clone(),
                                reason: None,
                            },
                        )),
                    };
                    info!(
                        executor_id = eid.get(),
                        container_id = %cid,
                        namespace = ?tracked_desc.as_ref().and_then(|d| d.function.as_ref()).and_then(|f| f.namespace.as_deref()).unwrap_or_default(),
                        app = ?tracked_desc.as_ref().and_then(|d| d.function.as_ref()).and_then(|f| f.application_name.as_deref()).unwrap_or_default(),
                        "fn" = ?tracked_desc.as_ref().and_then(|d| d.function.as_ref()).and_then(|f| f.function_name.as_deref()).unwrap_or_default(),
                        "command_generator: emitting RemoveContainer"
                    );
                    commands.push(cmd);
                }
                ExecutorEvent::ContainerDescriptionChanged(container_id) => {
                    if let Some(cmd) = build_update_container_description_command(
                        &mut emitter_guard,
                        &container_id,
                        &indexify_state,
                    )
                    .await
                    {
                        let cid = container_id.get().to_string();
                        let desc = emitter_guard.known_containers.get(&cid);
                        info!(
                            executor_id = eid.get(),
                            container_id = container_id.get(),
                            namespace = ?desc.and_then(|d| d.function.as_ref()).and_then(|f| f.namespace.as_deref()).unwrap_or_default(),
                            sandbox_id = ?desc.and_then(|d| d.sandbox_metadata.as_ref()).and_then(|m| m.sandbox_id.as_deref()).unwrap_or_default(),
                            "command_generator: emitting UpdateContainerDescription"
                        );
                        commands.push(cmd);
                    }
                }
                ExecutorEvent::SnapshotContainer {
                    container_id,
                    snapshot_id,
                    upload_uri,
                } => {
                    info!(
                        executor_id = eid.get(),
                        container_id = container_id.get(),
                        snapshot_id = %snapshot_id,
                        "command_generator: emitting SnapshotContainer"
                    );
                    let seq = emitter_guard.next_seq();
                    commands.push(executor_api_pb::Command {
                        seq,
                        command: Some(executor_api_pb::command::Command::SnapshotContainer(
                            executor_api_pb::SnapshotContainer {
                                container_id: container_id.get().to_string(),
                                snapshot_id,
                                upload_uri,
                            },
                        )),
                    });
                }
                ExecutorEvent::FullSync => {
                    do_full_sync_buffered(
                        &eid,
                        &executor_manager,
                        &mut emitter_guard,
                        &indexify_state,
                    )
                    .await;
                }
            }

            if !commands.is_empty() {
                let connections = indexify_state.executor_connections.read().await;
                if let Some(conn) = connections.get(&eid) {
                    conn.push_commands(commands).await;
                }
            }
        }
    });

    // Store the handle on the connection so it gets aborted on deregistration
    let mut connections = indexify_state_for_handle.executor_connections.write().await;
    if let Some(conn) = connections.get_mut(&executor_id) {
        // Abort any existing command generator
        if let Some(old_handle) = conn.command_generator_handle.take() {
            old_handle.abort();
        }
        conn.command_generator_handle = Some(handle);
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

    if let Some(seq) = acked_seq {
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
        let req = request.into_inner();
        let executor_id: ExecutorId = req
            .executor_id
            .ok_or(Status::invalid_argument("executor_id required"))?
            .into();

        // Touch the executor liveness
        self.executor_manager
            .heartbeat_v2(&executor_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Process full state if present
        let had_state = req.full_state.is_some();
        if let Some(full_state) = req.full_state {
            info!(
                executor_id = executor_id.get(),
                "processing full state sync"
            );
            self.handle_full_state(&executor_id, full_state).await?;
        }

        // Check if the server knows this executor. If unknown, skip report
        // processing (the reports refer to allocations that were already marked
        // failed when the executor was deregistered) and ask for full state.
        let executor_known = if had_state {
            true // just processed full state, executor is now known
        } else {
            let runtime_data = self.executor_manager.runtime_data_read().await;
            runtime_data.contains_key(&executor_id)
        };

        if executor_known {
            // Process command responses
            if !req.command_responses.is_empty() {
                process_command_responses(
                    &self.indexify_state,
                    &executor_id,
                    req.command_responses,
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            }

            // Process allocation outcomes
            for item in req.allocation_outcomes {
                match item.outcome {
                    Some(executor_api_pb::allocation_outcome::Outcome::Completed(completed)) => {
                        let fc_id = completed.function_call_id.clone();
                        if let Err(e) = process_allocation_completed(
                            &self.indexify_state,
                            &self.blob_storage_registry,
                            &executor_id,
                            completed.clone(),
                        )
                        .await
                        {
                            warn!(executor_id = executor_id.get(), error = %e, "heartbeat: process_allocation_completed failed");
                        }
                        if let Some(fc_id) = &fc_id {
                            try_route_result(
                                &self.function_call_result_router,
                                fc_id,
                                &completed,
                                &self.indexify_state,
                            )
                            .await;
                        }
                    }
                    Some(executor_api_pb::allocation_outcome::Outcome::Failed(failed)) => {
                        let fc_id = failed.function_call_id.clone();
                        if let Err(e) = process_allocation_failed(
                            &self.indexify_state,
                            &self.blob_storage_registry,
                            &executor_id,
                            failed.clone(),
                        )
                        .await
                        {
                            warn!(executor_id = executor_id.get(), error = %e, "heartbeat: process_allocation_failed failed");
                        }
                        if let Some(fc_id) = &fc_id {
                            try_route_failure(
                                &self.function_call_result_router,
                                fc_id,
                                &failed,
                                &self.indexify_state,
                            )
                            .await;
                        }
                    }
                    None => {}
                }
            }

            // Process allocation log entries (CallFunction)
            for log_entry in req.allocation_log_entries {
                if let Err(e) = handle_log_entry(
                    &log_entry,
                    &executor_id,
                    &self.function_call_result_router,
                    &self.indexify_state,
                    &self.blob_storage_registry,
                )
                .await
                {
                    warn!(executor_id = executor_id.get(), error = %e, "heartbeat: handle_log_entry_v2 failed");
                }
            }
        } else {
            let has_reports = !req.command_responses.is_empty() ||
                !req.allocation_outcomes.is_empty() ||
                !req.allocation_log_entries.is_empty();
            if has_reports {
                warn!(
                    executor_id = executor_id.get(),
                    command_responses = req.command_responses.len(),
                    allocation_outcomes = req.allocation_outcomes.len(),
                    allocation_log_entries = req.allocation_log_entries.len(),
                    "dropping reports from unknown executor"
                );
            }
        }

        let send_state = !executor_known;

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

/// Process an allocation log entry received via heartbeat.
/// Adapted from the old `handle_log_entry` -- uses `executor_id` for router
/// registration instead of a direct `result_tx` channel.
async fn handle_log_entry(
    log_entry: &executor_api_pb::AllocationLogEntry,
    executor_id: &ExecutorId,
    router: &Arc<FunctionCallResultRouter>,
    indexify_state: &Arc<IndexifyState>,
    blob_storage_registry: &Arc<BlobStorageRegistry>,
) -> Result<()> {
    let allocation_id = &log_entry.allocation_id;

    match &log_entry.entry {
        Some(executor_api_pb::allocation_log_entry::Entry::CallFunction(call)) => {
            // Register in router so results get pushed back to this executor.
            // We register each individual function call ID from the updates,
            // not just the root_function_call_id. When a CallFunction contains
            // multiple function calls (e.g. a .map() that fans out), each child
            // allocation completes with its own function_call_id. The router
            // must match on those individual IDs to route results back.
            if let Some(ref updates) = call.updates {
                for update in &updates.updates {
                    if let Some(ref op) = update.op &&
                        let executor_api_pb::execution_plan_update::Op::FunctionCall(fc) = op &&
                        let Some(ref individual_fc_id) = fc.id
                    {
                        let inserted = router
                            .register_if_absent(
                                individual_fc_id.clone(),
                                allocation_id.clone(),
                                individual_fc_id.clone(),
                                executor_id.clone(),
                            )
                            .await;

                        if inserted {
                            debug!(
                                executor_id = executor_id.get(),
                                allocation_id = %allocation_id,
                                function_call_id = %individual_fc_id,
                                "heartbeat: registered individual function call in router"
                            );
                        } else {
                            debug!(
                                executor_id = executor_id.get(),
                                allocation_id = %allocation_id,
                                function_call_id = %individual_fc_id,
                                "heartbeat: function call already in router (tail-call \
                                 re-registration), skipping overwrite"
                            );
                        }
                    }
                }
            }

            // Extract namespace/application/request_id from the proto message
            let namespace = call
                .namespace
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("CallFunction missing namespace"))?
                .clone();
            let application = call
                .application
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("CallFunction missing application"))?
                .clone();
            let request_id = call
                .request_id
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("CallFunction missing request_id"))?
                .clone();
            let source_function_call_id = call
                .source_function_call_id
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("CallFunction missing source_function_call_id"))?
                .clone();

            let updates = call
                .updates
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("CallFunction missing updates"))?;

            let root_function_call_id = updates
                .root_function_call_id
                .clone()
                .map(FunctionCallId::from)
                .unwrap_or_else(|| FunctionCallId::from(nanoid::nanoid!()));

            let mut compute_ops = Vec::new();
            for update in &updates.updates {
                compute_ops.push(to_internal_compute_op(
                    update.clone(),
                    blob_storage_registry,
                    Some(source_function_call_id.clone()),
                )?);
            }

            info!(
                executor_id = executor_id.get(),
                allocation_id = %allocation_id,
                namespace = %namespace,
                app = %application,
                request_id = %request_id,
                source_function_call_id = %source_function_call_id,
                "heartbeat: dispatching CallFunction to state machine"
            );

            let request = StateMachineUpdateRequest {
                payload: RequestPayload::CreateFunctionCall(
                    crate::state_store::requests::FunctionCallRequest {
                        namespace: namespace.clone(),
                        application_name: application.clone(),
                        request_id: request_id.clone(),
                        graph_updates: crate::state_store::requests::RequestUpdates {
                            request_updates: compute_ops,
                            output_function_call_id: root_function_call_id,
                        },
                        source_function_call_id: FunctionCallId::from(source_function_call_id),
                    },
                ),
            };

            indexify_state.write(request).await?;
        }
        Some(executor_api_pb::allocation_log_entry::Entry::FunctionCallResult(_)) => {
            // Server -> Executor direction only -- should not be received from client
            warn!(
                executor_id = executor_id.get(),
                allocation_id = %allocation_id,
                "heartbeat: unexpected FunctionCallResult from client"
            );
        }
        None => {
            warn!(
                executor_id = executor_id.get(),
                allocation_id = %allocation_id,
                "heartbeat: empty log entry"
            );
        }
    }
    Ok(())
}

/// Build a `FunctionCallResult` log entry to route back to the parent executor.
///
/// Shared by the `Value`/`None` arms of `try_route_result` to avoid
/// duplication.
fn build_result_log_entry(
    parent_allocation_id: String,
    original_function_call_id: String,
    completed: &executor_api_pb::AllocationCompleted,
    return_value: Option<executor_api_pb::DataPayload>,
) -> executor_api_pb::AllocationLogEntry {
    executor_api_pb::AllocationLogEntry {
        allocation_id: parent_allocation_id,
        clock: 0, // TODO: server-assigned monotonic clock
        entry: Some(
            executor_api_pb::allocation_log_entry::Entry::FunctionCallResult(
                executor_api_pb::FunctionCallResult {
                    namespace: completed
                        .function
                        .as_ref()
                        .and_then(|f| f.namespace.clone()),
                    request_id: completed.request_id.clone(),
                    function_call_id: Some(original_function_call_id),
                    outcome_code: Some(executor_api_pb::AllocationOutcomeCode::Success.into()),
                    failure_reason: None,
                    return_value,
                    request_error: None,
                },
            ),
        ),
    }
}

/// After processing a child AllocationCompleted, check if any parent is waiting
/// for this function call result and push it into their pending_results buffer.
///
/// When the child returns a direct `Value`, the result is pushed immediately
/// to the parent. When the child returns `Updates` (graph updates that spawn
/// downstream function calls), the pending entry is re-registered under the
/// downstream `root_function_call_id` so the final value in the chain gets
/// routed to the parent.
async fn try_route_result(
    router: &Arc<FunctionCallResultRouter>,
    function_call_id: &str,
    completed: &executor_api_pb::AllocationCompleted,
    indexify_state: &Arc<IndexifyState>,
) {
    if let Some(pending) = router.take(function_call_id).await {
        match &completed.return_value {
            Some(executor_api_pb::allocation_completed::ReturnValue::Value(dp)) => {
                debug!(
                    function_call_id = %function_call_id,
                    parent_allocation_id = %pending.parent_allocation_id,
                    "try_route_result: routing Value result to parent"
                );

                let log_entry = build_result_log_entry(
                    pending.parent_allocation_id,
                    pending.original_function_call_id.clone(),
                    completed,
                    Some(dp.clone()),
                );

                let connections = indexify_state.executor_connections.read().await;
                if let Some(conn) = connections.get(&pending.executor_id) {
                    conn.push_result(log_entry).await;
                } else {
                    warn!(
                        function_call_id = %function_call_id,
                        executor_id = pending.executor_id.get(),
                        "try_route_result: executor connection not found, dropping result"
                    );
                }
            }
            Some(executor_api_pb::allocation_completed::ReturnValue::Updates(updates)) => {
                // The child returned graph updates that will spawn downstream
                // function calls. Re-register the pending entry under the
                // downstream root_function_call_id so that when the final
                // function in the chain completes with a Value, the result
                // gets routed back to the parent.
                if let Some(ref downstream_fc_id) = updates.root_function_call_id {
                    info!(
                        function_call_id = %function_call_id,
                        downstream_function_call_id = %downstream_fc_id,
                        parent_allocation_id = %pending.parent_allocation_id,
                        original_function_call_id = %pending.original_function_call_id,
                        "try_route_result: child returned Updates, re-registering \
                         pending entry for downstream function call"
                    );
                    router
                        .register(
                            downstream_fc_id.clone(),
                            pending.parent_allocation_id,
                            pending.original_function_call_id,
                            pending.executor_id,
                        )
                        .await;
                } else {
                    warn!(
                        function_call_id = %function_call_id,
                        "try_route_result: child returned Updates without \
                         root_function_call_id, cannot re-register"
                    );
                }
            }
            None => {
                // No return value at all -- send success with empty payload
                debug!(
                    function_call_id = %function_call_id,
                    parent_allocation_id = %pending.parent_allocation_id,
                    "try_route_result: routing empty result to parent"
                );

                let log_entry = build_result_log_entry(
                    pending.parent_allocation_id,
                    pending.original_function_call_id.clone(),
                    completed,
                    None,
                );

                let connections = indexify_state.executor_connections.read().await;
                if let Some(conn) = connections.get(&pending.executor_id) {
                    conn.push_result(log_entry).await;
                } else {
                    warn!(
                        function_call_id = %function_call_id,
                        executor_id = pending.executor_id.get(),
                        "try_route_result: executor connection not found, dropping result"
                    );
                }
            }
        }
    } else {
        debug!(
            function_call_id = %function_call_id,
            allocation_id = %completed.allocation_id,
            "try_route_result: no match in router"
        );
    }
}

async fn try_route_failure(
    router: &Arc<FunctionCallResultRouter>,
    function_call_id: &str,
    failed: &executor_api_pb::AllocationFailed,
    indexify_state: &Arc<IndexifyState>,
) {
    if let Some(pending) = router.take(function_call_id).await {
        let log_entry = executor_api_pb::AllocationLogEntry {
            allocation_id: pending.parent_allocation_id,
            clock: 0,
            entry: Some(
                executor_api_pb::allocation_log_entry::Entry::FunctionCallResult(
                    executor_api_pb::FunctionCallResult {
                        namespace: failed.function.as_ref().and_then(|f| f.namespace.clone()),
                        request_id: failed.request_id.clone(),
                        function_call_id: Some(pending.original_function_call_id.clone()),
                        outcome_code: Some(executor_api_pb::AllocationOutcomeCode::Failure.into()),
                        failure_reason: Some(failed.reason),
                        return_value: None,
                        request_error: failed.request_error.clone(),
                    },
                ),
            ),
        };

        let connections = indexify_state.executor_connections.read().await;
        if let Some(conn) = connections.get(&pending.executor_id) {
            conn.push_result(log_entry).await;
        } else {
            warn!(
                function_call_id = %function_call_id,
                executor_id = pending.executor_id.get(),
                "try_route_failure: executor connection not found, dropping result"
            );
        }
    }
}

#[cfg(test)]
mod tests {
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

    use crate::{
        data_model::{self, ContainerTerminationReason, ContainerType},
        executors::ExecutorStateSnapshot,
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

    #[test]
    fn test_command_emitter_first_call_emits_full_state() {
        let mut emitter = super::CommandEmitter::new();

        let desired = ExecutorStateSnapshot {
            containers: vec![make_fe_description("c1")],
            allocations: vec![make_allocation("a1")],
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
            clock: Some(1),
        };
        emitter.emit_commands(&desired1);
        emitter.commit_snapshot(&desired1);

        // Second: container removed
        let desired2 = ExecutorStateSnapshot {
            containers: vec![],
            allocations: vec![],
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
            clock: Some(1),
        };
        emitter.emit_commands(&desired1);
        emitter.commit_snapshot(&desired1);

        // Second: same container, new allocation added
        let desired2 = ExecutorStateSnapshot {
            containers: vec![make_fe_description("c1")],
            allocations: vec![make_allocation("a1"), make_allocation("a2")],
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
            clock: Some(1),
        };
        emitter.emit_commands(&desired1);
        emitter.commit_snapshot(&desired1);

        // Second: allocation completed (removed from desired state)
        let desired2 = ExecutorStateSnapshot {
            containers: vec![make_fe_description("c1")],
            allocations: vec![],
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
            clock: Some(1),
        };
        let cmds1 = emitter.emit_commands(&desired1);
        assert_eq!(cmds1.last().unwrap().seq, 2);
        emitter.commit_snapshot(&desired1);

        // Second batch: 1 command (seq 3)
        let desired2 = ExecutorStateSnapshot {
            containers: vec![make_fe_description("c1")],
            allocations: vec![make_allocation("a1"), make_allocation("a2")],
            clock: Some(2),
        };
        let cmds2 = emitter.emit_commands(&desired2);
        assert_eq!(cmds2[0].seq, 3, "seq should continue from previous batch");
    }
}
