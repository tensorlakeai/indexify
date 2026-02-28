use std::{
    cmp,
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
    vec,
};

use anyhow::Result;
use priority_queue::PriorityQueue;
use tokio::{
    sync::{Mutex, RwLock, watch},
    time::Instant,
};
use tracing::{error, info, trace};

use crate::{
    blob_store::registry::BlobStorageRegistry,
    data_model::{
        self,
        ApplicationVersion,
        ChangeType,
        ContainerState,
        ExecutorId,
        ExecutorMetadata,
        ExecutorRemovedEvent,
        FunctionURI,
        SandboxStatus,
        StateChange,
        StateChangeBuilder,
        StateChangeId,
    },
    executor_api::executor_api_pb::{
        self,
        Allocation,
        ContainerDescription,
        ContainerType as ContainerTypePb,
        DataPayloadEncoding,
        FunctionRef,
        SandboxMetadata,
    },
    http_objects::{self, ExecutorAllocations, ExecutorsAllocationsResponse, FnExecutor},
    pb_helpers::*,
    state_store::{
        IndexifyState,
        in_memory_state::{self, DesiredStateFunctionExecutor},
        requests::{DeregisterExecutorRequest, RequestPayload, StateMachineUpdateRequest},
    },
    utils::{dynamic_sleep::DynamicSleepFuture, get_epoch_time_in_ms},
};

/// Snapshot of executor state returned by `get_executor_state`.
/// Contains the containers, allocations, and clock that describe what the
/// server wants the dataplane to converge to.
pub struct ExecutorStateSnapshot {
    pub containers: Vec<ContainerDescription>,
    pub allocations: Vec<Allocation>,
    #[allow(dead_code)]
    pub clock: Option<u64>,
}

/// Executor timeout duration for heartbeat
pub const EXECUTOR_TIMEOUT: Duration = Duration::from_secs(30);

/// Timeout duration before deregistering executors that haven't re-registered
/// at service startup
pub const STARTUP_EXECUTOR_TIMEOUT: Duration = Duration::from_secs(60);

/// Wrapper for `tokio::time::Instant` that reverses the ordering for deadline.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct ReverseInstant(pub Instant);

impl Ord for ReverseInstant {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        other.0.cmp(&self.0) // Reverse ordering
    }
}

impl PartialOrd for ReverseInstant {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Returns a far future time for the heartbeat deadline.
/// This is used whenever there are no executors.
fn far_future() -> Instant {
    Instant::now() + Duration::from_secs(24 * 60 * 60)
}

/// ExecutorRuntimeData stores runtime state for an executor that is not
/// persisted in the state machine but is needed for efficient operation.
/// Acts as a presence marker (checked via `runtime_data.contains_key()`)
/// and tracks the executor's clock for delta processing.
#[derive(Debug, Clone)]
pub(crate) struct ExecutorRuntimeData {
    pub last_executor_clock: u64,
}

impl ExecutorRuntimeData {
    pub fn new(clock: u64) -> Self {
        Self {
            last_executor_clock: clock,
        }
    }

    pub fn update_clock(&mut self, clock: u64) {
        self.last_executor_clock = clock;
    }
}

pub struct ExecutorManager {
    heartbeat_deadline_queue: Mutex<PriorityQueue<ExecutorId, ReverseInstant>>,
    heartbeat_future: Arc<Mutex<DynamicSleepFuture>>,
    heartbeat_deadline_updater: watch::Sender<Instant>,
    indexify_state: Arc<IndexifyState>,
    runtime_data: RwLock<HashMap<ExecutorId, ExecutorRuntimeData>>,
    blob_store_registry: Arc<BlobStorageRegistry>,
}

impl ExecutorManager {
    pub async fn new(
        indexify_state: Arc<IndexifyState>,
        blob_store_registry: Arc<BlobStorageRegistry>,
    ) -> Arc<Self> {
        let (heartbeat_future, heartbeat_sender) = DynamicSleepFuture::new(
            far_future(),
            // Chunk duration for the heartbeat future is set to 2 seconds before the timeout
            // to allow for a 2-second buffer for changes to the next heartbeat deadline.
            EXECUTOR_TIMEOUT - Duration::from_secs(2),
            // Set a minimum sleep time of 1 second to avoid excessive wake-ups for executors
            // that heartbeat in the same second.
            Some(Duration::from_secs(1)),
        );
        let heartbeat_future = Arc::new(Mutex::new(heartbeat_future));
        let em = ExecutorManager {
            indexify_state,
            runtime_data: RwLock::new(HashMap::new()),
            heartbeat_deadline_queue: Mutex::new(PriorityQueue::new()),
            heartbeat_deadline_updater: heartbeat_sender,
            heartbeat_future,
            blob_store_registry,
        };

        let em = Arc::new(em);

        em.clone().schedule_clean_lapsed_executors();

        em
    }

    pub fn schedule_clean_lapsed_executors(self: Arc<Self>) {
        let indexify_state = self.indexify_state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(STARTUP_EXECUTOR_TIMEOUT).await;

            // Get all executor IDs that haven't registered but have allocations or
            // running sandboxes assigned to them. We combine both sources into a
            // single set and call deregister_lapsed_executor on each, which will
            // handle sandbox termination through the DeregisterExecutor event.
            let missing_executor_ids: HashSet<ExecutorId> = {
                let state = indexify_state.app_state.load();

                // Find executors with allocations that haven't registered
                let executors_with_allocations: HashSet<_> = state
                    .indexes
                    .allocations_by_executor
                    .keys()
                    .filter(|id| !state.scheduler.executors.contains_key(&**id))
                    .cloned()
                    .collect();

                // Find executors with running sandboxes that haven't registered
                let executors_with_sandboxes: HashSet<_> = state
                    .indexes
                    .sandboxes
                    .iter()
                    .filter_map(|(_, sandbox)| {
                        if sandbox.status != SandboxStatus::Running {
                            return None;
                        }
                        let executor_id = sandbox.executor_id.as_ref()?;
                        if state.scheduler.executors.contains_key(executor_id) {
                            return None;
                        }
                        Some(executor_id.clone())
                    })
                    .collect();

                // Combine both sets
                executors_with_allocations
                    .union(&executors_with_sandboxes)
                    .cloned()
                    .collect()
            };

            for executor_id in missing_executor_ids {
                info!(
                    executor_id = executor_id.get(),
                    "deregistering lapsed executor"
                );
                let executor_id_clone = executor_id.clone();
                if let Err(err) = self.deregister_lapsed_executor(executor_id).await {
                    error!(
                        executor_id = executor_id_clone.get(),
                        "failed to deregister lapsed executor: {:?}", err
                    );
                }
            }
        });
    }

    /// Lightweight heartbeat for v2 protocol: only refreshes the executor's
    /// liveness deadline without processing any state.  Returns an error if
    /// the deadline-update channel is closed.
    pub async fn heartbeat_v2(&self, executor_id: &ExecutorId) -> Result<()> {
        let peeked_deadline = {
            let new_deadline = ReverseInstant(Instant::now() + EXECUTOR_TIMEOUT);

            let mut state = self.heartbeat_deadline_queue.lock().await;

            if state.change_priority(executor_id, new_deadline).is_none() {
                state.push(executor_id.clone(), new_deadline);
                info!(
                    executor_id = executor_id.get(),
                    "v2 heartbeat received (first)"
                );
            } else {
                trace!(executor_id = executor_id.get(), "v2 heartbeat received");
            }

            state
                .peek()
                .map(|(_, deadline)| deadline.0)
                .unwrap_or_else(far_future)
        };

        self.heartbeat_deadline_updater.send(peeked_deadline)?;
        Ok(())
    }

    /// Register a new executor (used by v2 heartbeat full-state sync).
    pub async fn register_executor(&self, metadata: ExecutorMetadata) -> Result<()> {
        let mut runtime_data_write = self.runtime_data.write().await;
        let is_new = !runtime_data_write.contains_key(&metadata.id);
        runtime_data_write
            .entry(metadata.id.clone())
            .and_modify(|data| data.update_clock(metadata.clock))
            .or_insert_with(|| ExecutorRuntimeData::new(metadata.clock));
        if is_new {
            info!(executor_id = metadata.id.get(), "Executor registered");
        } else {
            info!(
                executor_id = metadata.id.get(),
                "Executor re-registered (full state sync)"
            );
        }
        Ok(())
    }

    /// Wait for the an executor heartbeat deadline to lapse.
    pub(crate) async fn wait_executor_heartbeat_deadline(&self) {
        let mut fut = self.heartbeat_future.lock().await;

        trace!("Waiting for next executor deadline");
        (&mut *fut).await;
    }

    /// Starts the heartbeat monitoring loop for executors
    pub async fn start_heartbeat_monitor(self: Arc<Self>, mut shutdown_rx: watch::Receiver<()>) {
        loop {
            tokio::select! {
                _ = self.wait_executor_heartbeat_deadline() => {
                    trace!("Received deadline, processing lapsed executors");
                    if let Err(err) = self.process_lapsed_executors().await {
                        error!("Failed to process lapsed executors: {:?}", err);
                    }
                }
                _= shutdown_rx.changed() => {
                    trace!("Received shutdown signal, shutting down heartbeat monitor");
                    break;
                }
            }
        }
    }

    /// Processes and deregisters executors that have missed their heartbeat
    pub(crate) async fn process_lapsed_executors(&self) -> Result<()> {
        let now = Instant::now();

        // 1. Get all executor IDs that have lapsed
        let mut lapsed_executors = Vec::new();
        {
            // Lock heartbeat_state briefly
            let mut queue = self.heartbeat_deadline_queue.lock().await;

            while let Some((_, next_deadline)) = queue.peek() {
                if next_deadline.0 > now {
                    break;
                }

                // Remove from queue and store for later processing
                if let Some((executor_id, _)) = queue.pop() {
                    lapsed_executors.push(executor_id);
                } else {
                    error!("peeked executor not found in queue");
                    break;
                }
            }

            // 2. Update the heartbeat deadline with the earliest executor's deadline
            match queue.peek() {
                Some((_, next_deadline)) => {
                    if let Err(err) = self.heartbeat_deadline_updater.send(next_deadline.0) {
                        error!("Failed to update heartbeat deadline: {:?}", err);
                    }
                }
                None => {
                    // Set a far future deadline, no executors in queue
                    trace!("No executors in queue, setting far future deadline");
                    if let Err(err) = self.heartbeat_deadline_updater.send(far_future()) {
                        error!("Failed to update heartbeat deadline: {:?}", err);
                    }
                }
            }
        }

        if !lapsed_executors.is_empty() {
            info!(count = lapsed_executors.len(), "Found lapsed executors");
        }

        // 3. Deregister each lapsed executor without holding the lock
        for executor_id in lapsed_executors {
            self.deregister_lapsed_executor(executor_id).await?;
        }

        Ok(())
    }

    async fn deregister_lapsed_executor(&self, executor_id: ExecutorId) -> Result<()> {
        info!(
            executor_id = executor_id.get(),
            "Deregistering lapsed executor (heartbeat timeout)"
        );
        self.runtime_data.write().await.remove(&executor_id);

        // Drop the ExecutorConnection — this drops the event sender, which
        // causes any active command_stream_loop to exit via recv() → None.
        self.indexify_state
            .deregister_executor_connection(&executor_id)
            .await;

        let last_state_change_id = self.indexify_state.state_change_id_seq.clone();
        let state_changes = tombstone_executor(&last_state_change_id, &executor_id)?;
        let sm_req = StateMachineUpdateRequest {
            payload: RequestPayload::DeregisterExecutor(DeregisterExecutorRequest {
                executor_id,
                state_changes,
            }),
        };
        self.indexify_state.write(sm_req).await?;
        Ok(())
    }

    /// Get read access to executor runtime data (for v2 heartbeat hash
    /// comparison).
    pub async fn runtime_data_read(
        &self,
    ) -> tokio::sync::RwLockReadGuard<'_, HashMap<ExecutorId, ExecutorRuntimeData>> {
        self.runtime_data.read().await
    }

    pub async fn desired_state(
        &self,
        executor_id: &ExecutorId,
    ) -> Option<in_memory_state::DesiredExecutorState> {
        let state = self.indexify_state.app_state.load();
        if let Some(executor) = state.scheduler.executors.get(executor_id) {
            if executor.tombstoned {
                return None;
            }
        } else {
            return None;
        }

        let Some(executor_server_metadata) = state.scheduler.executor_states.get(executor_id)
        else {
            info!(
                executor_id = executor_id.get(),
                "executor not yet reconciled, skipping desired state"
            );
            return None;
        };

        let fc_ids = executor_server_metadata.function_container_ids.clone();

        info!(
            executor_id = executor_id.get(),
            num_fc_ids = fc_ids.len(),
            fc_ids = ?fc_ids.iter().map(|id| id.get().to_string()).collect::<Vec<_>>(),
            "computing desired state: function container IDs from executor state"
        );

        let mut active_function_containers = Vec::new();
        for container_id in fc_ids {
            let Some(fc) = state.scheduler.function_containers.get(&container_id) else {
                error!(
                    executor_id = executor_id.get(),
                    container_id = container_id.get(),
                    "function container ID in executor state but NOT in function_containers map!"
                );
                continue;
            };
            if let ContainerState::Terminated { .. } = &fc.desired_state {
                info!(
                    executor_id = executor_id.get(),
                    container_id = container_id.get(),
                    fn_name = fc.function_container.function_name,
                    desired_state = ?fc.desired_state,
                    "excluding function container from desired state: terminated"
                );
            } else {
                active_function_containers.push(fc);
            }
        }

        info!(
            executor_id = executor_id.get(),
            num_active_fcs = active_function_containers.len(),
            active_fcs = ?active_function_containers.iter().map(|fc| format!("{}:{}", fc.function_container.id.get(), fc.function_container.function_name)).collect::<Vec<_>>(),
            "active function containers for desired state"
        );
        let mut function_executors = Vec::new();
        let mut task_allocations = std::collections::HashMap::new();
        for fe_meta in active_function_containers {
            let fe = &fe_meta.function_container;

            let cg_version = state
                .indexes
                .application_versions
                .get(&ApplicationVersion::key_from(
                    &fe.namespace,
                    &fe.application_name,
                    &fe.version,
                ))
                .cloned();
            let cg_node = cg_version
                .as_ref()
                .and_then(|cg_version| cg_version.functions.get(&fe.function_name).cloned());
            let desired_fc = DesiredStateFunctionExecutor {
                function_executor: fe_meta.clone(),
                resources: fe.resources.clone(),
                secret_names: cg_node
                    .as_ref()
                    .and_then(|cg_node| cg_node.secret_names.clone())
                    .unwrap_or_default(),
                initialization_timeout_ms: cg_node
                    .as_ref()
                    .map(|cg_node| cg_node.initialization_timeout.0)
                    .unwrap_or_else(|| {
                        fe.timeout_secs
                            .saturating_mul(1000)
                            .try_into()
                            .unwrap_or(u32::MAX)
                    }),
                code_payload: cg_version
                    .and_then(|cg_version| cg_version.code)
                    .map(|code| data_model::DataPayload {
                        id: code.id.clone(),
                        metadata_size: 0,
                        path: code.path.clone(),
                        size: code.size,
                        sha256_hash: code.sha256_hash.clone(),
                        offset: 0, // Code always uses its full BLOB
                        encoding: DataPayloadEncoding::BinaryZip.as_str_name().to_string(),
                    }),
                image: fe.image.clone(),
                allocation_timeout_ms: cg_node.map(|cg_node| cg_node.timeout.0).unwrap_or_else(
                    || {
                        fe.timeout_secs
                            .saturating_mul(1000)
                            .try_into()
                            .unwrap_or(u32::MAX)
                    },
                ),
                sandbox_timeout_secs: fe.timeout_secs,
                entrypoint: fe.entrypoint.clone(),
                network_policy: fe.network_policy.clone(),
                snapshot_uri: fe.snapshot_uri.clone(),
            };

            function_executors.push(Box::new(desired_fc));

            let allocations = state
                .indexes
                .allocations_by_executor
                .get(executor_id)
                .and_then(|allocations| allocations.get(&fe_meta.function_container.id.clone()))
                .map(|allocations| {
                    allocations
                        .values()
                        .map(|allocation| allocation.as_ref().clone())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            task_allocations.insert(fe_meta.function_container.id.clone(), allocations);
        }

        Some(in_memory_state::DesiredExecutorState {
            containers: function_executors,
            function_run_allocations: task_allocations,
            clock: state.indexes.clock,
        })
    }

    /// Get the desired state for an executor
    ///
    /// Function call results are delivered via the allocation_stream RPC,
    /// not included in the executor state snapshot.
    pub async fn get_executor_state(
        &self,
        executor_id: &ExecutorId,
    ) -> Option<ExecutorStateSnapshot> {
        let desired_executor_state = self.desired_state(executor_id).await?;

        let mut containers_pb = vec![];
        let mut allocations_pb = vec![];
        for desired_state_fe in desired_executor_state.containers.iter() {
            let fe = &desired_state_fe.function_executor.function_container;

            // Build code_payload_pb only if code_payload exists (not for sandboxes)
            let code_payload_pb = desired_state_fe.code_payload.as_ref().map(|code_payload| {
                let blob_store_url_schema = self
                    .blob_store_registry
                    .get_blob_store(&fe.namespace)
                    .get_url_scheme();
                let blob_store_url = self
                    .blob_store_registry
                    .get_blob_store(&fe.namespace)
                    .get_url();
                executor_api_pb::DataPayload {
                    id: Some(code_payload.id.clone()),
                    uri: Some(blob_store_path_to_url(
                        &code_payload.path,
                        &blob_store_url_schema,
                        &blob_store_url,
                    )),
                    size: Some(code_payload.size),
                    sha256_hash: Some(code_payload.sha256_hash.clone()),
                    encoding: Some(DataPayloadEncoding::BinaryZip.into()),
                    encoding_version: Some(0),
                    offset: Some(code_payload.offset),
                    metadata_size: Some(code_payload.metadata_size),
                    source_function_call_id: None,
                    content_type: Some("application/zip".to_string()),
                }
            });

            // Convert container type to proto enum
            let fe_type_pb = match fe.container_type {
                data_model::ContainerType::Function => ContainerTypePb::Function,
                data_model::ContainerType::Sandbox => ContainerTypePb::Sandbox,
            };

            // Convert network policy to proto format (for sandboxes only)
            let network_policy_pb = desired_state_fe
                .network_policy
                .as_ref()
                .map(crate::executor_api::network_policy_to_pb);

            // Build sandbox_metadata for sandbox containers
            let sandbox_metadata = if fe.container_type == data_model::ContainerType::Sandbox {
                Some(SandboxMetadata {
                    image: desired_state_fe.image.clone(),
                    timeout_secs: if desired_state_fe.sandbox_timeout_secs > 0 {
                        Some(desired_state_fe.sandbox_timeout_secs)
                    } else {
                        None
                    },
                    entrypoint: desired_state_fe.entrypoint.clone(),
                    network_policy: network_policy_pb,
                    sandbox_id: fe.sandbox_id.as_ref().map(|s| s.get().to_string()),
                    snapshot_uri: desired_state_fe.snapshot_uri.clone(),
                })
            } else {
                None
            };

            let resources_pb = match desired_state_fe.resources.clone().try_into() {
                Ok(r) => Some(r),
                Err(e) => {
                    error!(
                        executor_id = executor_id.get(),
                        container_id = fe.id.get(),
                        error = %e,
                        "Failed to convert container resources, skipping function executor"
                    );
                    continue;
                }
            };

            let fe_description_pb = ContainerDescription {
                id: Some(fe.id.get().to_string()),
                function: Some(FunctionRef {
                    namespace: Some(fe.namespace.clone()),
                    application_name: Some(fe.application_name.clone()),
                    function_name: Some(fe.function_name.clone()),
                    application_version: Some(fe.version.to_string()),
                }),
                secret_names: desired_state_fe.secret_names.clone(),
                initialization_timeout_ms: Some(desired_state_fe.initialization_timeout_ms),
                application: code_payload_pb, // None for sandboxes
                allocation_timeout_ms: Some(desired_state_fe.allocation_timeout_ms),
                resources: resources_pb,
                max_concurrency: Some(fe.max_concurrency),
                sandbox_metadata,
                container_type: Some(fe_type_pb.into()),
                pool_id: fe.pool_id.as_ref().map(|p| p.get().to_string()),
            };
            containers_pb.push(fe_description_pb);
        }

        for (fe_id, allocations) in desired_executor_state.function_run_allocations.iter() {
            for allocation in allocations.iter() {
                let mut args = vec![];
                let blob_store_url_schema = self
                    .blob_store_registry
                    .get_blob_store(&allocation.namespace)
                    .get_url_scheme();
                let blob_store_url = self
                    .blob_store_registry
                    .get_blob_store(&allocation.namespace)
                    .get_url();
                for input_arg in &allocation.input_args {
                    args.push(executor_api_pb::DataPayload {
                        id: Some(input_arg.data_payload.id.clone()),
                        uri: Some(blob_store_path_to_url(
                            &input_arg.data_payload.path,
                            &blob_store_url_schema,
                            &blob_store_url,
                        )),
                        size: Some(input_arg.data_payload.size),
                        sha256_hash: Some(input_arg.data_payload.sha256_hash.clone()),
                        encoding: Some(
                            string_to_data_payload_encoding(&input_arg.data_payload.encoding)
                                .into(),
                        ),
                        encoding_version: Some(0),
                        offset: Some(input_arg.data_payload.offset),
                        metadata_size: Some(input_arg.data_payload.metadata_size),
                        source_function_call_id: input_arg
                            .function_call_id
                            .as_ref()
                            .map(|id| id.to_string()),
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
                let allocation_pb = Allocation {
                    function: Some(FunctionRef {
                        namespace: Some(allocation.namespace.clone()),
                        application_name: Some(allocation.application.clone()),
                        function_name: Some(allocation.function.clone()),
                        application_version: None,
                    }),
                    container_id: Some(fe_id.get().to_string()),
                    allocation_id: Some(allocation.id.to_string()),
                    function_call_id: Some(allocation.function_call_id.to_string()),
                    request_id: Some(allocation.request_id.to_string()),
                    args,
                    request_data_payload_uri_prefix: Some(request_data_payload_uri_prefix.clone()),
                    request_error_payload_uri_prefix: Some(request_data_payload_uri_prefix.clone()),
                    function_call_metadata: Some(allocation.call_metadata.clone().into()),
                    replay_mode: None,
                    last_event_clock: None,
                };
                allocations_pb.push(allocation_pb);
            }
        }

        Some(ExecutorStateSnapshot {
            containers: containers_pb,
            allocations: allocations_pb,
            clock: Some(desired_executor_state.clock),
        })
    }

    pub async fn list_executors(&self) -> Result<Vec<ExecutorMetadata>> {
        let mut executors = vec![];
        for executor in self
            .indexify_state
            .app_state
            .load()
            .scheduler
            .executors
            .values()
        {
            executors.push(*executor.clone());
        }
        Ok(executors)
    }

    pub async fn api_list_allocations(&self) -> ExecutorsAllocationsResponse {
        let app = self.indexify_state.app_state.load();
        let allocations_by_executor = &app.indexes.allocations_by_executor;
        let function_executors_by_executor = &app.scheduler.executor_states;

        let executors = function_executors_by_executor
            .iter()
            .map(|(executor_id, function_executor_metas)| {
                // Create a HashMap to collect and merge allocations by function URI
                let mut containers: Vec<FnExecutor> = vec![];

                // Process each function executor
                for container_id in function_executor_metas.function_container_ids.iter() {
                    let allocations: Vec<http_objects::Allocation> = allocations_by_executor
                        .get(executor_id)
                        .map(|allocations| {
                            allocations
                                .get(container_id)
                                .map(|allocations| {
                                    allocations
                                        .values()
                                        .map(|allocation| allocation.as_ref().clone().into())
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or_default()
                        })
                        .unwrap_or_default();

                    let Some(function_container_server_meta) =
                        app.scheduler.function_containers.get(container_id)
                    else {
                        continue;
                    };

                    // TODO: We won't have function uri for sandboxes. So we need to update
                    // the data model to support having function uri as optional.
                    let fn_uri = FunctionURI::from(function_container_server_meta);
                    containers.push(FnExecutor {
                        count: allocations.len(),
                        container_id: container_id.to_string(),
                        fn_uri: Some(fn_uri.to_string()),
                        state: function_container_server_meta
                            .function_container
                            .state
                            .as_ref()
                            .to_string(),
                        desired_state: function_container_server_meta
                            .desired_state
                            .as_ref()
                            .to_string(),
                        allocations,
                    });
                }

                ExecutorAllocations {
                    executor_id: executor_id.get().to_string(),
                    count: containers.len(),
                    containers,
                }
            })
            .collect();

        ExecutorsAllocationsResponse { executors }
    }
}

pub fn tombstone_executor(
    last_executor_state_change_id: &AtomicU64,
    executor_id: &ExecutorId,
) -> Result<Vec<StateChange>> {
    let last_executor_state_change_id =
        last_executor_state_change_id.fetch_add(1, Ordering::Relaxed);
    let state_change = StateChangeBuilder::default()
        .change_type(ChangeType::TombStoneExecutor(ExecutorRemovedEvent {
            executor_id: executor_id.clone(),
        }))
        .namespace(None)
        .application(None)
        .created_at(get_epoch_time_in_ms())
        .object_id(executor_id.get().to_string())
        .id(StateChangeId::new(last_executor_state_change_id))
        .processed_at(None)
        .build()?;
    Ok(vec![state_change])
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tokio::time;

    use super::*;
    use crate::{
        data_model::{ExecutorId, ExecutorMetadata, ExecutorMetadataBuilder},
        executor_api::sync_executor_full_state,
        testing,
    };

    /// Full state sync — calls the same `sync_executor_full_state` that
    /// the production RPC handler uses, plus `heartbeat_v2` for liveness.
    async fn sync_executor_state(
        test_srv: &testing::TestService,
        executor: ExecutorMetadata,
    ) -> Result<()> {
        test_srv
            .service
            .executor_manager
            .heartbeat_v2(&executor.id)
            .await?;

        sync_executor_full_state(
            &test_srv.service.executor_manager,
            test_srv.service.indexify_state.clone(),
            executor,
        )
        .await?;
        Ok(())
    }

    /// Lightweight heartbeat — v2 liveness only.
    /// Mirrors production heartbeat RPC without full_state.
    async fn heartbeat_v2(test_srv: &testing::TestService, executor_id: &ExecutorId) -> Result<()> {
        test_srv
            .service
            .executor_manager
            .heartbeat_v2(executor_id)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_heartbeat_lapsed_executor() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();
        let executor_manager = test_srv.service.executor_manager.clone();

        let executor1 = ExecutorMetadataBuilder::default()
            .id(ExecutorId::new("test-executor-1".to_string()))
            .executor_version("1.0".to_string())
            .function_allowlist(None)
            .addr("".to_string())
            .labels(Default::default())
            .containers(Default::default())
            .host_resources(Default::default())
            .state(Default::default())
            .tombstoned(false)
            .state_hash("state_hash".to_string())
            .clock(0)
            .build()
            .unwrap();

        let executor2 = ExecutorMetadataBuilder::default()
            .id(ExecutorId::new("test-executor-2".to_string()))
            .executor_version("1.0".to_string())
            .function_allowlist(None)
            .addr("".to_string())
            .labels(Default::default())
            .containers(Default::default())
            .host_resources(Default::default())
            .state(Default::default())
            .tombstoned(false)
            .state_hash("state_hash".to_string())
            .clock(0)
            .build()
            .unwrap();

        let executor3 = ExecutorMetadataBuilder::default()
            .id(ExecutorId::new("test-executor-3".to_string()))
            .executor_version("1.0".to_string())
            .function_allowlist(None)
            .addr("".to_string())
            .labels(Default::default())
            .containers(Default::default())
            .host_resources(Default::default())
            .state(Default::default())
            .tombstoned(false)
            .state_hash("state_hash".to_string())
            .clock(0)
            .build()
            .unwrap();

        // Pause time and register all executors (initial full state sync)
        {
            time::pause();

            sync_executor_state(&test_srv, executor1.clone()).await?;
            sync_executor_state(&test_srv, executor2.clone()).await?;
            sync_executor_state(&test_srv, executor3.clone()).await?;
        }

        // Lightweight heartbeat 5s later to reset their deadlines
        {
            time::advance(Duration::from_secs(5)).await;

            heartbeat_v2(&test_srv, &executor1.id).await?;
            heartbeat_v2(&test_srv, &executor2.id).await?;
            heartbeat_v2(&test_srv, &executor3.id).await?;

            executor_manager.process_lapsed_executors().await?;
            test_srv.process_all_state_changes().await?;

            // Ensure that no executor has been removed
            let executors = indexify_state.app_state.load().scheduler.executors.clone();
            assert_eq!(
                3,
                executors.len(),
                "Expected 3 executors, but found: {executors:?}"
            );
        }

        // Lightweight heartbeat 15s later to reset deadlines
        {
            time::advance(Duration::from_secs(15)).await;

            heartbeat_v2(&test_srv, &executor1.id).await?;
            heartbeat_v2(&test_srv, &executor2.id).await?;
            // Executor 3 goes offline — no heartbeat

            executor_manager.process_lapsed_executors().await?;
            test_srv.process_all_state_changes().await?;

            // Ensure that no executor has been removed
            let executors = indexify_state.app_state.load().scheduler.executors.clone();
            assert_eq!(
                3,
                executors.len(),
                "Expected 3 executors, but found: {executors:?}"
            );
        }

        // Advance time to lapse executor3
        {
            // 30s from the last executor3 heartbeat
            time::advance(Duration::from_secs(15)).await;

            executor_manager.wait_executor_heartbeat_deadline().await;
            executor_manager.process_lapsed_executors().await?;
            test_srv.process_all_state_changes().await?;

            let cs = indexify_state.app_state.load();
            // Executor metadata is preserved (tombstoned) so resources remain
            // visible for adoption when the executor reconnects.
            assert_eq!(
                3,
                cs.scheduler.executors.len(),
                "Expected 3 executors (1 tombstoned), but found: {:?}",
                cs.scheduler.executors
            );
            assert!(
                cs.scheduler
                    .executors
                    .get(&ExecutorId::new("test-executor-3".to_string()))
                    .unwrap()
                    .tombstoned,
                "executor-3 should be tombstoned"
            );
            // Server metadata should be removed — scheduler has no
            // containers or resource claims for this executor.
            assert!(
                !cs.scheduler
                    .executor_states
                    .contains_key(&ExecutorId::new("test-executor-3".to_string())),
                "executor-3 server metadata should be removed"
            );
        }

        // Advance time past the lapsed deadline to trigger the deregistration of all
        // executors
        {
            time::advance(EXECUTOR_TIMEOUT).await;

            executor_manager.wait_executor_heartbeat_deadline().await;

            trace!("Received deadline");
            executor_manager.process_lapsed_executors().await?;

            test_srv.process_all_state_changes().await?;
        }

        // All executors tombstoned, server metadata removed
        {
            let cs = indexify_state.app_state.load();
            assert_eq!(
                3,
                cs.scheduler.executors.len(),
                "Expected 3 tombstoned executors, but found: {:?}",
                cs.scheduler.executors
            );
            assert!(
                cs.scheduler.executors.values().all(|e| e.tombstoned),
                "All executors should be tombstoned"
            );
            assert!(
                cs.scheduler.executor_states.is_empty(),
                "All executor server metadata should be removed"
            );
        }

        Ok(())
    }
}
