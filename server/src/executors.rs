use std::{
    cmp::Ordering,
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    sync::Arc,
    time::Duration,
    vec,
};

use anyhow::Result;
use data_model::{ExecutorId, ExecutorMetadata};
use indexify_utils::dynamic_sleep::DynamicSleepFuture;
use priority_queue::PriorityQueue;
use state_store::{
    in_memory_state::DesiredStateFunctionExecutor,
    requests::{
        DeregisterExecutorRequest,
        RequestPayload,
        StateMachineUpdateRequest,
        UpsertExecutorRequest,
    },
    IndexifyState,
};
use tokio::{
    sync::{watch, Mutex, RwLock},
    time::Instant,
};
use tracing::{error, info, trace};

use crate::{
    executor_api::{
        blob_store_path_to_url,
        executor_api_pb::{
            DataPayload,
            DataPayloadEncoding,
            DesiredExecutorState,
            FunctionExecutorDescription,
            Task,
            TaskAllocation,
        },
    },
    http_objects::{self, ExecutorAllocations, ExecutorsAllocationsResponse, FnExecutor},
};

/// Executor timeout duration for heartbeat
pub const EXECUTOR_TIMEOUT: Duration = Duration::from_secs(30);

/// Timeout duration before deregistering executors that haven't re-registered
/// at service startup
pub const STARTUP_EXECUTOR_TIMEOUT: Duration = Duration::from_secs(60);

/// A struct containing only the computed fields needed for desired state
#[derive(Debug, Clone)]
pub struct ComputedTask {
    pub reducer_input_payload: Option<DataPayload>,
    pub output_payload_uri_prefix: String,
    pub input_payload: DataPayload,
}

/// Wrapper for `tokio::time::Instant` that reverses the ordering for deadline.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
struct ReverseInstant(pub Instant);

impl Ord for ReverseInstant {
    fn cmp(&self, other: &Self) -> Ordering {
        other.0.cmp(&self.0) // Reverse ordering
    }
}

impl PartialOrd for ReverseInstant {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Returns a far future time for the heartbeat deadline.
/// This is used whenever there are no executors.
fn far_future() -> Instant {
    Instant::now() + Duration::from_secs(24 * 60 * 60)
}

/// ExecutorRuntimeData stores runtime state for an executor that is not
/// persisted in the state machine but is needed for efficient operation
#[derive(Debug, Clone)]
struct ExecutorRuntimeData {
    /// Hash of the executor's overall state (used for heartbeat optimization)
    pub last_state_hash: String,
    /// Clock value when the state was last updated
    pub last_executor_clock: u64,
    /// Hash of the function executors' desired states (used for
    /// get_executor_state optimization)
    pub function_executors_hash: String,
    /// Clock value when the function executors state was last updated
    pub desired_server_clock: u64,
}

impl ExecutorRuntimeData {
    /// Create a new ExecutorRuntimeData
    pub fn new(state_hash: String, clock: u64) -> Self {
        Self {
            last_state_hash: state_hash,
            last_executor_clock: clock,
            function_executors_hash: String::new(),
            desired_server_clock: clock,
        }
    }

    /// Update the function executors state hash and clock
    pub fn update_function_executors_state(&mut self, hash: String, clock: u64) {
        self.function_executors_hash = hash;
        self.desired_server_clock = clock;
    }

    /// Update the overall state hash and clock
    pub fn update_state(&mut self, hash: String, clock: u64) {
        self.last_state_hash = hash;
        self.last_executor_clock = clock;
    }

    pub fn should_update(&self, hash: String, clock: u64) -> bool {
        self.last_state_hash != hash || self.last_executor_clock != clock
    }
}

pub struct ExecutorManager {
    blob_store_url_scheme: String,
    blob_store_url: String,
    heartbeat_deadline_queue: Mutex<PriorityQueue<ExecutorId, ReverseInstant>>,
    heartbeat_future: Arc<Mutex<DynamicSleepFuture>>,
    heartbeat_deadline_updater: watch::Sender<Instant>,
    indexify_state: Arc<IndexifyState>,
    runtime_data: RwLock<HashMap<ExecutorId, ExecutorRuntimeData>>,
}

impl ExecutorManager {
    pub async fn new(
        indexify_state: Arc<IndexifyState>,
        blob_store_url_scheme: String,
        blob_store_url: String,
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
            blob_store_url_scheme,
            blob_store_url,
        };

        let em = Arc::new(em);

        em.clone().schedule_clean_lapsed_executors();

        em
    }

    pub fn schedule_clean_lapsed_executors(self: Arc<Self>) {
        let indexify_state = self.indexify_state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(STARTUP_EXECUTOR_TIMEOUT).await;

            // Get all executor IDs of executors that haven't registered.
            let missing_executor_ids: Vec<_> = {
                let indexes = indexify_state.in_memory_state.read().await;

                indexes
                    .allocations_by_executor
                    .keys()
                    .filter(|id| !indexes.executors.contains_key(&**id))
                    .cloned()
                    .collect()
            };

            // Deregister all executors that haven't registered.
            for executor_id in missing_executor_ids {
                let executor_id_clone = executor_id.clone();
                if let Err(err) = self.deregister_lapsed_executor(executor_id).await {
                    error!(
                        executor_id = executor_id_clone.get(),
                        "Failed to deregister lapsed executor: {:?}", err
                    );
                }
            }
        });
    }

    /// Heartbeat an executor to keep it alive and update its metadata
    pub async fn heartbeat(&self, executor: ExecutorMetadata) -> Result<()> {
        let peeked_deadline = {
            // 1. Create new deadline
            let new_deadline = ReverseInstant(Instant::now() + EXECUTOR_TIMEOUT);

            trace!(executor_id = executor.id.get(), "Heartbeat received");

            // 2. Acquire a write lock on the heartbeat state
            let mut state = self.heartbeat_deadline_queue.lock().await;

            // 3. Update the executor's deadline or add it to the queue
            if state.change_priority(&executor.id, new_deadline).is_none() {
                state.push(executor.id.clone(), new_deadline);
            }

            // 4. Peek the next earliest deadline
            state
                .peek()
                .map(|(_, deadline)| deadline.0)
                .unwrap_or_else(far_future)
        };

        // 5. Update the heartbeat future with the new earliest deadline
        self.heartbeat_deadline_updater.send(peeked_deadline)?;

        // 6. Register the executor to upsert its metadata only if the state_hash is
        //    different to prevent doing duplicate work.
        let should_update = {
            let runtime_data_read = self.runtime_data.read().await;
            runtime_data_read
                .get(&executor.id)
                .map(|data| data.should_update(executor.state_hash.clone(), executor.clock))
                .unwrap_or(true)
        };

        if should_update {
            info!(
                executor_id = executor.id.get(),
                state_hash = executor.state_hash,
                clock = executor.clock,
                "Executor state hash changed, registering executor"
            );
            let existing_executor = self
                .indexify_state
                .in_memory_state
                .read()
                .await
                .executors
                .get(&executor.id)
                .cloned();
            let executor = match existing_executor {
                None => executor,
                Some(existing_executor) => {
                    let mut existing_executor = existing_executor.clone();
                    existing_executor.update(executor);
                    *existing_executor
                }
            };

            if let Err(e) = self.upsert_executor(executor.clone()).await {
                error!(
                    executor_id = executor.id.get(),
                    "failed to register executor: {:?}", e
                );
                return Err(e);
            }

            // Update runtime data with the new state hash and clock
            let mut runtime_data_write = self.runtime_data.write().await;
            runtime_data_write
                .entry(executor.id.clone())
                .and_modify(|data| data.update_state(executor.state_hash.clone(), executor.clock))
                .or_insert_with(|| {
                    ExecutorRuntimeData::new(executor.state_hash.clone(), executor.clock)
                });
        }

        Ok(())
    }

    /// Wait for the an executor heartbeat deadline to lapse.
    async fn wait_executor_heartbeat_deadline(&self) {
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
    async fn process_lapsed_executors(&self) -> Result<()> {
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

        trace!("Found {} lapsed executors", lapsed_executors.len());

        // 3. Deregister each lapsed executor without holding the lock
        for executor_id in lapsed_executors {
            self.deregister_lapsed_executor(executor_id).await?;
        }

        Ok(())
    }

    async fn deregister_lapsed_executor(
        &self,
        executor_id: ExecutorId,
    ) -> Result<(), anyhow::Error> {
        trace!(
            executor_id = executor_id.get(),
            "Deregistering lapsed executor"
        );
        self.runtime_data.write().await.remove(&executor_id);
        let sm_req = StateMachineUpdateRequest {
            payload: RequestPayload::DeregisterExecutor(DeregisterExecutorRequest { executor_id }),
            processed_state_changes: vec![],
        };
        self.indexify_state.write(sm_req).await?;
        Ok(())
    }

    pub async fn subscribe(&self, executor_id: &ExecutorId) -> Option<watch::Receiver<()>> {
        self.indexify_state
            .executor_states
            .write()
            .await
            .get_mut(executor_id)
            .map(|s| s.subscribe())
    }

    /// Get the desired state for an executor
    pub async fn get_executor_state(&self, executor_id: &ExecutorId) -> DesiredExecutorState {
        let desired_executor_state = self
            .indexify_state
            .in_memory_state
            .read()
            .await
            .clone()
            .read()
            .unwrap()
            .desired_state(executor_id);
        let current_fe_hash =
            compute_function_executors_hash(&desired_executor_state.function_executors);
        let mut function_executors_pb = vec![];
        let mut task_allocations = vec![];
        for desired_state_fe in desired_executor_state.function_executors.iter() {
            let code_payload_pb = DataPayload {
                uri: Some(blob_store_path_to_url(
                    &desired_state_fe.code_payload.path,
                    &self.blob_store_url_scheme,
                    &self.blob_store_url,
                )),
                size: Some(desired_state_fe.code_payload.size),
                sha256_hash: Some(desired_state_fe.code_payload.sha256_hash.clone()),
                encoding: Some(DataPayloadEncoding::BinaryPickle.into()),
                encoding_version: None,
            };
            let fe = &desired_state_fe.function_executor.function_executor;
            let fe_description_pb = FunctionExecutorDescription {
                id: Some(fe.id.get().to_string()),
                namespace: Some(fe.namespace.clone()),
                graph_name: Some(fe.compute_graph_name.clone()),
                graph_version: Some(fe.version.to_string()),
                function_name: Some(fe.compute_fn_name.clone()),
                image_uri: Some(desired_state_fe.image_uri.clone()),
                secret_names: desired_state_fe.secret_names.clone(),
                customer_code_timeout_ms: Some(desired_state_fe.customer_code_timeout_ms.clone()),
                graph: Some(code_payload_pb),
                resources: Some(desired_state_fe.resources.clone().try_into().unwrap()),
            };
            function_executors_pb.push(fe_description_pb);
        }

        for (fe_id, tasks) in desired_executor_state.task_allocations.iter() {
            for task in tasks.iter() {
                let computed_task = match self.extract_computed_fields(&task.task) {
                    Ok(computed_task) => computed_task,
                    Err(e) => {
                        error!(
                            executor_id = executor_id.get(),
                            task_id = task.task.id.get(),
                            "Failed to extract computed fields: {:?}",
                            e
                        );
                        continue;
                    }
                };
                let task_allocation_pb = TaskAllocation {
                    function_executor_id: Some(fe_id.get().to_string()),
                    allocation_id: Some(task.allocation_id.clone()),
                    task: Some(Task {
                        id: Some(task.task.id.get().to_string()),
                        namespace: Some(task.task.namespace.clone()),
                        graph_name: Some(task.task.compute_graph_name.clone()),
                        graph_version: Some(task.task.graph_version.to_string()),
                        function_name: Some(task.task.compute_fn_name.clone()),
                        graph_invocation_id: Some(task.task.invocation_id.clone()),
                        timeout_ms: Some(task.timeout_ms),
                        input: Some(computed_task.input_payload),
                        reducer_input: computed_task.reducer_input_payload,
                        output_payload_uri_prefix: Some(computed_task.output_payload_uri_prefix),
                        retry_policy: Some(task.retry_policy.clone().into()),
                    }),
                };
                task_allocations.push(task_allocation_pb);
            }
        }

        if let Some(runtime_data) = self.runtime_data.write().await.get_mut(&executor_id) {
            runtime_data
                .update_function_executors_state(current_fe_hash, desired_executor_state.clock);
        }

        DesiredExecutorState {
            function_executors: function_executors_pb,
            task_allocations,
            clock: Some(desired_executor_state.clock),
        }
    }

    /// Extracts only the computed fields from a data_model::Task
    pub fn extract_computed_fields(&self, task: &data_model::Task) -> anyhow::Result<ComputedTask> {
        let input_payload = DataPayload {
            uri: Some(blob_store_path_to_url(
                &task.input.path,
                &self.blob_store_url_scheme,
                &self.blob_store_url,
            )),
            size: Some(task.input.size),
            sha256_hash: Some(task.input.sha256_hash.clone()),
            encoding: Some(
                DataPayloadEncoding::try_from("application/octet-stream".to_string())?.into(),
            ),
            encoding_version: None,
        };

        let reducer_input = task.acc_input.clone().map(|input| DataPayload {
            uri: Some(blob_store_path_to_url(
                &input.path,
                &self.blob_store_url_scheme,
                &self.blob_store_url,
            )),
            size: Some(input.size),
            sha256_hash: Some(input.sha256_hash.clone()),
            encoding: Some(
                DataPayloadEncoding::try_from("application/octet-stream".to_string())
                    .unwrap()
                    .into(),
            ),
            encoding_version: None,
        });

        // Create output payload URI prefix
        let output_payload_uri_prefix = format!(
            "{}/{}.{}.{}.{}",
            self.blob_store_url,
            task.namespace,
            task.compute_graph_name,
            task.compute_fn_name,
            task.invocation_id,
        );

        Ok(ComputedTask {
            reducer_input_payload: reducer_input.clone(),
            output_payload_uri_prefix,
            input_payload,
        })
    }

    pub async fn upsert_executor(&self, executor: ExecutorMetadata) -> Result<()> {
        let sm_req = StateMachineUpdateRequest {
            payload: RequestPayload::UpsertExecutor(UpsertExecutorRequest { executor }),
            processed_state_changes: vec![],
        };
        self.indexify_state.write(sm_req).await
    }

    pub async fn list_executors(&self) -> Result<Vec<ExecutorMetadata>> {
        let mut executors = vec![];
        for executor in self
            .indexify_state
            .in_memory_state
            .read()
            .await
            .executors
            .values()
        {
            executors.push(*executor.clone());
        }
        Ok(executors)
    }

    pub async fn api_list_allocations(&self) -> ExecutorsAllocationsResponse {
        let state = self.indexify_state.in_memory_state.read().await;
        let allocations_by_executor = &state.allocations_by_executor;
        let function_executors_by_executor = &state.executor_states;

        let executors = function_executors_by_executor
            .iter()
            .map(|(executor_id, function_executor_metas)| {
                // Create a HashMap to collect and merge allocations by function URI
                let mut function_executors: Vec<FnExecutor> = vec![];

                // Process each function executor
                for (_, fe_meta) in function_executor_metas.function_executors.iter() {
                    // Get the function URI string
                    let fn_uri = fe_meta.fn_uri_str();

                    let allocations: Vec<http_objects::Allocation> = allocations_by_executor
                        .get(executor_id)
                        .map(|allocations| {
                            allocations
                                .get(&fe_meta.function_executor.id)
                                .map(|allocations| {
                                    allocations
                                        .iter()
                                        .map(|allocation| allocation.as_ref().clone().into())
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or_default()
                        })
                        .unwrap_or_default();

                    function_executors.push(FnExecutor {
                        count: allocations.len(),
                        function_executor_id: fe_meta
                            .function_executor
                            .id
                            .clone()
                            .get()
                            .to_string(),
                        fn_uri,
                        state: fe_meta.function_executor.state.as_ref().to_string(),
                        desired_state: fe_meta.desired_state.as_ref().to_string(),
                        allocations,
                    });
                }

                ExecutorAllocations {
                    executor_id: executor_id.get().to_string(),
                    count: function_executors.len(),
                    function_executors,
                }
            })
            .collect();

        ExecutorsAllocationsResponse { executors }
    }
}

/// Helper function to compute a hash of function executors' desired states
fn compute_function_executors_hash(
    function_executors: &Vec<Box<DesiredStateFunctionExecutor>>,
) -> String {
    let mut hasher = DefaultHasher::new();

    // Sort function executors by ID to ensure consistent hashing
    let mut sorted_executors = function_executors.into_iter().collect::<Vec<_>>();
    sorted_executors.sort_by(|a, b| {
        a.function_executor
            .function_executor
            .id
            .get()
            .cmp(b.function_executor.function_executor.id.get())
    });

    // Hash each function executor's ID and desired state
    for fe in sorted_executors {
        fe.function_executor
            .function_executor
            .id
            .get()
            .hash(&mut hasher);
        fe.function_executor.desired_state.hash(&mut hasher);
    }

    format!("{:x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use data_model::{ExecutorId, ExecutorMetadata};
    use tokio::time;

    use super::*;
    use crate::{service::Service, testing};

    #[tokio::test]
    async fn test_register_executor() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service {
            indexify_state,
            executor_manager,
            ..
        } = test_srv.service;

        let executor = ExecutorMetadata {
            id: ExecutorId::new("test".to_string()),
            executor_version: "1.0".to_string(),
            function_allowlist: None,
            addr: "".to_string(),
            labels: Default::default(),
            function_executors: Default::default(),
            host_resources: Default::default(),
            state: Default::default(),
            tombstoned: false,
            state_hash: "state_hash".to_string(),
            clock: 0,
        };
        executor_manager.upsert_executor(executor).await?;

        let executors = indexify_state
            .in_memory_state
            .read()
            .await
            .executors
            .clone();

        assert_eq!(executors.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_heartbeat_lapsed_executor() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service {
            indexify_state,
            executor_manager,
            ..
        } = test_srv.service.clone();

        let executor1 = ExecutorMetadata {
            id: ExecutorId::new("test-executor-1".to_string()),
            executor_version: "1.0".to_string(),
            function_allowlist: None,
            addr: "".to_string(),
            labels: Default::default(),
            function_executors: Default::default(),
            host_resources: Default::default(),
            state: Default::default(),
            tombstoned: false,
            state_hash: "state_hash".to_string(),
            clock: 0,
        };

        let executor2 = ExecutorMetadata {
            id: ExecutorId::new("test-executor-2".to_string()),
            executor_version: "1.0".to_string(),
            function_allowlist: None,
            addr: "".to_string(),
            labels: Default::default(),
            function_executors: Default::default(),
            host_resources: Default::default(),
            state: Default::default(),
            tombstoned: false,
            state_hash: "state_hash".to_string(),
            clock: 0,
        };

        let executor3 = ExecutorMetadata {
            id: ExecutorId::new("test-executor-3".to_string()),
            executor_version: "1.0".to_string(),
            function_allowlist: None,
            addr: "".to_string(),
            labels: Default::default(),
            function_executors: Default::default(),
            host_resources: Default::default(),
            state: Default::default(),
            tombstoned: false,
            state_hash: "state_hash".to_string(),
            clock: 0,
        };

        // Pause time and send an initial heartbeats
        {
            time::pause();

            executor_manager.heartbeat(executor1.clone()).await?;
            executor_manager.heartbeat(executor2.clone()).await?;
            executor_manager.heartbeat(executor3.clone()).await?;
        }

        // Heartbeat the executors 5s later to reset their deadlines
        {
            time::advance(Duration::from_secs(5)).await;

            executor_manager.heartbeat(executor1.clone()).await?;
            executor_manager.heartbeat(executor2.clone()).await?;
            executor_manager.heartbeat(executor3.clone()).await?;

            executor_manager.process_lapsed_executors().await?;
            test_srv.process_all_state_changes().await?;

            // Ensure that no executor has been removed
            let executors = indexify_state
                .in_memory_state
                .read()
                .await
                .executors
                .clone();
            assert_eq!(
                3,
                executors.len(),
                "Expected 3 executors, but found: {:?}",
                executors
            );
        }

        // Heartbeat executor 5s later to reset their deadlines
        {
            time::advance(Duration::from_secs(15)).await;

            executor_manager.heartbeat(executor1.clone()).await?;
            executor_manager.heartbeat(executor2.clone()).await?;
            // Executor 3 goes offline
            // executor_manager.heartbeat(executor3.clone()).await?;

            executor_manager.process_lapsed_executors().await?;
            test_srv.process_all_state_changes().await?;

            // Ensure that no executor has been removed
            let executors = indexify_state
                .in_memory_state
                .read()
                .await
                .executors
                .clone();
            assert_eq!(
                3,
                executors.len(),
                "Expected 3 executors, but found: {:?}",
                executors
            );
        }

        // Advance time to lapse executor3
        {
            // 30s from the last executor3 heartbeat
            time::advance(Duration::from_secs(15)).await;

            executor_manager.wait_executor_heartbeat_deadline().await;
            executor_manager.process_lapsed_executors().await?;
            test_srv.process_all_state_changes().await?;

            // Ensure that no executor has been removed
            let executors = indexify_state
                .in_memory_state
                .read()
                .await
                .executors
                .clone();
            assert_eq!(
                2,
                executors.len(),
                "Expected 2 executors, but found: {:?}",
                executors
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

        // Ensure that the executor has been removed
        {
            let executors = indexify_state
                .in_memory_state
                .read()
                .await
                .executors
                .clone();
            assert!(
                executors.is_empty(),
                "Expected no executors, but found: {:?}",
                executors
            );
        }

        Ok(())
    }
}
