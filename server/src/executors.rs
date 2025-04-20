use std::{cmp::Ordering, collections::HashMap, sync::Arc, time::Duration, vec};

use anyhow::Result;
use data_model::{Allocation, ExecutorId, ExecutorMetadata};
use indexify_utils::dynamic_sleep::DynamicSleepFuture;
use priority_queue::PriorityQueue;
use state_store::{
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
use tracing::{error, trace};

pub const EXECUTOR_TIMEOUT: Duration = Duration::from_secs(30);

/// Wrapper for `tokio::time::Instant` that reverses the ordering for deadline.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct ReverseInstant(pub Instant);

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

pub struct ExecutorManager {
    heartbeat_deadline_queue: Mutex<PriorityQueue<ExecutorId, ReverseInstant>>,
    heartbeat_future: Arc<Mutex<DynamicSleepFuture>>,
    heartbeat_deadline_updater: watch::Sender<Instant>,
    indexify_state: Arc<IndexifyState>,
    executor_hashes: RwLock<HashMap<ExecutorId, String>>,
}

impl ExecutorManager {
    pub async fn new(indexify_state: Arc<IndexifyState>) -> Self {
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
            executor_hashes: RwLock::new(HashMap::new()),
            heartbeat_deadline_queue: Mutex::new(PriorityQueue::new()),
            heartbeat_deadline_updater: heartbeat_sender,
            heartbeat_future,
        };

        em.schedule_clean_lapsed_executors();

        em
    }

    pub fn schedule_clean_lapsed_executors(&self) {
        let indexify_state = self.indexify_state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(EXECUTOR_TIMEOUT).await;

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
                let sm_req = StateMachineUpdateRequest {
                    payload: RequestPayload::DeregisterExecutor(DeregisterExecutorRequest {
                        executor_id: executor_id.clone(),
                    }),
                    processed_state_changes: vec![],
                };
                if let Err(err) = indexify_state.write(sm_req).await {
                    error!(
                        executor_id = executor_id.get(),
                        "failed to deregister lapsed executor: {:?}", err
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
        if !self
            .executor_hashes
            .read()
            .await
            .get(&executor.id)
            .map(|stored_hash| stored_hash == &executor.state_hash)
            .unwrap_or(false)
        {
            trace!(
                executor_id = executor.id.get(),
                state_hash = executor.state_hash,
                "Executor state hash changed, registering executor"
            );
            // TODO: Add clock check only act on the heartbeat for the latest state change
            if let Err(e) = self.register_executor(executor.clone()).await {
                error!(
                    executor_id = executor.id.get(),
                    "failed to register executor: {:?}", e
                );
                return Err(e);
            }
            self.executor_hashes
                .write()
                .await
                .insert(executor.id.clone(), executor.state_hash.clone());
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
        // 1. Get the current time
        let now = Instant::now();

        // 2. Get all executor IDs that have lapsed
        let mut lapsed_executors = Vec::new();
        {
            // Lock heartbeat_state briefly
            let mut queue = self.heartbeat_deadline_queue.lock().await;

            while let Some((_, next_deadline)) = queue.peek() {
                if next_deadline.0 > now {
                    break;
                }

                // Remove from queue and store for later processing
                if let Some((executor_id, deadline)) = queue.pop() {
                    lapsed_executors.push((executor_id, deadline.0));
                } else {
                    error!("peeked executor not found in queue");
                    break;
                }
            }

            // 3. Update the heartbeat deadline with the earliest executor's deadline
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

        // 4. Deregister each lapsed executor without holding the lock
        for (executor_id, deadline) in lapsed_executors {
            trace!(
                executor_id = executor_id.get(),
                lapsed_after_s = (deadline - now).as_secs_f64(),
                "Deregistering lapsed executor"
            );
            self.deregister_lapsed_executor(executor_id).await?;
        }

        Ok(())
    }

    async fn deregister_lapsed_executor(
        &self,
        executor_id: ExecutorId,
    ) -> Result<(), anyhow::Error> {
        let sm_req = StateMachineUpdateRequest {
            payload: RequestPayload::DeregisterExecutor(DeregisterExecutorRequest {
                executor_id: executor_id.clone(),
            }),
            processed_state_changes: vec![],
        };
        self.indexify_state.write(sm_req).await?;
        Ok(())
    }

    pub async fn register_executor(&self, executor: ExecutorMetadata) -> Result<()> {
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

    pub async fn list_allocations(&self) -> HashMap<ExecutorId, HashMap<String, Vec<Allocation>>> {
        let state = self.indexify_state.in_memory_state.read().await;
        let allocations_by_executor = &state.allocations_by_executor;
        let function_executors_by_executor = &state.function_executors_by_executor;

        function_executors_by_executor
            .iter()
            .map(|(executor_id, function_executors)| {
                // Create a HashMap to collect and merge allocations by function URI
                let mut function_allocations: HashMap<String, Vec<Allocation>> = HashMap::new();

                // Process each function executor
                for (_, function_executor) in function_executors {
                    // Get the function URI string
                    let fn_uri = function_executor.fn_uri_str();

                    function_allocations.entry(fn_uri.clone()).or_default();

                    // Find allocations for this function executor if they exist
                    if let Some(executor_allocations) = allocations_by_executor.get(executor_id) {
                        if let Some(fe_allocations) =
                            executor_allocations.get(&function_executor.id)
                        {
                            // Convert allocation HashMap values to a Vec
                            let allocation_vec: Vec<Allocation> =
                                fe_allocations.iter().map(|i| *i.clone()).collect();

                            // Merge with existing allocations for this function URI
                            function_allocations
                                .entry(fn_uri)
                                .and_modify(|existing| existing.extend(allocation_vec.clone()));
                        }
                    }
                }

                (executor_id.clone(), function_allocations)
            })
            .collect()
    }
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
            development_mode: true,
            function_allowlist: None,
            addr: "".to_string(),
            labels: Default::default(),
            function_executors: Default::default(),
            host_resources: Default::default(),
            free_resources: Default::default(),
            state: Default::default(),
            tombstoned: false,
            state_hash: "state_hash".to_string(),
            clock_updated_at: 0,
        };
        executor_manager.register_executor(executor).await?;

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
            development_mode: true,
            function_allowlist: None,
            addr: "".to_string(),
            labels: Default::default(),
            function_executors: Default::default(),
            host_resources: Default::default(),
            free_resources: Default::default(),
            state: Default::default(),
            tombstoned: false,
            state_hash: "state_hash".to_string(),
            clock_updated_at: 0,
        };

        let executor2 = ExecutorMetadata {
            id: ExecutorId::new("test-executor-2".to_string()),
            executor_version: "1.0".to_string(),
            development_mode: true,
            function_allowlist: None,
            addr: "".to_string(),
            labels: Default::default(),
            function_executors: Default::default(),
            host_resources: Default::default(),
            free_resources: Default::default(),
            state: Default::default(),
            tombstoned: false,
            state_hash: "state_hash".to_string(),
            clock_updated_at: 0,
        };

        let executor3 = ExecutorMetadata {
            id: ExecutorId::new("test-executor-3".to_string()),
            executor_version: "1.0".to_string(),
            development_mode: true,
            function_allowlist: None,
            addr: "".to_string(),
            labels: Default::default(),
            function_executors: Default::default(),
            host_resources: Default::default(),
            free_resources: Default::default(),
            state: Default::default(),
            tombstoned: false,
            state_hash: "state_hash".to_string(),
            clock_updated_at: 0,
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
