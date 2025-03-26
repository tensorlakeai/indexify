use std::{cmp::Ordering, collections::HashMap, sync::Arc, time::Duration, vec};

use anyhow::Result;
use data_model::{Allocation, ExecutorId, ExecutorMetadata};
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
    sync::{watch, Notify, RwLock},
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

pub struct ExecutorManager {
    heartbeat_state: RwLock<PriorityQueue<ExecutorId, ReverseInstant>>,
    /// Used to wake the monitor only when necessary
    notify: Arc<Notify>,
    indexify_state: Arc<IndexifyState>,
}

/// Represents the possible outcomes of waiting for an event in the heartbeat
/// monitor
enum WaitResult {
    /// A deadline for executor heartbeat has been reached
    Deadline,
    /// A notification about executor state has been received
    Notified,
    /// A shutdown signal has been received
    Shutdown,
}

impl ExecutorManager {
    pub async fn new(indexify_state: Arc<IndexifyState>) -> Self {
        let em = ExecutorManager {
            indexify_state,
            heartbeat_state: RwLock::new(PriorityQueue::new()),
            notify: Arc::new(Notify::new()),
        };

        em.schedule_clean_lapsed_executors();

        em
    }

    pub fn schedule_clean_lapsed_executors(&self) {
        let indexify_state = self.indexify_state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(EXECUTOR_TIMEOUT).await;

            // Get all executor IDs of executors that haven't registered.
            let missing_executor_ids: Vec<String> = {
                let indexes = indexify_state.in_memory_state.read().await;

                indexes
                    .allocations_by_fn
                    .keys()
                    .filter(|id| !indexes.executors.contains_key(&**id))
                    .cloned()
                    .collect()
            };

            // Deregister all executors that haven't registered.
            for executor_id in missing_executor_ids {
                let sm_req = StateMachineUpdateRequest {
                    payload: RequestPayload::DeregisterExecutor(DeregisterExecutorRequest {
                        executor_id: ExecutorId::new(executor_id.clone()),
                    }),
                    processed_state_changes: vec![],
                };
                if let Err(err) = indexify_state.write(sm_req).await {
                    error!(
                        executor_id = executor_id,
                        "failed to deregister lapsed executor: {:?}", err
                    );
                }
            }
        });
    }

    /// Heartbeat an executor to keep it alive and update its metadata
    pub async fn heartbeat(&self, executor: ExecutorMetadata) -> Result<()> {
        let first_executor = {
            // 1. Create new deadline
            let new_deadline = ReverseInstant(Instant::now() + EXECUTOR_TIMEOUT);

            trace!(executor_id = executor.id.get(), "Heartbeat received");

            // 2. Acquire a write lock on the heartbeat state
            let mut state = self.heartbeat_state.write().await;

            let was_empty = state.is_empty();

            // 3. Update the executor's deadline or add it to the queue
            if state.change_priority(&executor.id, new_deadline).is_none() {
                state.push(executor.id.clone(), new_deadline);
            }

            was_empty
        };

        // 4. Notify of a first executor to wake up the monitor
        if first_executor {
            self.notify.notify_one();
        }

        // 5. Register the executor to upsert its metadata
        let err = self.register_executor(executor.clone(), false).await;
        if let Err(e) = err {
            error!("failed to register executor {}: {:?}", executor.id.get(), e);
            return Err(e);
        }

        Ok(())
    }

    /// Waits for the next event in the heartbeat monitoring process
    async fn wait_for_heartbeat_event(&self, shutdown_rx: &mut watch::Receiver<()>) -> WaitResult {
        // 1. Retrieve the next deadline from the queue
        let next_deadline = {
            let state = self.heartbeat_state.read().await;
            // Peek at the next deadline without removing it from the queue
            state.peek().map(|(_, deadline)| deadline.0)
        };

        match next_deadline {
            // 2. If there is a deadline in the queue
            Some(deadline) => {
                trace!(
                    deadline = (deadline - Instant::now()).as_secs_f64(),
                    "Waiting for next deadline"
                );
                tokio::select! {
                    // 2.1 Wait until the deadline arrives
                    _ = tokio::time::sleep_until(deadline) => WaitResult::Deadline,

                    // 2.2 Handle early notification about executor state changes
                    _ = self.notify.notified() => WaitResult::Notified,

                    // 2.3 Handle potential shutdown signal
                    _ = shutdown_rx.changed() => WaitResult::Shutdown,
                }
            }
            // 3. If no deadline is present
            None => {
                trace!("Waiting for notification, no deadline to wait for");
                tokio::select! {
                    // 3.1 Wait for a notification about executor registration
                    _ = self.notify.notified() => WaitResult::Notified,

                    // 3.2 Handle potential shutdown signal
                    _ = shutdown_rx.changed() => WaitResult::Shutdown,
                }
            }
        }
    }

    /// Starts the heartbeat monitoring loop for executors
    pub async fn start_heartbeat_monitor(self: Arc<Self>, mut shutdown_rx: watch::Receiver<()>) {
        loop {
            match self.wait_for_heartbeat_event(&mut shutdown_rx).await {
                // Deadline reached - process lapsed executors
                WaitResult::Deadline => {
                    trace!("Received deadline");
                    // Attempt to deregister executors that have missed their heartbeat
                    if let Err(err) = self.process_lapsed_executors().await {
                        error!("Failed to process lapsed executors: {:?}", err);
                    }

                    // Wait for 1 second to batch potential subsequent executor lapsing
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }

                // Notification received - restart the loop to reprocess the next deadline
                WaitResult::Notified => {
                    trace!("Received notification");
                    continue;
                }

                // Shutdown signal received - exit the loop
                WaitResult::Shutdown => {
                    trace!("Received shutdown signal");
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
            let mut state = self.heartbeat_state.write().await;

            while let Some((_, next_deadline)) = state.peek() {
                trace!(
                    check_lapsed_s = (next_deadline.0 - now).as_secs_f64(),
                    "Check for lapsed executor"
                );
                if next_deadline.0 > now {
                    trace!(
                    "Next executor's deadline is in the future, stop checking for lapsed executors"
                );
                    break;
                }

                // Remove from queue and store for later processing
                if let Some((executor_id, deadline)) = state.pop() {
                    lapsed_executors.push((executor_id, deadline.0));
                } else {
                    error!("peeked executor not found in queue");
                    break;
                }
            }
        }

        // 3. Deregister each lapsed executor without holding the lock
        for (executor_id, deadline) in lapsed_executors {
            trace!(
                executor_id = executor_id.get(),
                lapsed_after_s = (deadline - now).as_secs_f64(),
                "Deregistering lapsed executor"
            );

            let sm_req = StateMachineUpdateRequest {
                payload: RequestPayload::DeregisterExecutor(DeregisterExecutorRequest {
                    executor_id: executor_id.clone(),
                }),
                processed_state_changes: vec![],
            };

            self.indexify_state.write(sm_req).await?;
        }

        Ok(())
    }

    pub async fn register_executor(
        &self,
        executor: ExecutorMetadata,
        for_task_stream: bool,
    ) -> Result<()> {
        let sm_req = StateMachineUpdateRequest {
            payload: RequestPayload::UpsertExecutor(UpsertExecutorRequest {
                executor,
                for_task_stream,
            }),
            processed_state_changes: vec![],
        };
        self.indexify_state.write(sm_req).await
    }

    pub async fn deregister_executor(&self, executor_id: ExecutorId) -> Result<()> {
        let sm_req = StateMachineUpdateRequest {
            payload: RequestPayload::DeregisterExecutor(DeregisterExecutorRequest { executor_id }),
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

    pub async fn list_allocations(&self) -> HashMap<String, HashMap<String, Vec<Allocation>>> {
        self.indexify_state
            .in_memory_state
            .read()
            .await
            .allocations_by_fn
            .iter()
            .map(|(executor_id, fns)| {
                let executor_id = executor_id.clone();
                let fns = fns
                    .iter()
                    .map(|(fn_name, allocations)| {
                        let fn_name = fn_name.clone();
                        let mut allocs: Vec<Allocation> = vec![];
                        for allocation in allocations {
                            allocs.push((**allocation).clone());
                        }
                        (fn_name, allocs)
                    })
                    .collect();
                (executor_id, fns)
            })
            .collect()
    }
}

pub fn schedule_deregister(ex: Arc<ExecutorManager>, executor_id: ExecutorId, duration: Duration) {
    tokio::spawn(async move {
        trace!(
            executor_id = executor_id.get(),
            deregister_after_s = duration.as_secs_f64(),
            "Scheduling deregistration of executor"
        );
        tokio::time::sleep(duration).await;
        let ret = ex.deregister_executor(executor_id.clone()).await;
        if let Err(e) = ret {
            error!(
                executor_id = executor_id.get(),
                "failed to deregister executor: {:?}", e
            );
        }
    });
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
        };
        executor_manager.register_executor(executor, true).await?;

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
    async fn test_deregister_executor() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service {
            indexify_state,
            executor_manager,
            ..
        } = test_srv.service.clone();

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
        };
        executor_manager
            .register_executor(executor.clone(), true)
            .await?;

        let executors = indexify_state
            .in_memory_state
            .read()
            .await
            .executors
            .clone();

        assert_eq!(executors.len(), 1);

        executor_manager
            .register_executor(executor.clone(), true)
            .await?;
        schedule_deregister(
            executor_manager.clone(),
            executor.id.clone(),
            Duration::from_millis(100),
        );
        schedule_deregister(
            executor_manager.clone(),
            executor.id.clone(),
            Duration::from_millis(200),
        );

        let executors = indexify_state
            .in_memory_state
            .read()
            .await
            .executors
            .clone();
        assert_eq!(executors.len(), 1);

        let time = std::time::Instant::now();
        loop {
            test_srv.process_all_state_changes().await?;

            let executors = indexify_state
                .in_memory_state
                .read()
                .await
                .executors
                .clone();
            if executors.is_empty() {
                break;
            }
            if time.elapsed().as_secs() > 10 {
                panic!("executor was not deregistered");
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

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

        // Create a shutdown watcher to pass to wait_for_next_event
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(());

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

            match executor_manager
                .wait_for_heartbeat_event(&mut shutdown_rx)
                .await
            {
                WaitResult::Deadline => {
                    trace!("Received deadline");
                    executor_manager.process_lapsed_executors().await?;
                }
                WaitResult::Notified => {
                    trace!("Got notified, only possible in test since we don't run the monitor in a separate task");
                    match executor_manager
                        .wait_for_heartbeat_event(&mut shutdown_rx)
                        .await
                    {
                        WaitResult::Deadline => {
                            trace!("Received deadline");
                            executor_manager.process_lapsed_executors().await?;
                        }
                        WaitResult::Notified => {
                            panic!("Expected a deadline event, not a notification")
                        }
                        WaitResult::Shutdown => {
                            panic!("Expected a deadline event, not a shutdown signal")
                        }
                    }
                }
                WaitResult::Shutdown => {
                    panic!("Expected a deadline event, not a shutdown signal")
                }
            }

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
