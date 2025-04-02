use std::{cmp::Ordering, collections::HashMap, sync::Arc, time::Duration, vec};

use anyhow::Result;
use data_model::{Allocation, ExecutorId, ExecutorMetadata};
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
    sync::{watch, Mutex},
    time::Instant,
};
use tokio_util::time::{DelayQueue, delay_queue::Key};
use tracing::{error, trace};
use futures::StreamExt;

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
    heartbeat_queue: Mutex<DelayQueue<ExecutorId>>,
    heartbeat_keys: Mutex<HashMap<ExecutorId, Key>>,
    indexify_state: Arc<IndexifyState>,
    timeout: Duration,
}

impl ExecutorManager {
    pub async fn new(indexify_state: Arc<IndexifyState>, timeout: Duration) -> Self {
        ExecutorManager {
            indexify_state,
            heartbeat_queue: Mutex::new(DelayQueue::new()),
            heartbeat_keys: Mutex::new(HashMap::new()),
            timeout,
        }
    }

    /// Heartbeat an executor to keep it alive and update its metadata
    pub async fn heartbeat(&self, executor: ExecutorMetadata) -> Result<()> {
        trace!(executor_id = executor.id.get(), "Heartbeat received");

        // Update or insert the executor's heartbeat in the delay queue
        let mut queue = self.heartbeat_queue.lock().await;
        let mut keys = self.heartbeat_keys.lock().await;
        
        if let Some(key) = keys.get(&executor.id) {
            println!("resetting heartbeat for executor {:?}", executor.id.get());
            queue.reset(key, self.timeout);
        } else {
            println!("inserting heartbeat for executor {:?}", executor.id.get());
            let key = queue.insert(executor.id.clone(), self.timeout);
            keys.insert(executor.id.clone(), key);
        }

        // Register the executor to upsert its metadata
        let err = self.register_executor(executor.clone()).await;
        if let Err(e) = err {
            error!("failed to register executor {}: {:?}", executor.id.get(), e);
            return Err(e);
        }

        Ok(())
    }


    async fn deregister_executor(&self, executor_id: ExecutorId) -> Result<()> {
        trace!(executor_id = executor_id.get(), "Executor heartbeat expired");
        // Remove the key from our tracking map
        let mut keys = self.heartbeat_keys.lock().await;
        keys.remove(&executor_id);
                    
        // Deregister the lapsed executor
        let sm_req = StateMachineUpdateRequest {
            payload: RequestPayload::DeregisterExecutor(DeregisterExecutorRequest {
                executor_id: executor_id.clone(),
            }),
            processed_state_changes: vec![],
        };
        self.indexify_state.write(sm_req).await
    }

    /// Starts the heartbeat monitoring loop for executors
    pub async fn start_heartbeat_monitor(self: Arc<Self>, mut shutdown_rx: watch::Receiver<()>) {
        loop {
            tokio::select! {
                Some(expired) = async {
                    let mut queue = self.heartbeat_queue.lock().await;
                    queue.next().await
                } => {
                    let executor_id = expired.into_inner();
                    if let Err(err) = self.deregister_executor(executor_id.clone()).await {
                        error!(executor_id = executor_id.get(), "failed to deregister lapsed executor: {:?}", err);
                    }
                }
                _ = shutdown_rx.changed() => {
                    trace!("Received shutdown signal, shutting down heartbeat monitor");
                    break;
                }
            }
        }
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
        let test_srv = testing::TestService::new_with_executor_timeout(Duration::from_millis(10)).await?;
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
            tokio::time::sleep(Duration::from_millis(4)).await;

            executor_manager.heartbeat(executor1.clone()).await?;
            executor_manager.heartbeat(executor2.clone()).await?;
            executor_manager.heartbeat(executor3.clone()).await?;

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
            time::advance(Duration::from_secs(2)).await;

            executor_manager.heartbeat(executor1.clone()).await?;
            executor_manager.heartbeat(executor2.clone()).await?;
            // Executor 3 goes offline
            // executor_manager.heartbeat(executor3.clone()).await?;

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
            tokio::time::sleep(Duration::from_millis(8)).await;
            let mut queue = executor_manager.heartbeat_queue.lock().await;
            println!("queue len: {:?}", queue.len());
            while let Some(expired) = queue.next().await {
                let executor_id = expired.into_inner();
                println!("deregistering executor 2222: {:?}", executor_id);
                executor_manager.deregister_executor(executor_id).await?;
            }
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
            tokio::time::sleep(Duration::from_millis(10)).await;
            let mut queue = executor_manager.heartbeat_queue.lock().await;
            while let Some(expired) = queue.next().await {
                let executor_id = expired.into_inner();
                println!("deregistering executor 3333: {:?}", executor_id);
                executor_manager.deregister_executor(executor_id).await?;
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
