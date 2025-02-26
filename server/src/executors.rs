use std::{collections::HashMap, sync::Arc, time::Duration, vec};

use anyhow::Result;
use data_model::{Allocation, ExecutorId, ExecutorMetadata};
use state_store::{
    requests::{
        DeregisterExecutorRequest,
        RegisterExecutorRequest,
        RequestPayload,
        StateMachineUpdateRequest,
    },
    IndexifyState,
};
use tracing::error;

pub const EXECUTOR_TIMEOUT: Duration = Duration::from_secs(5);
pub struct ExecutorManager {
    indexify_state: Arc<IndexifyState>,
}

impl ExecutorManager {
    pub async fn new(indexify_state: Arc<IndexifyState>) -> Self {
        let cs = indexify_state.clone();
        let executors = cs.reader().get_all_executors().unwrap_or_default();
        tokio::spawn(async move {
            tokio::time::sleep(EXECUTOR_TIMEOUT).await;
            for executor in executors {
                let sm_req = StateMachineUpdateRequest {
                    payload: RequestPayload::DeregisterExecutor(DeregisterExecutorRequest {
                        executor_id: executor.id.clone(),
                    }),
                    processed_state_changes: vec![],
                };
                if let Err(err) = cs.write(sm_req).await {
                    error!(
                        "failed to deregister executor on startup {}: {:?}",
                        executor.id, err
                    );
                }
            }
        });

        ExecutorManager { indexify_state }
    }

    pub async fn register_executor(&self, executor: ExecutorMetadata) -> Result<()> {
        let sm_req = StateMachineUpdateRequest {
            payload: RequestPayload::RegisterExecutor(RegisterExecutorRequest { executor }),
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

    pub async fn list_allocations(&self) -> HashMap<String, Vec<Allocation>> {
        self.indexify_state
            .in_memory_state
            .read()
            .await
            .allocations_by_executor
            .iter()
            .map(|(executor_id, allocations)| {
                let executor_id = executor_id.clone();
                let mut allocs = vec![];
                for allocation in allocations {
                    allocs.push(*allocation.clone());
                }
                (executor_id, allocs)
            })
            .collect()
    }
}

pub fn schedule_deregister(ex: Arc<ExecutorManager>, executor_id: ExecutorId, duration: Duration) {
    tokio::spawn(async move {
        tokio::time::sleep(duration).await;
        let ret = ex.deregister_executor(executor_id.clone()).await;
        if let Err(e) = ret {
            error!("failed to deregister executor {}: {:?}", executor_id, e);
        }
    });
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use data_model::{ExecutorId, ExecutorMetadata};

    use super::*;
    use crate::{service::Service, testing};

    #[tokio::test]
    async fn test_register_executor() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service {
            indexify_state,
            executor_manager,
            graph_processor,
            shutdown_rx,
            ..
        } = test_srv.service;

        tokio::spawn(async move {
            graph_processor.start(shutdown_rx).await;
        });

        let executor = ExecutorMetadata {
            id: ExecutorId::new("test".to_string()),
            executor_version: "1.0".to_string(),
            function_allowlist: None,
            addr: "".to_string(),
            labels: Default::default(),
        };
        executor_manager.register_executor(executor).await?;

        let executors = indexify_state.reader().get_all_executors()?;

        assert_eq!(executors.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_deregister_executor() -> Result<()> {
        let test_srv = testing::TestService::new().await?;
        let Service {
            indexify_state,
            executor_manager,
            graph_processor,
            shutdown_rx,
            ..
        } = test_srv.service;

        tokio::spawn(async move {
            graph_processor.start(shutdown_rx).await;
        });

        let executor = ExecutorMetadata {
            id: ExecutorId::new("test".to_string()),
            executor_version: "1.0".to_string(),
            function_allowlist: None,
            addr: "".to_string(),
            labels: Default::default(),
        };
        executor_manager.register_executor(executor.clone()).await?;

        let executors = indexify_state.reader().get_all_executors()?;

        assert_eq!(executors.len(), 1);

        executor_manager.register_executor(executor.clone()).await?;
        schedule_deregister(
            executor_manager.clone(),
            executor.id.clone(),
            Duration::from_secs(1),
        );
        schedule_deregister(
            executor_manager.clone(),
            executor.id.clone(),
            Duration::from_secs(2),
        );

        let executors = indexify_state.reader().get_all_executors()?;

        assert_eq!(executors.len(), 1);
        let time = std::time::Instant::now();
        loop {
            let executors = indexify_state.reader().get_all_executors()?;
            if executors.is_empty() {
                break;
            }
            if time.elapsed().as_secs() > 10 {
                panic!("executor was not deregistered");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Ok(())
    }
}
