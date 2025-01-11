use std::{sync::Arc, time::Duration};

use anyhow::Result;
use data_model::{ExecutorId, ExecutorMetadata};
use processor::dispatcher::Dispatcher;
use state_store::{
    requests::{DeregisterExecutorRequest, RegisterExecutorRequest, RequestPayload},
    IndexifyState,
};
use tracing::error;

pub const EXECUTOR_TIMEOUT: Duration = Duration::from_secs(5);
pub struct ExecutorManager {
    indexify_state: Arc<IndexifyState<Dispatcher>>,
    dispatcher: Arc<Dispatcher>,
}

impl ExecutorManager {
    pub async fn new(
        indexify_state: Arc<IndexifyState<Dispatcher>>,
        dispatcher: Arc<Dispatcher>,
    ) -> Self {
        let cs = indexify_state.clone();
        let dispatch = dispatcher.clone();
        let executors = cs.reader().get_all_executors().unwrap_or_default();
        tokio::spawn(async move {
            tokio::time::sleep(EXECUTOR_TIMEOUT).await;
            for executor in executors {
                if let Err(err) = dispatch
                    .dispatch_requests(RequestPayload::DeregisterExecutor(
                        DeregisterExecutorRequest {
                            executor_id: executor.id.clone(),
                        },
                    ))
                    .await
                {
                    error!(
                        "failed to deregister executor on startup {}: {:?}",
                        executor.id, err
                    );
                }
            }
        });

        ExecutorManager {
            indexify_state,
            dispatcher,
        }
    }

    pub async fn register_executor(&self, executor: ExecutorMetadata) -> Result<()> {
        self.dispatcher
            .dispatch_requests(RequestPayload::RegisterExecutor(RegisterExecutorRequest {
                executor,
            }))
            .await
    }

    pub async fn deregister_executor(&self, executor_id: ExecutorId) -> Result<()> {
        self.dispatcher
            .dispatch_requests(RequestPayload::DeregisterExecutor(
                DeregisterExecutorRequest { executor_id },
            ))
            .await
    }

    pub async fn list_executors(&self) -> Result<Vec<ExecutorMetadata>> {
        self.indexify_state.reader().get_all_executors()
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
            task_allocator,
            shutdown_rx,
            ..
        } = test_srv.service;

        tokio::spawn(async move {
            task_allocator.start(shutdown_rx.clone()).await;
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
            task_allocator,
            shutdown_rx,
            ..
        } = test_srv.service;

        tokio::spawn(async move {
            task_allocator.start(shutdown_rx.clone()).await;
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

        executor_manager
            .deregister_executor(executors[0].id.clone())
            .await?;

        let executors = indexify_state.reader().get_all_executors()?;

        assert_eq!(executors.len(), 0);

        executor_manager.register_executor(executor.clone()).await?;

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
