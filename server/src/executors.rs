use std::{sync::Arc, time::Duration};

use anyhow::Result;
use data_model::{ExecutorId, ExecutorMetadata};
use state_store::{
    requests::{
        DeregisterExecutorRequest, RegisterExecutorRequest, RequestPayload,
        StateMachineUpdateRequest,
    },
    IndexifyState,
};

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
                let _ = cs
                    .write(StateMachineUpdateRequest {
                        payload: RequestPayload::DeregisterExecutor(DeregisterExecutorRequest {
                            executor_id: executor.id,
                        }),
                        state_changes_processed: vec![],
                    })
                    .await;
            }
        });

        ExecutorManager { indexify_state }
    }

    pub async fn register_executor(&self, executor: ExecutorMetadata) -> Result<()> {
        self.indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::RegisterExecutor(RegisterExecutorRequest { executor }),
                state_changes_processed: vec![],
            })
            .await
    }

    pub async fn deregister_executor(&self, executor_id: ExecutorId) -> Result<()> {
        self.indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::DeregisterExecutor(DeregisterExecutorRequest {
                    executor_id,
                }),
                state_changes_processed: vec![],
            })
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
            tracing::error!("failed to deregister executor {}: {:?}", executor_id, e);
        }
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use data_model::{ExecutorId, ExecutorMetadata};
    use state_store::IndexifyState;

    use super::*;

    #[tokio::test]
    async fn test_register_executor() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let indexify_state = IndexifyState::new(temp_dir.path().join("state"))
            .await
            .unwrap();
        let ex = Arc::new(ExecutorManager::new(indexify_state.clone()).await);
        let executor = ExecutorMetadata {
            id: ExecutorId::new("test".to_string()),
            image_name: "test".to_string(),
            addr: "".to_string(),
            labels: Default::default(),
        };
        ex.register_executor(executor).await?;

        let executors = indexify_state.reader().get_all_executors()?;

        assert_eq!(executors.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_deregister_executor() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let indexify_state = IndexifyState::new(temp_dir.path().join("state"))
            .await
            .unwrap();
        let ex = Arc::new(ExecutorManager::new(indexify_state.clone()).await);
        let executor = ExecutorMetadata {
            id: ExecutorId::new("test".to_string()),
            image_name: "test".to_string(),
            addr: "".to_string(),
            labels: Default::default(),
        };
        ex.register_executor(executor.clone()).await?;

        let executors = indexify_state.reader().get_all_executors()?;

        assert_eq!(executors.len(), 1);

        ex.deregister_executor(executors[0].id.clone()).await?;

        let executors = indexify_state.reader().get_all_executors()?;

        assert_eq!(executors.len(), 0);

        ex.register_executor(executor.clone()).await?;

        schedule_deregister(ex.clone(), executor.id.clone(), Duration::from_secs(2));

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
