use std::sync::Arc;

use anyhow::Result;
use data_model::{ExecutorId, ExecutorMetadata};
use state_store::{
    requests::{
        DeregisterExecutorRequest,
        RegisterExecutorRequest,
        RequestPayload,
        StateMachineUpdateRequest,
    },
    IndexifyState,
};

pub struct ExecutorManager {
    indexify_state: Arc<IndexifyState>,
}

impl ExecutorManager {
    pub fn new(indexify_state: Arc<IndexifyState>) -> Self {
        ExecutorManager { indexify_state }
    }

    pub async fn register_executor(&self, executor: &ExecutorMetadata) -> Result<()> {
        self.indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::RegisterExecutor(RegisterExecutorRequest {
                    executor: executor.clone(),
                }),
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
}
