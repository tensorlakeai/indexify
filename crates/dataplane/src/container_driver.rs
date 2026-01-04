use anyhow::Result;
use async_trait::async_trait;

use crate::objects::{ContainerState, FunctionExecutor};

#[async_trait]
pub trait ContainerDriver: Send + Sync {
    async fn start(&self, function_executor: &FunctionExecutor) -> Result<()>;
    async fn stop(&self, function_executor: &FunctionExecutor) -> Result<()>;
    async fn status(&self, function_executor: &FunctionExecutor) -> Result<ContainerState>;
}
