use anyhow::Result;
use tokio::process::Child;

use super::server::{FunctionExecutorServer, FunctionExecutorServerStatus};

pub struct SubprocessFunctionExecutorServer {
    process: Child,
    address: String,
}

impl SubprocessFunctionExecutorServer {
    pub fn new(process: Child, address: String) -> Self {
        Self { process, address }
    }
}

#[async_trait::async_trait]
impl FunctionExecutorServer for SubprocessFunctionExecutorServer {
    async fn status(&self) -> FunctionExecutorServerStatus {
        // TODO: implement proper status checking
        FunctionExecutorServerStatus::Running
    }

    fn address(&self) -> &str {
        &self.address
    }

    fn pid(&self) -> Option<u32> {
        self.process.id()
    }

    async fn kill(&mut self) -> Result<()> {
        self.process.kill().await?;
        self.process.wait().await?;
        Ok(())
    }
}
