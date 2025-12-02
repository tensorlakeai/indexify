use anyhow::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionExecutorServerStatus {
    Running,
    Exited,
    OomKilled,
}

#[async_trait::async_trait]
pub trait FunctionExecutorServer: Send + Sync {
    async fn status(&self) -> FunctionExecutorServerStatus;

    fn address(&self) -> &str;

    fn pid(&self) -> Option<u32>;

    async fn kill(&mut self) -> Result<()>;
}
