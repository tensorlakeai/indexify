use std::net::TcpListener;
use std::process::Stdio;

use tokio::process::Command;

use crate::executor::function_executor::server::FunctionExecutorServer;

#[derive(Debug, Clone)]
pub struct FunctionExecutorServerConfiguration {
    pub executor_id: String,
    pub function_executor_id: String,
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub function_name: String,
    pub secret_names: Vec<String>,
    pub cpu_ms_per_sec: u32,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub gpu_count: u32,
}

#[async_trait::async_trait]
pub trait FunctionExecutorServerFactory: Send + Sync {
    async fn create(
        &self,
        config: FunctionExecutorServerConfiguration,
    ) -> Result<Box<dyn FunctionExecutorServer>, Box<dyn std::error::Error>>;

    async fn destroy(&self) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct SubprocessFunctionExecutorServerFactory {
    verbose_logs: bool,
}

impl SubprocessFunctionExecutorServerFactory {
    pub fn new(verbose_logs: bool) -> Self {
        Self { verbose_logs }
    }

    fn find_free_port() -> Result<u16, Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        Ok(port)
    }

    fn server_address(port: u16) -> String {
        format!("localhost:{}", port)
    }
}

#[async_trait::async_trait]
impl FunctionExecutorServerFactory for SubprocessFunctionExecutorServerFactory {
    async fn create(
        &self,
        config: FunctionExecutorServerConfiguration,
    ) -> Result<Box<dyn FunctionExecutorServer>, Box<dyn std::error::Error>> {
        let port = Self::find_free_port()?;
        let address = Self::server_address(port);

        let mut args = vec![
            format!("--executor-id={}", config.executor_id),
            format!("--function-executor-id={}", config.function_executor_id),
            "--address".to_string(),
            address.clone(),
        ];

        if self.verbose_logs {
            args.push("--dev".to_string());
        }

        let child = Command::new("function-executor")
            .args(&args)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?;

        Ok(Box::new(
            crate::executor::function_executor::subprocess::SubprocessFunctionExecutorServer::new(
                child, address,
            ),
        ))
    }

    async fn destroy(&self) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: Implement server destruction
        Ok(())
    }
}
