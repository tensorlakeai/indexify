use std::{sync::Arc, time::Duration};

use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::error;

use crate::executor::{
    function_executor::{
        function_executor_service::{
            function_executor_client::FunctionExecutorClient, InfoRequest,
        },
        health_checker::HealthChecker,
    },
    metrics::Metrics,
};

pub mod function_executor_service {
    tonic::include_proto!("function_executor_service");
}

pub mod health_checker;
pub mod server;
pub mod server_factory;
pub mod subprocess;

pub use function_executor_service::{InitializeRequest, InitializeResponse};
pub use server::{FunctionExecutorServer, FunctionExecutorServerStatus};
pub use server_factory::{FunctionExecutorServerConfiguration, FunctionExecutorServerFactory};
pub use subprocess::SubprocessFunctionExecutorServer;

#[derive(Debug)]
pub struct FunctionExecutorInitializationResult {
    pub is_timeout: bool,
    pub is_oom: bool,
    pub response: Option<InitializeResponse>,
}

#[derive(Clone)]
pub struct FunctionExecutor {
    server_factory: Arc<dyn FunctionExecutorServerFactory>,
    server: Option<Arc<Mutex<dyn FunctionExecutorServer>>>,
    channel: Option<Channel>,
    metrics: Arc<Mutex<Metrics>>,
    health_checker: Option<Arc<HealthChecker>>,
}

impl FunctionExecutor {
    pub fn new(
        server_factory: Arc<dyn FunctionExecutorServerFactory>,
        metrics: Arc<Mutex<Metrics>>,
    ) -> Self {
        Self {
            server_factory,
            server: None,
            channel: None,
            metrics,
            health_checker: None,
        }
    }

    pub fn channel(&self) -> Option<&Channel> {
        self.channel.as_ref()
    }

    pub async fn server_status(&self) -> Option<FunctionExecutorServerStatus> {
        self.server
            .as_ref()
            .map(|_| Some(FunctionExecutorServerStatus::Running))?
    }

    pub async fn initialize(
        &mut self,
        config: FunctionExecutorServerConfiguration,
        initialize_request: InitializeRequest,
        customer_code_timeout_sec: f64,
    ) -> Result<FunctionExecutorInitializationResult, Box<dyn std::error::Error>> {
        let server = self.server_factory.create(config).await?;
        self.server = Some(server);

        self.channel = Some(self.establish_channel(customer_code_timeout_sec).await?);
        if let Some(channel) = self.channel.as_ref() {
            self.health_checker = Some(Arc::new(HealthChecker::new(channel.clone())));
        }

        match self.channel() {
            Some(channel) => {
                let mut client = FunctionExecutorClient::new(channel.clone());

                match tokio::time::timeout(
                    Duration::from_secs_f64(customer_code_timeout_sec),
                    client.initialize(initialize_request),
                )
                .await
                {
                    Ok(Ok(response)) => Ok(FunctionExecutorInitializationResult {
                        is_timeout: false,
                        is_oom: false,
                        response: Some(response.into_inner()),
                    }),
                    Ok(Err(err)) => {
                        error!("Initialize RPC failed: {:?}", err);
                        if err.code() == tonic::Code::DeadlineExceeded {
                            Ok(FunctionExecutorInitializationResult {
                                is_timeout: true,
                                is_oom: false,
                                response: None,
                            })
                        } else {
                            let is_oom = if let Some(server) = self.server.as_ref() {
                                server.lock().await.status().await
                                    == FunctionExecutorServerStatus::OomKilled
                            } else {
                                false
                            };

                            Ok(FunctionExecutorInitializationResult {
                                is_timeout: false,
                                is_oom,
                                response: None,
                            })
                        }
                    }
                    Err(_) => Ok(FunctionExecutorInitializationResult {
                        is_timeout: true,
                        is_oom: false,
                        response: None,
                    }),
                }
            }
            None => Ok(FunctionExecutorInitializationResult {
                is_timeout: true,
                is_oom: false,
                response: None,
            }),
        }
    }

    pub async fn destroy(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(_channel) = self.channel.take() {
            // Channel will be dropped
        }

        if let Some(server) = self.server.take() {
            server.lock().await.kill().await?;
        }

        Ok(())
    }

    async fn establish_channel(
        &mut self,
        customer_code_timeout_sec: f64,
    ) -> Result<Channel, Box<dyn std::error::Error>> {
        if let Some(server) = self.server.as_ref() {
            let address = format!("http://{}", server.lock().await.address().to_string());
            let channel = Channel::from_shared(address)?
                .connect_timeout(Duration::from_secs_f64(customer_code_timeout_sec))
                .connect()
                .await?;
            Ok(channel)
        } else {
            Err("Server not initialized".into())
        }
    }

    async fn server_info(&self, channel: &Channel) -> Result<(), Box<dyn std::error::Error>> {
        let mut client = FunctionExecutorClient::new(channel.clone());
        let _info = client.get_info(InfoRequest {}).await?.into_inner();
        // TODO: set labels
        Ok(())
    }
}
