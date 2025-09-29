use std::time::Duration;

use anyhow::Result;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};

#[allow(non_camel_case_types)]
pub mod function_executor_pb {
    tonic::include_proto!("function_executor_service");
}

use function_executor_pb::{
    function_executor_client::FunctionExecutorClient,
    HealthCheckRequest,
    HealthCheckResponse,
    InfoRequest,
    InfoResponse,
    InitializeRequest,
    InitializeResponse,
};

pub struct FunctionExecutorServer {
    address: String,
    client: Mutex<FunctionExecutorClient<Channel>>, // tonic client is cheap to clone internally
}

impl FunctionExecutorServer {
    pub async fn connect(address: String) -> Result<Self> {
        let endpoint = Endpoint::from_shared(format!("http://{address}"))?;
        let channel = endpoint.connect().await?;
        let client = FunctionExecutorClient::new(channel);
        Ok(Self {
            address,
            client: Mutex::new(client),
        })
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub async fn get_info(&self) -> Result<InfoResponse> {
        let mut client = self.client.lock().await;
        let resp = client.get_info(InfoRequest {}).await?;
        Ok(resp.into_inner())
    }

    pub async fn initialize(
        &self,
        req: InitializeRequest,
        timeout: Duration,
    ) -> Result<InitializeResponse> {
        let mut client = self.client.lock().await;
        let fut = client.initialize(req);
        let resp = tokio::time::timeout(timeout, fut).await??;
        Ok(resp.into_inner())
    }

    pub async fn check_health(&self) -> Result<HealthCheckResponse> {
        let mut client = self.client.lock().await;
        let resp = client.check_health(HealthCheckRequest {}).await?;
        Ok(resp.into_inner())
    }
}
