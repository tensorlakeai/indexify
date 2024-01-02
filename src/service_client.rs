use anyhow::{anyhow, Ok, Result};
use tonic::transport::Channel;

use crate::indexify_coordinator::coordinator_service_client::CoordinatorServiceClient;

pub struct CoordinatorClient {
    addr: String,
    client: CoordinatorServiceClient<Channel>,
}

impl CoordinatorClient {
    pub async fn new(addr: String) -> Result<Self> {
        let client = CoordinatorServiceClient::connect(addr.clone())
            .await
            .map_err(|e| anyhow!("unable to connect to coordinator: {}", e))?;
        Ok(Self { addr, client })
    }

    pub fn get(&self) -> CoordinatorServiceClient<Channel> {
        self.client.clone()
    }
}
