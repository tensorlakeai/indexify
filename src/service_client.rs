use anyhow::{anyhow, Ok, Result};
use tonic::transport::Channel;

use crate::indexify_coordinator::coordinator_service_client::CoordinatorServiceClient;

#[derive(Debug)]
pub struct CoordinatorClient {
    _addr: String,
    client: CoordinatorServiceClient<Channel>,
}

impl CoordinatorClient {
    pub async fn new(addr: String) -> Result<Self> {
        let client = CoordinatorServiceClient::connect(format!("http://{}", addr.clone()))
            .await
            .map_err(|e| anyhow!("unable to connect to coordinator: {}", e))?;
        Ok(Self {
            _addr: addr,
            client,
        })
    }

    pub fn get(&self) -> CoordinatorServiceClient<Channel> {
        self.client.clone()
    }
}
