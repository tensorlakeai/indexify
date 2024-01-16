use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Ok, Result};
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::indexify_coordinator::coordinator_service_client::CoordinatorServiceClient;

#[derive(Debug)]
pub struct CoordinatorClient {
    addr: String,
    clients: Arc<Mutex<HashMap<String, CoordinatorServiceClient<Channel>>>>,
}

impl CoordinatorClient {
    pub fn new(addr: &str) -> Self {
        Self {
            addr: addr.to_string(),
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get(&self) -> Result<CoordinatorServiceClient<Channel>> {
        let mut clients = self.clients.lock().await;
        if let Some(client) = clients.get(&self.addr) {
            return Ok(client.clone());
        }

        let client = CoordinatorServiceClient::connect(format!("http://{}", &self.addr))
            .await
            .map_err(|e| {
                anyhow!(
                    "unable to connect to coordinator: {} at addr {}",
                    e,
                    self.addr
                )
            })?;
        clients.insert(self.addr.to_string(), client.clone());
        Ok(client)
    }
}
