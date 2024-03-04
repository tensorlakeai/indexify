use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};
use indexify_proto::indexify_raft::raft_api_client::RaftApiClient;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::info;

use super::NodeId;

pub struct RaftClient {
    clients: Arc<Mutex<HashMap<String, RaftApiClient<Channel>>>>,
    current_leader: Option<(NodeId, String)>,
}

impl Default for RaftClient {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftClient {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
            current_leader: None,
        }
    }

    pub async fn get(&self, addr: &str) -> Result<RaftApiClient<Channel>> {
        let mut clients = self.clients.lock().await;
        if let Some(client) = clients.get(addr) {
            return Ok(client.clone());
        }

        info!("connecting to raft at {}", addr);

        let client = RaftApiClient::connect(format!("http://{}", addr))
            .await
            .map_err(|e| anyhow!("unable to connect to raft: {} at addr {}", e, addr))?;
        clients.insert(addr.to_string(), client.clone());
        Ok(client)
    }
}
