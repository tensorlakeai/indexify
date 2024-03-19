use axum::http::StatusCode;
use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Ok, Result};
use axum::Json;
use indexify_proto::indexify_coordinator::{
    self, coordinator_service_client::CoordinatorServiceClient,
};
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::api::{IndexifyAPIError, RaftMetricsSnapshotResponse};

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

    pub async fn get_raft_metrics_snapshot(
        &self,
    ) -> Result<Json<RaftMetricsSnapshotResponse>, IndexifyAPIError> {
        let mut client = self
            .get()
            .await
            .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let grpc_res = client
            .get_raft_metrics_snapshot(tonic::Request::new(
                indexify_coordinator::GetRaftMetricsSnapshotRequest {},
            ))
            .await
            .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let raft_metrics = grpc_res.into_inner();
        let snapshot_response = RaftMetricsSnapshotResponse {
            fail_connect_to_peer: raft_metrics.fail_connect_to_peer,
            sent_bytes: raft_metrics.sent_bytes,
            recv_bytes: raft_metrics.recv_bytes,
            sent_failures: raft_metrics.sent_failures,
            snapshot_send_success: raft_metrics.snapshot_send_success,
            snapshot_send_failure: raft_metrics.snapshot_send_failure,
            snapshot_recv_success: raft_metrics.snapshot_recv_success,
            snapshot_recv_failure: raft_metrics.snapshot_recv_failure,
            snapshot_send_inflights: raft_metrics.snapshot_send_inflights,
            snapshot_recv_inflights: raft_metrics.snapshot_recv_inflights,
            snapshot_sent_seconds: raft_metrics
                .snapshot_sent_seconds
                .into_iter()
                .map(|(k, v)| (k, v.values))
                .collect(),
            snapshot_recv_seconds: raft_metrics
                .snapshot_recv_seconds
                .into_iter()
                .map(|(k, v)| (k, v.values))
                .collect(),
            snapshot_size: raft_metrics.snapshot_size,
            last_snapshot_creation_time_millis: raft_metrics.last_snapshot_creation_time_millis,
            running_state_ok: raft_metrics.running_state_ok,
            id: raft_metrics.id,
            current_term: raft_metrics.current_term,
            vote: raft_metrics.vote,
            last_log_index: raft_metrics.last_log_index,
            current_leader: raft_metrics.current_leader,
        };
        Ok(Json(snapshot_response))
            .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
    }
}
