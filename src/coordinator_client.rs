use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Ok, Result};
use axum::{http::StatusCode, Json};
use indexify_internal_api::StructuredDataSchema;
use indexify_proto::indexify_coordinator::{
    self,
    coordinator_service_client::CoordinatorServiceClient,
};
use itertools::Itertools;
use tokio::sync::Mutex;
use tonic::transport::{Channel, ClientTlsConfig};

use crate::{
    api::{IndexifyAPIError, RaftMetricsSnapshotResponse, TaskAssignments},
    server_config::ServerConfig,
};

#[derive(Debug)]
pub struct CoordinatorClient {
    config: Arc<ServerConfig>,
    addr: String,
    clients: Arc<Mutex<HashMap<String, CoordinatorServiceClient<Channel>>>>,
}

impl CoordinatorClient {
    pub fn new(config: Arc<ServerConfig>) -> Self {
        Self {
            addr: config.coordinator_addr.to_string(),
            config,
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get_coordinator(&self, addr: &str) -> Result<CoordinatorServiceClient<Channel>> {
        let mut clients = self.clients.lock().await;
        if let Some(client) = clients.get(addr) {
            return Ok(client.clone());
        }

        tracing::info!("connecting to coordinator at {}", addr);

        let client = CoordinatorServiceClient::connect(format!("http://{}", addr))
            .await
            .map_err(|e| anyhow!("unable to connect to coordinator: {} at addr {}", e, addr))?;
        clients.insert(addr.to_string(), client.clone());
        Ok(client)
    }

    async fn create_tls_channel(&self) -> Result<CoordinatorServiceClient<Channel>> {
        if let Some(tls_config) = self.config.coordinator_client_tls.as_ref() {
            if tls_config.api {
                tracing::info!("connecting via mTLS to coordinator service");
                let cert = std::fs::read(tls_config.cert_file.clone())?;
                let key = std::fs::read(tls_config.key_file.clone())?;
                let ca_cert = tls_config
                    .ca_file
                    .as_ref()
                    .ok_or_else(|| anyhow!("ca_file is required"))?;
                let ca_cert_contents = std::fs::read(ca_cert)?;

                let tls_config = ClientTlsConfig::new()
                    .ca_certificate(tonic::transport::Certificate::from_pem(&ca_cert_contents))
                    .identity(tonic::transport::Identity::from_pem(&cert, &key))
                    .domain_name("localhost");
                let endpoint = format!("https://{}", &self.addr);
                let channel = Channel::from_shared(endpoint)?
                    .tls_config(tls_config)?
                    .connect()
                    .await?;
                return Ok(CoordinatorServiceClient::new(channel));
            } else {
                tracing::info!("connecting without TLS to coordinator service");
                let client = CoordinatorServiceClient::connect(format!("http://{}", &self.addr))
                    .await?;
                return Ok(client);
            }
        }
        tracing::info!("connecting without TLS to coordinator service");
        let client = CoordinatorServiceClient::connect(format!("http://{}", &self.addr))
            .await?;
        Ok(client)
    }

    pub async fn get(&self) -> Result<CoordinatorServiceClient<Channel>> {
        let mut clients = self.clients.lock().await;
        if let Some(client) = clients.get(&self.addr) {
            return Ok(client.clone());
        }

        let client = self.create_tls_channel().await?;
        clients.insert(self.addr.to_string(), client.clone());
        Ok(client)
    }

    pub async fn get_raft_metrics_snapshot(
        &self,
    ) -> Result<Json<RaftMetricsSnapshotResponse>, IndexifyAPIError> {
        let mut client = self.get().await.map_err(IndexifyAPIError::internal_error)?;
        let grpc_res = client
            .get_raft_metrics_snapshot(tonic::Request::new(
                indexify_coordinator::GetRaftMetricsSnapshotRequest {},
            ))
            .await
            .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.message()))?;
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
        Ok(Json(snapshot_response)).map_err(IndexifyAPIError::internal_error)
    }

    pub async fn get_structured_schemas(
        &self,
        namespace: &str,
    ) -> Result<Vec<StructuredDataSchema>> {
        let request =
            tonic::Request::new(indexify_proto::indexify_coordinator::GetAllSchemaRequest {
                namespace: namespace.to_string(),
            });
        let response = self.get().await?.list_schemas(request).await?.into_inner();
        let schemas = response
            .schemas
            .into_iter()
            .map(|schema| StructuredDataSchema {
                namespace: namespace.to_string(),
                columns: serde_json::from_str(&schema.columns).unwrap(),
                content_source: schema.content_source,
                id: "".to_string(),
            })
            .collect_vec();
        Ok(schemas)
    }

    pub async fn all_task_assignments(&self) -> Result<TaskAssignments> {
        let request = tonic::Request::new(
            indexify_proto::indexify_coordinator::GetAllTaskAssignmentRequest {},
        );
        let response = self.get().await?.get_all_task_assignments(request).await?;
        Ok(TaskAssignments {
            assignments: response.into_inner().assignments,
        })
    }
}
