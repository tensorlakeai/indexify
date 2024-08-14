use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};
use axum::{http::StatusCode, Json};
use indexify_internal_api::StructuredDataSchema;
use indexify_proto::indexify_coordinator::{
    self,
    coordinator_service_client,
    ContentMetadata,
    ExtractionPolicy,
    Task,
};
use itertools::Itertools;
use opentelemetry::propagation::{Injector, TextMapPropagator};
use tokio::sync::Mutex;
use tonic::{
    service::Interceptor,
    transport::{Channel, ClientTlsConfig},
    Request,
    Status,
};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{
    api::{IndexifyAPIError, RaftMetricsSnapshotResponse, TaskAssignments},
    server_config::ServerConfig,
    state::grpc_config::GrpcConfig,
};

#[derive(Debug, Clone)]
pub struct OpenTelemetryInjector;

struct MetadataMap<'a>(&'a mut tonic::metadata::MetadataMap);

impl Interceptor for OpenTelemetryInjector {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let propagator = opentelemetry_sdk::propagation::TraceContextPropagator::new();
        let ctx = Span::current().context();
        propagator.inject_context(&ctx, &mut MetadataMap(request.metadata_mut()));
        Ok(request)
    }
}

impl<'a> Injector for MetadataMap<'a> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(key) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes()) {
            if let Ok(val) = tonic::metadata::MetadataValue::try_from(&value) {
                self.0.insert(key, val);
            }
        }
    }
}

pub type CoordinatorServiceClient = coordinator_service_client::CoordinatorServiceClient<
    tonic::service::interceptor::InterceptedService<Channel, OpenTelemetryInjector>,
>;

#[derive(Debug)]
pub struct CoordinatorClient {
    pub config: Arc<ServerConfig>,
    addr: String,
    clients: Arc<Mutex<HashMap<String, CoordinatorServiceClient>>>,
}

impl CoordinatorClient {
    pub fn new(config: Arc<ServerConfig>) -> Self {
        Self {
            addr: config.coordinator_addr.to_string(),
            config,
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get_coordinator(&self, addr: &str) -> Result<CoordinatorServiceClient> {
        let mut clients = self.clients.lock().await;
        if let Some(client) = clients.get(addr) {
            return Ok(client.clone());
        }

        tracing::info!("connecting to coordinator at {}", addr);

        let channel = Channel::from_shared(addr.to_string())?.connect().await?;
        let client = coordinator_service_client::CoordinatorServiceClient::with_interceptor(
            channel,
            OpenTelemetryInjector,
        );
        clients.insert(addr.to_string(), client.clone());
        Ok(client)
    }

    async fn create_tls_channel(&self) -> Result<CoordinatorServiceClient> {
        let channel = if let Some(tls_config) = self.config.coordinator_client_tls.as_ref() {
            if tls_config.api {
                tracing::info!(
                    "connecting via mTLS to coordinator service, address: {}",
                    &self.addr
                );
                let cert = std::fs::read(tls_config.cert_file.clone())?;
                let key = std::fs::read(tls_config.key_file.clone())?;
                let ca_cert = tls_config
                    .ca_file
                    .as_ref()
                    .ok_or_else(|| anyhow!("ca_file is required"))?;
                let ca_cert_contents = std::fs::read(ca_cert)?;

                let tls_config = ClientTlsConfig::new()
                    .ca_certificate(tonic::transport::Certificate::from_pem(ca_cert_contents))
                    .identity(tonic::transport::Identity::from_pem(cert, key))
                    .domain_name("localhost");
                Channel::from_shared(format!("https://{}", &self.addr))?.tls_config(tls_config)?
            } else {
                tracing::info!(
                    "connecting without TLS to coordinator service, address: {}",
                    &self.addr
                );
                Channel::from_shared(format!("http://{}", &self.addr))?
            }
        } else {
            tracing::info!(
                "connecting without TLS to coordinator service, address: {}",
                &self.addr
            );
            Channel::from_shared(format!("http://{}", &self.addr))?
        };
        let channel = channel.connect().await?;
        let client = coordinator_service_client::CoordinatorServiceClient::with_interceptor(
            channel,
            OpenTelemetryInjector,
        )
        .max_decoding_message_size(GrpcConfig::MAX_DECODING_SIZE)
        .max_encoding_message_size(GrpcConfig::MAX_ENCODING_SIZE);
        Ok(client)
    }

    pub async fn get(&self) -> Result<CoordinatorServiceClient> {
        let mut clients = self.clients.lock().await;
        if let Some(client) = clients.get(&self.addr) {
            return Ok(client.clone());
        }

        let client = self.create_tls_channel().await?;
        clients.insert(self.addr.to_string(), client.clone());
        Ok(client)
    }

    pub async fn list_extraction_graphs(
        &self,
        namespace: &str,
    ) -> Result<Vec<indexify_coordinator::ExtractionGraph>> {
        let request = tonic::Request::new(
            indexify_proto::indexify_coordinator::ListExtractionGraphRequest {
                namespace: namespace.to_string(),
            },
        );
        let response = self.get().await?.list_extraction_graphs(request).await?;
        Ok(response.into_inner().graphs)
    }

    pub async fn get_extraction_graph_analytics(
        &self,
        namespace: &str,
        extraction_graph_name: &str,
    ) -> Result<indexify_coordinator::GetExtractionGraphAnalyticsResponse> {
        let request = tonic::Request::new(
            indexify_proto::indexify_coordinator::GetExtractionGraphAnalyticsRequest {
                namespace: namespace.to_string(),
                extraction_graph: extraction_graph_name.to_string(),
            },
        );
        let response = self
            .get()
            .await?
            .get_extraction_graph_analytics(request)
            .await?;
        Ok(response.into_inner())
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
                id: "".to_string(),
                extraction_graph_name: schema.extraction_graph_name,
                namespace: namespace.to_string(),
                columns: serde_json::from_str(&schema.columns).unwrap(),
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

    pub async fn get_task(
        &self,
        task_id: &str,
    ) -> Result<Option<indexify_proto::indexify_coordinator::Task>> {
        let request = tonic::Request::new(indexify_proto::indexify_coordinator::GetTaskRequest {
            task_id: task_id.to_string(),
        });
        let response = self.get().await?.get_task(request).await?;
        Ok(response.into_inner().task)
    }

    pub async fn get_content_metadata_tree(
        &self,
        namespace: &str,
        extraction_graph_name: &str,
        extraction_policy: &str,
        content_id: &str,
    ) -> Result<indexify_proto::indexify_coordinator::GetContentTreeMetadataResponse> {
        let req = indexify_coordinator::GetContentTreeMetadataRequest {
            namespace: namespace.to_string(),
            extraction_graph_name: extraction_graph_name.to_string(),
            extraction_policy: extraction_policy.to_string(),
            content_id: content_id.to_string(),
        };
        let resp = self.get().await?.get_content_tree_metadata(req).await?;
        Ok(resp.into_inner())
    }

    pub async fn get_metadata_for_ingestion(
        &self,
        task_id: &str,
    ) -> Result<(
        Option<Task>,
        Option<ContentMetadata>,
        Option<ExtractionPolicy>,
    )> {
        let req = tonic::Request::new(
            indexify_proto::indexify_coordinator::GetIngestionInfoRequest {
                task_id: task_id.to_string(),
            },
        );
        let response = self.get().await?.get_ingestion_info(req).await?;
        let resp = response.into_inner();
        Ok((resp.task, resp.root_content, resp.extraction_policy))
    }
}
