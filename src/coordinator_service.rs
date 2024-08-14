use std::{
    collections::HashMap,
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::SystemTime,
};

use anyhow::{anyhow, Result};
use axum::{extract::State, routing::get};
use futures::StreamExt;
use hyper::StatusCode;
use indexify_internal_api::{
    self as internal_api,
    ContentOffset,
    ContentSourceFilter,
    ExtractionGraphLink,
};
use indexify_proto::indexify_coordinator::{
    self,
    coordinator_service_server::CoordinatorService,
    ContentStreamItem,
    CoordinatorCommand,
    CreateContentRequest,
    CreateContentResponse,
    CreateExtractionGraphRequest,
    CreateExtractionGraphResponse,
    CreateGcTasksRequest,
    CreateGcTasksResponse,
    ExecutorsHeartbeatRequest,
    ExecutorsHeartbeatResponse,
    ExtractionGraphLinksRequest,
    ExtractionGraphLinksResponse,
    GcTask,
    GcTaskAcknowledgement,
    GetAllSchemaRequest,
    GetAllSchemaResponse,
    GetAllTaskAssignmentRequest,
    GetContentMetadataRequest,
    GetContentTreeMetadataRequest,
    GetExtractorCoordinatesRequest,
    GetIndexRequest,
    GetIndexResponse,
    GetIngestionInfoRequest,
    GetIngestionInfoResponse,
    GetRaftMetricsSnapshotRequest,
    GetSchemaRequest,
    GetSchemaResponse,
    GetTaskRequest,
    GetTaskResponse,
    HeartbeatRequest,
    HeartbeatResponse,
    ListActiveContentsRequest,
    ListActiveContentsResponse,
    ListContentRequest,
    ListContentResponse,
    ListExtractionGraphRequest,
    ListExtractionGraphResponse,
    ListExtractorsRequest,
    ListExtractorsResponse,
    ListIndexesRequest,
    ListIndexesResponse,
    ListStateChangesRequest,
    ListTasksRequest,
    ListTasksResponse,
    RaftMetricsSnapshotResponse,
    RegisterExecutorRequest,
    RegisterExecutorResponse,
    RegisterIngestionServerRequest,
    RegisterIngestionServerResponse,
    RemoveIngestionServerRequest,
    RemoveIngestionServerResponse,
    TaskAssignments,
    TaskOutcomeFilter,
    TombstoneContentRequest,
    TombstoneContentResponse,
    Uint64List,
    UpdateIndexesStateRequest,
    UpdateIndexesStateResponse,
    UpdateTaskRequest,
    UpdateTaskResponse,
    WaitContentExtractionRequest,
    WaitContentExtractionResponse,
};
use internal_api::{
    ContentSource,
    ExtractionGraph,
    ExtractionGraphBuilder,
    ExtractionPolicyBuilder,
    StateChangeId,
    Task,
};
use itertools::Itertools;
use opentelemetry::{
    global,
    metrics::{Histogram, UpDownCounter},
    propagation::Extractor,
    KeyValue,
};
use prometheus::Encoder;
use tokio::{
    select,
    signal,
    sync::{
        broadcast,
        mpsc,
        watch::{self, Receiver, Sender},
    },
    task::JoinHandle,
    time::{timeout, Duration},
};
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{body::BoxBody, Request, Response, Status, Streaming};
use tower::{Layer, Service, ServiceBuilder};
use tracing::{error, info, warn, Instrument};

use crate::{
    api::{IndexifyAPIError, NewContentStreamStart},
    coordinator::Coordinator,
    coordinator_client::CoordinatorClient,
    coordinator_filters::content_filter,
    garbage_collector::GarbageCollector,
    server_config::ServerConfig,
    state::{self, grpc_config::GrpcConfig},
};

type HBResponseStream = Pin<Box<dyn Stream<Item = Result<HeartbeatResponse, Status>> + Send>>;
type GCTasksResponseStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<CoordinatorCommand, Status>> + Send + Sync>>;

pub struct ExtractionPolicyCreationResult {
    extraction_policies: Vec<internal_api::ExtractionPolicy>,
    extractors: Vec<internal_api::ExtractorDescription>,
}

pub struct CoordinatorServiceServer {
    coordinator: Arc<Coordinator>,
    shutdown_rx: Receiver<()>,
}

const DEFAULT_MAX_PENDING_TASKS: u64 = 20;

struct MetadataMap<'a>(&'a reqwest::header::HeaderMap);

impl<'a> Extractor for MetadataMap<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|metadata| metadata.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|key| key.as_str()).collect::<Vec<_>>()
    }
}

// How often we expect the executor to send us heartbeats.
pub const EXECUTOR_HEARTBEAT_PERIOD: Duration = Duration::new(5, 0);

impl CoordinatorServiceServer {
    fn create_extraction_policies_for_graph(
        &self,
        extraction_graph: &CreateExtractionGraphRequest,
    ) -> Result<ExtractionPolicyCreationResult> {
        let mut name_to_policy_mapping = HashMap::new();
        for extraction_policy in &extraction_graph.policies {
            name_to_policy_mapping
                .insert(extraction_policy.name.clone(), extraction_policy.clone());
        }

        let mut extraction_policies = Vec::new();
        let mut extractors = Vec::new();

        for (_, policy_request) in name_to_policy_mapping.iter() {
            let input_params = serde_json::from_str(&policy_request.input_params)
                .map_err(|e| anyhow!(format!("unable to parse input_params: {}", e)))?;
            let extractor = self.coordinator.get_extractor(&policy_request.extractor)?;
            let content_source = if policy_request.content_source.eq("") {
                internal_api::ContentSource::Ingestion
            } else {
                internal_api::ContentSource::ExtractionPolicyName(
                    policy_request.content_source.clone(),
                )
            };

            let _source = &content_source.to_string();
            if !_source.is_empty() && !name_to_policy_mapping.contains_key(_source) {
                let message = format!(
                    "content source '{_source}' is not found in the graph. \
                    make sure the extraction policy name used as the source is defined."
                );
                return Err(anyhow!(message));
            }

            let expressions: Result<Vec<_>> = policy_request
                .filter
                .iter()
                .map(|e| filter::Expression::from_str(e))
                .collect();

            let policy = ExtractionPolicyBuilder::default()
                .namespace(policy_request.namespace.clone())
                .name(policy_request.name.clone())
                .extractor(policy_request.extractor.clone())
                .filter(filter::LabelsFilter(expressions?))
                .input_params(input_params)
                .content_source(content_source)
                .build(&extraction_graph.name, extractor.clone())
                .map_err(|e| anyhow!(e))?;
            extraction_policies.push(policy.clone());
            extractors.push(extractor.clone());
        }
        Ok(ExtractionPolicyCreationResult {
            extraction_policies,
            extractors,
        })
    }
}

#[tonic::async_trait]
impl CoordinatorService for CoordinatorServiceServer {
    type ContentStreamStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<ContentStreamItem, Status>> + Send + Sync>>;
    type GCTasksStreamStream = GCTasksResponseStream;
    type HeartbeatStream = HBResponseStream;

    async fn delete_extraction_graph(
        &self,
        request: tonic::Request<indexify_coordinator::DeleteExtractionGraphRequest>,
    ) -> Result<tonic::Response<indexify_coordinator::Empty>, tonic::Status> {
        let request = request.into_inner();
        self.coordinator
            .delete_extraction_graph(request.namespace, request.extraction_graph)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(indexify_coordinator::Empty {}))
    }

    async fn content_stream(
        &self,
        request: tonic::Request<indexify_coordinator::ContentStreamRequest>,
    ) -> Result<tonic::Response<Self::ContentStreamStream>, tonic::Status> {
        let request = request.into_inner();
        let start = if request.change_offset == u64::MAX {
            NewContentStreamStart::FromLast
        } else {
            NewContentStreamStart::FromOffset(ContentOffset(request.change_offset))
        };
        let stream = self.coordinator.new_content_stream(start);
        let stream = stream
            .filter(move |item| {
                futures::future::ready(match item {
                    Err(_) => true,
                    Ok(item) => {
                        item.namespace == request.namespace &&
                            item.extraction_graph_names
                                .contains(&request.extraction_graph) &&
                            item.source ==
                                ContentSource::ExtractionPolicyName(
                                    request.extraction_policy.clone(),
                                )
                    }
                })
            })
            .map(|item| match item {
                Err(e) => Err(tonic::Status::aborted(e.to_string())),
                Ok(item) => {
                    let offset = item.change_offset.0;
                    let content: Result<indexify_proto::indexify_coordinator::ContentMetadata> =
                        item.try_into();
                    match content {
                        Ok(content) => Ok(ContentStreamItem {
                            content: Some(content),
                            change_offset: offset,
                        }),
                        Err(e) => Err(tonic::Status::aborted(e.to_string())),
                    }
                }
            });
        Ok(tonic::Response::new(Box::pin(stream)))
    }

    async fn add_graph_to_content(
        &self,
        request: tonic::Request<indexify_coordinator::AddGraphToContentRequest>,
    ) -> Result<tonic::Response<indexify_coordinator::AddGraphToContentResponse>, tonic::Status>
    {
        let request = request.into_inner();
        self.coordinator
            .add_graph_to_content(
                request.namespace,
                request.extraction_graph,
                request.content_ids,
            )
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(
            indexify_coordinator::AddGraphToContentResponse {},
        ))
    }

    async fn extraction_graph_links(
        &self,
        request: tonic::Request<ExtractionGraphLinksRequest>,
    ) -> Result<tonic::Response<ExtractionGraphLinksResponse>, tonic::Status> {
        let request = request.into_inner();
        let links = self
            .coordinator
            .get_extraction_graph_links(&request.namespace, &request.source_graph_name)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(ExtractionGraphLinksResponse { links }))
    }

    async fn list_extraction_graphs(
        &self,
        request: tonic::Request<ListExtractionGraphRequest>,
    ) -> Result<Response<ListExtractionGraphResponse>, tonic::Status> {
        let request = request.into_inner();
        let graphs = self
            .coordinator
            .list_extraction_graphs(&request.namespace)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        let mut proto_graphs = vec![];
        for graph in graphs {
            let proto_graph = graph.try_into().map_err(|e| {
                tonic::Status::aborted(format!("unable to convert extraction graph: {}", e))
            })?;
            proto_graphs.push(proto_graph);
        }
        Ok(Response::new(ListExtractionGraphResponse {
            graphs: proto_graphs,
        }))
    }

    async fn get_extraction_graph_analytics(
        &self,
        request: tonic::Request<indexify_coordinator::GetExtractionGraphAnalyticsRequest>,
    ) -> Result<
        tonic::Response<indexify_coordinator::GetExtractionGraphAnalyticsResponse>,
        tonic::Status,
    > {
        let request = request.into_inner();
        let analytics = self
            .coordinator
            .get_graph_analytics(&request.namespace, &request.extraction_graph)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        let mut proto_analytics = indexify_coordinator::GetExtractionGraphAnalyticsResponse {
            task_analytics: HashMap::new(),
        };
        if let Some(analytics) = analytics {
            for (extraction_policy, task_analytics) in analytics.task_analytics.iter() {
                proto_analytics.task_analytics.insert(
                    extraction_policy.clone(),
                    indexify_coordinator::TaskAnalytics {
                        pending: task_analytics.pending_tasks,
                        success: task_analytics.successful_tasks,
                        failure: task_analytics.failed_tasks,
                    },
                );
            }
        }
        Ok(tonic::Response::new(proto_analytics))
    }

    async fn link_extraction_graphs(
        &self,
        request: tonic::Request<indexify_coordinator::LinkExtractionGraphsRequest>,
    ) -> Result<tonic::Response<indexify_coordinator::LinkExtractionGraphsResponse>, tonic::Status>
    {
        let link: ExtractionGraphLink = request.into_inner().into();
        self.coordinator
            .link_graphs(link)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(
            indexify_coordinator::LinkExtractionGraphsResponse {},
        ))
    }

    async fn executors_heartbeat(
        &self,
        request: tonic::Request<ExecutorsHeartbeatRequest>,
    ) -> Result<tonic::Response<ExecutorsHeartbeatResponse>, tonic::Status> {
        let request = request.into_inner();
        let mut executors = self.coordinator.get_locked_all_executors();
        let now = SystemTime::now();
        for id in request.executors {
            if let Some(last_time) = executors.get_mut(&id) {
                *last_time = now;
            }
        }
        Ok(tonic::Response::new(ExecutorsHeartbeatResponse {}))
    }

    async fn create_content(
        &self,
        request: tonic::Request<CreateContentRequest>,
    ) -> Result<tonic::Response<CreateContentResponse>, tonic::Status> {
        let content_meta = request
            .into_inner()
            .content
            .ok_or(tonic::Status::aborted("content is missing"))?;
        let content_meta: indexify_internal_api::ContentMetadata =
            content_meta.try_into().map_err(|e| {
                tonic::Status::aborted(format!("unable to convert content metadata: {}", e))
            })?;
        let content_list = vec![content_meta];
        let statuses = self
            .coordinator
            .create_content_metadata(content_list)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(CreateContentResponse {
            status: *statuses
                .first()
                .ok_or_else(|| tonic::Status::aborted("result invalid"))?
                as i32,
        }))
    }

    async fn update_labels(
        &self,
        request: tonic::Request<indexify_coordinator::UpdateLabelsRequest>,
    ) -> Result<tonic::Response<indexify_coordinator::UpdateLabelsResponse>, tonic::Status> {
        let request = request.into_inner();
        let labels = internal_api::utils::convert_map_prost_to_serde_json(request.labels)
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        self.coordinator
            .update_labels(&request.namespace, &request.content_id, labels)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(
            indexify_coordinator::UpdateLabelsResponse {},
        ))
    }

    async fn tombstone_content(
        &self,
        request: tonic::Request<TombstoneContentRequest>,
    ) -> Result<tonic::Response<TombstoneContentResponse>, tonic::Status> {
        let req = request.into_inner();
        let content_ids = req.content_ids;
        self.coordinator
            .tombstone_content_metadatas(&content_ids)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(TombstoneContentResponse {}))
    }

    async fn list_content(
        &self,
        request: tonic::Request<ListContentRequest>,
    ) -> Result<tonic::Response<ListContentResponse>, tonic::Status> {
        let req = request.into_inner();
        let expressions: Result<Vec<_>> = req
            .labels_filter
            .iter()
            .map(|e| filter::Expression::from_str(e))
            .collect();
        let labels_filter =
            filter::LabelsFilter(expressions.map_err(|e| tonic::Status::aborted(e.to_string()))?);

        let start_id = if req.start_id.is_empty() {
            None
        } else {
            Some(req.start_id)
        };
        let limit = if req.limit == 0 {
            None
        } else {
            Some(req.limit)
        };
        let source_filter: ContentSourceFilter = req.source.try_into()?;

        let filter = |c: &internal_api::ContentMetadata| {
            c.namespace == req.namespace &&
                (req.graph.is_empty() || c.extraction_graph_names.contains(&req.graph)) &&
                (req.parent_id.is_empty() ||
                    Some(&req.parent_id) == c.parent_id.as_ref().map(|id| &id.id)) &&
                (req.ingested_content_id.is_empty() ||
                    Some(&req.ingested_content_id) == c.root_content_id.as_ref()) &&
                content_filter(c, &source_filter, &labels_filter)
        };
        let response = self
            .coordinator
            .list_content(filter, start_id, limit)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;

        Ok(tonic::Response::new(response.try_into().map_err(|e| {
            tonic::Status::aborted(format!("unable to convert content metadata: {}", e))
        })?))
    }

    async fn list_active_contents(
        &self,
        request: tonic::Request<ListActiveContentsRequest>,
    ) -> Result<tonic::Response<ListActiveContentsResponse>, tonic::Status> {
        let req = request.into_inner();
        let content_ids = self
            .coordinator
            .list_active_contents(&req.namespace)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?
            .into_iter()
            .collect_vec();
        Ok(tonic::Response::new(ListActiveContentsResponse {
            content_ids,
        }))
    }

    async fn create_extraction_graph(
        &self,
        request: tonic::Request<CreateExtractionGraphRequest>,
    ) -> Result<tonic::Response<CreateExtractionGraphResponse>, tonic::Status> {
        let request = request.into_inner();
        let graph_id = ExtractionGraph::create_id(&request.name, &request.namespace);
        let creation_result = self
            .create_extraction_policies_for_graph(&request)
            .map_err(|e| {
                tonic::Status::aborted(format!("unable to create extraction policies: {}", e))
            })?;
        let description = if request.description.is_empty() {
            None
        } else {
            Some(request.description)
        };
        let graph = ExtractionGraphBuilder::default()
            .id(graph_id)
            .namespace(request.namespace.clone())
            .description(description)
            .name(request.name.clone())
            .extraction_policies(creation_result.extraction_policies.clone())
            .build()
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        let indexes = self
            .coordinator
            .create_extraction_graph(graph.clone())
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        let mut policies = HashMap::new();
        for policy in creation_result.extraction_policies {
            let extraction_policy = policy
                .clone()
                .try_into()
                .map_err(|e: anyhow::Error| tonic::Status::aborted(e.to_string()))?;
            policies.insert(policy.name.clone(), extraction_policy);
        }
        let extractors: HashMap<_, _> = creation_result
            .extractors
            .iter()
            .map(|extractor| (extractor.name.clone(), extractor.clone().into()))
            .collect();
        let indexes = indexes
            .into_iter()
            .map(|index| index.into())
            .collect::<Vec<indexify_coordinator::Index>>();
        Ok(tonic::Response::new(CreateExtractionGraphResponse {
            graph_id: graph.id,
            extractors,
            policies,
            indexes,
        }))
    }

    async fn create_ns(
        &self,
        request: tonic::Request<indexify_coordinator::CreateNamespaceRequest>,
    ) -> Result<tonic::Response<indexify_coordinator::CreateNamespaceResponse>, tonic::Status> {
        let request = request.into_inner();
        self.coordinator
            .create_namespace(&request.name)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(
            indexify_coordinator::CreateNamespaceResponse {
                name: request.name,
                created_at: 0,
            },
        ))
    }

    async fn list_ns(
        &self,
        _request: tonic::Request<indexify_coordinator::ListNamespaceRequest>,
    ) -> Result<tonic::Response<indexify_coordinator::ListNamespaceResponse>, tonic::Status> {
        let namespaces = self
            .coordinator
            .list_namespaces()
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(
            indexify_coordinator::ListNamespaceResponse { namespaces },
        ))
    }

    async fn list_extractors(
        &self,
        _request: tonic::Request<ListExtractorsRequest>,
    ) -> Result<tonic::Response<ListExtractorsResponse>, tonic::Status> {
        let extractors = self
            .coordinator
            .list_extractors()
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        let extractors = extractors
            .into_iter()
            .map(|e| e.into())
            .collect::<Vec<indexify_coordinator::Extractor>>();
        Ok(tonic::Response::new(ListExtractorsResponse { extractors }))
    }

    async fn register_executor(
        &self,
        request: tonic::Request<RegisterExecutorRequest>,
    ) -> Result<tonic::Response<RegisterExecutorResponse>, tonic::Status> {
        let request = request.into_inner();

        let extractors = request
            .extractors
            .into_iter()
            .map(|e| e.into())
            .collect::<Vec<internal_api::ExtractorDescription>>();

        let _resp = self
            .coordinator
            .register_executor(&request.addr, &request.executor_id, extractors)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;

        Ok(tonic::Response::new(RegisterExecutorResponse {
            executor_id: request.executor_id,
        }))
    }

    async fn register_ingestion_server(
        &self,
        request: tonic::Request<RegisterIngestionServerRequest>,
    ) -> Result<tonic::Response<RegisterIngestionServerResponse>, tonic::Status> {
        let request = request.into_inner();
        self.coordinator
            .register_ingestion_server(&request.ingestion_server_id)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;

        Ok(tonic::Response::new(RegisterIngestionServerResponse {}))
    }

    async fn remove_ingestion_server(
        &self,
        request: tonic::Request<RemoveIngestionServerRequest>,
    ) -> Result<tonic::Response<RemoveIngestionServerResponse>, tonic::Status> {
        let request = request.into_inner();
        self.coordinator
            .remove_ingestion_server(&request.ingestion_server_id)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;

        Ok(tonic::Response::new(RemoveIngestionServerResponse {}))
    }

    async fn create_gc_tasks(
        &self,
        request: tonic::Request<CreateGcTasksRequest>,
    ) -> Result<tonic::Response<CreateGcTasksResponse>, tonic::Status> {
        let request = request.into_inner();
        let state_change = request.state_change.ok_or_else(|| {
            tonic::Status::aborted("missing state change in create gc tasks request")
        })?;
        let state_change: indexify_internal_api::StateChange =
            state_change.try_into().map_err(|e| {
                tonic::Status::aborted(format!(
                    "unable to convert state change to internal api: {}",
                    e
                ))
            })?;
        self.coordinator
            .create_gc_tasks(state_change)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(CreateGcTasksResponse {}))
    }

    async fn gc_tasks_stream(
        &self,
        request: tonic::Request<Streaming<GcTaskAcknowledgement>>,
    ) -> Result<tonic::Response<Self::GCTasksStreamStream>, Status> {
        let mut gc_task_allocation_event_rx = self.coordinator.subscribe_to_gc_events().await;
        let (tx, rx) = mpsc::channel(100);

        let mut inbound = request.into_inner();
        let coordinator_clone = self.coordinator.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();

        let mut ingestion_server_id: Option<String> = None;

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        info!("Shutdown signal received, terminating gc_tasks_stream.");
                        break;
                    }
                    task_ack = inbound.next() => {
                        match task_ack {
                            Some(Ok(task_ack)) => {
                                //  check for heartbeat
                                if task_ack.task_id.is_empty() {
                                    ingestion_server_id.replace(task_ack.ingestion_server_id);
                                    if let Err(e) = coordinator_clone.register_ingestion_server(ingestion_server_id.as_ref().unwrap()).await {
                                        tracing::error!("Error registering ingestion server: {}", e);
                                    }
                                    continue;
                                }

                                tracing::info!(
                                    "Received gc task acknowledgement {:?}, marking the gc task as complete",
                                    task_ack
                                );
                                if let Err(e) = coordinator_clone
                                .update_gc_task(&task_ack.task_id, task_ack.completed.into())
                                .await
                                {
                                    tracing::error!(
                                        "Error updating GC task with id {}: {}",
                                        task_ack.task_id,
                                        e
                                    );
                                }
                            }
                            Some(Err(e)) => {
                                tracing::error!("Stream error, likely disconnection: {}", e);
                                break;
                            }
                            None => {
                                tracing::info!("GC tasks stream ended, client disconnected.");
                                break;
                            }
                        }
                    }
                    task_allocation_event = gc_task_allocation_event_rx.recv() => {
                        match task_allocation_event {
                            Ok(task_allocation) => {
                                let task = task_allocation;
                                if let Some(ref server_id) = ingestion_server_id {
                                    if task.assigned_to.is_some() && &task.assigned_to.clone().unwrap() == server_id {
                                        let serialized_task: GcTask = task.into();
                                        let command = CoordinatorCommand {
                                            gc_task: Some(serialized_task)
                                        };
                                        tx.send(command).await.unwrap();
                                    }
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                tracing::error!("Skipped {} messages due to lagging", n);
                            }
                            Err(e) => {
                                tracing::error!("Error receiving gc task allocation event: {}", e);
                            }
                        }
                    }
                }
            }

            //  Notify the garbage collector that the ingestion server has disconnected
            if let Some(server_id) = ingestion_server_id {
                if let Err(e) = coordinator_clone.remove_ingestion_server(&server_id).await {
                    tracing::error!("Error removing ingestion server: {}", e);
                }
            }
        });

        let response_stream = ReceiverStream::new(rx).map(Ok);
        Ok(tonic::Response::new(
            Box::pin(response_stream) as Self::GCTasksStreamStream
        ))
    }

    async fn heartbeat(
        &self,
        request: tonic::Request<Streaming<HeartbeatRequest>>,
    ) -> Result<tonic::Response<Self::HeartbeatStream>, tonic::Status> {
        struct Context {
            coordinator: Arc<Coordinator>,
            executor_id: Option<String>,
        }
        let mut max_pending_tasks = DEFAULT_MAX_PENDING_TASKS;

        impl Drop for Context {
            fn drop(&mut self) {
                let coordinator = self.coordinator.clone();
                if let Some(executor_id) = self.executor_id.clone() {
                    tokio::spawn(async move {
                        if let Err(err) = coordinator.remove_executor(&executor_id).await {
                            error!("error removing executor: {}", err);
                        }
                    });
                }
            }
        }

        let mut in_stream = request.into_inner();
        let coordinator = self.coordinator.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        let stream = async_stream::stream! {
            let mut ctx = Context {
                coordinator,
                executor_id: None,
            };
            let mut new_task_channel: Option<broadcast::Receiver<()>> = None;
            loop {
                select! {
                    _ = shutdown_rx.changed() => {
                        info!("shutting down server, stopping heartbeats");
                        break;
                    }
                    result =  timeout(EXECUTOR_HEARTBEAT_PERIOD * 3, in_stream.next()) => {
                        match result {
                            Ok(Some(Ok(hb_request))) => {
                                if ctx.executor_id.is_none() {
                                    ctx.executor_id.replace(hb_request.executor_id.clone());
                                    new_task_channel.replace(ctx.coordinator.subscribe_to_new_tasks(&hb_request.executor_id).await);
                                    if hb_request.max_pending_tasks > 0 {
                                        max_pending_tasks = hb_request.max_pending_tasks;
                                    }
                                    yield ctx.coordinator.heartbeat(&hb_request.executor_id, max_pending_tasks).await.map_err(|e| tonic::Status::aborted(e.to_string()));
                                }
                            }
                            Ok(Some(Err(err))) => {
                                error!("error receiving heartbeat request: {:?}", err);
                                break;
                            }
                            Ok(None) => {
                                info!("heartbeat stream ended, client disconnected");
                                break;
                            }
                            Err(_) => {
                                warn!("executor heartbeat timeout");
                                break;
                            }
                        }
                    }
                    _ = async {
                        if let Some(ref mut channel) = new_task_channel {
                            channel.recv().await
                        } else {
                            futures::future::pending().await
                        }
                    } => {
                        if let Some(ref executor_id) = ctx.executor_id {
                            yield ctx.coordinator.heartbeat(executor_id, max_pending_tasks).await.map_err(|e| tonic::Status::aborted(e.to_string()));
                        }
                    }
                }
            }
        };

        Ok(tonic::Response::new(Box::pin(stream)))
    }

    async fn update_task(
        &self,
        request: tonic::Request<UpdateTaskRequest>,
    ) -> Result<tonic::Response<UpdateTaskResponse>, tonic::Status> {
        let request = request.into_inner();
        let outcome: internal_api::TaskOutcome = request.outcome().into();
        let _ = self
            .coordinator
            .update_task(&request.task_id, &request.executor_id, outcome)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(UpdateTaskResponse {}))
    }

    async fn list_indexes(
        &self,
        request: Request<ListIndexesRequest>,
    ) -> Result<Response<ListIndexesResponse>, Status> {
        let request = request.into_inner();
        let indexes = self
            .coordinator
            .list_indexes(&request.namespace)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        let indexes = indexes
            .into_iter()
            .map(|i| i.into())
            .collect::<Vec<indexify_coordinator::Index>>();
        Ok(tonic::Response::new(ListIndexesResponse { indexes }))
    }

    async fn get_index(
        &self,
        request: Request<GetIndexRequest>,
    ) -> Result<Response<GetIndexResponse>, Status> {
        let request = request.into_inner();
        let index = self
            .coordinator
            .get_index(&request.namespace, &request.name)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(GetIndexResponse {
            index: Some(index.into()),
        }))
    }

    async fn update_indexes_state(
        &self,
        request: Request<UpdateIndexesStateRequest>,
    ) -> Result<Response<UpdateIndexesStateResponse>, Status> {
        let indexes: Vec<internal_api::Index> = request
            .into_inner()
            .indexes
            .into_iter()
            .map(|proto_index| {
                let mut index: internal_api::Index = proto_index.into();
                index.visibility = true;
                index
            })
            .collect();
        self.coordinator
            .update_indexes_state(indexes)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(Response::new(UpdateIndexesStateResponse {}))
    }

    async fn get_extractor_coordinates(
        &self,
        req: Request<GetExtractorCoordinatesRequest>,
    ) -> Result<Response<indexify_coordinator::GetExtractorCoordinatesResponse>, Status> {
        let req = req.into_inner();
        let extractor_coordinates = self
            .coordinator
            .get_extractor_coordinates(&req.extractor)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(Response::new(
            indexify_coordinator::GetExtractorCoordinatesResponse {
                addrs: extractor_coordinates,
            },
        ))
    }

    async fn get_content_metadata(
        &self,
        req: Request<GetContentMetadataRequest>,
    ) -> Result<Response<indexify_coordinator::GetContentMetadataResponse>, Status> {
        let req = req.into_inner();
        let content_metadata = self
            .coordinator
            .get_content_metadata(req.content_list)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(Response::new(
            indexify_coordinator::GetContentMetadataResponse {
                content_list: content_metadata,
            },
        ))
    }

    async fn get_task(
        &self,
        req: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskResponse>, Status> {
        let req = req.into_inner();
        let task = self
            .coordinator
            .get_task(&req.task_id)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(Response::new(GetTaskResponse { task: Some(task) }))
    }

    async fn get_ingestion_info(
        &self,
        req: Request<GetIngestionInfoRequest>,
    ) -> Result<Response<GetIngestionInfoResponse>, Status> {
        let req = req.into_inner();
        let (task, root_content) = self
            .coordinator
            .get_task_and_root_content(&req.task_id)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;

        let root_content: Option<indexify_coordinator::ContentMetadata> =
            if let Some(metadata) = root_content {
                Some(
                    metadata
                        .try_into()
                        .map_err(|e: anyhow::Error| tonic::Status::aborted(e.to_string()))?,
                )
            } else {
                None
            };

        let extraction_policy = self
            .coordinator
            .get_extraction_policy(
                &task.namespace,
                &task.extraction_graph_name,
                &task.extraction_policy_name,
            )
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;

        let proto_extraction_policy: indexify_coordinator::ExtractionPolicy = extraction_policy
            .try_into()
            .map_err(|e: anyhow::Error| tonic::Status::aborted(e.to_string()))?;

        Ok(Response::new(GetIngestionInfoResponse {
            task: Some(
                task.try_into()
                    .map_err(|e: anyhow::Error| tonic::Status::aborted(e.to_string()))?,
            ),
            root_content,
            extraction_policy: Some(proto_extraction_policy),
        }))
    }

    async fn get_content_tree_metadata(
        &self,
        req: Request<GetContentTreeMetadataRequest>,
    ) -> Result<Response<indexify_coordinator::GetContentTreeMetadataResponse>, Status> {
        let req = req.into_inner();
        let content_tree_metadata = self
            .coordinator
            .get_content_tree_metadata(&req.content_id)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(Response::new(
            indexify_coordinator::GetContentTreeMetadataResponse {
                content_list: content_tree_metadata,
            },
        ))
    }

    async fn list_state_changes(
        &self,
        _req: Request<ListStateChangesRequest>,
    ) -> Result<Response<indexify_coordinator::ListStateChangesResponse>, Status> {
        let state_changes = self
            .coordinator
            .list_state_changes()
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?
            .into_iter()
            .map(|c| c.into())
            .collect();
        Ok(Response::new(
            indexify_coordinator::ListStateChangesResponse {
                changes: state_changes,
            },
        ))
    }

    async fn list_tasks(
        &self,
        req: Request<ListTasksRequest>,
    ) -> Result<Response<ListTasksResponse>, Status> {
        let req = req.into_inner();
        let outcome: TaskOutcomeFilter = req.outcome.try_into().map_err(|e| {
            tonic::Status::aborted(format!("unable to convert task outcome filter: {}", e))
        })?;
        let outcome: internal_api::TaskOutcomeFilter = outcome.into();
        let filter = |task: &Task| {
            task.namespace == req.namespace &&
                (req.extraction_graph.is_empty() ||
                    task.extraction_graph_name == req.extraction_graph) &&
                (req.extraction_policy.is_empty() ||
                    task.extraction_policy_name == req.extraction_policy) &&
                (req.content_id.is_empty() || task.content_metadata.id.id == req.content_id) &&
                outcome.matches(task.outcome)
        };
        let response = self
            .coordinator
            .list_tasks(filter, Some(req.start_id), Some(req.limit))
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(Response::new(response))
    }

    async fn get_schema(
        &self,
        req: Request<GetSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        let req = req.into_inner();
        let schema = self
            .coordinator
            .get_schema(&req.namespace, &req.extraction_graph_name)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(Response::new(GetSchemaResponse {
            schema: Some(indexify_coordinator::StructuredDataSchema {
                id: schema.id,
                extraction_graph_name: schema.extraction_graph_name,
                namespace: schema.namespace,
                columns: serde_json::to_string(&schema.columns).unwrap(),
            }),
        }))
    }

    async fn list_schemas(
        &self,
        req: Request<GetAllSchemaRequest>,
    ) -> Result<Response<GetAllSchemaResponse>, Status> {
        let req = req.into_inner();
        let schemas = self
            .coordinator
            .list_schemas(&req.namespace)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;

        Ok(Response::new(GetAllSchemaResponse {
            schemas: schemas
                .into_iter()
                .map(|s| indexify_coordinator::StructuredDataSchema {
                    id: s.id,
                    extraction_graph_name: s.extraction_graph_name,
                    namespace: s.namespace,
                    columns: serde_json::to_string(&s.columns).unwrap(),
                })
                .collect(),
        }))
    }

    async fn get_raft_metrics_snapshot(
        &self,
        _req: Request<GetRaftMetricsSnapshotRequest>,
    ) -> Result<Response<RaftMetricsSnapshotResponse>, Status> {
        let metrics = self.coordinator.get_raft_metrics();
        let metrics_snapshot = metrics.raft_metrics;
        let openraft_metrics = metrics.openraft_metrics;

        // Conversion from MetricsSnapshot to RaftMetricsSnapshotResponse
        let response = RaftMetricsSnapshotResponse {
            fail_connect_to_peer: metrics_snapshot.fail_connect_to_peer,
            sent_bytes: metrics_snapshot.sent_bytes,
            recv_bytes: metrics_snapshot.recv_bytes,
            sent_failures: metrics_snapshot.sent_failures,
            snapshot_send_success: metrics_snapshot.snapshot_send_success,
            snapshot_send_failure: metrics_snapshot.snapshot_send_failure,
            snapshot_recv_success: metrics_snapshot.snapshot_recv_success,
            snapshot_recv_failure: metrics_snapshot.snapshot_recv_failure,
            snapshot_send_inflights: metrics_snapshot.snapshot_send_inflights,
            snapshot_recv_inflights: metrics_snapshot.snapshot_recv_inflights,
            snapshot_sent_seconds: metrics_snapshot
                .snapshot_sent_seconds
                .into_iter()
                .map(|(k, v)| {
                    (
                        k,
                        Uint64List {
                            values: v.into_iter().map(|d| d.as_millis() as u64).collect(),
                        },
                    )
                })
                .collect(),
            snapshot_recv_seconds: metrics_snapshot
                .snapshot_recv_seconds
                .into_iter()
                .map(|(k, v)| {
                    (
                        k,
                        Uint64List {
                            values: v.into_iter().map(|d| d.as_millis() as u64).collect(),
                        },
                    )
                })
                .collect(),
            snapshot_size: metrics_snapshot.snapshot_size,
            last_snapshot_creation_time_millis: metrics_snapshot
                .last_snapshot_creation_time
                .as_millis() as u64,
            running_state_ok: openraft_metrics.running_state.is_ok(),
            id: openraft_metrics.id,
            current_term: openraft_metrics.current_term,
            vote: openraft_metrics.vote.leader_id.node_id,
            last_log_index: openraft_metrics.last_log_index.unwrap_or(0),
            current_leader: openraft_metrics.current_leader.unwrap_or(0),
        };

        Ok(Response::new(response))
    }

    async fn get_all_task_assignments(
        &self,
        _req: Request<GetAllTaskAssignmentRequest>,
    ) -> Result<Response<TaskAssignments>, Status> {
        let assignments = self
            .coordinator
            .all_task_assignments()
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(Response::new(TaskAssignments { assignments }))
    }

    async fn wait_content_extraction(
        &self,
        req: Request<WaitContentExtractionRequest>,
    ) -> Result<Response<WaitContentExtractionResponse>, Status> {
        let req = req.into_inner();
        self.coordinator
            .wait_content_extraction(&req.content_id)
            .await;
        Ok(Response::new(WaitContentExtractionResponse {}))
    }
}

pub struct CoordinatorServer {
    addr: SocketAddr,
    coordinator: Arc<Coordinator>,
    shared_state: Arc<state::App>,
    config: Arc<ServerConfig>,
    server_handle: axum_server::Handle,
}

async fn metrics_handler(
    State(app): State<Arc<state::App>>,
) -> Result<axum::response::Response<axum::body::Body>, IndexifyAPIError> {
    let metric_families = app.registry.gather();
    let mut buffer = vec![];
    let encoder = prometheus::TextEncoder::new();
    encoder.encode(&metric_families, &mut buffer).map_err(|_| {
        IndexifyAPIError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to encode metrics",
        )
    })?;

    Ok(axum::response::Response::new(axum::body::Body::from(
        buffer,
    )))
}

use std::borrow::Cow;

#[derive(Debug, Clone, Default)]
struct TraceLayer {
    name: &'static str,
}

impl<S> Layer<S> for TraceLayer {
    type Service = TraceWrapper<S>;

    fn layer(&self, service: S) -> Self::Service {
        TraceWrapper::new(self.name, service)
    }
}

#[derive(Clone)]
struct Metrics {
    pub req_active: UpDownCounter<i64>,

    pub req_duration: Histogram<f64>,
}

impl Metrics {
    pub fn new(name: impl Into<Cow<'static, str>>) -> Self {
        let meter = opentelemetry::global::meter(name);

        let req_active = meter
            .i64_up_down_counter("grpc.server.req_active")
            .with_description("Number of active requests")
            .init();
        let req_duration = meter
            .f64_histogram("grpc.server.req_duration")
            .with_description("Request duration in seconds")
            .init();
        Self {
            req_active,
            req_duration,
        }
    }
}

#[derive(Clone)]
struct TraceWrapper<S> {
    inner: S,

    metrics: Metrics,
}

impl<S> TraceWrapper<S> {
    pub fn new(name: impl Into<Cow<'static, str>>, inner: S) -> Self {
        Self {
            inner,
            metrics: Metrics::new(name),
        }
    }
}

type BoxFuture<'a, T> = Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

use tracing_opentelemetry::OpenTelemetrySpanExt;

impl<S, ReqBody> Service<tonic::codegen::http::request::Request<ReqBody>> for TraceWrapper<S>
where
    S: Service<
            tonic::codegen::http::request::Request<ReqBody>,
            Response = tonic::codegen::http::response::Response<BoxBody>,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
{
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;
    type Response = S::Response;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: tonic::codegen::http::request::Request<ReqBody>) -> Self::Future {
        let mut inner = self.inner.clone();
        let metrics = self.metrics.clone();

        let remote_ctx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(req.headers())));

        let uri_path = req.uri().path().to_string();
        let span = tracing::span!(tracing::Level::TRACE, "grpc", uri=%uri_path);
        span.set_parent(remote_ctx);

        let future = async move {
            let start = std::time::Instant::now();

            let mut labels = [
                KeyValue::new("grpc.method", uri_path),
                KeyValue::new("grpc.status", 0i64),
            ];

            metrics.req_active.add(1, &[]);

            let out = inner.call(req).await;

            metrics.req_active.add(-1, &[]);

            labels[1].value = if let Ok(response) = &out {
                (response.status().as_u16() as i64).into()
            } else {
                1i64.into()
            };

            metrics
                .req_duration
                .record(start.elapsed().as_secs_f64(), &labels);

            out
        }
        .instrument(span);

        Box::pin(future)
    }
}

fn start_server(app: &CoordinatorServer) -> Result<JoinHandle<Result<()>>> {
    let server = axum::Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(app.shared_state.clone());
    let addr: SocketAddr = format!(
        "{}:{}",
        app.config.listen_if, app.config.coordinator_http_port
    )
    .parse()?;
    let handle = app.server_handle.clone();

    Ok(tokio::spawn(async move {
        axum_server::bind(addr)
            .handle(handle)
            .serve(server.into_make_service())
            .await?;
        Ok(())
    }))
}

impl CoordinatorServer {
    pub async fn new(
        config: Arc<ServerConfig>,
        registry: Arc<prometheus::Registry>,
    ) -> Result<Self, anyhow::Error> {
        let addr: SocketAddr = config.coordinator_lis_addr_sock()?;
        let garbage_collector = GarbageCollector::new();
        let shared_state = state::App::new(
            config.clone(),
            None,
            Arc::clone(&garbage_collector),
            &config.coordinator_addr,
            registry,
        )
        .await?;
        let coordinator_client = CoordinatorClient::new(Arc::clone(&config));

        let coordinator = Coordinator::new(
            shared_state.clone(),
            coordinator_client,
            Arc::clone(&garbage_collector),
        );
        info!("coordinator listening on: {}", addr.to_string());
        Ok(Self {
            addr,
            coordinator,
            shared_state,
            config,
            server_handle: axum_server::Handle::new(),
        })
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let svc = CoordinatorServiceServer {
            coordinator: self.coordinator.clone(),
            shutdown_rx: shutdown_rx.clone(),
        };
        let srvr =
            indexify_coordinator::coordinator_service_server::CoordinatorServiceServer::new(svc)
                .max_decoding_message_size(GrpcConfig::MAX_DECODING_SIZE)
                .max_encoding_message_size(GrpcConfig::MAX_ENCODING_SIZE);

        let shared_state = self.shared_state.clone();
        shared_state
            .initialize_raft()
            .await
            .map_err(|e| anyhow!("unable to initialize shared state: {}", e.to_string()))?;
        let leader_change_watcher = self.coordinator.get_leader_change_watcher();
        let coordinator_clone = self.coordinator.clone();
        let state_watcher_rx = self.coordinator.get_state_watcher();
        if let Err(e) = start_server(self) {
            error!("unable to start metrics server: {}", e);
        }
        let shutdown_rx_clone = shutdown_rx.clone();
        tokio::spawn(async move {
            let _ = run_scheduler(
                shutdown_rx_clone,
                leader_change_watcher,
                state_watcher_rx,
                coordinator_clone,
            )
            .await;
        });

        let heartbeat_coordinator = self.coordinator.clone();
        tokio::spawn(async move {
            heartbeat_coordinator
                .run_executor_heartbeat(shutdown_rx)
                .await;
        });

        let layer = ServiceBuilder::new()
            .layer(TraceLayer {
                name: "indexify-coordinator-grpc",
            })
            .into_inner();

        if let Some(tls_config) = self.config.coordinator_tls.as_ref() {
            if tls_config.api {
                tracing::info!("starting coordinator grpc server with TLS enabled");
                let cert = tokio::fs::read(tls_config.cert_file.clone()).await?;
                let key = tokio::fs::read(tls_config.key_file.clone()).await?;
                let identity = tonic::transport::Identity::from_pem(cert, key);

                let mut tonic_tls_config =
                    tonic::transport::ServerTlsConfig::new().identity(identity);
                if let Some(ca_file) = &tls_config.ca_file {
                    let client_ca_cert = tokio::fs::read(ca_file).await?;
                    let client_ca_cert = tonic::transport::Certificate::from_pem(client_ca_cert);
                    tonic_tls_config = tonic_tls_config.client_ca_root(client_ca_cert);
                }

                tonic::transport::Server::builder()
                    .tls_config(tonic_tls_config)?
                    .layer(layer)
                    .add_service(srvr)
                    .serve_with_shutdown(self.addr, async move {
                        let _ = shutdown_signal(shutdown_tx).await;
                        let res = shared_state.stop().await;
                        if let Err(err) = res {
                            error!("error stopping server: {:?}", err);
                        }
                        self.server_handle.shutdown();
                    })
                    .await
                    .map_err(|e| {
                        anyhow!(
                            "unable to start grpc server: {} addr: {}",
                            e.to_string(),
                            self.addr
                        )
                    })?;
                return Ok(());
            }
        }

        tracing::info!("starting coordinator grpc server with TLS disabled");
        tonic::transport::Server::builder()
            .layer(layer)
            .add_service(srvr)
            .serve_with_shutdown(self.addr, async move {
                let _ = shutdown_signal(shutdown_tx).await;
                let res = shared_state.stop().await;
                if let Err(err) = res {
                    error!("error stopping server: {:?}", err);
                }
                self.server_handle.shutdown();
            })
            .await
            .map_err(|e| {
                anyhow!(
                    "unable to start grpc server: {} addr: {}",
                    e.to_string(),
                    self.addr
                )
            })?;
        Ok(())
    }

    // Used only for tests
    pub fn get_coordinator(&self) -> Arc<Coordinator> {
        self.coordinator.clone()
    }
}

async fn run_scheduler(
    mut shutdown_rx: Receiver<()>,
    mut leader_changed: Receiver<bool>,
    mut state_watcher_rx: Receiver<StateChangeId>,
    coordinator: Arc<Coordinator>,
) -> Result<()> {
    let is_leader = AtomicBool::new(false);

    loop {
        tokio::select! {
            _ = state_watcher_rx.changed() => {
                if is_leader.load(Ordering::Relaxed) {
                   let _state_change = *state_watcher_rx.borrow_and_update();
                   if let Err(err) = coordinator.run_scheduler().await {
                          error!("error processing and distributing work: {:?}", err);
                   }
                }
            },
            _ = shutdown_rx.changed() => {
                info!("scheduler shutting down");
                break;
            }
            _ = leader_changed.changed() => {
                let leader_state = *leader_changed.borrow_and_update();
                info!("leader changed detected: {:?}", leader_state);
                is_leader.store(leader_state, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
    Ok(())
}

#[tracing::instrument]
async fn shutdown_signal(shutdown_tx: Sender<()>) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
        },
        _ = terminate => {
        },
    }
    shutdown_tx.send(()).unwrap();
    info!("signal received, shutting down server gracefully");
}
