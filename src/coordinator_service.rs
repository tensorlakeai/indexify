use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{anyhow, Result};
use indexify_internal_api as internal_api;
use indexify_proto::indexify_coordinator::{
    self,
    coordinator_service_server::CoordinatorService,
    CreateContentRequest,
    CreateContentResponse,
    CreateIndexRequest,
    CreateIndexResponse,
    ExtractionPolicyRequest,
    ExtractionPolicyResponse,
    GetAllSchemaRequest,
    GetAllSchemaResponse,
    GetContentMetadataRequest,
    GetExtractorCoordinatesRequest,
    GetIndexRequest,
    GetIndexResponse,
    GetRaftMetricsSnapshotRequest,
    GetSchemaRequest,
    GetSchemaResponse,
    HeartbeatRequest,
    HeartbeatResponse,
    ListContentRequest,
    ListContentResponse,
    ListExtractionPoliciesRequest,
    ListExtractionPoliciesResponse,
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
    Uint64List,
    UpdateTaskRequest,
    UpdateTaskResponse,
};
use internal_api::StateChange;
use itertools::Itertools;
use tokio::{
    select,
    signal,
    sync::{
        mpsc,
        watch::{self, Receiver, Sender},
    },
};
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info};

use crate::{
    coordinator::Coordinator,
    server_config::ServerConfig,
    state::{self},
    tonic_streamer::DropReceiver,
    utils::timestamp_secs,
};

type HBResponseStream = Pin<Box<dyn Stream<Item = Result<HeartbeatResponse, Status>> + Send>>;

pub struct CoordinatorServiceServer {
    coordinator: Arc<Coordinator>,
    shutdown_rx: Receiver<()>,
}

#[tonic::async_trait]
impl CoordinatorService for CoordinatorServiceServer {
    type HeartbeatStream = HBResponseStream;

    async fn create_content(
        &self,
        request: tonic::Request<CreateContentRequest>,
    ) -> Result<tonic::Response<CreateContentResponse>, tonic::Status> {
        let content_meta = request
            .into_inner()
            .content
            .ok_or(tonic::Status::aborted("content is missing"))?;
        let id = content_meta.id.clone();
        let content_list = vec![content_meta];
        let _ = self
            .coordinator
            .create_content_metadata(content_list)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(CreateContentResponse { id }))
    }

    async fn list_content(
        &self,
        request: tonic::Request<ListContentRequest>,
    ) -> Result<tonic::Response<ListContentResponse>, tonic::Status> {
        let req = request.into_inner();
        let content_list = self
            .coordinator
            .list_content(&req.namespace, &req.source, &req.parent_id, &req.labels_eq)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(ListContentResponse {
            content_list: content_list
                .into_iter()
                .map(|c| c.into())
                .collect::<Vec<indexify_coordinator::ContentMetadata>>(),
        }))
    }

    async fn create_extraction_policy(
        &self,
        request: tonic::Request<ExtractionPolicyRequest>,
    ) -> Result<tonic::Response<ExtractionPolicyResponse>, tonic::Status> {
        let request = request.into_inner();
        let extraction_policy = request.policy.clone().unwrap();
        let mut s = DefaultHasher::new();
        request.namespace.hash(&mut s);
        request.policy.unwrap().name.hash(&mut s);
        let id = s.finish().to_string();
        let input_params = serde_json::from_str(&extraction_policy.input_params)
            .map_err(|e| tonic::Status::aborted(format!("unable to parse input_params: {}", e)))?;

        let extractor = self
            .coordinator
            .get_extractor(&extraction_policy.extractor)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        let mut index_name_table_mapping = HashMap::new();
        let mut output_index_name_mapping = HashMap::new();
        for output_name in extractor.outputs.keys() {
            let index_name = format!("{}.{}", extraction_policy.name, output_name);
            let index_table_name = format!(
                "{}.{}.{}",
                request.namespace, extraction_policy.name, output_name
            );
            index_name_table_mapping.insert(index_name.clone(), index_table_name.clone());
            output_index_name_mapping.insert(output_name.clone(), index_name.clone());
        }

        let extraction_policy = internal_api::ExtractionPolicy {
            id,
            extractor: extraction_policy.extractor,
            name: extraction_policy.name,
            namespace: request.namespace,
            filters: extraction_policy.filters,
            input_params,
            output_index_name_mapping: output_index_name_mapping.clone(),
            index_name_table_mapping: index_name_table_mapping.clone(),
            content_source: extraction_policy.content_source,
        };
        let _ = self
            .coordinator
            .create_policy(extraction_policy, extractor.clone())
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(ExtractionPolicyResponse {
            created_at: timestamp_secs() as i64,
            extractor: Some(extractor.into()),
            index_name_table_mapping,
            output_index_name_mapping,
        }))
    }

    async fn list_extraction_policies(
        &self,
        request: tonic::Request<ListExtractionPoliciesRequest>,
    ) -> Result<tonic::Response<ListExtractionPoliciesResponse>, tonic::Status> {
        let request = request.into_inner();
        let extraction_policies = self
            .coordinator
            .list_policies(&request.namespace)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        let policies = extraction_policies
            .into_iter()
            .map(|b| b.into())
            .collect::<Vec<indexify_coordinator::ExtractionPolicy>>();

        Ok(tonic::Response::new(ListExtractionPoliciesResponse {
            policies,
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
        let namespaces = namespaces
            .into_iter()
            .map(|r| r.into())
            .collect::<Vec<indexify_coordinator::Namespace>>();
        Ok(tonic::Response::new(
            indexify_coordinator::ListNamespaceResponse { namespaces },
        ))
    }

    async fn get_ns(
        &self,
        request: tonic::Request<indexify_coordinator::GetNamespaceRequest>,
    ) -> Result<tonic::Response<indexify_coordinator::GetNamespaceResponse>, tonic::Status> {
        let namespace = request.into_inner().name;
        let namespace = self
            .coordinator
            .get_namespace(&namespace)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;

        Ok(tonic::Response::new(
            indexify_coordinator::GetNamespaceResponse {
                namespace: namespace.map(|n| n.into()),
            },
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
        let extractor = request
            .extractor
            .ok_or(tonic::Status::aborted("missing extractor"))?;
        let _resp = self
            .coordinator
            .register_executor(&request.addr, &request.executor_id, extractor.into())
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(RegisterExecutorResponse {
            executor_id: request.executor_id,
        }))
    }

    async fn heartbeat(
        &self,
        request: tonic::Request<Streaming<HeartbeatRequest>>,
    ) -> Result<tonic::Response<Self::HeartbeatStream>, tonic::Status> {
        let mut in_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
        let rx = DropReceiver { inner: rx };
        let coordinator = self.coordinator.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        tokio::spawn(async move {
            let mut executor_id = String::new();
            loop {
                select! {
                    _ = shutdown_rx.changed() => {
                        info!("shutting down server, stopping heartbeats from executor: {}", executor_id);
                        break;
                    }
                    frame = in_stream.next() => {
                        // Ensure the frame has something
                        if frame.as_ref().is_none() {
                            break;
                        }
                        if let Err(err) = frame.as_ref().unwrap() {
                            info!("error receiving heartbeat request: {:?}", err);
                            break;
                        }
                        // We could have used Option<> here but it would be inconvenient to dereference
                        // it every time we need to use it below
                        if executor_id.is_empty() {
                            executor_id = frame.unwrap().unwrap().executor_id;
                        }
                        let tasks = coordinator.heartbeat(&executor_id).await;
                            if let Err(err) = &tasks {
                                if let Err(err) =
                                    tx.send(Err(tonic::Status::internal(err.to_string()))).await
                                {
                                    error!(
                                        "error sending error message in heartbeat response: {}",
                                        err
                                    );
                                    break;
                                }
                                continue;
                            }
                            let tasks = tasks
                                .unwrap()
                                .into_iter()
                                .map(|t| t.into())
                                .collect::<Vec<indexify_coordinator::Task>>();
                            let resp = HeartbeatResponse {
                                executor_id: executor_id.clone(),
                                tasks,
                            };
                            if let Err(err) = tx.send(Ok(resp)).await {
                                error!("error sending heartbeat response: {:?}", err);
                                break;
                            }
                    }
                }
            }
            info!("heartbeats stopped, removing executor: {}", executor_id);
            if let Err(err) = coordinator.remove_executor(&executor_id).await {
                error!("error removing executor: {}", err);
            }
        });
        Ok(tonic::Response::new(Box::pin(rx) as HBResponseStream))
    }

    async fn update_task(
        &self,
        request: tonic::Request<UpdateTaskRequest>,
    ) -> Result<tonic::Response<UpdateTaskResponse>, tonic::Status> {
        let request = request.into_inner();
        let outcome: internal_api::TaskOutcome = request.outcome().into();
        let _ = self
            .coordinator
            .update_task(
                &request.task_id,
                &request.executor_id,
                outcome,
                request.content_list,
            )
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

    async fn create_index(
        &self,
        request: Request<CreateIndexRequest>,
    ) -> Result<Response<CreateIndexResponse>, Status> {
        let request = request.into_inner();
        let index: internal_api::Index = request.index.unwrap().into();
        let namespace = index.namespace.clone();
        self.coordinator
            .create_index(&namespace, index)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(CreateIndexResponse {}))
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
        let content_metadata_list = self
            .coordinator
            .get_content_metadata(req.content_list)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        let content_metadata = content_metadata_list
            .iter()
            .map(|c| c.clone().into())
            .collect_vec();
        Ok(Response::new(
            indexify_coordinator::GetContentMetadataResponse {
                content_list: content_metadata,
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
        let extraction_policy = if req.extraction_policy.is_empty() {
            None
        } else {
            Some(req.extraction_policy)
        };
        let tasks = self
            .coordinator
            .list_tasks(&req.namespace, extraction_policy)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        let tasks = tasks.into_iter().map(|t| t.into()).collect();
        Ok(Response::new(indexify_coordinator::ListTasksResponse {
            tasks,
        }))
    }

    async fn get_schema(
        &self,
        req: Request<GetSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        let req = req.into_inner();
        let schema = self
            .coordinator
            .get_schema(&req.namespace, &req.content_source)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(Response::new(GetSchemaResponse {
            schema: Some(indexify_coordinator::StructuredDataSchema {
                columns: serde_json::to_string(&schema.columns).unwrap(),
                content_source: schema.content_source,
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
                    columns: serde_json::to_string(&s.columns).unwrap(),
                    content_source: s.content_source,
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
}

pub struct CoordinatorServer {
    addr: SocketAddr,
    coordinator: Arc<Coordinator>,
    shared_state: Arc<state::App>,
}

impl CoordinatorServer {
    pub async fn new(config: Arc<ServerConfig>) -> Result<Self, anyhow::Error> {
        let addr: SocketAddr = config.coordinator_lis_addr_sock()?;
        let shared_state = state::App::new(config.clone(), None).await?;

        let coordinator = Coordinator::new(shared_state.clone());
        info!("coordinator listening on: {}", addr.to_string());
        Ok(Self {
            addr,
            coordinator,
            shared_state,
        })
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let svc = CoordinatorServiceServer {
            coordinator: self.coordinator.clone(),
            shutdown_rx: shutdown_rx.clone(),
        };
        let srvr =
            indexify_coordinator::coordinator_service_server::CoordinatorServiceServer::new(svc);
        let shared_state = self.shared_state.clone();
        shared_state
            .initialize_raft()
            .await
            .map_err(|e| anyhow!("unable to initialize shared state: {}", e.to_string()))?;
        let leader_change_watcher = self.coordinator.get_leader_change_watcher();
        let coordinator_clone = self.coordinator.clone();
        let state_watcher_rx = self.coordinator.get_state_watcher();
        tokio::spawn(async move {
            let _ = run_scheduler(
                shutdown_rx,
                leader_change_watcher,
                state_watcher_rx,
                coordinator_clone,
            )
            .await;
        });
        tonic::transport::Server::builder()
            .add_service(srvr)
            .serve_with_shutdown(self.addr, async move {
                let _ = shutdown_signal(shutdown_tx).await;
                let res = shared_state.stop().await;
                if let Err(err) = res {
                    error!("error stopping server: {:?}", err);
                }
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
}

async fn run_scheduler(
    mut shutdown_rx: Receiver<()>,
    mut leader_changed: Receiver<bool>,
    mut state_watcher_rx: Receiver<StateChange>,
    coordinator: Arc<Coordinator>,
) -> Result<()> {
    let is_leader = AtomicBool::new(false);

    loop {
        tokio::select! {
            _ = state_watcher_rx.changed() => {
                if is_leader.load(Ordering::Relaxed) {
                    let _state_change = state_watcher_rx.borrow_and_update().clone();
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
