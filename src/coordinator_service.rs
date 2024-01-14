use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{anyhow, Result};
use tokio::{
    signal,
    sync::watch::{self, Receiver, Sender},
};
use tonic::{Request, Response, Status};
use tracing::{error, info};

use crate::{
    coordinator::Coordinator,
    indexify_coordinator::{
        self,
        coordinator_service_server::CoordinatorService,
        CreateContentRequest,
        CreateContentResponse,
        CreateIndexRequest,
        CreateIndexResponse,
        CreateRepositoryRequest,
        CreateRepositoryResponse,
        ExtractorBindRequest,
        ExtractorBindResponse,
        GetExtractorCoordinatesRequest,
        GetIndexRequest,
        GetIndexResponse,
        GetRepositoryRequest,
        GetRepositoryResponse,
        HeartbeatRequest,
        HeartbeatResponse,
        ListBindingsRequest,
        ListBindingsResponse,
        ListContentRequest,
        ListContentResponse,
        ListExtractorsRequest,
        ListExtractorsResponse,
        ListIndexesRequest,
        ListIndexesResponse,
        ListRepositoriesRequest,
        ListRepositoriesResponse,
        RegisterExecutorRequest,
        RegisterExecutorResponse,
        UpdateTaskRequest,
        UpdateTaskResponse,
    },
    internal_api,
    server_config::ServerConfig,
    state,
    utils::timestamp_secs,
};

pub struct CoordinatorServiceServer {
    coordinator: Arc<Coordinator>,
}

#[tonic::async_trait]
impl CoordinatorService for CoordinatorServiceServer {
    async fn create_content(
        &self,
        request: tonic::Request<CreateContentRequest>,
    ) -> Result<tonic::Response<CreateContentResponse>, tonic::Status> {
        let id = self
            .coordinator
            .create_content_metadata(request.into_inner())
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(CreateContentResponse { id }))
    }

    async fn list_content(
        &self,
        request: tonic::Request<ListContentRequest>,
    ) -> Result<tonic::Response<ListContentResponse>, tonic::Status> {
        let content_list = self
            .coordinator
            .list_content(&request.into_inner().repository)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(ListContentResponse {
            content_list: content_list
                .into_iter()
                .map(|c| c.into())
                .collect::<Vec<indexify_coordinator::ContentMetadata>>(),
        }))
    }

    async fn create_binding(
        &self,
        request: tonic::Request<ExtractorBindRequest>,
    ) -> Result<tonic::Response<ExtractorBindResponse>, tonic::Status> {
        let request = request.into_inner();
        let extractor_binding = request.binding.clone().unwrap();
        let mut s = DefaultHasher::new();
        request.repository.hash(&mut s);
        request.binding.unwrap().name.hash(&mut s);
        let id = s.finish().to_string();
        let input_params = serde_json::from_str(&extractor_binding.input_params)
            .map_err(|e| tonic::Status::aborted(format!("unable to parse input_params: {}", e)))?;
        let mut filters = HashMap::new();
        for filter in extractor_binding.filters {
            let value = serde_json::from_str(&filter.1).map_err(|e| {
                tonic::Status::aborted(format!("unable to parse filter value: {}", e))
            })?;
            filters.insert(filter.0, value);
        }

        let extractor = self
            .coordinator
            .get_extractor(&extractor_binding.extractor)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        let mut index_name_table_mapping = HashMap::new();
        let mut output_index_name_mapping = HashMap::new();
        for output_name in extractor.outputs.keys() {
            let index_name = format!("{}.{}", extractor_binding.name, output_name);
            let index_table_name = format!(
                "{}.{}.{}",
                request.repository, extractor_binding.name, output_name
            );
            index_name_table_mapping.insert(index_name.clone(), index_table_name.clone());
            output_index_name_mapping.insert(output_name.clone(), index_name.clone());
        }

        let extractor_binding = internal_api::ExtractorBinding {
            id,
            extractor: extractor_binding.extractor,
            name: extractor_binding.name,
            repository: request.repository,
            filters,
            input_params,
            output_index_name_mapping: output_index_name_mapping.clone(),
            index_name_table_mapping: index_name_table_mapping.clone(),
        };
        let _ = self
            .coordinator
            .create_binding(extractor_binding, extractor.clone())
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(ExtractorBindResponse {
            created_at: timestamp_secs() as i64,
            extractor: Some(extractor.into()),
            index_name_table_mapping,
            output_index_name_mapping,
        }))
    }

    async fn list_bindings(
        &self,
        request: tonic::Request<ListBindingsRequest>,
    ) -> Result<tonic::Response<ListBindingsResponse>, tonic::Status> {
        let request = request.into_inner();
        let bindings = self
            .coordinator
            .list_bindings(&request.repository)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        let bindings = bindings
            .into_iter()
            .map(|b| b.into())
            .collect::<Vec<indexify_coordinator::ExtractorBinding>>();

        Ok(tonic::Response::new(ListBindingsResponse { bindings }))
    }

    async fn create_repository(
        &self,
        request: tonic::Request<CreateRepositoryRequest>,
    ) -> Result<tonic::Response<CreateRepositoryResponse>, tonic::Status> {
        let request = request.into_inner();
        self.coordinator
            .create_repository(&request.name)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(CreateRepositoryResponse {
            name: request.name,
            created_at: 0,
        }))
    }

    async fn list_repositories(
        &self,
        _request: tonic::Request<ListRepositoriesRequest>,
    ) -> Result<tonic::Response<ListRepositoriesResponse>, tonic::Status> {
        let repositories = self
            .coordinator
            .list_repositories()
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        let repositories = repositories
            .into_iter()
            .map(|r| r.into())
            .collect::<Vec<indexify_coordinator::Repository>>();
        Ok(tonic::Response::new(ListRepositoriesResponse {
            repositories,
        }))
    }

    async fn get_repository(
        &self,
        request: tonic::Request<GetRepositoryRequest>,
    ) -> Result<tonic::Response<GetRepositoryResponse>, tonic::Status> {
        let repository = request.into_inner().name;
        let repository = self
            .coordinator
            .get_repository(&repository)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;

        Ok(tonic::Response::new(GetRepositoryResponse {
            repository: Some(repository.into()),
        }))
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
        request: tonic::Request<HeartbeatRequest>,
    ) -> Result<tonic::Response<HeartbeatResponse>, tonic::Status> {
        let request = request.into_inner();
        let tasks = self
            .coordinator
            .heartbeat(&request.executor_id)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        let tasks = tasks
            .into_iter()
            .map(|t| t.into())
            .collect::<Vec<indexify_coordinator::Task>>();
        Ok(tonic::Response::new(HeartbeatResponse {
            executor_id: "".to_string(),
            tasks,
        }))
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
            .list_indexes(&request.repository)
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
            .get_index(&request.repository, &request.name)
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
        let repository = index.repository.clone();
        self.coordinator
            .create_index(&repository, index)
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
}

pub struct CoordinatorServer {
    addr: SocketAddr,
    coordinator: Arc<Coordinator>,
    shared_state: Arc<state::App>,
}

impl CoordinatorServer {
    pub async fn new(config: Arc<ServerConfig>) -> Result<Self, anyhow::Error> {
        let addr: SocketAddr = config.coordinator_lis_addr_sock()?;
        let shared_state = state::App::new(config.clone()).await?;

        let coordinator = Coordinator::new(shared_state.clone());
        info!("coordinator listening on: {}", addr.to_string());
        Ok(Self {
            addr,
            coordinator,
            shared_state,
        })
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let svc = CoordinatorServiceServer {
            coordinator: self.coordinator.clone(),
        };
        let srvr =
            indexify_coordinator::coordinator_service_server::CoordinatorServiceServer::new(svc);
        let shared_state = self.shared_state.clone();
        shared_state
            .initialize_raft()
            .await
            .map_err(|e| anyhow!(e.to_string()))?;
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let leader_change_watcher = self.coordinator.get_leader_change_watcher();
        let coordinator_clone = self.coordinator.clone();
        tokio::spawn(async move {
            let _ = run_scheduler(shutdown_rx, leader_change_watcher, coordinator_clone).await;
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
            .await?;
        Ok(())
    }
}

async fn run_scheduler(
    mut shutdown_rx: Receiver<()>,
    mut leader_changed: Receiver<bool>,
    coordinator: Arc<Coordinator>,
) -> Result<()> {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
    let is_leader = AtomicBool::new(false);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                if is_leader.load(Ordering::Relaxed) {
                   if let Err(err) = coordinator.process_and_distribute_work().await {
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
