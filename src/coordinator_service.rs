use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    net::SocketAddr,
    sync::Arc,
};

use anyhow::anyhow;
use tokio::signal;
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
    },
    internal_api,
    server_config::ServerConfig,
    state,
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
        let input_params = serde_json::from_str(&extractor_binding.input_params).map_err(|e| {
            tonic::Status::aborted(format!("unable to parse input_params: {}", e.to_string()))
        })?;
        let mut filters = HashMap::new();
        for filter in extractor_binding.filters {
            let value = serde_json::from_str(&filter.1).map_err(|e| {
                tonic::Status::aborted(format!("unable to parse filter value: {}", e.to_string()))
            })?;
            filters.insert(filter.0, value);
        }

        let extractor = internal_api::ExtractorBinding {
            id,
            extractor: extractor_binding.extractor,
            name: extractor_binding.name,
            repository: request.repository,
            filters,
            input_params,
        };
        let extractor = self
            .coordinator
            .create_binding(extractor)
            .await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(ExtractorBindResponse {
            created_at: 0,
            extractor: Some(extractor.into()),
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
        // let indexes = indexes.into_iter().map(|i|
        // i.into()).collect::<Vec<indexify_coordinator::Index>>();
        let indexes = vec![];
        Ok(tonic::Response::new(ListIndexesResponse { indexes }))
    }

    async fn get_index(
        &self,
        request: Request<GetIndexRequest>,
    ) -> Result<Response<GetIndexResponse>, Status> {
        Err(tonic::Status::unimplemented("not implemented"))
    }

    async fn create_index(
        &self,
        request: Request<CreateIndexRequest>,
    ) -> Result<Response<CreateIndexResponse>, Status> {
        Err(tonic::Status::unimplemented("not implemented"))
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
        let _ = shared_state
            .initialize_raft()
            .await
            .map_err(|e| anyhow!(e.to_string()))?;
        tonic::transport::Server::builder()
            .add_service(srvr)
            .serve_with_shutdown(self.addr, async move {
                let _ = shutdown_signal().await;
                let res = shared_state.stop().await;
                if let Err(err) = res {
                    error!("error stopping server: {:?}", err);
                }
            })
            .await?;
        Ok(())
    }
}

#[tracing::instrument]
async fn shutdown_signal() {
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
    info!("signal received, shutting down server gracefully");
}
