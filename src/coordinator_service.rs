use std::{net::SocketAddr, sync::Arc, collections::{hash_map::DefaultHasher},
    hash::{Hash, Hasher}}; 

use serde_json::json;
use tokio::signal;
use tracing::{error, info};

use crate::{
    attribute_index::AttributeIndexManager,
    coordinator::Coordinator,
    indexify_coordinator::{
        self,
        coordinator_service_server::CoordinatorService,
        CreateContentRequest,
        CreateContentResponse,
        CreateRepositoryRequest,
        CreateRepositoryResponse,
        ExtractorBindRequest,
        ExtractorBindResponse,
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
        ListRepositoriesRequest,
        ListRepositoriesResponse,
        RegisterExecutorRequest,
        RegisterExecutorResponse,
        WriteExtractedDataRequest,
        WriteExtractedDataResponse,
    },
    persistence::Repository,
    server_config::ServerConfig,
    state,
    vector_index::VectorIndexManager,
    vectordbs, internal_api,
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

        let extractor = internal_api::ExtractorBinding{
            id,
            extractor: extractor_binding.extractor,
            name: extractor_binding.name,
            repository: request.repository,
            filters: vec![],
            input_params: json!({}),
        };
        let indexes = self.coordinator
            .create_binding(extractor)
            .await.map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(ExtractorBindResponse {
            index_names: indexes,
            created_at: 0,
        }))
    }

    async fn list_bindings(
        &self,
        request: tonic::Request<ListBindingsRequest>,
    ) -> Result<tonic::Response<ListBindingsResponse>, tonic::Status> {
        let request = request.into_inner();
        let bindings = self.coordinator.list_bindings(&request.repository).await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        let bindings = bindings.into_iter().map(|b| b.into()).collect::<Vec<indexify_coordinator::ExtractorBinding>>();

        Ok(tonic::Response::new(ListBindingsResponse {
            bindings: bindings,
        }))
    }

    async fn create_repository(
        &self,
        request: tonic::Request<CreateRepositoryRequest>,
    ) -> Result<tonic::Response<CreateRepositoryResponse>, tonic::Status> {
        let request = request.into_inner();
        self.coordinator.create_repository(&request.name).await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        Ok(tonic::Response::new(CreateRepositoryResponse {
            name: request.name,
            created_at: 0,
        }))
    }

    async fn list_repositories(
        &self,
        _request : tonic::Request<ListRepositoriesRequest>,
    ) -> Result<tonic::Response<ListRepositoriesResponse>, tonic::Status> {
        let repositories = self.coordinator.list_repositories().await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        let repositories = repositories.into_iter().map(|r| r.into()).collect::<Vec<indexify_coordinator::Repository>>();
        Ok(tonic::Response::new(ListRepositoriesResponse {
            repositories: repositories,
        }))
    }

    async fn get_repository(
        &self,
        request: tonic::Request<GetRepositoryRequest>,
    ) -> Result<tonic::Response<GetRepositoryResponse>, tonic::Status> {
        let repository = request.into_inner().name;
        let repository = self.coordinator.get_repository(&repository).await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;

        Ok(tonic::Response::new(GetRepositoryResponse {
            repository: Some(repository.into()),
        }))
    }

    async fn list_extractors(
        &self,
        _request: tonic::Request<ListExtractorsRequest>,
    ) -> Result<tonic::Response<ListExtractorsResponse>, tonic::Status> {
        let extractors = self.coordinator.list_extractors().await
            .map_err(|e| tonic::Status::aborted(e.to_string()))?;
        let extractors = extractors.into_iter().map(|e| e.into()).collect::<Vec<indexify_coordinator::Extractor>>();
        Ok(tonic::Response::new(ListExtractorsResponse {
            extractors: extractors,
        }))
    }

    async fn register_executor(
        &self,
        request: tonic::Request<RegisterExecutorRequest>,
    ) -> Result<tonic::Response<RegisterExecutorResponse>, tonic::Status> {
        Ok(tonic::Response::new(RegisterExecutorResponse {
            executor_id: "12121".to_string(),
        }))
    }

    async fn heartbeat(
        &self,
        request: tonic::Request<HeartbeatRequest>,
    ) -> Result<tonic::Response<HeartbeatResponse>, tonic::Status> {
        Ok(tonic::Response::new(HeartbeatResponse {
            executor_id: "".to_string(),
            tasks: vec![],
        }))
    }

    async fn write_extracted_data(
        &self,
        request: tonic::Request<WriteExtractedDataRequest>,
    ) -> Result<tonic::Response<WriteExtractedDataResponse>, tonic::Status> {
        Ok(tonic::Response::new(WriteExtractedDataResponse {}))
    }
}

pub struct CoordinatorServer {
    addr: SocketAddr,
    coordinator: Arc<Coordinator>,
}

impl CoordinatorServer {
    pub async fn new(config: Arc<ServerConfig>) -> Result<Self, anyhow::Error> {
        let addr: SocketAddr = config.coordinator_lis_addr_sock()?;
        let repository = Arc::new(Repository::new(&config.db_url).await?);
        let vector_db = vectordbs::create_vectordb(
            config.index_config.clone(),
            repository.get_db_conn_clone(),
        )?;
        let vector_index_manager = Arc::new(VectorIndexManager::new(
            repository.clone(),
            vector_db,
            config.coordinator_lis_addr_sock().unwrap().to_string(),
        ));
        let attribute_index_manager = Arc::new(AttributeIndexManager::new(repository.clone()));
        let shared_state = state::App::new(config.clone()).await?;

        let coordinator = Coordinator::new(
            repository,
            vector_index_manager,
            attribute_index_manager,
            shared_state,
        );
        info!("coordinator listening on: {}", addr.to_string());
        Ok(Self { addr, coordinator })
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let svc = CoordinatorServiceServer {
            coordinator: self.coordinator.clone(),
        };
        let srvr =
            indexify_coordinator::coordinator_service_server::CoordinatorServiceServer::new(svc);
        tonic::transport::Server::builder()
            .add_service(srvr)
            .serve(self.addr)
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
