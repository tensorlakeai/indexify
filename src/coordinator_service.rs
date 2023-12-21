use std::{net::SocketAddr, sync::Arc, time::SystemTime};

use axum::{
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    routing::{get, post},
    Json,
    Router,
};
use axum_otel_metrics::HttpMetricsLayerBuilder;
use axum_tracing_opentelemetry::middleware::OtelAxumLayer;
use sea_query::Write;
use tokio::signal;
use tracing::{error, info};

use crate::{
    api::IndexifyAPIError,
    attribute_index::AttributeIndexManager,
    coordinator::Coordinator,
    internal_api::{
        CoordinateRequest,
        CoordinateResponse,
        CreateWork,
        CreateWorkResponse,
        ExecutorHeartbeatResponse,
        ExecutorMetadata,
        ExtractorHeartbeat,
        ListExecutors,
        WriteRequest,
        WriteResponse,
    },
    persistence::Repository,
    server_config::ServerConfig,
    state,
    state::{
        network::{api, management, raft},
        SharedState,
    },
    vector_index::VectorIndexManager,
    vectordbs,
};

pub struct CoordinatorServer {
    addr: SocketAddr,
    coordinator: Arc<Coordinator>,
    config: Arc<ServerConfig>,
    shared_state: SharedState,
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
            shared_state.clone(),
        );
        info!("coordinator listening on: {}", addr.to_string());
        Ok(Self {
            addr,
            coordinator,
            config,
            shared_state: shared_state.clone(),
        })
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let metrics = HttpMetricsLayerBuilder::new().build();
        info!("raft node id: {}", &self.config.node_id);
        let app = Router::new()
            .merge(metrics.routes())
            .route("/", get(root))
            .route(
                "/heartbeat",
                post(extractor_heartbeat).with_state(self.coordinator.clone()),
            )
            .route(
                "/write-extracted-data",
                post(write_extracted_data).with_state(self.coordinator.clone()),
            )
            .route(
                "/executors",
                get(list_executors).with_state(self.coordinator.clone()),
            )
            .route(
                "/create_work",
                post(create_work).with_state(self.coordinator.clone()),
            )
            .route(
                "/coordinates",
                post(get_coordinate).with_state(self.coordinator.clone()),
            )
            .route(
                "/raft-append",
                post(raft::append).with_state(self.shared_state.clone()),
            )
            .route(
                "/raft-snapshot",
                post(raft::snapshot).with_state(self.shared_state.clone()),
            )
            .route(
                "/raft-vote",
                post(raft::vote).with_state(self.shared_state.clone()),
            )
            // admin API
            .route(
                "/init",
                post(management::init).with_state(self.shared_state.clone()),
            )
            .route(
                "/add-learner",
                post(management::add_learner).with_state(self.shared_state.clone()),
            )
            .route(
                "/change-membership",
                post(management::change_membership).with_state(self.shared_state.clone()),
            )
            .route(
                "/raft-metrics",
                get(management::metrics).with_state(self.shared_state.clone()),
            )
            // application API
            .route(
                "/write",
                post(api::write).with_state(self.shared_state.clone()),
            )
            .route(
                "/read",
                post(api::read).with_state(self.shared_state.clone()),
            )
            .route(
                "/consistent-read",
                post(api::consistent_read).with_state(self.shared_state.clone()),
            )
            //start OpenTelemetry trace on incoming request
            .layer(OtelAxumLayer::default())
            .layer(metrics)
            .layer(DefaultBodyLimit::disable());
        axum::Server::bind(&self.addr)
            .serve(app.into_make_service())
            .with_graceful_shutdown(shutdown_signal())
            .await?;
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn run_extractors(&self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

async fn root() -> &'static str {
    "Indexify Coordinator"
}

#[tracing::instrument(skip(coordinator))]
#[axum_macros::debug_handler]
async fn list_executors(
    State(coordinator): State<Arc<Coordinator>>,
) -> Result<Json<ListExecutors>, IndexifyAPIError> {
    let executors = coordinator
        .shared_state
        .store
        .state_machine
        .read()
        .await
        .executors
        .values()
        .cloned()
        .collect();
    Ok(Json(ListExecutors { executors }))
}

#[tracing::instrument(level = "debug", skip(coordinator))]
#[tracing::instrument(skip(coordinator, write_request))]
#[axum_macros::debug_handler]
async fn write_extracted_data(
    State(coordinator): State<Arc<Coordinator>>,
    Json(write_request): Json<WriteRequest>,
) -> Result<Json<WriteResponse>, IndexifyAPIError> {
    Ok(Json(WriteResponse {}))
}

#[tracing::instrument(level = "debug", skip(coordinator))]
#[tracing::instrument(skip(coordinator, executor))]
#[axum_macros::debug_handler]
async fn extractor_heartbeat(
    State(coordinator): State<Arc<Coordinator>>,
    Json(executor): Json<ExtractorHeartbeat>,
) -> Result<Json<ExecutorHeartbeatResponse>, IndexifyAPIError> {
    let request = crate::state::store::Request::ExecutorHeartbeat {
        executor: executor.clone(),
    };
    let _response = coordinator
        .shared_state
        .raft
        .client_write(request)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ExecutorHeartbeatResponse {
        content_to_process: vec![],
    }))
}

#[tracing::instrument(skip(coordinator))]
#[axum_macros::debug_handler]
async fn get_coordinate(
    State(coordinator): State<Arc<Coordinator>>,
    Json(query): Json<CoordinateRequest>,
) -> Result<Json<CoordinateResponse>, IndexifyAPIError> {
    let executor = coordinator
        .get_executor(&query.extractor_name)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(CoordinateResponse {
        content: vec![executor.addr],
    }))
}

#[axum_macros::debug_handler]
async fn create_work(
    State(coordinator): State<Arc<Coordinator>>,
    Json(create_work): Json<CreateWork>,
) -> Result<Json<CreateWorkResponse>, IndexifyAPIError> {
    if let Err(err) = coordinator.publish_work(create_work).await {
        error!("unable to send create work request: {}", err.to_string());
    }
    Ok(Json(CreateWorkResponse {}))
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
