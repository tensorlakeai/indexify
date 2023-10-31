use axum_otel_metrics::HttpMetricsLayerBuilder;
use axum_tracing_opentelemetry::middleware::OtelAxumLayer;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::signal;
use tracing::{error, info};

use crate::api::IndexifyAPIError;
use crate::attribute_index::AttributeIndexManager;
use crate::internal_api::{
    CreateWork, CreateWorkResponse, EmbedQueryRequest, EmbedQueryResponse, ExecutorInfo,
    SyncExecutor, SyncWorkerResponse,
};
use crate::persistence::Repository;
use crate::server_config::ServerConfig;
use crate::vector_index::VectorIndexManager;
use crate::vectordbs;
use crate::{coordinator::Coordinator, internal_api::ListExecutors};
use axum::{extract::State, http::StatusCode, routing::get, routing::post, Json, Router};

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

        let coordinator =
            Coordinator::new(repository, vector_index_manager, attribute_index_manager);
        info!("coordinator listening on: {}", addr.to_string());
        Ok(Self { addr, coordinator })
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let metrics = HttpMetricsLayerBuilder::new().build();
        let app = Router::new()
            .merge(metrics.routes())
            .route("/", get(root))
            .route(
                "/sync_executor",
                post(sync_executor).with_state(self.coordinator.clone()),
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
                "/embed_query",
                post(embed_query).with_state(self.coordinator.clone()),
            )
            //start OpenTelemetry trace on incoming request
            .layer(OtelAxumLayer::default())
            .layer(metrics);
        axum::Server::bind(&self.addr)
            .serve(app.into_make_service())
            .with_graceful_shutdown(shutdown_signal())
            .await?;
        Ok(())
    }

    pub async fn run_extractors(&self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

async fn root() -> &'static str {
    "Indexify Coordinator"
}

#[tracing::instrument]
#[axum_macros::debug_handler]
async fn list_executors(
    State(coordinator): State<Arc<Coordinator>>,
) -> Result<Json<ListExecutors>, IndexifyAPIError> {
    let executors = coordinator
        .get_executors()
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ListExecutors { executors }))
}

#[tracing::instrument(level = "debug", skip(coordinator))]
#[tracing::instrument(skip(coordinator, executor))]
#[axum_macros::debug_handler]
async fn sync_executor(
    State(coordinator): State<Arc<Coordinator>>,
    Json(executor): Json<SyncExecutor>,
) -> Result<Json<SyncWorkerResponse>, IndexifyAPIError> {
    // Record the health check of the worker
    let worker_id = executor.executor_id.clone();
    let _ = coordinator
        .record_executor(ExecutorInfo {
            id: worker_id.clone(),
            last_seen: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            addr: executor.addr.clone(),
            extractor: executor.extractor.clone(),
        })
        .await;

    coordinator
        .write_extracted_data(executor.work_status)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Record the extractors available on the executor
    coordinator
        .record_extractor(executor.extractor)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Find more work for the worker
    let queued_work = coordinator
        .get_work_for_worker(&executor.executor_id)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Respond
    Ok(Json(SyncWorkerResponse {
        content_to_process: queued_work,
    }))
}

#[tracing::instrument]
#[axum_macros::debug_handler]
async fn embed_query(
    State(coordinator): State<Arc<Coordinator>>,
    Json(query): Json<EmbedQueryRequest>,
) -> Result<Json<EmbedQueryResponse>, IndexifyAPIError> {
    let executor = coordinator
        .get_executor(&query.extractor_name)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let response = reqwest::Client::new()
        .post(&format!("http://{}/embed_query", executor.addr))
        .json(&query)
        .send()
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .json::<EmbedQueryResponse>()
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(EmbedQueryResponse {
        embedding: response.embedding,
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
