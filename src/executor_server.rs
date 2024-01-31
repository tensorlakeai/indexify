use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use anyhow::Result;
use axum::{
    extract::{DefaultBodyLimit, State},
    routing::{get, post},
    Json,
    Router,
};
use axum_otel_metrics::HttpMetricsLayerBuilder;
use axum_tracing_opentelemetry::middleware::OtelAxumLayer;
use indexify_internal_api as internal_api;
use indexify_proto::indexify_coordinator::HeartbeatRequest;
use tokio::{
    signal,
    sync::{watch, watch::Receiver},
    time::interval,
};
use tracing::{error, info};

use crate::{
    api::{IndexifyAPIError, WriteExtractedContent},
    coordinator_client::CoordinatorClient,
    executor::{heartbeat, ExtractorExecutor},
    extractor::{extractor_runner, py_extractors, python_path},
    server_config::ExecutorConfig,
    task_store::TaskStore,
};

pub struct ExecutorServer {
    executor_config: Arc<ExecutorConfig>,
    coordinator_client: Arc<CoordinatorClient>,
}

#[derive(Debug)]
pub struct ApiEndpointState {
    executor: Arc<ExtractorExecutor>,
    coordinator_client: Arc<CoordinatorClient>,
}

impl ExecutorServer {
    pub async fn new(executor_config: Arc<ExecutorConfig>) -> Result<Self> {
        let coordinator_client =
            Arc::new(CoordinatorClient::new(&executor_config.coordinator_addr));

        Ok(Self {
            executor_config,
            coordinator_client,
        })
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let addr: SocketAddr = self.executor_config.listen_addr_sock()?;
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let listen_addr = listener.local_addr()?.to_string();
        let listen_port = listener.local_addr()?.port();
        let advertise_addr = format!("{}:{}", self.executor_config.advertise_if, listen_port);
        python_path::set_python_path(&self.executor_config.extractor_path)?;
        let extractor = py_extractors::PythonExtractor::new_from_extractor_path(
            &self.executor_config.extractor_path,
        )?;
        let extractor_runner = extractor_runner::ExtractorRunner::new(Arc::new(extractor));
        let task_store = Arc::new(TaskStore::new());
        let executor = Arc::new(
            ExtractorExecutor::new(
                self.executor_config.clone(),
                extractor_runner,
                advertise_addr.clone(),
                task_store.clone(),
            )
            .await?,
        );
        let endpoint_state = Arc::new(ApiEndpointState {
            executor: executor.clone(),
            coordinator_client: self.coordinator_client.clone(),
        });
        let metrics = HttpMetricsLayerBuilder::new().build();
        let app = Router::new()
            .merge(metrics.routes())
            .route("/", get(root))
            .route(
                "/sync_executor",
                post(sync_worker).with_state(endpoint_state.clone()),
            )
            .route("/extract", post(extract).with_state(endpoint_state.clone()))
            //start OpenTelemetry trace on incoming request
            .layer(OtelAxumLayer::default())
            .layer(metrics)
            .layer(DefaultBodyLimit::disable());

        info!(
            "starting executor server on: {}, advertising: {}, server id: {} ",
            listen_addr,
            advertise_addr.clone(),
            executor.executor_id
        );
        let (shutdown_tx, shutdown_rx) = watch::channel::<()>(());
        let (heartbeat_tx, heartbeat_rx) = watch::channel::<HeartbeatRequest>(HeartbeatRequest {
            executor_id: executor.executor_id.clone(),
        });
        run_extractors(
            task_store.clone(),
            executor.clone(),
            self.executor_config.ingestion_api_addr.clone(),
            shutdown_rx.clone(),
        );
        let coordinator_client = self.coordinator_client.clone();
        let coordinator_addr = self.executor_config.coordinator_addr.clone();
        tokio::spawn(async move {
            let mut shutdown_rx = shutdown_rx.clone();
            let mut int = interval(Duration::from_secs(5));
            int.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let has_registered = Arc::new(AtomicBool::new(false));
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        info!("shutting down executor server");
                        break;
                    },
                    _ = int.tick() => {
                        if !has_registered.load(std::sync::atomic::Ordering::Relaxed) {
                            info!("registering executor with coordinator at address {}", coordinator_addr);
                            if let Err(err) = executor.register(coordinator_client.clone()).await {
                                error!("unable to register : {}", err.to_string());
                                continue;
                            }
                            has_registered.store(true, std::sync::atomic::Ordering::SeqCst);
                            let task_store = task_store.clone();
                            let coordinator_client = coordinator_client.clone();
                            let heartbeat_rx = heartbeat_rx.clone();
                            let has_registered = has_registered.clone();
                            tokio::spawn(async move {
                                if let Err(err) = heartbeat(task_store, coordinator_client, heartbeat_rx.clone()).await {
                                    error!("unable to send heartbeat: {}", err.to_string());
                                }
                                has_registered.store(false, std::sync::atomic::Ordering::SeqCst);
                            });
                        }
                        if heartbeat_tx.send(HeartbeatRequest{executor_id: executor.executor_id.clone()}).is_err(){
                            error!("heartbeat channel closed, so we will try registering again");
                            has_registered.store(false, std::sync::atomic::Ordering::SeqCst);
                            }
                        }
                };
            }
        });
        axum::serve(listener, app.into_make_service())
            .with_graceful_shutdown(async move {
                let _ = shutdown_signal().await;
                shutdown_tx.send(()).unwrap()
            })
            .await?;
        Ok(())
    }
}

#[tracing::instrument]
async fn root() -> &'static str {
    "Indexify Extractor Server"
}

#[tracing::instrument]
#[axum::debug_handler]
async fn extract(
    endpoint_state: State<Arc<ApiEndpointState>>,
    Json(query): Json<internal_api::ExtractRequest>,
) -> Result<Json<internal_api::ExtractResponse>, IndexifyAPIError> {
    let content = endpoint_state
        .executor
        .extract(query.content, query.input_params)
        .await;

    match content {
        Ok(content) => Ok(Json(internal_api::ExtractResponse { content })),
        Err(err) => {
            error!("unable to extract content: {}", err.to_string());
            Err(IndexifyAPIError::new(
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                err.to_string(),
            ))
        }
    }
}

fn run_extractors(
    task_store: Arc<TaskStore>,
    executor: Arc<ExtractorExecutor>,
    ingestion_addr: String,
    mut shutdown_rx: Receiver<()>,
) {
    let mut watch_rx = task_store.get_watcher().clone();
    let ingestion_api = format!("http://{}/write_content", ingestion_addr);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    info!("shutting down executor server");
                    break;
                },
                _ = watch_rx.changed() => {
                    info!("pulling work from task store");
                    if let Err(err) = executor.execute_pending_tasks().await {
                        error!("unable to execute pending tasks: {}", err.to_string());
                    }
                    let task_results = task_store.finished_tasks();
                    if task_results.is_empty() {
                        continue;
                    }

                    // write extracted data
                    for task_result in task_results {
                        let task = task_store.get_task(&task_result.task_id);
                        if task.is_none() {
                            error!("unable to find task: {}", task_result.task_id);
                            continue;
                        }
                        let task = task.unwrap();
                        let content_by_index = split_content_list_by_index_names(task_result.extracted_content.clone(), task.output_index_table_mapping.clone());
                        for (index_name, content_list) in content_by_index {
                            let req = WriteExtractedContent{
                                parent_content_id: task.content_metadata.id.clone(),
                                task_id: task.id.clone(),
                                repository: task.repository.clone(),
                                content_list: content_list.clone(),
                                index_table_name: Some(index_name.clone()),
                                executor_id: executor.executor_id.clone(),
                                task_outcome: task_result.outcome.clone(),
                                extractor_binding: task.extractor_binding.clone(),
                            };
                            let write_result = reqwest::Client::new()
                            .post(&ingestion_api)
                            .json(&req)
                            .send()
                            .await;
                            if let Err(err) = write_result {
                                error!("unable to write extracted content: {}", err.to_string());
                                continue;
                            }
                            if let Err(err) = write_result {
                                error!("unable to write extracted content: {}", err.to_string());
                                continue;
                            }
                            task_store.clear_completed_task(&task.id);
                        }
                    }
                }
            };
        }
    });
}

#[axum::debug_handler]
async fn sync_worker(endpoint_state: State<Arc<ApiEndpointState>>) -> Result<(), IndexifyAPIError> {
    endpoint_state
        .executor
        .register(endpoint_state.coordinator_client.clone())
        .await
        .map_err(|e| {
            IndexifyAPIError::new(axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;
    Ok(())
}

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

fn split_content_list_by_index_names(
    content_list: Vec<internal_api::Content>,
    index_mapping: HashMap<String, String>,
) -> HashMap<String, Vec<internal_api::Content>> {
    let mut content_map: HashMap<String, Vec<internal_api::Content>> = HashMap::new();
    for content in &content_list {
        if content.features.is_empty() {
            content_map
                .entry("".to_string())
                .or_default()
                .push(content.clone());
            continue;
        }
        for feature in &content.features {
            let index_name = index_mapping.get(&feature.name).unwrap();
            content_map
                .entry(index_name.clone())
                .or_default()
                .push(content.clone());
        }
    }
    content_map
}
