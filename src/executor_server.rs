use std::{
    net::{SocketAddr, TcpListener},
    sync::Arc,
};

use anyhow::Result;
use axum::{
    extract::State,
    routing::{get, post},
    Json,
    Router,
};
use axum_otel_metrics::HttpMetricsLayerBuilder;
use axum_tracing_opentelemetry::middleware::OtelAxumLayer;
use reqwest::StatusCode;
use tokio::{
    signal,
    sync::{broadcast, mpsc},
};
use tracing::{error, info};

use crate::{
    api::IndexifyAPIError,
    executor::ExtractorExecutor,
    internal_api::{ExtractRequest, ExtractResponse},
    server_config::ExecutorConfig,
};

enum TickerMessage {
    Shutdown,
    Heartbeat,
}

async fn heartbeat(
    tx: mpsc::Sender<TickerMessage>,
    mut rx: mpsc::Receiver<TickerMessage>,
    executor: Arc<ExtractorExecutor>,
) -> Result<()> {
    info!("starting executor heartbeat");
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    loop {
        let message = rx.recv().await;
        match message {
            Some(TickerMessage::Shutdown) => {
                info!("received shutdown signal");
                break;
            }
            Some(TickerMessage::Heartbeat) => {
                if let Err(err) = executor.sync_repo().await {
                    error!("unable to sync repo: {}", err.to_string());
                }
                interval.tick().await;
                if let Err(err) = tx.try_send(TickerMessage::Heartbeat) {
                    error!("unable to send heartbeat: {:?}", err.to_string());
                }
            }
            None => {
                info!("ticker channel closed");
                break;
            }
        }
    }
    Ok(())
}

pub struct ExecutorServer {
    executor_config: Arc<ExecutorConfig>,
    extractor_config_path: String,
}

impl ExecutorServer {
    pub async fn new(
        extractor_config_path: &str,
        executor_config: Arc<ExecutorConfig>,
    ) -> Result<Self> {
        Ok(Self {
            executor_config,
            extractor_config_path: extractor_config_path.into(),
        })
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let addr: SocketAddr = self.executor_config.listen_addr_sock()?;
        let listener = TcpListener::bind(addr)?;
        let listen_addr = listener.local_addr()?.to_string();
        let listen_port = listener.local_addr()?.port();
        let advertise_addr = format!("{}:{}", self.executor_config.advertise_if, listen_port);
        let executor = Arc::new(
            ExtractorExecutor::new(
                self.executor_config.clone(),
                &self.extractor_config_path,
                advertise_addr.clone(),
            )
            .await?,
        );
        let metrics = HttpMetricsLayerBuilder::new().build();
        let app = Router::new()
            .merge(metrics.routes())
            .route("/", get(root))
            .route(
                "/sync_executor",
                post(sync_worker).with_state(executor.clone()),
            )
            .route("/extract", post(extract).with_state(executor.clone()))
            //start OpenTelemetry trace on incoming request
            .layer(OtelAxumLayer::default())
            .layer(metrics);

        info!(
            "starting executor server on: {}, advertising: {}",
            listen_addr,
            advertise_addr.clone()
        );
        let (tx, rx) = mpsc::channel(32);
        if let Err(err) = tx.send(TickerMessage::Heartbeat).await {
            error!("unable to send heartbeat: {:?}", err.to_string());
        }
        tokio::spawn(heartbeat(tx.clone(), rx, executor.clone()));
        axum::Server::from_tcp(listener)?
            .serve(app.into_make_service())
            .with_graceful_shutdown(shutdown_signal(tx.clone()))
            .await?;
        Ok(())
    }
}

#[tracing::instrument]
async fn root() -> &'static str {
    "Indexify Extractor Server"
}

#[tracing::instrument]
#[axum_macros::debug_handler]
async fn extract(
    extractor_executor: State<Arc<ExtractorExecutor>>,
    Json(query): Json<ExtractRequest>,
) -> Result<Json<ExtractResponse>, IndexifyAPIError> {
    let content = extractor_executor
        .extract(query.content, query.input_params)
        .await;

    match content {
        Ok(content) => Ok(Json(ExtractResponse { content })),
        Err(err) => {
            error!("unable to extract content: {}", err.to_string());
            Err(IndexifyAPIError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                err.to_string(),
            ))
        }
    }
}

#[axum_macros::debug_handler]
async fn sync_worker(
    extractor_executor: State<Arc<ExtractorExecutor>>,
) -> Result<(), IndexifyAPIError> {
    let extractor_executor = extractor_executor;
    tokio::spawn(async move {
        let _ = extractor_executor.sync_repo().await;
    });
    Ok(())
}

#[tracing::instrument(skip(tx))]
async fn shutdown_signal(tx: mpsc::Sender<TickerMessage>) {
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
            let _ = tx.try_send(TickerMessage::Shutdown);
        },
        _ = terminate => {
            let _ = tx.try_send(TickerMessage::Shutdown);
        },
    }
    info!("signal received, shutting down server gracefully");
}
