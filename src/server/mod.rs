use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{anyhow, Result};
use axum::{
    extract::{DefaultBodyLimit, Request, State},
    http::{HeaderValue, StatusCode},
    routing::{get, post},
    Extension,
    Json,
    Router,
};
use axum_tracing_opentelemetry::middleware::OtelAxumLayer;
use hyper::{body::Incoming, header};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    pin,
    signal,
    sync::{watch::Receiver, Notify},
};
use tonic::transport::server::Routes;
use tower::Service;
use tracing::{error, info};

use self::coordinator::coordinator::Coordinator;
use crate::{
    api::*,
    attribute_index::AttributeIndexManager,
    blob_storage::BlobStorageBuilder,
    caching::caches_extension::Caches,
    coordinator_client::CoordinatorClient,
    data_repository_manager::DataRepositoryManager,
    server_config::ServerConfig,
    state::store::StateChange,
    tls::build_mtls_acceptor,
    vector_index::VectorIndexManager,
    vectordbs,
};

mod api_doc;
mod coordinator;
mod executors;
mod extractors;
mod metrics;
mod repositories;

#[derive(Clone, Debug)]
pub struct RepositoryEndpointState {
    repository_manager: Arc<DataRepositoryManager>,
    coordinator_client: Arc<CoordinatorClient>,
}

pub struct Server {
    addr: SocketAddr,
    config: Arc<ServerConfig>,
    dev_mode: bool,
}
impl Server {
    pub fn new(config: Arc<super::server_config::ServerConfig>, dev_mode: bool) -> Result<Self> {
        let addr: SocketAddr = config.listen_addr_sock()?;
        Ok(Self {
            addr,
            config,
            dev_mode,
        })
    }

    pub async fn run(&self) -> Result<()> {
        // TLS is set to true if the "tls" field is present in the config and the
        // TlsConfig "api" field is set to true
        let use_tls = self.config.tls.is_some() && self.config.tls.as_ref().unwrap().api;
        match use_tls {
            true => {
                match self.config.tls.as_ref().unwrap().ca_file {
                    Some(_) => info!("starting indexify server with mTLS enabled"),
                    None => info!("starting indexify server with TLS enabled. No CA file provided, so mTLS is disabled"),
                }
            }
            false => info!("starting indexify server with TLS disabled"),
        }
        let blob_storage =
            BlobStorageBuilder::new(Arc::new(self.config.blob_storage.clone())).build()?;
        let vector_db = vectordbs::create_vectordb(self.config.index_config.clone()).await?;
        let coordinator_client = Arc::new(CoordinatorClient::new(&self.config.coordinator_addr));
        let vector_index_manager = Arc::new(
            VectorIndexManager::new(coordinator_client.clone(), vector_db.clone())
                .map_err(|e| anyhow!("unable to create vector index {}", e))?,
        );
        let attribute_index_manager = Arc::new(
            AttributeIndexManager::new(&self.config.db_url, coordinator_client.clone()).await?,
        );

        let repository_manager = Arc::new(
            DataRepositoryManager::new(
                vector_index_manager,
                attribute_index_manager,
                blob_storage.clone(),
                coordinator_client.clone(),
            )
            .await?,
        );
        let repository_endpoint_state = RepositoryEndpointState {
            repository_manager: repository_manager.clone(),
            coordinator_client: coordinator_client.clone(),
        };
        let server = Router::new()
            .route("/", get(root))
            .route("/write_content", post(write_extracted_content))
            .merge(repositories::get_router())
            .merge(extractors::get_router())
            .merge(executors::get_router())
            .with_state(repository_endpoint_state)
            .merge(metrics::get_metrics_layer().routes())
            .merge(api_doc::get_router())
            .layer(OtelAxumLayer::default())
            .layer(metrics::get_metrics_layer())
            .layer(Extension(Caches::new(self.config.cache.clone())))
            .layer(DefaultBodyLimit::disable());

        let coordinator = coordinator::get_coordinator_server(self.config.clone()).await?;
        let leader_change_watcher = coordinator.get_leader_change_watcher();
        let coordinator_clone = coordinator.get_coordinator();
        let state_watcher_rx = coordinator.get_state_watcher();
        let coordinator = coordinator.into_service().await?;

        let (signal_tx, signal_rx) = tokio::sync::watch::channel(());
        let signal_tx = Arc::new(signal_tx);
        tokio::spawn(async move {
            shutdown_signal().await;
            info!("received graceful shutdown signal. Telling tasks to shutdown");
            drop(signal_rx);
        });

        let listener = tokio::net::TcpListener::bind(&self.addr).await?;

        let shutdown_notify = Arc::new(Notify::new());
        let notify_clone = shutdown_notify.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            notify_clone.notify_waiters();

            // Notify coordinator of shutdown
            let _ = run_scheduler(leader_change_watcher, state_watcher_rx, coordinator_clone).await;
        });

        let acceptor = match use_tls {
            true => Some(build_mtls_acceptor(self.config.tls.as_ref().unwrap()).await?),
            false => None,
        };

        tokio::spawn(async move {});

        loop {
            // clone for loop
            let server = server.clone();
            let coordinator = coordinator.clone();
            let acceptor = acceptor.clone();
            let shutdown_notify = shutdown_notify.clone();

            // pick up the next connection
            let (tcp_stream, remote_addr) = tokio::select! {
                conn = listener.accept() => conn?,
                _ = signal_tx.closed() => {
                    info!("graceful shutdown signal received. Shutting down server");
                    break Ok(()); // graceful shutdown
                }
            };
            info!("accepted connection from: {}", remote_addr);

            match use_tls {
                true => {
                    tokio::task::spawn(async move {
                        let acceptor = acceptor.unwrap().clone();
                        let tls_stream = match acceptor.accept(tcp_stream).await {
                            Ok(tls_stream) => tls_stream,
                            Err(err) => {
                                error!("failed to perform TLS Handshake: {}", err);
                                return;
                            }
                        };
                        match Self::handle_connection(
                            Box::new(tls_stream),
                            server.clone(),
                            coordinator,
                            shutdown_notify,
                        )
                        .await
                        {
                            Ok(_) => {}
                            Err(err) => {
                                error!("failed to handle connection: {}", err);
                            }
                        }
                    });
                }
                false => {
                    tokio::task::spawn(async move {
                        match Self::handle_connection(
                            Box::new(tcp_stream),
                            server.clone(),
                            coordinator.clone(),
                            shutdown_notify,
                        )
                        .await
                        {
                            Ok(_) => {}
                            Err(err) => {
                                error!("failed to handle connection: {}", err);
                            }
                        }
                    });
                }
            }
        }
    }

    async fn handle_connection(
        tcp_stream: Box<dyn Stream>,
        app: Router,
        coordinator: Routes,
        notify_shutdown: Arc<Notify>,
    ) -> Result<()> {
        let tower_service = app.clone();
        let hyper_service = hyper::service::service_fn(move |request: Request<Incoming>| async {
            // We have to clone `tower_service` because hyper's `Service` uses `&self`
            // whereas tower's `Service` requires `&mut self`.
            //
            // We don't need to call `poll_ready` since `Router` is always ready.
            if request
                .headers()
                .get(header::CONTENT_TYPE)
                .map(Clone::clone)
                .unwrap_or_else(|| HeaderValue::from_static("")) ==
                "application/grpc"
            {
                coordinator.call(request).await
            } else {
                tower_service.call(request).await
            }
        });

        let builder = Builder::new(TokioExecutor::new());
        let conn = builder.serve_connection(TokioIo::new(tcp_stream), hyper_service);

        pin!(conn);

        // TODO: make configurable
        let timeout_duration = 60;
        let timeout = tokio::time::sleep(std::time::Duration::from_secs(timeout_duration));

        let res = tokio::select! {
            res = conn.as_mut() => {
                match res {
                    Ok(_) => Ok(()),
                    Err(e) => Err(anyhow::anyhow!("failed to serve connection: {}", e)),
                }
            }
            _ = timeout => {
                info!("connection timed out after {} seconds", timeout_duration);
                Ok(())
            }
            _ = notify_shutdown.notified() => {
                info!("graceful shutdown signal received. Shutting down connection");
                Ok(()) // graceful shutdown
            }
        };
        res.map_err(|e| anyhow::anyhow!("failed to serve connection: {}", e))
    }
}

// implement Stream for all types that implement AsyncRead + AsyncWrite + Send +
// Unpin i.e. TcpStream and TlsStream
pub trait Stream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Stream for T {}

#[tracing::instrument]
async fn root() -> &'static str {
    "Indexify Server"
}

#[axum::debug_handler]
async fn write_extracted_content(
    State(state): State<RepositoryEndpointState>,
    Json(payload): Json<WriteExtractedContent>,
) -> Result<Json<()>, IndexifyAPIError> {
    let result = state
        .repository_manager
        .write_extracted_content(payload)
        .await;
    if let Err(err) = &result {
        info!("failed to write extracted content: {:?}", err);
        return Err(IndexifyAPIError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            err.to_string(),
        ));
    }

    Ok(Json(()))
}

async fn run_scheduler(
    mut leader_changed: Receiver<bool>,
    mut state_watcher_rx: Receiver<StateChange>,
    coordinator: Arc<Coordinator>,
) -> Result<()> {
    //let mut interval =
    // tokio::time::interval(tokio::time::Duration::from_secs(5));
    let is_leader = AtomicBool::new(false);

    // Throw away the first value since it's garbage
    _ = state_watcher_rx.changed().await;
    loop {
        tokio::select! {
            _ = state_watcher_rx.changed() => {
                if is_leader.load(Ordering::Relaxed) {
                    let _state_change = state_watcher_rx.borrow_and_update().clone();
                   if let Err(err) = coordinator.process_and_distribute_work().await {
                          error!("error processing and distributing work: {:?}", err);
                   }
                }
            },
            _ = shutdown_signal() => {
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
