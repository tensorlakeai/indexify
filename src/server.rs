use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use axum::{
    extract::{DefaultBodyLimit, Multipart, Path, Query, Request, State},
    http::StatusCode,
    routing::{get, post},
    Extension,
    Json,
    Router,
};
use axum_otel_metrics::HttpMetricsLayerBuilder;
use axum_tracing_opentelemetry::middleware::OtelAxumLayer;
use hyper::body::Incoming;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    pin,
    signal,
    sync::Notify,
};
use tower::Service;
use tracing::{error, info};
use utoipa::OpenApi;
use utoipa_rapidoc::RapiDoc;
use utoipa_redoc::{Redoc, Servable};
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    api::{self, *},
    attribute_index::AttributeIndexManager,
    blob_storage::BlobStorageBuilder,
    caching::caches_extension::Caches,
    coordinator_client::CoordinatorClient,
    data_repository_manager::DataRepositoryManager,
    extractor_router::ExtractorRouter,
    server_config::ServerConfig,
    tls::build_mtls_acceptor,
    vector_index::VectorIndexManager,
    vectordbs,
};

const DEFAULT_SEARCH_LIMIT: u64 = 5;

#[derive(Clone, Debug)]
pub struct RepositoryEndpointState {
    repository_manager: Arc<DataRepositoryManager>,
    coordinator_client: Arc<CoordinatorClient>,
}

#[derive(OpenApi)]
#[openapi(
        paths(
            create_repository,
            list_repositories,
            get_repository,
            add_texts,
            list_indexes,
            index_search,
            list_extractors,
            bind_extractor,
            attribute_lookup,
            list_executors
        ),
        components(
            schemas(CreateRepository, CreateRepositoryResponse, IndexDistance,
                TextAddRequest, TextAdditionResponse, Text, IndexSearchResponse,
                DocumentFragment, ListIndexesResponse, ExtractorOutputSchema, Index, SearchRequest, ListRepositoriesResponse, ListExtractorsResponse
            , ExtractorDescription, DataRepository, ExtractorBinding, ExtractorBindRequest, ExtractorBindResponse, Executor,
            AttributeLookupResponse, ExtractedAttributes, ListExecutorsResponse)
        ),
        tags(
            (name = "indexify", description = "Indexify API")
        )
    )]
struct ApiDoc;

pub struct Server {
    addr: SocketAddr,
    config: Arc<ServerConfig>,
}
impl Server {
    pub fn new(config: Arc<super::server_config::ServerConfig>) -> Result<Self> {
        let addr: SocketAddr = config.listen_addr_sock()?;
        Ok(Self { addr, config })
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
        let vector_db = vectordbs::create_vectordb(self.config.index_config.clone()).await?;
        let coordinator_client = Arc::new(CoordinatorClient::new(&self.config.coordinator_addr));
        let vector_index_manager = Arc::new(VectorIndexManager::new(
            coordinator_client.clone(),
            vector_db.clone(),
        ));
        let attribute_index_manager = Arc::new(
            AttributeIndexManager::new(&self.config.db_url, coordinator_client.clone()).await?,
        );

        let blob_storage =
            BlobStorageBuilder::new(Arc::new(self.config.blob_storage.clone())).build()?;

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
        let caches = Caches::new(self.config.cache.clone());
        let metrics = HttpMetricsLayerBuilder::new().build();
        let app = Router::new()
            .merge(metrics.routes())
            .merge(SwaggerUi::new("/api-docs-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
            .merge(Redoc::with_url("/redoc", ApiDoc::openapi()))
            .merge(RapiDoc::new("/api-docs/openapi.json").path("/rapidoc"))
            .route("/", get(root))
            .route(
                "/repositories/:repository_name/extractor_bindings",
                post(bind_extractor).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/repositories/:repository_name/indexes",
                get(list_indexes).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/repositories/:repository_name/add_texts",
                post(add_texts).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/repositories/:repository_name/content",
                get(list_content).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/repositories/:repository_name/upload_file",
                post(upload_file).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/repositories/:repository_name/search",
                post(index_search).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/repositories/:repository_name/attributes",
                get(attribute_lookup).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/repositories",
                post(create_repository).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/repositories",
                get(list_repositories).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/repositories/:repository_name",
                get(get_repository).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/executors",
                get(list_executors).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/write_content",
                post(write_extracted_content).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/extractors",
                get(list_extractors).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/extractors/extract",
                post(extract_content).with_state(repository_endpoint_state.clone()),
            )
            .layer(OtelAxumLayer::default())
            .layer(metrics)
            .layer(Extension(caches))
            .layer(DefaultBodyLimit::disable());

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
        });

        let acceptor = match use_tls {
            true => Some(build_mtls_acceptor(self.config.tls.as_ref().unwrap()).await?),
            false => None,
        };

        loop {
            // clone for loop
            let app = app.clone();
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
                            app.clone(),
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
                            app.clone(),
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
        notify_shutdown: Arc<Notify>,
    ) -> Result<()> {
        let tower_service = app.clone();
        let hyper_service = hyper::service::service_fn(move |request: Request<Incoming>| {
            // We have to clone `tower_service` because hyper's `Service` uses `&self`
            // whereas tower's `Service` requires `&mut self`.
            //
            // We don't need to call `poll_ready` since `Router` is always ready.
            tower_service.clone().call(request)
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

#[tracing::instrument]
#[axum::debug_handler]
#[utoipa::path(
    post,
    path = "/repositories",
    request_body = CreateRepository,
    tag = "indexify",
    responses(
        (status = 200, description = "Repository synced successfully", body = CreateRepositoryResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to sync repository")
    ),
)]
async fn create_repository(
    State(state): State<RepositoryEndpointState>,
    Json(payload): Json<CreateRepository>,
) -> Result<Json<CreateRepositoryResponse>, IndexifyAPIError> {
    let data_repository = api::DataRepository {
        name: payload.name.clone(),
        extractor_bindings: payload.extractor_bindings.clone(),
    };
    state
        .repository_manager
        .create(&data_repository)
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to sync repository: {}", e),
            )
        })?;
    Ok(Json(CreateRepositoryResponse {}))
}

#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/repositories",
    tag = "indexify",
    responses(
        (status = 200, description = "List of Data Repositories registered on the server", body = ListRepositoriesResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to sync repository")
    ),
)]
async fn list_repositories(
    State(state): State<RepositoryEndpointState>,
) -> Result<Json<ListRepositoriesResponse>, IndexifyAPIError> {
    let repositories = state
        .repository_manager
        .list_repositories()
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to list repositories: {}", e),
            )
        })?;
    let data_repos = repositories.into_iter().collect();
    Ok(Json(ListRepositoriesResponse {
        repositories: data_repos,
    }))
}

#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/repositories/{repository_name}",
    tag = "indexify",
    responses(
        (status = 200, description = "repository with a given name"),
        (status = 404, description = "Repository not found"),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to get repository")
    ),
)]
async fn get_repository(
    Path(repository_name): Path<String>,
    State(state): State<RepositoryEndpointState>,
) -> Result<Json<GetRepositoryResponse>, IndexifyAPIError> {
    let data_repo = state
        .repository_manager
        .get(&repository_name)
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to get repository: {}", e),
            )
        })?;
    Ok(Json(GetRepositoryResponse {
        repository: data_repo,
    }))
}

#[utoipa::path(
    post,
    path = "/repositories/{repository_name}/extractor_bindings",
    request_body = ExtractorBindRequest,
    tag = "indexify",
    responses(
        (status = 200, description = "Extractor binded successfully", body = ExtractorBindResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to bind extractor to repository")
    ),
)]
#[axum::debug_handler]
async fn bind_extractor(
    // FIXME: this throws a 500 when the binding already exists
    // FIXME: also throws a 500 when the index name already exists
    Path(repository_name): Path<String>,
    State(state): State<RepositoryEndpointState>,
    Json(payload): Json<ExtractorBindRequest>,
) -> Result<Json<ExtractorBindResponse>, IndexifyAPIError> {
    let index_names = state
        .repository_manager
        .add_extractor_binding(&repository_name, &payload.extractor_binding)
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to bind extractor: {}", e),
            )
        })?
        .into_iter()
        .collect();
    Ok(Json(ExtractorBindResponse { index_names }))
}

#[tracing::instrument]
#[utoipa::path(
    post,
    path = "/repositories/{repository_name}/add_texts",
    request_body = TextAddRequest,
    tag = "indexify",
    responses(
        (status = 200, description = "Texts were successfully added to the repository", body = TextAdditionResponse),
        (status = BAD_REQUEST, description = "Unable to add texts")
    ),
)]
#[axum::debug_handler]
async fn add_texts(
    Path(repository_name): Path<String>,
    State(state): State<RepositoryEndpointState>,
    Json(payload): Json<TextAddRequest>,
) -> Result<Json<TextAdditionResponse>, IndexifyAPIError> {
    let content = payload
        .documents
        .iter()
        .map(|d| api::Content {
            content_type: mime::TEXT_PLAIN.to_string(),
            bytes: d.text.as_bytes().to_vec(),
            metadata: d.metadata.clone(),
            feature: None,
        })
        .collect();
    state
        .repository_manager
        .add_texts(&repository_name, content)
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::BAD_REQUEST,
                format!("failed to add text: {}", e),
            )
        })?;
    Ok(Json(TextAdditionResponse::default()))
}

async fn list_content(
    Path(repository_name): Path<String>,
    State(state): State<RepositoryEndpointState>,
) -> Result<Json<ListContentResponse>, IndexifyAPIError> {
    let content_list = state
        .repository_manager
        .list_content(&repository_name)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ListContentResponse { content_list }))
}

#[tracing::instrument]
#[axum::debug_handler]
async fn upload_file(
    Path(repository_name): Path<String>,
    State(state): State<RepositoryEndpointState>,
    mut files: Multipart,
) -> Result<(), IndexifyAPIError> {
    while let Some(file) = files.next_field().await.unwrap() {
        let name = file.file_name().unwrap().to_string();
        let data = file.bytes().await.unwrap();
        info!(
            "writing to blog store, file name = {:?}, data = {:?}",
            name,
            data.len()
        );
        state
            .repository_manager
            .upload_file(&repository_name, &name, data)
            .await
            .map_err(|e| {
                IndexifyAPIError::new(
                    StatusCode::BAD_REQUEST,
                    format!("failed to upload file: {}", e),
                )
            })?;
    }
    Ok(())
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

#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/executors",
    tag = "indexify",
    responses(
        (status = 200, description = "List of currently running executors", body = ListExecutorsResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to load executors")
    ),
)]
#[axum::debug_handler]
async fn list_executors(
    State(_state): State<RepositoryEndpointState>,
) -> Result<Json<ListExecutorsResponse>, IndexifyAPIError> {
    Ok(Json(ListExecutorsResponse { executors: vec![] }))
}

#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/extractors",
    tag = "indexify",
    responses(
        (status = 200, description = "List of extractors available", body = ListExtractorsResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to search index")
    ),
)]
#[axum::debug_handler]
async fn list_extractors(
    State(state): State<RepositoryEndpointState>,
) -> Result<Json<ListExtractorsResponse>, IndexifyAPIError> {
    let extractors = state
        .repository_manager
        .list_extractors()
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .into_iter()
        .collect();
    Ok(Json(ListExtractorsResponse { extractors }))
}

#[axum::debug_handler]
async fn extract_content(
    State(repository_endpoint): State<RepositoryEndpointState>,
    Extension(caches): Extension<Caches>,
    Json(request): Json<ExtractRequest>,
) -> Result<Json<ExtractResponse>, IndexifyAPIError> {
    let cache = caches.cache_extract_content.clone();
    let extractor_router =
        ExtractorRouter::new(repository_endpoint.coordinator_client.clone()).with_cache(cache);
    let content_list = extractor_router
        .extract_content(&request.name, request.content, request.input_params)
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to extract content: {}", e),
            )
        })?;
    Ok(Json(ExtractResponse {
        content: content_list,
    }))
}

#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/repositories/{repository_name}/indexes",
    tag = "indexify",
    responses(
        (status = 200, description = "List of indexes in a repository", body = ListIndexesResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to list indexes in repository")
    ),
)]
#[axum::debug_handler]
async fn list_indexes(
    Path(repository_name): Path<String>,
    State(state): State<RepositoryEndpointState>,
) -> Result<Json<ListIndexesResponse>, IndexifyAPIError> {
    let indexes = state
        .repository_manager
        .list_indexes(&repository_name)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .into_iter()
        .collect();
    Ok(Json(ListIndexesResponse { indexes }))
}

#[tracing::instrument]
#[utoipa::path(
    post,
    path = "/repository/{repository_name}/search",
    tag = "indexify",
    responses(
        (status = 200, description = "Index search results", body = IndexSearchResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to search index")
    ),
)]
#[axum::debug_handler]
async fn index_search(
    Path(repository_name): Path<String>,
    State(state): State<RepositoryEndpointState>,
    Json(query): Json<SearchRequest>,
) -> Result<Json<IndexSearchResponse>, IndexifyAPIError> {
    let results = state
        .repository_manager
        .search(
            &repository_name,
            &query.index,
            &query.query,
            query.k.unwrap_or(DEFAULT_SEARCH_LIMIT),
        )
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let document_fragments: Vec<DocumentFragment> = results
        .iter()
        .map(|text| DocumentFragment {
            content_id: text.content_id.clone(),
            text: text.text.clone(),
            metadata: text.metadata.clone(),
            confidence_score: text.confidence_score,
        })
        .collect();
    Ok(Json(IndexSearchResponse {
        results: document_fragments,
    }))
}

#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/repository/{repository_name}/attributes",
    tag = "indexify",
    params(AttributeLookupRequest),
    responses(
        (status = 200, description = "List of Events in a repository", body = AttributeLookupResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to list events in repository")
    ),
)]
#[axum::debug_handler]
async fn attribute_lookup(
    Path(repository_name): Path<String>,
    State(state): State<RepositoryEndpointState>,
    Query(query): Query<AttributeLookupRequest>,
) -> Result<Json<AttributeLookupResponse>, IndexifyAPIError> {
    let attributes = state
        .repository_manager
        .attribute_lookup(&repository_name, &query.index, query.content_id.as_ref())
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(AttributeLookupResponse {
        attributes: attributes.into_iter().map(|r| r.into()).collect(),
    }))
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
