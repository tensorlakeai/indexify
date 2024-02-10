use std::{net::SocketAddr, sync::Arc};

use anyhow::{anyhow, Result};
use axum::{
    extract::{DefaultBodyLimit, Multipart, Path, Query, Request, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Extension,
    Json,
    Router,
};
use axum_otel_metrics::HttpMetricsLayerBuilder;
use axum_tracing_opentelemetry::middleware::OtelAxumLayer;
use hyper::{body::Incoming, Method};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
};
use indexify_internal_api as internal_api;
use indexify_proto::indexify_coordinator::{ListStateChangesRequest, ListTasksRequest};
use rust_embed::RustEmbed;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    pin,
    signal,
    sync::Notify,
};
use tower::Service;
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info};
use utoipa::OpenApi;
use utoipa_rapidoc::RapiDoc;
use utoipa_redoc::{Redoc, Servable};
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    api::{self, *},
    caching::caches_extension::Caches,
    coordinator_client::CoordinatorClient,
    data_manager::DataManager,
    extractor_router::ExtractorRouter,
    metadata_storage::{self, MetadataStorageTS},
    server_config::ServerConfig,
    tls::build_mtls_acceptor,
    vector_index::VectorIndexManager,
    vectordbs,
};

const DEFAULT_SEARCH_LIMIT: u64 = 5;

#[derive(RustEmbed)]
#[folder = "ui/build"]
pub struct UiAssets;

#[derive(Clone, Debug)]
pub struct NamespaceEndpointState {
    data_manager: Arc<DataManager>,
    coordinator_client: Arc<CoordinatorClient>,
}

#[derive(OpenApi)]
#[openapi(
        paths(
            create_namespace,
            list_namespaces,
            get_namespace,
            add_texts,
            list_indexes,
            index_search,
            list_extractors,
            bind_extractor,
            metadata_lookup,
            list_executors,
            list_content,
            read_content,
            upload_file,
            write_extracted_content,
            list_tasks,
            extract_content
        ),
        components(
            schemas(CreateNamespace, CreateNamespaceResponse, IndexDistance,
                TextAddRequest, TextAdditionResponse, Text, IndexSearchResponse,
                DocumentFragment, ListIndexesResponse, ExtractorOutputSchema, Index, SearchRequest, ListNamespacesResponse, ListExtractorsResponse
            , ExtractorDescription, DataNamespace, ExtractorBinding, ExtractorBindRequest, ExtractorBindResponse, Executor,
            MetadataResponse, ExtractedMetadata, ListExecutorsResponse, EmbeddingSchema, ExtractResponse, ExtractRequest,
            Content, Feature, FeatureType, WriteExtractedContent, GetRawContentResponse, ListTasksResponse, internal_api::Task, internal_api::TaskOutcome,
            internal_api::Content, internal_api::ContentMetadata, ListContentResponse, GetNamespaceResponse
        )
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
        let vector_index_manager = Arc::new(
            VectorIndexManager::new(coordinator_client.clone(), vector_db.clone())
                .map_err(|e| anyhow!("unable to create vector index {}", e))?,
        );
        let metadata_index_manager: MetadataStorageTS =
            metadata_storage::from_config(&self.config.metadata_storage)?;
        let data_manager = Arc::new(
            DataManager::new(
                vector_index_manager,
                metadata_index_manager,
                self.config.blob_storage.clone(),
                coordinator_client.clone(),
            )
            .await?,
        );
        let namespace_endpoint_state = NamespaceEndpointState {
            data_manager: data_manager.clone(),
            coordinator_client: coordinator_client.clone(),
        };
        let caches = Caches::new(self.config.cache.clone());
        let cors = CorsLayer::new()
            .allow_methods([Method::GET, Method::POST])
            .allow_origin(Any);

        let metrics = HttpMetricsLayerBuilder::new().build();
        let app = Router::new()
            .merge(metrics.routes())
            .merge(SwaggerUi::new("/api-docs-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
            .merge(Redoc::with_url("/redoc", ApiDoc::openapi()))
            .merge(RapiDoc::new("/api-docs/openapi.json").path("/rapidoc"))
            .route("/", get(root))
            .route(
                "/namespaces/:namespace/extractor_bindings",
                post(bind_extractor).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/indexes",
                get(list_indexes).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/add_texts",
                post(add_texts).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/content",
                get(list_content).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/content/:content_id",
                get(read_content).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/upload_file",
                post(upload_file).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/search",
                post(index_search).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces",
                post(create_namespace).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces",
                get(list_namespaces).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace",
                get(get_namespace).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/metadata",
                get(metadata_lookup).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/executors",
                get(list_executors).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/write_content",
                post(write_extracted_content).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/extractors",
                get(list_extractors).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/state_changes",
                get(list_state_changes).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/tasks",
                get(list_tasks).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/extractors/extract",
                post(extract_content).with_state(namespace_endpoint_state.clone()),
            )
            .route("/ui", get(ui_index_handler))
            .route("/ui/*rest", get(ui_handler))
            .layer(OtelAxumLayer::default())
            .layer(metrics)
            .layer(Extension(caches))
            .layer(cors)
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
    path = "/namespaces",
    request_body = CreateNamespace,
    tag = "indexify",
    responses(
        (status = 200, description = "Namespace synced successfully", body = CreateNamespaceResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to sync namespace")
    ),
)]
async fn create_namespace(
    State(state): State<NamespaceEndpointState>,
    Json(payload): Json<CreateNamespace>,
) -> Result<Json<CreateNamespaceResponse>, IndexifyAPIError> {
    let data_namespace = api::DataNamespace {
        name: payload.name.clone(),
        extractor_bindings: payload.extractor_bindings.clone(),
    };
    state
        .data_manager
        .create(&data_namespace)
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to sync namespace: {}", e),
            )
        })?;
    Ok(Json(CreateNamespaceResponse {}))
}

#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/namespaces",
    tag = "indexify",
    responses(
        (status = 200, description = "List of Data Namespaces registered on the server", body = ListNamespacesResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to sync namespace")
    ),
)]
async fn list_namespaces(
    State(state): State<NamespaceEndpointState>,
) -> Result<Json<ListNamespacesResponse>, IndexifyAPIError> {
    let namespaces = state.data_manager.list_namespaces().await.map_err(|e| {
        IndexifyAPIError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to list namespaces: {}", e),
        )
    })?;
    let data_namespaces: Vec<DataNamespace> = namespaces.into_iter().collect();
    Ok(Json(ListNamespacesResponse {
        namespaces: data_namespaces,
    }))
}

#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}",
    tag = "indexify",
    responses(
        (status = 200, description = "namespace with a given name", body=GetNamespaceResponse),
        (status = 404, description = "Namespace not found"),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to get namespace")
    ),
)]
async fn get_namespace(
    Path(namespace): Path<String>,
    State(state): State<NamespaceEndpointState>,
) -> Result<Json<GetNamespaceResponse>, IndexifyAPIError> {
    let data_namespace = state.data_manager.get(&namespace).await.map_err(|e| {
        IndexifyAPIError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to get namespace: {}", e),
        )
    })?;
    Ok(Json(GetNamespaceResponse {
        namespace: data_namespace,
    }))
}

#[utoipa::path(
    post,
    path = "/namespace/{namespace}/extractor_bindings",
    request_body = ExtractorBindRequest,
    tag = "indexify",
    responses(
        (status = 200, description = "Extractor binded successfully", body = ExtractorBindResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to bind extractor to namespace")
    ),
)]
#[axum::debug_handler]
async fn bind_extractor(
    // FIXME: this throws a 500 when the binding already exists
    // FIXME: also throws a 500 when the index name already exists
    Path(namespace): Path<String>,
    State(state): State<NamespaceEndpointState>,
    Json(payload): Json<ExtractorBindRequest>,
) -> Result<Json<ExtractorBindResponse>, IndexifyAPIError> {
    let index_names = state
        .data_manager
        .add_extractor_binding(&namespace, &payload.extractor_binding)
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
    path = "/namespaces/{namespace}/add_texts",
    request_body = TextAddRequest,
    tag = "indexify",
    responses(
        (status = 200, description = "Texts were successfully added to the namespace", body = TextAdditionResponse),
        (status = BAD_REQUEST, description = "Unable to add texts")
    ),
)]
#[axum::debug_handler]
async fn add_texts(
    Path(namespace): Path<String>,
    State(state): State<NamespaceEndpointState>,
    Json(payload): Json<TextAddRequest>,
) -> Result<Json<TextAdditionResponse>, IndexifyAPIError> {
    let content = payload
        .documents
        .iter()
        .map(|d| api::Content {
            content_type: mime::TEXT_PLAIN.to_string(),
            bytes: d.text.as_bytes().to_vec(),
            labels: d.labels.clone(),
            features: vec![],
        })
        .collect();
    state
        .data_manager
        .add_texts(&namespace, content)
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::BAD_REQUEST,
                format!("failed to add text: {}", e),
            )
        })?;
    Ok(Json(TextAdditionResponse::default()))
}

#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/content",
    tag = "indexify",
    responses(
        (status = 200, description = "Lists the contents in the namespace", body = ListContentResponse),
        (status = BAD_REQUEST, description = "Unable to list contents")
    ),
)]
#[axum::debug_handler]
async fn list_content(
    Path(namespace): Path<String>,
    State(state): State<NamespaceEndpointState>,
    filter: Query<super::api::ListContentFilters>,
) -> Result<Json<ListContentResponse>, IndexifyAPIError> {
    let content_list = state
        .data_manager
        .list_content(
            &namespace,
            &filter.source,
            &filter.parent_id,
            filter.labels_eq.as_ref(),
        )
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(ListContentResponse { content_list }))
}

#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/content/{content_id}",
    tag = "indexify",
    responses(
        (status = 200, description = "Reads a specific content in the namespace", body = GetRawContentResponse),
        (status = BAD_REQUEST, description = "Unable to read content")
    ),
)]
#[axum::debug_handler]
async fn read_content(
    Path((namespace, content_id)): Path<(String, String)>,
    State(state): State<NamespaceEndpointState>,
) -> Result<Json<GetRawContentResponse>, IndexifyAPIError> {
    let content_list = state
        .data_manager
        .read_content(&namespace, vec![content_id])
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(GetRawContentResponse { content_list }))
}

#[tracing::instrument]
#[utoipa::path(
    post,
    path = "/namespaces/{namespace}/upload_file",
    request_body(content_type = "multipart/form-data", content = Vec<u8>),
    tag = "indexify",
    responses(
        (status = 200, description = "Uploads a file to the namespace"),
        (status = BAD_REQUEST, description = "Unable to upload file")
    ),
)]
#[axum::debug_handler]
async fn upload_file(
    Path(namespace): Path<String>,
    State(state): State<NamespaceEndpointState>,
    mut files: Multipart,
) -> Result<(), IndexifyAPIError> {
    while let Some(file) = files.next_field().await.unwrap() {
        let name = file.file_name().unwrap().to_string();
        let data = file.bytes().await.unwrap();
        info!(
            "writing to blob store, file name = {:?}, data = {:?}",
            name,
            data.len()
        );
        state
            .data_manager
            .upload_file(&namespace, data, &name)
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

#[tracing::instrument(skip(state, payload))]
#[utoipa::path(
    post,
    path = "/write_content",
    request_body = WriteExtractedContent,
    tag = "indexify",
    responses(
        (status = 200, description = "Write Extracted Content to a Namespace"),
        (status = BAD_REQUEST, description = "Unable to add texts")
    ),
)]
#[axum::debug_handler]
async fn write_extracted_content(
    State(state): State<NamespaceEndpointState>,
    Json(payload): Json<WriteExtractedContent>,
) -> Result<Json<()>, IndexifyAPIError> {
    let result = state.data_manager.write_extracted_content(payload).await;
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
    State(_state): State<NamespaceEndpointState>,
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
    State(state): State<NamespaceEndpointState>,
) -> Result<Json<ListExtractorsResponse>, IndexifyAPIError> {
    let extractors = state
        .data_manager
        .list_extractors()
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .into_iter()
        .collect();
    Ok(Json(ListExtractorsResponse { extractors }))
}

#[utoipa::path(
    post,
    path = "/extractors/extract",
    tag = "indexify",
    request_body = ExtractRequest,
    responses(
        (status = 200, description = "Extract content from an extractor", body = ExtractResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to search index")
    ),
)]
#[axum::debug_handler]
async fn list_state_changes(
    State(state): State<NamespaceEndpointState>,
    Query(_query): Query<ListStateChanges>,
) -> Result<Json<ListStateChangesResponse>, IndexifyAPIError> {
    let state_changes = state
        .coordinator_client
        .get()
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .list_state_changes(ListStateChangesRequest {})
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .into_inner()
        .changes;

    let state_changes: Vec<indexify_internal_api::StateChange> = state_changes
        .into_iter()
        .map(|c| c.try_into())
        .filter_map(|c| c.ok())
        .collect();

    Ok(Json(ListStateChangesResponse { state_changes }))
}

#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/tasks",
    tag = "indexify",
    responses(
        (status = 200, description = "Lists tasks", body = ListTasksResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to list tasks")
    ),
)]
async fn list_tasks(
    State(state): State<NamespaceEndpointState>,
    Query(query): Query<ListTasks>,
) -> Result<Json<ListTasksResponse>, IndexifyAPIError> {
    let tasks = state
        .coordinator_client
        .get()
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .list_tasks(ListTasksRequest {
            namespace: query.namespace.clone(),
            extractor_binding: query.extractor_binding.unwrap_or("".to_string()),
        })
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .into_inner()
        .tasks;
    let tasks = tasks
        .into_iter()
        .map(|t| t.try_into())
        .filter_map(|t| t.ok())
        .collect();
    Ok(Json(ListTasksResponse { tasks }))
}

#[utoipa::path(
    post,
    path = "/extractors/extract",
    request_body = ExtractRequest,
    tag = "indexify",
    responses(
        (status = 200, description = "Extract content from extractors", body = ExtractResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to list tasks")
    ),
)]
#[axum::debug_handler]
async fn extract_content(
    State(namespace_endpoint): State<NamespaceEndpointState>,
    Extension(caches): Extension<Caches>,
    Json(request): Json<ExtractRequest>,
) -> Result<Json<ExtractResponse>, IndexifyAPIError> {
    let cache = caches.cache_extract_content.clone();
    let extractor_router = ExtractorRouter::new(namespace_endpoint.coordinator_client.clone())
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .with_cache(cache);
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
    path = "/namespaces/{namespace}/indexes",
    tag = "indexify",
    responses(
        (status = 200, description = "List of indexes in a namespace", body = ListIndexesResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to list indexes in namespace")
    ),
)]
#[axum::debug_handler]
async fn list_indexes(
    Path(namespace): Path<String>,
    State(state): State<NamespaceEndpointState>,
) -> Result<Json<ListIndexesResponse>, IndexifyAPIError> {
    let indexes = state
        .data_manager
        .list_indexes(&namespace)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .into_iter()
        .collect();
    Ok(Json(ListIndexesResponse { indexes }))
}

#[utoipa::path(
    post,
    path = "/namespace/{namespace}/search",
    tag = "indexify",
    responses(
        (status = 200, description = "Index search results", body = IndexSearchResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to search index")
    ),
)]
#[axum::debug_handler]
async fn index_search(
    Path(namespace): Path<String>,
    State(state): State<NamespaceEndpointState>,
    Json(query): Json<SearchRequest>,
) -> Result<Json<IndexSearchResponse>, IndexifyAPIError> {
    let results = state
        .data_manager
        .search(
            &namespace,
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
            labels: text.labels.clone(),
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
    path = "/namespace/{namespace}/metadata",
    tag = "indexify",
    params(MetadataRequest),
    responses(
        (status = 200, description = "List of Events in a namespace", body = MetadataResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to list events in namespace")
    ),
)]
#[axum::debug_handler]
async fn metadata_lookup(
    Path(namespace): Path<String>,
    State(state): State<NamespaceEndpointState>,
    Query(query): Query<MetadataRequest>,
) -> Result<Json<MetadataResponse>, IndexifyAPIError> {
    let attributes = state
        .data_manager
        .metadata_lookup(&namespace, &query.index, &query.content_id)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(MetadataResponse {
        attributes: attributes.into_iter().map(|r| r.into()).collect(),
    }))
}

#[axum::debug_handler]
#[tracing::instrument(skip_all)]
async fn ui_index_handler() -> impl IntoResponse {
    let content = UiAssets::get("index.html").unwrap();
    (
        [(hyper::header::CONTENT_TYPE, content.metadata.mimetype())],
        content.data,
    )
        .into_response()
}

#[axum::debug_handler]
#[tracing::instrument(skip_all)]
async fn ui_handler(Path(url): Path<String>) -> impl IntoResponse {
    let content = UiAssets::get(url.trim_start_matches('/'))
        .unwrap_or_else(|| UiAssets::get("index.html").unwrap());
    (
        [(hyper::header::CONTENT_TYPE, content.metadata.mimetype())],
        content.data,
    )
        .into_response()
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
