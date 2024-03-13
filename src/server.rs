use std::{net::SocketAddr, sync::Arc};

use anyhow::{anyhow, Result};
use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Multipart, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension,
    Json,
    Router,
};
use axum_otel_metrics::HttpMetricsLayerBuilder;
use axum_server::Handle;
use axum_tracing_opentelemetry::middleware::OtelAxumLayer;
use axum_typed_websockets::{Message, WebSocket, WebSocketUpgrade};
use hyper::{header::CONTENT_TYPE, Method};
use indexify_internal_api as internal_api;
use indexify_proto::indexify_coordinator::{self, ListStateChangesRequest, ListTasksRequest};
use rust_embed::RustEmbed;
use tokio::signal;
use tokio_stream::StreamExt;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;
use utoipa::OpenApi;
use utoipa_rapidoc::RapiDoc;
use utoipa_redoc::{Redoc, Servable};
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    api::{self, *},
    blob_storage::{BlobStorage, ContentReader},
    caching::caches_extension::Caches,
    coordinator_client::CoordinatorClient,
    data_manager::DataManager,
    extractor_router::ExtractorRouter,
    metadata_storage::{self, MetadataStorageTS},
    server_config::ServerConfig,
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
    content_reader: Arc<ContentReader>,
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
            create_extraction_policy,
            list_executors,
            list_content,
            get_content_metadata,
            upload_file,
            list_tasks,
            extract_content
        ),
        components(
            schemas(CreateNamespace, CreateNamespaceResponse, IndexDistance,
                TextAddRequest, TextAdditionResponse, Text, IndexSearchResponse,
                DocumentFragment, ListIndexesResponse, ExtractorOutputSchema, Index, SearchRequest, ListNamespacesResponse, ListExtractorsResponse
            , ExtractorDescription, DataNamespace, ExtractionPolicy, ExtractionPolicyRequest, ExtractionPolicyResponse, Executor,
            MetadataResponse, ExtractedMetadata, ListExecutorsResponse, EmbeddingSchema, ExtractResponse, ExtractRequest,
            Content, Feature, FeatureType, GetContentMetadataResponse, ListTasksResponse, internal_api::Task, internal_api::TaskOutcome,
            internal_api::Content, internal_api::ContentMetadata, ListContentResponse, GetNamespaceResponse, ExtractionPolicyResponse,
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
        let blob_storage = Arc::new(BlobStorage::new_with_config(
            self.config.blob_storage.clone(),
        ));
        let data_manager = Arc::new(
            DataManager::new(
                vector_index_manager,
                metadata_index_manager,
                blob_storage.clone(),
                coordinator_client.clone(),
            )
            .await?,
        );
        let namespace_endpoint_state = NamespaceEndpointState {
            data_manager: data_manager.clone(),
            coordinator_client: coordinator_client.clone(),
            content_reader: Arc::new(ContentReader::new()),
        };
        let caches = Caches::new(self.config.cache.clone());
        let cors = CorsLayer::new()
            .allow_methods([Method::GET, Method::POST])
            .allow_origin(Any)
            .allow_headers([CONTENT_TYPE]);

        let metrics = HttpMetricsLayerBuilder::new().build();
        let app = Router::new()
            .merge(metrics.routes())
            .merge(SwaggerUi::new("/api-docs-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
            .merge(Redoc::with_url("/redoc", ApiDoc::openapi()))
            .merge(RapiDoc::new("/api-docs/openapi.json").path("/rapidoc"))
            .route("/", get(root))
            .route(
                "/namespaces/:namespace/extraction_policies",
                post(create_extraction_policy).with_state(namespace_endpoint_state.clone()),
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
                get(get_content_metadata).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/content/:content_id/metadata",
                get(get_extracted_metadata).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/content/:content_id/download",
                get(download_content).with_state(namespace_endpoint_state.clone()),
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
                "/executors",
                get(list_executors).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/write_content",
                get(ingest_extracted_content).with_state(namespace_endpoint_state.clone()),
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
                "/namespaces/:namespace/tasks",
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

        let handle = Handle::new();

        let handle_sh = handle.clone();
        tokio::spawn(async move {
            shutdown_signal(handle_sh).await;
            info!("received graceful shutdown signal. Telling tasks to shutdown");
        });

        // TODO: Bring all this back once axum_server upgrades to rustls 0.22
        // since our TLS code is based on that
        //if let Some(tls_config) = self.config.tls.clone() {
        //    let config: Arc<rustls::ServerConfig> =
        // build_mtls_config(&tls_config).await?;    let rustls_config =
        // RustlsConfig::from_config(config);    let handle = handle.clone();
        //    axum_server::bind_rustls(self.addr, rustls_config)
        //        .handle(handle)
        //        .serve(app.into_make_service())
        //        .await?;
        //} else {
        let handle = handle.clone();
        axum_server::bind(self.addr)
            .handle(handle)
            .serve(app.into_make_service())
            .await?;
        //}

        Ok(())
    }
}

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
        extraction_policies: payload.extraction_policies.clone(),
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
    path = "/namespace/{namespace}/extraction_policies",
    request_body = ExtractionPolicyRequest,
    tag = "indexify",
    responses(
        (status = 200, description = "Extractor policy added successfully", body = ExtractionPolicyResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to add extraction policy to namespace")
    ),
)]
#[axum::debug_handler]
async fn create_extraction_policy(
    // FIXME: this throws a 500 when the binding already exists
    // FIXME: also throws a 500 when the index name already exists
    Path(namespace): Path<String>,
    State(state): State<NamespaceEndpointState>,
    Json(payload): Json<ExtractionPolicyRequest>,
) -> Result<Json<ExtractionPolicyResponse>, IndexifyAPIError> {
    let index_names = state
        .data_manager
        .create_extraction_policy(&namespace, &payload.policy)
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to bind extractor: {}", e),
            )
        })?
        .into_iter()
        .collect();
    Ok(Json(ExtractionPolicyResponse { index_names }))
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
async fn get_content_metadata(
    Path((namespace, content_id)): Path<(String, String)>,
    State(state): State<NamespaceEndpointState>,
) -> Result<Json<GetContentMetadataResponse>, IndexifyAPIError> {
    let content_list = state
        .data_manager
        .get_content_metadata(&namespace, vec![content_id])
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let content_metadata = content_list.first().ok_or_else(|| {
        IndexifyAPIError::new(StatusCode::NOT_FOUND, "content not found".to_string())
    })?;

    Ok(Json(GetContentMetadataResponse {
        content_metadata: content_metadata.clone(),
    }))
}

#[axum::debug_handler]
async fn download_content(
    Path((namespace, content_id)): Path<(String, String)>,
    State(state): State<NamespaceEndpointState>,
) -> impl IntoResponse {
    let content_list = state
        .data_manager
        .get_content_metadata(&namespace, vec![content_id])
        .await;
    if let Err(err) = content_list.as_ref() {
        return Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from(format!("unable to get content: {}", err)))
            .unwrap();
    }
    if content_list.as_ref().unwrap().is_empty() {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("content not found"))
            .unwrap();
    }
    let content_metadata = content_list.unwrap().first().unwrap().clone();
    Response::builder()
        .header("Content-Length", content_metadata.size)
        .header("Content-Type", content_metadata.mime_type.clone())
        .body(Body::from_stream(async_stream::stream! {
            let storage_url = &content_metadata.storage_url.clone();
            let content_reader = state.content_reader.clone();
            let reader = content_reader.get(storage_url);
            let mut content_stream = reader.get(storage_url);
            while let Some(buf)  = content_stream.next().await {
                yield buf;
            }
        }))
        .unwrap()
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
async fn ingest_extracted_content(
    ws: WebSocketUpgrade<IngestExtractedContentResponse, IngestExtractedContent>,
    State(state): State<NamespaceEndpointState>,
) -> impl IntoResponse {
    // TODO - Figure out a protocol which breaks up large messages into smaller
    // chunks and reassembles them
    ws.map(|ws| ws.max_message_size(592323536))
        .map(|ws| ws.max_frame_size(592323536))
        .on_upgrade(|socket| inner_ingest_extracted_content(socket, state))
}

// Send a ping and measure how long time it takes to get a pong back
async fn inner_ingest_extracted_content(
    mut socket: WebSocket<IngestExtractedContentResponse, IngestExtractedContent>,
    state: NamespaceEndpointState,
) {
    let _ = socket.send(Message::Ping(vec![])).await;
    let mut ingest_metadata: Option<BeginExtractedContentIngest> = None;
    let mut content_metadata: Option<indexify_coordinator::ContentMetadata> = None;
    while let Some(msg) = socket.recv().await {
        if let Err(err) = &msg {
            tracing::error!("error receiving message: {:?}", err);
            return;
        }
        if let Ok(Message::Item(msg)) = msg {
            match msg {
                IngestExtractedContent::BeginExtractedContentIngest(payload) => {
                    info!("beginning extraction ingest for task: {}", payload.task_id);
                    ingest_metadata.replace(payload);
                }
                IngestExtractedContent::ExtractedContent(payload) => {
                    if ingest_metadata.is_none() {
                        tracing::error!("received extracted content without header metadata");
                        return;
                    }
                    let _ = state
                        .data_manager
                        .write_extracted_content(ingest_metadata.clone().unwrap(), payload)
                        .await;
                }
                IngestExtractedContent::ExtractedFeatures(payload) => {
                    if ingest_metadata.is_none() {
                        tracing::error!("received extracted features without header metadata");
                        return;
                    }
                    if content_metadata.is_none() {
                        content_metadata = Some(
                            state
                                .coordinator_client
                                .get()
                                .await
                                .unwrap()
                                .get_content_metadata(
                                    indexify_coordinator::GetContentMetadataRequest {
                                        content_list: vec![payload.content_id.clone()],
                                    },
                                )
                                .await
                                .unwrap()
                                .into_inner()
                                .content_list
                                .first()
                                .unwrap()
                                .clone(),
                        );
                    }
                    let content_meta = content_metadata.clone().unwrap();
                    let _ = state
                        .data_manager
                        .write_extracted_features(
                            &ingest_metadata.clone().unwrap().extractor,
                            &ingest_metadata.clone().unwrap().extraction_policy,
                            &content_meta,
                            payload.features,
                            ingest_metadata
                                .clone()
                                .unwrap()
                                .output_to_index_table_mapping
                                .clone(),
                        )
                        .await;
                }
                IngestExtractedContent::FinishExtractedContentIngest(_payload) => {
                    if ingest_metadata.is_none() {
                        tracing::error!(
                            "received finished extraction ingest without header metadata"
                        );
                        return;
                    }
                    let _ = state
                        .data_manager
                        .finish_extracted_content_write(ingest_metadata.clone().unwrap())
                        .await;

                    info!(
                        "finished writing extracted content for task: {}",
                        ingest_metadata.clone().unwrap().task_id
                    );
                }
            };
        }
    }
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
    path = "/namespaces/{namespace}/tasks",
    path = "/tasks",
    tag = "indexify",
    responses(
        (status = 200, description = "Lists tasks", body = ListTasksResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to list tasks")
    ),
)]
async fn list_tasks(
    Path(namespace): Path<String>,
    State(state): State<NamespaceEndpointState>,
    Query(query): Query<ListTasks>,
) -> Result<Json<ListTasksResponse>, IndexifyAPIError> {
    let tasks = state
        .coordinator_client
        .get()
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .list_tasks(ListTasksRequest {
            namespace: namespace.clone(),
            extraction_policy: query.extraction_policy.unwrap_or("".to_string()),
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
            mime_type: text.mime_type.clone(),
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
    path = "/namespace/{namespace}/content/{content_id}/metadata",
    tag = "indexify",
    params(MetadataRequest),
    responses(
        (status = 200, description = "List of Events in a namespace", body = MetadataResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to list events in namespace")
    ),
)]
#[tracing::instrument]
async fn get_extracted_metadata(
    Path((namespace, content_id)): Path<(String, String)>,
    State(state): State<NamespaceEndpointState>,
) -> Result<Json<MetadataResponse>, IndexifyAPIError> {
    let extracted_metadata = state
        .data_manager
        .metadata_lookup(&namespace, &content_id)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(MetadataResponse {
        metadata: extracted_metadata.into_iter().map(|r| r.into()).collect(),
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
async fn shutdown_signal(handle: Handle) {
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
    handle.shutdown();
    info!("signal received, shutting down server gracefully");
}
