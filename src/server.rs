use std::{collections::HashMap, net::SocketAddr, str::FromStr, sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Multipart, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
    Extension,
    Json,
    Router,
};
use axum_otel_metrics::HttpMetricsLayerBuilder;
use axum_server::{tls_rustls::RustlsConfig, Handle};
use axum_tracing_opentelemetry::middleware::OtelAxumLayer;
use axum_typed_websockets::WebSocketUpgrade;
use hyper::{header::CONTENT_TYPE, Method};
use indexify_internal_api as internal_api;
use indexify_proto::indexify_coordinator::{
    self,
    GcTaskAcknowledgement,
    ListStateChangesRequest,
    ListTasksRequest,
};
use indexify_ui::Assets as UiAssets;
use mime::Mime;
use prometheus::Encoder;
use tokio::{
    signal,
    sync::{mpsc, watch},
};
use tokio_stream::StreamExt;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;
use utoipa::{
    openapi::{self, InfoBuilder, OpenApiBuilder},
    OpenApi,
    ToSchema,
};
use utoipa_rapidoc::RapiDoc;
use utoipa_redoc::{Redoc, Servable};
use utoipa_swagger_ui::SwaggerUi;

use crate::{
    api::{self, *},
    blob_storage::{BlobStorage, ContentReader},
    caching::caches_extension::Caches,
    coordinator_client::CoordinatorClient,
    data_manager::DataManager,
    ingest_extracted_content::IngestExtractedContentState,
    metadata_storage::{self, MetadataReaderTS, MetadataStorageTS},
    metrics,
    server_config::ServerConfig,
    tls::build_mtls_config,
    vector_index::VectorIndexManager,
    vectordbs,
};

const DEFAULT_SEARCH_LIMIT: u64 = 5;

#[derive(Clone, Debug)]
pub struct NamespaceEndpointState {
    pub data_manager: Arc<DataManager>,
    pub coordinator_client: Arc<CoordinatorClient>,
    pub content_reader: Arc<ContentReader>,
    pub registry: Arc<prometheus::Registry>,
    pub metrics: Arc<metrics::server::Metrics>,
}

#[derive(OpenApi)]
#[openapi(
        paths(
            create_namespace,
            list_namespaces,
            get_namespace,
            list_indexes,
            list_extractors,
            list_executors,
            list_content,
            get_content_metadata,
            list_state_changes,
            list_extraction_graphs,
            upload_file,
            list_tasks,
            index_search,
        ),
        components(
            schemas(CreateNamespace, CreateNamespaceResponse, IndexDistance,
                TextAddRequest, TextAdditionResponse, Text, IndexSearchResponse,
                DocumentFragment, ListIndexesResponse, ExtractorOutputSchema, Index, SearchRequest, ListNamespacesResponse, ListExtractorsResponse
            , ExtractorDescription, DataNamespace, ExtractionPolicy, ExtractionPolicyRequest, ExtractionPolicyResponse, Executor,
            MetadataResponse, ExtractedMetadata, ListExecutorsResponse, EmbeddingSchema, ExtractResponse, ExtractRequest,
            Feature, FeatureType, GetContentMetadataResponse, ListTasksResponse,  Task, ExtractionGraph,
            Content, ContentMetadata, ListContentResponse, GetNamespaceResponse, ExtractionPolicyResponse,
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

    pub async fn run(&self, registry: Arc<prometheus::Registry>) -> Result<()> {
        // let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

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
        let coordinator_client = Arc::new(CoordinatorClient::new(Arc::clone(&self.config)));
        let vector_index_manager = Arc::new(
            VectorIndexManager::new(coordinator_client.clone(), vector_db.clone())
                .map_err(|e| anyhow!("unable to create vector index {}", e))?,
        );
        let metadata_index_manager: MetadataStorageTS =
            metadata_storage::from_config(&self.config.metadata_storage)?;
        let metadata_reader: MetadataReaderTS =
            metadata_storage::from_config_reader(&self.config.metadata_storage)?;
        let blob_storage = Arc::new(BlobStorage::new_with_config(
            self.config.blob_storage.clone(),
        ));
        let data_manager = Arc::new(DataManager::new(
            vector_index_manager,
            metadata_index_manager,
            metadata_reader,
            blob_storage.clone(),
            coordinator_client.clone(),
        ));
        let ingestion_server_id = nanoid::nanoid!(16);

        self.start_gc_tasks_stream(
            coordinator_client.clone(),
            &ingestion_server_id,
            data_manager.clone(),
            shutdown_rx.clone(),
        );
        let namespace_endpoint_state = NamespaceEndpointState {
            data_manager: data_manager.clone(),
            coordinator_client: coordinator_client.clone(),
            content_reader: Arc::new(ContentReader::new(self.config.clone())),
            registry,
            metrics: Arc::new(crate::metrics::server::Metrics::new()),
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
                "/namespaces/:namespace/openapi.json",
                get(namespace_open_api).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/extraction_graphs",
                post(create_extraction_graph).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/extraction_graphs",
                get(list_extraction_graphs).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/extraction_graphs/:extraction_graph/extract",
                post(upload_file).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/extraction_graphs/:extraction_graph/extract_remote",
                post(ingest_remote_file).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/extraction_graphs/:extraction_graph/content",
                get(list_content).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/extraction_graphs/:extraction_graph/extraction_policies/:extraction_policy/tasks",
                get(list_tasks).with_state(namespace_endpoint_state.clone()),
            )
            .route("/namespaces/:namespace/content/:content_id/download",
                get(download_content).with_state(namespace_endpoint_state.clone()))
            .route("/namespaces/:namespace/extraction_graphs/:extraction_graph/extraction_policies/:extraction_policy/content/:content_id",
                get(get_content_tree_metadata).with_state(namespace_endpoint_state.clone()))
            .route(
                "/namespaces/:namespace/extraction_graphs/:graph/links",
                post(link_extraction_graphs).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/extraction_graphs/:graph/links",
                get(extraction_graph_links).with_state(namespace_endpoint_state.clone()),
            )
            .route("/namespaces/:namespace/sql_query",
                post(run_sql_query).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/indexes",
                get(list_indexes).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/indexes/:index/search",
                post(index_search).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/active_content",
                get(active_content).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/content/:content_id/metadata",
                get(get_content_metadata).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/content/:content_id/labels",
                put(update_labels).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/content/:content_id/wait",
                get(wait_content_extraction).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/content/:content_id",
                put(update_content)
                    .with_state(namespace_endpoint_state.clone())
                    .clone(),
            )
            .route(
                "/namespaces/:namespace/content",
                delete(delete_content).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/namespaces/:namespace/schemas",
                get(list_schemas).with_state(namespace_endpoint_state.clone()),
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
                "/task_assignments",
                get(list_task_assignments).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/metrics/raft",
                get(get_raft_metrics_snapshot).with_state(namespace_endpoint_state.clone()),
            )
            .route(
                "/metrics/ingest",
                get(ingest_metrics).with_state(namespace_endpoint_state.clone()),
            )
            .route("/ui", get(ui_index_handler))
            .route("/ui/*rest", get(ui_handler))
            .layer(OtelAxumLayer::default())
            .layer(metrics)
            .layer(Extension(caches))
            .layer(cors)
            .layer(DefaultBodyLimit::disable())
            .layer(tower_http::trace::TraceLayer::new_for_http());

        let handle = Handle::new();

        let handle_sh = handle.clone();
        tokio::spawn(async move {
            shutdown_signal(handle_sh).await;
            info!("received graceful shutdown signal. Telling tasks to shutdown");

            let _ = shutdown_tx.send(true);
        });

        // Create the default namespace. It's idempotent so we can keep trying
        while let Err(err) = data_manager
            .create_namespace(&DataNamespace {
                name: "default".to_string(),
            })
            .await
        {
            info!("failed to create default namespace: {}", err);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

            if *shutdown_rx.borrow() {
                info!("shutting down create namespace loop");
                break;
            }
        }

        let handle = handle.clone();
        if use_tls {
            if let Some(tls_config) = self.config.tls.clone() {
                let config = build_mtls_config(&tls_config)?;
                let rustls_config = RustlsConfig::from_config(config);
                axum_server::tls_rustls::bind_rustls(self.addr, rustls_config)
                    .handle(handle)
                    .serve(app.into_make_service())
                    .await?;
            } else {
                return Err(anyhow!("TLS is enabled but no TLS config provided"));
            }
        } else {
            let handle = handle.clone();
            axum_server::bind(self.addr)
                .handle(handle)
                .serve(app.into_make_service())
                .await?;
        }

        Ok(())
    }

    pub fn start_gc_tasks_stream(
        &self,
        coordinator_client: Arc<CoordinatorClient>,
        ingestion_server_id: &str,
        data_manager: Arc<DataManager>,
        shutdown_rx: watch::Receiver<bool>,
    ) {
        let mut attempt = 0;
        let delay = 2;
        let ingestion_server_id = ingestion_server_id.to_string();
        let coordinator_addr = self.config.coordinator_addr.clone();

        tokio::spawn(async move {
            loop {
                let client_result = coordinator_client.get().await;
                if let Err(e) = client_result {
                    attempt += 1;
                    tracing::error!(
                        "Attempt {}: Unable to connect to the coordinator: {}",
                        attempt,
                        e
                    );
                    tokio::time::sleep(Duration::from_secs(delay)).await;
                    continue;
                }

                let mut client = client_result.unwrap();

                let (ack_tx, mut ack_rx) = mpsc::channel(4);

                let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
                let heartbeat = GcTaskAcknowledgement {
                    task_id: "".to_string(),
                    completed: false,
                    ingestion_server_id: ingestion_server_id.clone(),
                };
                let request = tonic::Request::new(async_stream::stream! {
                    loop {
                        tokio::select! {
                            _ = interval.tick() => {
                                yield heartbeat.clone();
                            },
                            ack = ack_rx.recv() => {
                                if let Some(ack) = ack {
                                    yield ack;
                                }
                            }
                        }
                    }
                });

                match client.gc_tasks_stream(request).await {
                    Ok(response) => {
                        let mut stream = response.into_inner();

                        while let Ok(Some(command)) = stream.message().await {
                            if let Some(gc_task) = command.gc_task {
                                if let Err(e) = data_manager.perform_gc_task(&gc_task).await {
                                    tracing::error!(
                                        "Failed to delete content for task {:?}: {}",
                                        gc_task,
                                        e
                                    );
                                    continue;
                                }
                                if let Err(e) = ack_tx
                                    .send(GcTaskAcknowledgement {
                                        task_id: gc_task.task_id.clone(),
                                        completed: true,
                                        ingestion_server_id: ingestion_server_id.clone(),
                                    })
                                    .await
                                {
                                    tracing::error!(
                                        "Failed to send ack for task {:?}: {}",
                                        gc_task,
                                        e
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to start gc_tasks_stream: {}, address: {}, retrying...",
                            e,
                            coordinator_addr.clone()
                        );
                        attempt += 1;
                        tokio::time::sleep(Duration::from_secs(delay)).await;

                        if *shutdown_rx.borrow() {
                            tracing::info!("shutting down gc_tasks_stream loop");
                            break;
                        }

                        continue;
                    }
                }

                if *shutdown_rx.borrow() {
                    tracing::info!("shutting down gc_tasks_stream loop");
                    break;
                }
            }
        });
    }
}

#[tracing::instrument]
async fn root() -> &'static str {
    "Indexify Server"
}

/// Create a new namespace
#[tracing::instrument]
#[axum::debug_handler]
#[utoipa::path(
    post,
    path = "/namespaces",
    request_body = CreateNamespace,
    tag = "ingestion",
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
    };
    state
        .data_manager
        .create_namespace(&data_namespace)
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("failed to sync namespace: {}", e),
            )
        })?;
    Ok(Json(CreateNamespaceResponse {}))
}

/// List all namespaces registered on the server
#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/namespaces",
    tag = "ingestion",
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
            &format!("failed to list namespaces: {}", e),
        )
    })?;
    let data_namespaces: Vec<DataNamespace> = namespaces.into_iter().collect();
    Ok(Json(ListNamespacesResponse {
        namespaces: data_namespaces,
    }))
}

async fn namespace_open_api(
    Path(namespace): Path<String>,
    State(state): State<NamespaceEndpointState>,
) -> Result<Json<openapi::OpenApi>, IndexifyAPIError> {
    let extraction_graphs = state
        .data_manager
        .list_extraction_graphs(&namespace)
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("failed to get namespace: {}", e),
            )
        })?;
    let mut builder = OpenApiBuilder::default();
    let info = InfoBuilder::default()
        .title("Indexify API")
        .version(env!("CARGO_PKG_VERSION"))
        .build();
    builder = builder.info(info);
    let mut paths = utoipa::openapi::PathsBuilder::default();
    for eg in extraction_graphs {
        let response = utoipa::openapi::response::ResponseBuilder::default()
            .description("content uploaded successfully")
            .content(
                "application/json",
                utoipa::openapi::content::ContentBuilder::default()
                    .schema(utoipa::openapi::Ref::from_schema_name("UploadFileResponse"))
                    .build(),
            )
            .build();
        let operation = utoipa::openapi::path::OperationBuilder::default()
            .summary(Some(format!(
                "upload and extract content using graph '{}'",
                eg.name
            )))
            .request_body(Some(
                utoipa::openapi::request_body::RequestBodyBuilder::default()
                    .description(Some("content to be uploaded and extracted"))
                    .required(Some(openapi::Required::True))
                    .build(),
            ))
            .response("200", response)
            .response(
                "500",
                utoipa::openapi::response::ResponseBuilder::new()
                    .description("Internal Server Error")
                    .build(),
            )
            .parameter(
                openapi::path::ParameterBuilder::default()
                    .name("id")
                    .parameter_in(openapi::path::ParameterIn::Query)
                    .description(Some("id of the content"))
                    .required(openapi::Required::False)
                    .build(),
            )
            .description(eg.description);
        let item = utoipa::openapi::path::PathItemBuilder::default()
            .operation(utoipa::openapi::path::PathItemType::Post, operation.build())
            .build();
        paths = paths.path(
            format!(
                "/namespaces/{}/extraction_graphs/{}/extract",
                namespace, eg.name
            ),
            item,
        );
    }
    let components = utoipa::openapi::ComponentsBuilder::default()
        .schema_from::<UploadFileResponse>()
        .schema_from::<UploadFileQueryParams>()
        .build();
    builder = builder.paths(paths.build()).components(Some(components));
    let openapi = builder.build();
    Ok(Json(openapi))
}

/// Get metadata for a specific namespace
#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}",
    tag = "ingestion",
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
            &format!("failed to get namespace: {}", e),
        )
    })?;
    Ok(Json(GetNamespaceResponse {
        namespace: data_namespace,
    }))
}

// Create a new extraction graph in the namespace
#[utoipa::path(
    post,
    path = "/namespace/{namespace}/extraction_graph",
    request_body = ExtractionGraphRequest,
    tag = "ingestion",
    responses(
        (status = 200, description = "Extraction graph added successfully", body = ExtractionGraphResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to add extraction graph to namespace")
    ),
)]
#[axum::debug_handler]
async fn create_extraction_graph(
    // FIXME: this throws a 500 when the binding already exists
    // FIXME: also throws a 500 when the index name already exists
    Path(namespace): Path<String>,
    State(state): State<NamespaceEndpointState>,
    Json(payload): Json<ExtractionGraphRequest>,
) -> Result<Json<ExtractionGraphResponse>, IndexifyAPIError> {
    let indexes = state
        .data_manager
        .create_extraction_graph(&namespace, payload)
        .await
        .map_err(IndexifyAPIError::internal_error)?
        .into_iter()
        .collect();
    Ok(Json(ExtractionGraphResponse { indexes }))
}

#[utoipa::path(
    post,
    path = "/namespace/{namespace}/extraction_graphs/{graph}/links",
    request_body = ExtractionGraphLink,
    tag = "ingestion",
    responses(
        (status = 200, description = "Extraction graphs linked successfully"),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to link extraction graphs")
    ),
)]
#[axum::debug_handler]
async fn link_extraction_graphs(
    Path((namespace, graph_name)): Path<(String, String)>,
    State(state): State<NamespaceEndpointState>,
    Json(payload): Json<ExtractionGraphLink>,
) -> Result<(), IndexifyAPIError> {
    state
        .data_manager
        .link_extraction_graphs(namespace, graph_name, payload)
        .await
        .map_err(IndexifyAPIError::internal_error)
}

#[utoipa::path(
    get,
    path = "/namespace/{namespace}/extraction_graphs/{graph}/links",
    tag = "ingestion",
    responses(
        (status = 200, description = "List of extraction graph links", body = ExtractionGraphLinksResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to list links")
    ),
)]
#[axum::debug_handler]
async fn extraction_graph_links(
    Path((namespace, graph_name)): Path<(String, String)>,
    State(state): State<NamespaceEndpointState>,
) -> Result<Json<Vec<ExtractionGraphLink>>, IndexifyAPIError> {
    let res = state
        .data_manager
        .extraction_graph_links(namespace, graph_name)
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    Ok(Json(res))
}

#[axum::debug_handler]
async fn ingest_remote_file(
    Path(namespace): Path<String>,
    State(state): State<NamespaceEndpointState>,
    Json(payload): Json<IngestRemoteFile>,
) -> Result<Json<IngestRemoteFileResponse>, IndexifyAPIError> {
    let content_id = state
        .data_manager
        .ingest_remote_file(
            &namespace,
            payload.id,
            &payload.url,
            &payload.mime_type,
            payload.labels,
            &payload.extraction_graph_names,
        )
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::BAD_REQUEST,
                &format!("failed to add text: {}", e),
            )
        })?;
    Ok(Json(IngestRemoteFileResponse { content_id }))
}

#[tracing::instrument]
#[utoipa::path(
    put,
    path = "/namespaces/{namespace}/content/{content_id}/labels",
    request_body = UpdateLabelsRequest,
    tag = "indexify",
    responses(
        (status = 200, description = "Labels updated successfully"),
        (status = BAD_REQUEST, description = "Unable to update labels")
    ),
)]
#[axum::debug_handler]
async fn update_labels(
    Path((namespace, content_id)): Path<(String, String)>,
    State(state): State<NamespaceEndpointState>,
    Json(body): Json<UpdateLabelsRequest>,
) -> Result<(), IndexifyAPIError> {
    state
        .data_manager
        .update_labels(&namespace, &content_id, body.labels)
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    Ok(())
}

/// List all the content ingested into an extraction graph
#[tracing::instrument]
#[utoipa::path(
    get,
    path= "/namespaces/{namespace}/extraction_graphs/{extraction_graph}/content",
    tag = "retrieval",
    responses(
        (status = 200, description = "Lists the contents in the namespace", body = ListContentResponse),
        (status = BAD_REQUEST, description = "Unable to list contents")
    ),
)]
#[axum::debug_handler]
async fn list_content(
    Path((namespace, extraction_graph)): Path<(String, String)>,
    State(state): State<NamespaceEndpointState>,
    filter: Query<super::api::ListContent>,
) -> Result<Json<ListContentResponse>, IndexifyAPIError> {
    let response = state
        .data_manager
        .list_content(
            &namespace,
            &extraction_graph,
            &filter.source,
            &filter.parent_id,
            filter.labels_eq.as_ref(),
            filter.start_id.clone().unwrap_or_default(),
            filter.limit.unwrap_or(10),
            filter.return_total,
        )
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    Ok(Json(response))
}

#[tracing::instrument]
#[utoipa::path(
    delete,
    path = "/namespaces/{namespace}/content",
    tag = "indexify",
    responses(
        (status = 200, description = "Deletes specified pieces of content", body = DeleteContentResponse),
        (status = BAD_REQUEST, description = "Unable to find a piece of content to delete")
    ),
)]
#[axum::debug_handler]
async fn delete_content(
    Path(namespace): Path<String>,
    State(state): State<NamespaceEndpointState>,
    Json(body): Json<super::api::TombstoneContentRequest>,
) -> Result<Json<()>, IndexifyAPIError> {
    let request = indexify_coordinator::TombstoneContentRequest {
        namespace: namespace.clone(),
        content_ids: body.content_ids.clone(),
    };

    state
        .coordinator_client
        .get()
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to get coordinator client: {}", e).as_str(),
            )
        })?
        .tombstone_content(request)
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to delete content: {}", e).as_str(),
            )
        })?;

    Ok(Json(()))
}

/// Get content metadata for a specific content id
#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/content/{content_id}",
    tag = "retrieval",
    responses(
        (status = 200, description = "Reads a specific content in the namespace", body = GetContentMetadataResponse),
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
        .map_err(IndexifyAPIError::internal_error)?;
    let content_metadata = content_list
        .first()
        .ok_or_else(|| IndexifyAPIError::new(StatusCode::NOT_FOUND, "content not found"))?;

    Ok(Json(GetContentMetadataResponse {
        content_metadata: content_metadata.clone(),
    }))
}

#[tracing::instrument]
#[utoipa::path(
    get,
    path ="/namespaces/{namespace}/content/{content_id}/wait",
    tag = "indexify",
    responses(
        (status = 200, description = "wait for all extraction tasks for content to complete"),
    ),
)]
#[axum::debug_handler]
async fn wait_content_extraction(
    Path((namespace, content_id)): Path<(String, String)>,
    State(state): State<NamespaceEndpointState>,
) -> Result<(), IndexifyAPIError> {
    state
        .data_manager
        .wait_content_extraction(&content_id)
        .await
        .map_err(IndexifyAPIError::internal_error)
}

#[tracing::instrument]
#[utoipa::path(
    get,
    path ="/namespaces/{namespace}/active_content",
    tag = "indexify",
    responses(
        (status = 200, description = "wait for all extraction tasks for content to complete"),
    ),
)]
#[axum::debug_handler]
async fn active_content(
    Path(namespace): Path<String>,
    State(state): State<NamespaceEndpointState>,
) -> Result<Json<Vec<String>>, IndexifyAPIError> {
    let res = state
        .data_manager
        .list_active_contents(&namespace)
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    Ok(Json(res))
}

#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/{extraction_graph}/content/{content_id}/{extraction_policy}",
    tag = "retrieval",
    responses(
        (status = 200, description = "Gets a content tree rooted at a specific content id in the namespace"),
        (status = BAD_REQUEST, description = "Unable to read content tree")
    )
)]
#[axum::debug_handler]
async fn get_content_tree_metadata(
    Path((namespace, extraction_graph, extraction_policy, content_id)): Path<(
        String,
        String,
        String,
        String,
    )>,
    State(state): State<NamespaceEndpointState>,
) -> Result<Json<GetContentTreeMetadataResponse>, IndexifyAPIError> {
    let content_tree_metadata = state
        .data_manager
        .get_content_tree_metadata(
            &namespace,
            &content_id,
            &extraction_graph,
            &extraction_policy,
        )
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    Ok(Json(GetContentTreeMetadataResponse {
        content_tree_metadata,
    }))
}

#[axum::debug_handler]
async fn download_content(
    Path((namespace, content_id)): Path<(String, String)>,
    State(state): State<NamespaceEndpointState>,
) -> Result<Response<Body>, IndexifyAPIError> {
    let content_list = state
        .data_manager
        .get_content_metadata(&namespace, vec![content_id])
        .await;
    let content_list = content_list.map_err(IndexifyAPIError::internal_error)?;
    let content_metadata = content_list
        .first()
        .ok_or(anyhow!("content not found"))
        .map_err(|e| IndexifyAPIError::not_found(&e.to_string()))?
        .clone();
    let mut resp_builder =
        Response::builder().header("Content-Type", content_metadata.mime_type.clone());
    if content_metadata.size > 0 {
        resp_builder = resp_builder.header("Content-Length", content_metadata.size);
    }

    let storage_reader = state.content_reader.get(&content_metadata.storage_url);
    let content_stream = storage_reader
        .get(&content_metadata.storage_url)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()))?;

    resp_builder
        .body(Body::from_stream(content_stream))
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()))
}

#[derive(Debug, serde::Deserialize, ToSchema)]
struct UploadFileQueryParams {
    id: Option<String>,
}

/// List all extraction graphs in a namespace
#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/extraction_graphs",
    tag = "ingestion",
    responses(
        (status = 200, description = "List of Extraction Graphs registered on the server", body = ListExtractionGraphResponse),
        (status = BAD_REQUEST, description = "Unable to list extraction graphs")
    ),
)]
async fn list_extraction_graphs(
    Path(namespace): Path<String>,
    State(state): State<NamespaceEndpointState>,
) -> Result<Json<ListExtractionGraphResponse>, IndexifyAPIError> {
    let graphs = state
        .data_manager
        .list_extraction_graphs(&namespace)
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("failed to list extraction graphs: {}", e),
            )
        })?;
    Ok(Json(ListExtractionGraphResponse {
        extraction_graphs: graphs,
    }))
}

/// Upload a file to an extraction graph in a namespace
#[tracing::instrument(skip(state))]
#[utoipa::path(
    post,
    path = "/namespaces/{namespace}/extraction_graphs/{extraction_graph}/extract",
    request_body(content_type = "multipart/form-data", content = Vec<u8>),
    tag = "ingestion",
    responses(
        (status = 200, description = "Uploads a file to the namespace"),
        (status = BAD_REQUEST, description = "Unable to upload file")
    ),
)]
#[axum::debug_handler]
async fn upload_file(
    Path((namespace, extraction_graph)): Path<(String, String)>,
    State(state): State<NamespaceEndpointState>,
    Query(params): Query<UploadFileQueryParams>,
    mut files: Multipart,
) -> Result<Json<UploadFileResponse>, IndexifyAPIError> {
    let mut labels: HashMap<String, serde_json::Value> = HashMap::new();

    let id = params.id.clone().unwrap_or_else(DataManager::make_id);
    if !DataManager::is_hex_string(&id) {
        return Err(IndexifyAPIError::new(
            StatusCode::BAD_REQUEST,
            "Invalid ID format, ID must be a hex string",
        ));
    }

    //  check if the id already exists for content metadata
    let retrieved_content = state
        .data_manager
        .get_content_metadata(&namespace, vec![id.clone()])
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    if !retrieved_content.is_empty() {
        return Err(IndexifyAPIError::new(
            StatusCode::BAD_REQUEST,
            "content with the provided id already exists",
        ));
    }

    while let Some(field) = files.next_field().await.unwrap() {
        if let Some(name) = field.file_name() {
            info!("user provided file name = {:?}", name);
            let ext = std::path::Path::new(&name)
                .extension()
                .unwrap_or_default()
                .to_str()
                .unwrap_or_default();
            let name = nanoid::nanoid!(16);
            let name = if !ext.is_empty() {
                format!("{}.{}", name, ext)
            } else {
                name
            };
            let content_mime = labels.get("mime_type").map(|v| v.as_str()).flatten();
            let content_mime = content_mime.map(|v| Mime::from_str(v).unwrap());
            let content_mime =
                content_mime.unwrap_or(mime_guess::from_ext(ext).first_or_octet_stream());
            info!("writing to blob store, file name = {:?}", name);

            let stream = field.map(|res| res.map_err(|err| anyhow::anyhow!(err)));
            let content_metadata = state
                .data_manager
                .upload_file(
                    &namespace,
                    stream,
                    &name,
                    content_mime,
                    labels,
                    Some(&id),
                    vec![extraction_graph],
                )
                .await
                .map_err(|e| {
                    IndexifyAPIError::new(
                        StatusCode::BAD_REQUEST,
                        &format!("failed to upload file: {}", e),
                    )
                })?;
            let size_bytes = content_metadata.size_bytes;
            state
                .data_manager
                .create_content_metadata(content_metadata)
                .await
                .map_err(|e| {
                    IndexifyAPIError::new(
                        StatusCode::BAD_REQUEST,
                        &format!("failed to create content for file: {}", e),
                    )
                })?;
            state.metrics.node_content_uploads.add(1, &[]);
            state
                .metrics
                .node_content_bytes_uploaded
                .add(size_bytes, &[]);
            return Ok(Json(UploadFileResponse { content_id: id }));
        } else if let Some(name) = field.name() {
            if name != "labels" {
                continue;
            }

            labels = serde_json::from_str(&field.text().await.unwrap()).map_err(|e| {
                IndexifyAPIError::new(
                    StatusCode::BAD_REQUEST,
                    &format!("failed to upload file: {}", e),
                )
            })?;
        }
    }
    Err(IndexifyAPIError::new(
        StatusCode::BAD_REQUEST,
        "no file provided",
    ))
}

#[tracing::instrument]
#[utoipa::path(
    put,
    path = "/namespaces/{namespace}/content/:content_id",
    tag = "ingestion",
    responses(
        (status = 200, description = "Updates a specified piece of content", body = UpdateContentResponse),
        (status = BAD_REQUEST, description = "Unable to find a piece of content to update")
    ),
)]
#[axum::debug_handler]
async fn update_content(
    Path((namespace, content_id)): Path<(String, String)>,
    State(state): State<NamespaceEndpointState>,
    mut files: Multipart,
) -> Result<(), IndexifyAPIError> {
    //  check that the content exists
    let content_metadata = state
        .data_manager
        .get_content_metadata(&namespace, vec![content_id.clone()])
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    let content_metadata = content_metadata
        .first()
        .ok_or_else(|| IndexifyAPIError::not_found(&format!("content {} not found", content_id)))?;

    while let Some(file) = files.next_field().await.unwrap() {
        let name = file
            .file_name()
            .ok_or(IndexifyAPIError::new(
                StatusCode::BAD_REQUEST,
                "file_name is not present",
            ))?
            .to_string();
        info!("user provided file name = {:?}", name);
        let ext = std::path::Path::new(&name)
            .extension()
            .unwrap_or_default()
            .to_str()
            .unwrap_or_default();
        let name = nanoid::nanoid!(16);
        let name = if !ext.is_empty() {
            format!("{}.{}", name, ext)
        } else {
            name
        };
        let content_mime = mime_guess::from_ext(ext).first_or_octet_stream();
        info!("writing to blob store, file name = {:?}", name);

        let stream = file.map(|res| res.map_err(|err| anyhow::anyhow!(err)));
        let new_content_metadata = state
            .data_manager
            .upload_file(
                &namespace,
                stream,
                &name,
                content_mime,
                content_metadata.labels.clone(),
                Some(&content_metadata.id),
                vec![],
            )
            .await
            .map_err(|e| {
                IndexifyAPIError::new(
                    StatusCode::BAD_REQUEST,
                    &format!("failed to upload file: {}", e),
                )
            })?;

        if new_content_metadata.hash == content_metadata.hash {
            info!("the content received is the same, not creating content metadata and removing the created file");
            let _ = state
                .data_manager
                .delete_file(&new_content_metadata.storage_url)
                .await
                .map_err(|e| {
                    tracing::error!("failed to delete file: {}", e);
                });
            return Ok(());
        }

        state
            .data_manager
            .create_content_metadata(new_content_metadata)
            .await
            .map_err(|e| {
                IndexifyAPIError::new(
                    StatusCode::BAD_REQUEST,
                    &format!("failed to create content for file: {}", e),
                )
            })?;
    }

    Ok(())
}

async fn ingest_extracted_content(
    ws: WebSocketUpgrade<IngestExtractedContentResponse, IngestExtractedContent>,
    State(state): State<NamespaceEndpointState>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| IngestExtractedContentState::new(state).run(socket))
}

/// List all executors running extractors in the cluster
#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/executors",
    tag = "operations",
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

/// List all extractors available in the cluster
#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/extractors",
    tag = "operations",
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
        .map_err(IndexifyAPIError::internal_error)?
        .into_iter()
        .collect();
    Ok(Json(ListExtractorsResponse { extractors }))
}

/// List the state changes in the system
#[utoipa::path(
    get,
    path = "/state_changes",
    tag = "operations",
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
        .map_err(IndexifyAPIError::internal_error)?
        .list_state_changes(ListStateChangesRequest {})
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()))?
        .into_inner()
        .changes;

    let state_changes: Vec<indexify_internal_api::StateChange> = state_changes
        .into_iter()
        .map(|c| c.try_into())
        .filter_map(|c| c.ok())
        .collect();

    Ok(Json(ListStateChangesResponse { state_changes }))
}

/// List Tasks generated for a given content and a given extraction policy
#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/extraction_graphs/{extraction_graph}/extraction_policies/{extraction_policy}/tasks",
    tag = "operations",
    responses(
        (status = 200, description = "Lists tasks", body = ListTasksResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to list tasks")
    ),
)]
async fn list_tasks(
    Path((namespace, extraction_graph, extraction_policy)): Path<(String, String, String)>,
    State(state): State<NamespaceEndpointState>,
    Query(query): Query<ListTasks>,
) -> Result<Json<ListTasksResponse>, IndexifyAPIError> {
    let outcome: indexify_coordinator::TaskOutcomeFilter = query.outcome.into();
    let resp = state
        .coordinator_client
        .get()
        .await
        .map_err(IndexifyAPIError::internal_error)?
        .list_tasks(ListTasksRequest {
            namespace: namespace.clone(),
            extraction_policy,
            start_id: query.start_id.clone().unwrap_or_default(),
            limit: query.limit.unwrap_or(10),
            content_id: query.content_id.unwrap_or_default(),
            return_total: query.return_total,
            outcome: outcome as i32,
        })
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.message()))?
        .into_inner();
    Ok(Json(resp.try_into()?))
}

#[axum::debug_handler]
async fn list_task_assignments(
    State(namespace_endpoint): State<NamespaceEndpointState>,
) -> Result<Json<TaskAssignments>, IndexifyAPIError> {
    let response = namespace_endpoint
        .coordinator_client
        .all_task_assignments()
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    Ok(Json(response))
}

/// List all the indexes in a given namespace
#[tracing::instrument]
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/indexes",
    tag = "retrieval",
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
        .map_err(IndexifyAPIError::internal_error)?
        .into_iter()
        .collect();
    Ok(Json(ListIndexesResponse { indexes }))
}

/// Search a vector index in a namespace
#[utoipa::path(
    post,
    path = "/namespaces/{namespace}/indexes/{index}/search",
    tag = "retrieval",
    responses(
        (status = 200, description = "Index search results", body = IndexSearchResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to search index")
    ),
)]
#[axum::debug_handler]
async fn index_search(
    Path((namespace, index)): Path<(String, String)>,
    State(state): State<NamespaceEndpointState>,
    Json(query): Json<SearchRequest>,
) -> Result<Json<IndexSearchResponse>, IndexifyAPIError> {
    let results = state
        .data_manager
        .search(
            &namespace,
            &index,
            &query.query,
            query.k.unwrap_or(DEFAULT_SEARCH_LIMIT),
            query.filters,
            query.include_content.unwrap_or(true),
        )
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    let document_fragments: Vec<DocumentFragment> = results
        .iter()
        .map(|text| DocumentFragment {
            content_id: text.content_id.clone(),
            mime_type: text.mime_type.clone(),
            text: text.text.clone(),
            labels: text.labels.clone(),
            confidence_score: text.confidence_score,
            root_content_metadata: text.root_content_metadata.clone().map(|r| r.into()),
            content_metadata: text.content_metadata.clone().into(),
        })
        .collect();
    Ok(Json(IndexSearchResponse {
        results: document_fragments,
    }))
}

#[axum::debug_handler]
async fn run_sql_query(
    Path(namespace): Path<String>,
    State(state): State<NamespaceEndpointState>,
    Json(query): Json<SQLQuery>,
) -> Result<Json<SqlQueryResponse>, IndexifyAPIError> {
    let results = state
        .data_manager
        .query_content_source(&namespace, &query.query)
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    let mut json_result = Vec::new();
    for result in results {
        let result_value = serde_json::to_value(&result).map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("failed to serialize result {:?}: error: {}", result, e),
            )
        })?;
        json_result.push(result_value);
    }
    Ok(Json(SqlQueryResponse { rows: json_result }))
}

#[utoipa::path(
    post,
    path = "/namespace/{namespace}/schemas",
    tag = "indexify",
    responses(
        (status = 200, description = "List of Schemas", body = GetStructuredDataSchemasResponse),
        (status = INTERNAL_SERVER_ERROR, description = "List Structured Data Schemas")
    ),
)]
#[axum::debug_handler]
async fn list_schemas(
    Path(namespace): Path<String>,
    State(state): State<NamespaceEndpointState>,
) -> Result<Json<GetStructuredDataSchemasResponse>, IndexifyAPIError> {
    let results = state
        .coordinator_client
        .get()
        .await
        .map_err(IndexifyAPIError::internal_error)?
        .list_schemas(tonic::Request::new(
            indexify_coordinator::GetAllSchemaRequest {
                namespace: namespace.clone(),
            },
        ))
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()))?;
    let results = results.into_inner().schemas;
    let mut ddls = HashMap::new();
    let mut schemas = Vec::new();
    for schema in results {
        let columns = serde_json::from_str(&schema.columns).map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("failed to parse schema columns: {} {}", schema.columns, e),
            )
        })?;

        let schema = internal_api::StructuredDataSchema {
            id: internal_api::StructuredDataSchema::schema_id(
                &namespace,
                &schema.extraction_graph_name,
            ),
            extraction_graph_name: schema.extraction_graph_name,
            namespace: namespace.clone(),
            columns,
        };

        ddls.insert(schema.extraction_graph_name.to_string(), schema.to_ddl());
        schemas.push(schema);
    }

    Ok(Json(GetStructuredDataSchemasResponse { schemas, ddls }))
}

#[axum::debug_handler]
#[tracing::instrument]
async fn get_raft_metrics_snapshot(
    State(state): State<NamespaceEndpointState>,
) -> Result<Json<RaftMetricsSnapshotResponse>, IndexifyAPIError> {
    state.coordinator_client.get_raft_metrics_snapshot().await
}

#[axum::debug_handler]
#[tracing::instrument]
async fn ingest_metrics(
    State(state): State<NamespaceEndpointState>,
) -> Result<Response<Body>, IndexifyAPIError> {
    let metric_families = state.registry.gather();
    let mut buffer = vec![];
    let encoder = prometheus::TextEncoder::new();
    encoder.encode(&metric_families, &mut buffer).map_err(|_| {
        IndexifyAPIError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to encode metrics",
        )
    })?;

    Ok(Response::new(Body::from(buffer)))
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
pub async fn shutdown_signal(handle: Handle) {
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
