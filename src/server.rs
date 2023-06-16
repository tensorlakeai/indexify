use crate::data_repository_manager::{DataRepositoryManager, DEFAULT_REPOSITORY_NAME};
use crate::extractors::ExtractorRunner;
use crate::index::IndexManager;
use crate::persistence::{
    ContentType, DataConnector, DataRepository, ExtractorConfig, ExtractorType, Repository,
    SourceType, Text,
};
use crate::text_splitters::TextSplitterKind;
use crate::{EmbeddingRouter, IndexDistance, MemoryManager, Message, ServerConfig};
use strum_macros::{Display, EnumString};

use anyhow::Result;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{extract::State, routing::get, routing::post, Json, Router};
use pyo3::Python;
use tokio::signal;
use tracing::info;

use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;
use std::collections::HashMap;

use std::net::SocketAddr;
use std::sync::Arc;

const DEFAULT_SEARCH_LIMIT: u64 = 5;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "extractor_type")]
enum ApiExtractorType {
    #[serde(rename = "embedding")]
    Embedding {
        model: String,
        distance: ApiIndexDistance,
        text_splitter: ApiTextSplitterKind,
    },
}

impl From<ExtractorType> for ApiExtractorType {
    fn from(value: ExtractorType) -> Self {
        match value {
            ExtractorType::Embedding {
                model,
                text_splitter,
                distance,
            } => ApiExtractorType::Embedding {
                model,
                distance: distance.into(),
                text_splitter: text_splitter.into(),
            },
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, Clone, EnumString, Serialize, Deserialize)]
enum ApiExtractorContentType {
    #[strum(serialize = "text")]
    #[serde(rename = "text")]
    Text,

    #[strum(serialize = "memory")]
    #[serde(rename = "memory")]
    Memory,
}

impl From<ContentType> for ApiExtractorContentType {
    fn from(value: ContentType) -> Self {
        match value {
            ContentType::Text => ApiExtractorContentType::Text,
            ContentType::Memory => ApiExtractorContentType::Memory,
        }
    }
}

impl From<ApiExtractorContentType> for ContentType {
    fn from(val: ApiExtractorContentType) -> Self {
        match val {
            ApiExtractorContentType::Text => ContentType::Text,
            ApiExtractorContentType::Memory => ContentType::Memory,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "extractor")]
struct ApiExtractor {
    pub name: String,
    pub extractor_type: ApiExtractorType,
    pub content_type: ApiExtractorContentType,
}

impl From<ExtractorConfig> for ApiExtractor {
    fn from(value: ExtractorConfig) -> Self {
        Self {
            name: value.name,
            extractor_type: value.extractor_type.into(),
            content_type: value.content_type.into(),
        }
    }
}

impl From<ApiExtractor> for ExtractorConfig {
    fn from(val: ApiExtractor) -> Self {
        ExtractorConfig {
            name: val.name,
            extractor_type: match val.extractor_type {
                ApiExtractorType::Embedding {
                    model,
                    distance,
                    text_splitter,
                } => ExtractorType::Embedding {
                    model,
                    distance: distance.into(),
                    text_splitter: text_splitter.into(),
                },
            },
            content_type: val.content_type.into(),
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
struct ApiDataRepository {
    pub name: String,
    pub extractors: Vec<ApiExtractor>,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl From<DataRepository> for ApiDataRepository {
    fn from(value: DataRepository) -> Self {
        let ap_extractors = value.extractors.into_iter().map(|e| e.into()).collect();
        ApiDataRepository {
            name: value.name,
            extractors: ap_extractors,
            metadata: value.metadata,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "source_type")]
pub enum ApiSourceType {
    // todo: replace metadata with actual request parameters for GoogleContactApi
    #[serde(rename = "google_contact")]
    GoogleContact { metadata: Option<String> },
    // todo: replace metadata with actual request parameters for gmail API
    #[serde(rename = "gmail")]
    Gmail { metadata: Option<String> },
}

impl From<ApiSourceType> for SourceType {
    fn from(value: ApiSourceType) -> Self {
        match value {
            ApiSourceType::GoogleContact { metadata } => SourceType::GoogleContact { metadata },
            ApiSourceType::Gmail { metadata } => SourceType::Gmail { metadata },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "data_connector")]
pub struct ApiDataConnector {
    pub source: ApiSourceType,
}

impl From<ApiDataConnector> for DataConnector {
    fn from(value: ApiDataConnector) -> Self {
        Self {
            source: value.source.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, SmartDefault)]
struct SyncRepository {
    pub name: String,
    pub extractors: Vec<ApiExtractor>,
    pub metadata: HashMap<String, serde_json::Value>,
    pub data_connectors: Vec<ApiDataConnector>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SyncRepositoryResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GetRepository {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GetRepositoryResponse {
    pub repository: ApiDataRepository,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListRepositories {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ListRepositoriesResponse {
    pub repositories: Vec<ApiDataRepository>,
}

#[derive(Debug, Serialize, Deserialize)]
struct GenerateEmbeddingRequest {
    /// Input texts for which embeddings will be generated.
    inputs: Vec<String>,
    /// Name of the model to use for generating embeddings.
    model: String,
}

/// Response payload for generating text embeddings.
#[derive(Debug, Serialize, Deserialize)]
struct GenerateEmbeddingResponse {
    embeddings: Option<Vec<Vec<f32>>>,
}

/// An embedding model and its properties.
#[derive(Debug, Serialize, Deserialize)]
struct EmbeddingModel {
    /// Name of the embedding model.
    name: String,
    /// Number of dimensions in the embeddings generated by this model.
    dimensions: u64,
}

/// Response payload for listing available embedding models.
#[derive(Debug, Serialize, Deserialize)]
struct ListEmbeddingModelsResponse {
    /// List of available embedding models.
    models: Vec<EmbeddingModel>,
}

#[derive(SmartDefault, Debug, Serialize, Deserialize, strum::Display, Clone)]
#[strum(serialize_all = "snake_case")]
enum ApiTextSplitterKind {
    // Do not split text.
    #[serde(rename = "none")]
    None,

    /// Split text by new lines.
    #[default]
    #[serde(rename = "new_line")]
    NewLine,

    /// Split a document across the regex boundary
    #[serde(rename = "regex")]
    Regex { pattern: String },
}

impl From<TextSplitterKind> for ApiTextSplitterKind {
    fn from(value: TextSplitterKind) -> Self {
        match value {
            TextSplitterKind::Noop => ApiTextSplitterKind::None,
            TextSplitterKind::NewLine => ApiTextSplitterKind::NewLine,
            TextSplitterKind::Regex { pattern } => ApiTextSplitterKind::Regex { pattern },
        }
    }
}

impl From<ApiTextSplitterKind> for TextSplitterKind {
    fn from(val: ApiTextSplitterKind) -> Self {
        match val {
            ApiTextSplitterKind::None => TextSplitterKind::Noop,
            ApiTextSplitterKind::NewLine => TextSplitterKind::NewLine,
            ApiTextSplitterKind::Regex { pattern } => TextSplitterKind::Regex { pattern },
        }
    }
}

#[derive(Display, Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename = "distance")]
enum ApiIndexDistance {
    #[serde(rename = "dot")]
    #[strum(serialize = "dot")]
    #[default]
    Dot,

    #[serde(rename = "cosine")]
    #[strum(serialize = "cosine")]
    Cosine,

    #[serde(rename = "euclidean")]
    #[strum(serialize = "euclidean")]
    Euclidean,
}

impl From<ApiIndexDistance> for IndexDistance {
    fn from(value: ApiIndexDistance) -> Self {
        match value {
            ApiIndexDistance::Dot => IndexDistance::Dot,
            ApiIndexDistance::Cosine => IndexDistance::Cosine,
            ApiIndexDistance::Euclidean => IndexDistance::Euclidean,
        }
    }
}

impl From<IndexDistance> for ApiIndexDistance {
    fn from(val: IndexDistance) -> Self {
        match val {
            IndexDistance::Dot => ApiIndexDistance::Dot,
            IndexDistance::Cosine => ApiIndexDistance::Cosine,
            IndexDistance::Euclidean => ApiIndexDistance::Euclidean,
        }
    }
}

/// Request payload for creating a new vector index.
#[derive(Debug, Serialize, Deserialize, Clone)]
struct ExtractorAddRequest {
    repository: Option<String>,
    extractor: ApiExtractor,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct ExtractorAddResponse {}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiText {
    pub text: String,
    pub metadata: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TextAddRequest {
    repository: Option<String>,
    documents: Vec<ApiText>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct IndexAdditionResponse {
    sequence: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct SearchRequest {
    index: String,
    query: String,
    k: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CreateMemorySessionRequest {
    session_id: Option<String>,
    repository: Option<String>,
    extractor: Option<ApiExtractor>,
    metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Serialize, Deserialize)]
struct CreateMemorySessionResponse {
    session_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct MemorySessionAddRequest {
    session_id: String,
    repository: Option<String>,
    messages: Vec<Message>,
}

#[derive(Serialize, Deserialize)]
struct MemorySessionAddResponse {}

#[derive(Debug, Serialize, Deserialize)]
struct MemorySessionRetrieveRequest {
    session_id: String,
    repository: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct MemorySessionRetrieveResponse {
    messages: Vec<Message>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MemorySessionSearchRequest {
    session_id: String,
    repository: Option<String>,
    query: String,
    k: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct MemorySessionSearchResponse {
    messages: Vec<Message>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct DocumentFragment {
    text: String,
    metadata: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct IndexSearchResponse {
    results: Vec<DocumentFragment>,
}

pub struct IndexifyAPIError {
    status_code: StatusCode,
    message: String,
}

impl IndexifyAPIError {
    fn new(status_code: StatusCode, message: String) -> Self {
        Self {
            status_code,
            message,
        }
    }
}

impl IntoResponse for IndexifyAPIError {
    fn into_response(self) -> Response {
        (self.status_code, self.message).into_response()
    }
}

#[derive(Clone)]
pub struct IndexEndpointState {
    index_manager: Arc<IndexManager>,
}

#[derive(Clone)]
pub struct MemoryEndpointState {
    memory_manager: Arc<MemoryManager>,
    extractor_runner: Arc<ExtractorRunner>,
}

pub struct DataSync {}

#[derive(Clone)]
pub struct RepositoryEndpointState {
    repository_manager: Arc<DataRepositoryManager>,
    extractor_worker: Arc<ExtractorRunner>,
}

pub struct Server {
    addr: SocketAddr,
    config: Arc<ServerConfig>,
}
impl Server {
    pub fn new(config: Arc<super::server_config::ServerConfig>) -> Result<Self> {
        let addr: SocketAddr = config.listen_addr.parse()?;
        Ok(Self { addr, config })
    }

    pub async fn run(&self) -> Result<()> {
        let embedding_router = Arc::new(EmbeddingRouter::new(self.config.clone())?);
        let repository = Arc::new(Repository::new(&self.config.db_url).await?);
        let index_manager = Arc::new(IndexManager::new(
            repository.clone(),
            self.config.index_config.clone(),
            embedding_router.clone(),
        )?);
        let extractor_runner = Arc::new(ExtractorRunner::new(
            repository.clone(),
            index_manager.clone(),
        ));
        let repository_manager =
            Arc::new(DataRepositoryManager::new(repository.clone(), index_manager.clone()).await?);
        repository_manager
            .create_default_repository(&self.config)
            .await?;
        let repository_endpoint_state = RepositoryEndpointState {
            repository_manager: repository_manager.clone(),
            extractor_worker: extractor_runner.clone(),
        };
        let memory_manager = Arc::new(
            MemoryManager::new(
                repository_manager.clone(),
                &self.config.default_model().model_kind.to_string(),
            )
            .await?,
        );
        let index_state = IndexEndpointState {
            index_manager: index_manager.clone(),
        };
        let memory_state = MemoryEndpointState {
            memory_manager: memory_manager.clone(),
            extractor_runner: extractor_runner.clone(),
        };
        let app = Router::new()
            .route("/", get(root))
            .route(
                "/embeddings/models",
                get(list_embedding_models).with_state(embedding_router.clone()),
            )
            .route(
                "/embeddings/generate",
                get(generate_embedding).with_state(embedding_router.clone()),
            )
            .route(
                "/repository/add_extractor",
                post(index_create).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/repository/add_text",
                post(add_texts).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/index/search",
                get(index_search).with_state(index_state.clone()),
            )
            .route(
                "/memory/create",
                post(create_memory_session).with_state(memory_state.clone()),
            )
            .route(
                "/memory/add",
                post(add_to_memory_session).with_state(memory_state.clone()),
            )
            .route(
                "/memory/get",
                get(get_from_memory_session).with_state(memory_manager.clone()),
            )
            .route(
                "/memory/search",
                get(search_memory_session).with_state(memory_manager.clone()),
            )
            .route(
                "/repository/sync",
                post(sync_repository).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/repository/list",
                get(list_repositories).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/repository/get",
                get(get_repository).with_state(repository_endpoint_state.clone()),
            );
        info!("server is listening at addr {:?}", &self.addr.to_string());
        axum::Server::bind(&self.addr)
            .serve(app.into_make_service())
            .with_graceful_shutdown(shutdown_signal())
            .await?;
        Ok(())
    }
}

async fn root() -> &'static str {
    "Indexify Server"
}

#[axum_macros::debug_handler]
async fn sync_repository(
    State(state): State<RepositoryEndpointState>,
    Json(payload): Json<SyncRepository>,
) -> Result<Json<SyncRepositoryResponse>, IndexifyAPIError> {
    let extractors = payload
        .extractors
        .clone()
        .into_iter()
        .map(|e| e.into())
        .collect();
    let data_connectors = payload
        .data_connectors
        .clone()
        .into_iter()
        .map(|dc| dc.into())
        .collect();
    let data_repository = &DataRepository {
        name: payload.name.clone(),
        extractors,
        data_connectors,
        metadata: payload.metadata.clone(),
    };
    state
        .repository_manager
        .sync(data_repository)
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to sync repository: {}", e),
            )
        })?;
    Ok(Json(SyncRepositoryResponse {}))
}

async fn list_repositories(
    State(state): State<RepositoryEndpointState>,
    _payload: Option<Json<ListRepositories>>,
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
    let data_repos = repositories.into_iter().map(|r| r.into()).collect();
    Ok(Json(ListRepositoriesResponse {
        repositories: data_repos,
    }))
}

async fn get_repository(
    State(state): State<RepositoryEndpointState>,
    Json(payload): Json<GetRepository>,
) -> Result<Json<GetRepositoryResponse>, IndexifyAPIError> {
    let data_repo = state
        .repository_manager
        .get(&payload.name)
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to get repository: {}", e),
            )
        })?;
    Ok(Json(GetRepositoryResponse {
        repository: data_repo.into(),
    }))
}

#[axum_macros::debug_handler]
async fn index_create(
    State(state): State<RepositoryEndpointState>,
    Json(payload): Json<ExtractorAddRequest>,
) -> Result<Json<ExtractorAddResponse>, IndexifyAPIError> {
    let repository = get_or_default_repository(payload.repository);
    state
        .repository_manager
        .add_extractor(&repository, payload.extractor.into())
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to add extractor: {}", e),
            )
        })?;
    Ok(Json(ExtractorAddResponse {}))
}

#[axum_macros::debug_handler]
async fn add_texts(
    State(state): State<RepositoryEndpointState>,
    Json(payload): Json<TextAddRequest>,
) -> Result<Json<IndexAdditionResponse>, IndexifyAPIError> {
    let repo = get_or_default_repository(payload.repository);
    let texts = payload
        .documents
        .iter()
        .map(|d| Text {
            text: d.text.to_owned(),
            metadata: d.metadata.to_owned(),
        })
        .collect();
    state
        .repository_manager
        .add_texts(&repo, texts, None)
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::BAD_REQUEST,
                format!("failed to add text: {}", e),
            )
        })?;

    state.extractor_worker.sync_repo(&repo).await;
    Ok(Json(IndexAdditionResponse::default()))
}

#[axum_macros::debug_handler]
async fn create_memory_session(
    State(state): State<MemoryEndpointState>,
    Json(payload): Json<CreateMemorySessionRequest>,
) -> Result<Json<CreateMemorySessionResponse>, IndexifyAPIError> {
    let repo = &get_or_default_repository(payload.repository);
    let extractor: Option<ExtractorConfig> = payload.extractor.map(|e| e.into());
    let session_id = state
        .memory_manager
        .create_session(
            repo,
            payload.session_id,
            extractor,
            payload.metadata.unwrap_or_default(),
        )
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    state.extractor_runner.sync_repo(repo).await;

    Ok(Json(CreateMemorySessionResponse { session_id }))
}

#[axum_macros::debug_handler]
async fn add_to_memory_session(
    State(state): State<MemoryEndpointState>,
    Json(payload): Json<MemorySessionAddRequest>,
) -> Result<Json<MemorySessionAddResponse>, IndexifyAPIError> {
    let repo = get_or_default_repository(payload.repository);
    state.memory_manager
        .add_messages(&repo, &payload.session_id, payload.messages)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    state.extractor_runner.sync_repo(&repo).await;

    Ok(Json(MemorySessionAddResponse {}))
}

#[axum_macros::debug_handler]
async fn get_from_memory_session(
    State(memory_manager): State<Arc<MemoryManager>>,
    Json(payload): Json<MemorySessionRetrieveRequest>,
) -> Result<Json<MemorySessionRetrieveResponse>, IndexifyAPIError> {
    let repo = get_or_default_repository(payload.repository);
    let messages = memory_manager
        .retrieve_messages(&repo, payload.session_id)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(MemorySessionRetrieveResponse { messages }))
}

#[axum_macros::debug_handler]
async fn search_memory_session(
    State(memory_manager): State<Arc<MemoryManager>>,
    Json(payload): Json<MemorySessionSearchRequest>,
) -> Result<Json<MemorySessionSearchResponse>, IndexifyAPIError> {
    let repo = get_or_default_repository(payload.repository);
    let messages = memory_manager
        .search(&repo, &payload.session_id, &payload.query, payload.k.unwrap_or(DEFAULT_SEARCH_LIMIT))
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(MemorySessionSearchResponse { messages }))
}

#[axum_macros::debug_handler]
async fn index_search(
    State(state): State<IndexEndpointState>,
    Json(query): Json<SearchRequest>,
) -> Result<Json<IndexSearchResponse>, IndexifyAPIError> {
    let index = state
        .index_manager
        .load(&query.index)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let results = index.search(&query.query, query.k.unwrap_or(DEFAULT_SEARCH_LIMIT)).await;
    if let Err(err) = results {
        return Err(IndexifyAPIError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            err.to_string(),
        ));
    }

    let document_fragments: Vec<DocumentFragment> = results
        .unwrap()
        .iter()
        .map(|text| DocumentFragment {
            text: text.text.to_owned(),
            metadata: text.metadata.to_owned(),
        })
        .collect();
    Ok(Json(IndexSearchResponse {
        results: document_fragments,
    }))
}

#[axum_macros::debug_handler]
async fn list_embedding_models(
    State(embedding_router): State<Arc<EmbeddingRouter>>,
) -> Json<ListEmbeddingModelsResponse> {
    let model_names = embedding_router.list_models();
    let mut models: Vec<EmbeddingModel> = Vec::new();
    for model_name in model_names {
        let model = embedding_router.get_model(&model_name).unwrap();
        models.push(EmbeddingModel {
            name: model_name.clone(),
            dimensions: model.dimensions(),
        })
    }
    Json(ListEmbeddingModelsResponse { models })
}

#[axum_macros::debug_handler]
async fn generate_embedding(
    State(embedding_router): State<Arc<EmbeddingRouter>>,
    Json(payload): Json<GenerateEmbeddingRequest>,
) -> Result<Json<GenerateEmbeddingResponse>, IndexifyAPIError> {
    let try_embedding_generator = embedding_router.get_model(&payload.model);
    if let Err(err) = &try_embedding_generator {
        return Err(IndexifyAPIError::new(
            StatusCode::NOT_ACCEPTABLE,
            err.to_string(),
        ));
    }
    let embeddings = try_embedding_generator
        .unwrap()
        .generate_embeddings(payload.inputs)
        .await;

    if let Err(err) = embeddings {
        return Err(IndexifyAPIError::new(
            StatusCode::EXPECTATION_FAILED,
            err.to_string(),
        ));
    }

    Ok(Json(GenerateEmbeddingResponse {
        embeddings: Some(embeddings.unwrap()),
    }))
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
            let _ = Python::with_gil(|py| py.check_signals());
        },
        _ = terminate => {
            let _ = Python::with_gil(|py| py.check_signals());
        },
    }
    info!("signal received, shutting down server gracefully");
}

fn get_or_default_repository(repo: Option<String>) -> String {
    repo.unwrap_or(DEFAULT_REPOSITORY_NAME.into())
}
