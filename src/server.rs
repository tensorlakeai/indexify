use crate::data_repository_manager::{DataRepositoryManager, DEFAULT_REPOSITORY_NAME};
use crate::index::IndexManager;
use crate::persistence::{DataRepository, Repository};
use crate::{api::*, persistence, CreateWork, CreateWorkResponse};
use crate::{EmbeddingRouter, MemoryManager, ServerConfig};

use anyhow::Result;
use axum::http::StatusCode;
use axum::{extract::State, routing::get, routing::post, Json, Router};
use pyo3::Python;
use tokio::signal;
use tracing::{error, info};

use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use std::net::SocketAddr;
use std::sync::Arc;

const DEFAULT_SEARCH_LIMIT: u64 = 5;

#[derive(Clone)]
pub struct IndexEndpointState {
    index_manager: Arc<IndexManager>,
}

#[derive(Clone)]
pub struct MemoryEndpointState {
    memory_manager: Arc<MemoryManager>,
    coordinator_addr: SocketAddr,
}

#[derive(Clone)]
pub struct RepositoryEndpointState {
    repository_manager: Arc<DataRepositoryManager>,
    coordinator_addr: SocketAddr,
}

#[derive(OpenApi)]
#[openapi(
        paths(
            sync_repository,
            add_texts,
            index_search,
        ),
        components(
            schemas(SyncRepository, SyncRepositoryResponse, DataConnector, Extractor,
                TextSplitterKind, IndexDistance, ExtractorType, ExtractorContentType,
                SourceType, TextAddRequest, IndexAdditionResponse, Text, IndexSearchResponse,
                DocumentFragment, SearchRequest,)
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
        let repository_manager =
            Arc::new(DataRepositoryManager::new(repository.clone(), index_manager.clone()).await?);
        if let Err(err) = repository_manager
            .create_default_repository(&self.config)
            .await
        {
            panic!("failed to create default repository: {}", err)
        }
        let coordinator_addr: SocketAddr = self.config.coordinator_addr.parse()?;
        let repository_endpoint_state = RepositoryEndpointState {
            repository_manager: repository_manager.clone(),
            coordinator_addr,
        };
        let memory_manager = Arc::new(MemoryManager::new(repository_manager.clone()).await?);
        let index_state = IndexEndpointState {
            index_manager: index_manager.clone(),
        };
        let memory_state = MemoryEndpointState {
            memory_manager: memory_manager.clone(),
            coordinator_addr,
        };
        let app = Router::new()
            .merge(SwaggerUi::new("/api-docs-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
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
                "/repository/add_texts",
                post(add_texts).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/repository/run_extractors",
                post(run_extractors).with_state(repository_endpoint_state.clone()),
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
#[utoipa::path(
    post,
    path = "/repository/sync",
    request_body = SyncRepository,
    tag = "indexify",
    responses(
        (status = 200, description = "Repository synced successfully", body = SyncRepositoryResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to sync repository")
    ),
)]
async fn sync_repository(
    State(state): State<RepositoryEndpointState>,
    Json(payload): Json<SyncRepository>,
) -> Result<Json<SyncRepositoryResponse>, IndexifyAPIError> {
    let extractor_bindings = payload
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
        extractor_bindings,
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
        .add_extractor(&repository, payload.extractor_binding.into())
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to add extractor: {}", e),
            )
        })?;
    Ok(Json(ExtractorAddResponse {}))
}

#[utoipa::path(
    post,
    path = "/repository/add_texts",
    request_body = TextAddRequest,
    tag = "indexify",
    responses(
        (status = 200, description = "Texts were successfully added to the repository", body = IndexAdditionResponse),
        (status = BAD_REQUEST, description = "Unable to add texts")
    ),
)]
#[axum_macros::debug_handler]
async fn add_texts(
    State(state): State<RepositoryEndpointState>,
    Json(payload): Json<TextAddRequest>,
) -> Result<Json<IndexAdditionResponse>, IndexifyAPIError> {
    let repo = get_or_default_repository(payload.repository);
    let texts = payload
        .documents
        .iter()
        .map(|d| persistence::Text::from_text(&repo, &d.text, None, d.metadata.clone()))
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

    if let Err(err) = _run_extractors(&repo, &state.coordinator_addr.to_string()).await {
        error!("unable to run extractors: {}", err.to_string());
    }

    Ok(Json(IndexAdditionResponse::default()))
}

async fn _run_extractors(repository: &str, coordinator_addr: &str) -> Result<(), anyhow::Error> {
    let req = CreateWork {
        repository_name: repository.into(),
        content: None,
    };
    let _resp = reqwest::Client::new()
        .post(&format!("http://{}/create_work", coordinator_addr,))
        .json(&req)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("failed to send create work request: {}", e))?
        .json::<CreateWorkResponse>()
        .await?;
    Ok(())
}

async fn run_extractors(
    State(state): State<RepositoryEndpointState>,
    Json(payload): Json<RunExtractors>,
) -> Result<Json<RunExtractorsResponse>, IndexifyAPIError> {
    _run_extractors(&payload.repository, &state.coordinator_addr.to_string())
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(RunExtractorsResponse {}))
}

#[axum_macros::debug_handler]
async fn create_memory_session(
    State(state): State<MemoryEndpointState>,
    Json(payload): Json<CreateMemorySessionRequest>,
) -> Result<Json<CreateMemorySessionResponse>, IndexifyAPIError> {
    let repo = &get_or_default_repository(payload.repository);
    let extractor_binding = payload.extractor_binding.map(|e| e.into());
    let session_id = state
        .memory_manager
        .create_session(
            repo,
            payload.session_id,
            extractor_binding,
            payload.metadata.unwrap_or_default(),
        )
        .await
        .map_err(|e| {
            error!("unable to create memory session: {}", e.to_string());
            IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        })?;

    Ok(Json(CreateMemorySessionResponse { session_id }))
}

#[axum_macros::debug_handler]
async fn add_to_memory_session(
    State(state): State<MemoryEndpointState>,
    Json(payload): Json<MemorySessionAddRequest>,
) -> Result<Json<MemorySessionAddResponse>, IndexifyAPIError> {
    let repo = get_or_default_repository(payload.repository);
    let messages = payload.messages.iter().map(|m| m.clone().into()).collect();
    state
        .memory_manager
        .add_messages(&repo, &payload.session_id, messages)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if let Err(err) = _run_extractors(&repo, &state.coordinator_addr.to_string()).await {
        error!("unable to run extractors: {}", err.to_string());
    }

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
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .iter()
        .map(|m| m.to_owned().into())
        .collect();

    Ok(Json(MemorySessionRetrieveResponse { messages }))
}

#[axum_macros::debug_handler]
async fn search_memory_session(
    State(memory_manager): State<Arc<MemoryManager>>,
    Json(payload): Json<MemorySessionSearchRequest>,
) -> Result<Json<MemorySessionSearchResponse>, IndexifyAPIError> {
    let repo = get_or_default_repository(payload.repository);
    let messages = memory_manager
        .search(
            &repo,
            &payload.session_id,
            &payload.query,
            payload.k.unwrap_or(DEFAULT_SEARCH_LIMIT),
        )
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .iter()
        .map(|m| m.to_owned().into())
        .collect();

    Ok(Json(MemorySessionSearchResponse { messages }))
}

#[utoipa::path(
    get,
    path = "/index/search",
    request_body = SearchRequest,
    tag = "indexify",
    responses(
        (status = 200, description = "Index search results", body = IndexSearchResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to search index")
    ),
)]
#[axum_macros::debug_handler]
async fn index_search(
    State(state): State<IndexEndpointState>,
    Json(query): Json<SearchRequest>,
) -> Result<Json<IndexSearchResponse>, IndexifyAPIError> {
    let index = state
        .index_manager
        .load(&query.repository, &query.index)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let results = index
        .search(&query.query, query.k.unwrap_or(DEFAULT_SEARCH_LIMIT))
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let document_fragments: Vec<DocumentFragment> = results
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
