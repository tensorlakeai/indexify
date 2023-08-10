use crate::attribute_index::AttributeIndexManager;
use crate::data_repository_manager::{DataRepositoryManager, DEFAULT_REPOSITORY_NAME};
use crate::persistence::{DataRepository, Repository};
use crate::vector_index::VectorIndexManager;
use crate::ServerConfig;
use crate::{api::*, persistence, vectordbs, CreateWork, CreateWorkResponse};

use anyhow::Result;
use axum::extract::Path;
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
pub struct RepositoryEndpointState {
    repository_manager: Arc<DataRepositoryManager>,
    coordinator_addr: SocketAddr,
}

#[derive(OpenApi)]
#[openapi(
        paths(
            create_repository,
            add_texts,
            index_search,
        ),
        components(
            schemas(CreateRepository, SyncRepositoryResponse, DataConnector,
                IndexDistance, ExtractorType, ExtractorContentType,
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
        let repository = Arc::new(Repository::new(&self.config.db_url).await?);
        let vectordb = vectordbs::create_vectordb(self.config.index_config.clone())?;
        let vector_index_manager = Arc::new(VectorIndexManager::new(
            self.config.clone(),
            repository.clone(),
            vectordb.clone(),
        ));
        let attribute_index_manager = Arc::new(AttributeIndexManager::new(repository.clone()));

        let repository_manager = Arc::new(
            DataRepositoryManager::new(
                repository.clone(),
                vector_index_manager,
                attribute_index_manager,
            )
            .await?,
        );
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
        // TODO: add a method for creating a repository (instead of syncing)
        let app = Router::new()
            .merge(SwaggerUi::new("/api-docs-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
            .route("/", get(root))
            .route(
                "/repository/:repository_name/extractor_bindings",
                post(bind_extractor).with_state(repository_endpoint_state.clone()),
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
                // FIXME: this should be /index/search
                "/repository/search",
                get(index_search).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/repository/attribute_lookup",
                get(attribute_lookup).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/events",
                post(add_events).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/events",
                get(list_events).with_state(repository_endpoint_state.clone()),
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
                "/extractors",
                get(list_extractors).with_state(repository_endpoint_state.clone()),
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
    path = "/repository/create",
    request_body = SyncRepository,
    tag = "indexify",
    responses(
        (status = 200, description = "Repository synced successfully", body = SyncRepositoryResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to sync repository")
    ),
)]
async fn create_repository(
    State(state): State<RepositoryEndpointState>,
    Json(payload): Json<CreateRepository>,
) -> Result<Json<SyncRepositoryResponse>, IndexifyAPIError> {
    let extractor_bindings = payload
        .extractors
        .clone()
        .into_iter()
        .map(|e| e.into())
        .collect();
    let data_repository = &DataRepository {
        name: payload.name.clone(),
        extractor_bindings,
        metadata: payload.metadata.clone(),
        data_connectors: vec![],
    };
    state
        .repository_manager
        .create(data_repository)
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
        repository: data_repo.into(),
    }))
}

#[axum_macros::debug_handler]
async fn bind_extractor(
    State(state): State<RepositoryEndpointState>,
    Json(payload): Json<ExtractorBindRequest>,
) -> Result<Json<ExtractorBindResponse>, IndexifyAPIError> {
    let repository = get_or_default_repository(payload.repository);
    state
        .repository_manager
        .add_extractor_binding(&repository, payload.extractor_binding.into())
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to add extractor: {}", e),
            )
        })?;
    Ok(Json(ExtractorBindResponse {}))
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
        .map(|d| persistence::Text::from_text(&repo, &d.text, d.metadata.clone()))
        .collect();
    state
        .repository_manager
        .add_texts(&repo, texts)
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
async fn add_events(
    State(state): State<RepositoryEndpointState>,
    Json(payload): Json<EventAddRequest>,
) -> Result<Json<EventAddResponse>, IndexifyAPIError> {
    let repo = get_or_default_repository(payload.repository);
    let events = payload.events.iter().map(|m| m.clone().into()).collect();
    state
        .repository_manager
        .add_events(&repo, events)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if let Err(err) = _run_extractors(&repo, &state.coordinator_addr.to_string()).await {
        error!("unable to run extractors: {}", err.to_string());
    }

    Ok(Json(EventAddResponse {}))
}

#[axum_macros::debug_handler]
async fn list_events(
    State(state): State<RepositoryEndpointState>,
    Json(payload): Json<EventsRetrieveRequest>,
) -> Result<Json<EventsSessionRetrieveResponse>, IndexifyAPIError> {
    let repo = get_or_default_repository(payload.repository);
    let messages = state
        .repository_manager
        .list_events(&repo)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .iter()
        .map(|m| m.to_owned().into())
        .collect();

    Ok(Json(EventsSessionRetrieveResponse { messages }))
}

#[axum_macros::debug_handler]
async fn list_extractors(
    State(state): State<RepositoryEndpointState>,
) -> Result<Json<ListExtractorsResponse>, IndexifyAPIError> {
    let extractors = state
        .repository_manager
        .list_extractors()
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .into_iter()
        .map(|e| e.into())
        .collect();
    Ok(Json(ListExtractorsResponse { extractors }))
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
    State(state): State<RepositoryEndpointState>,
    Json(query): Json<SearchRequest>,
) -> Result<Json<IndexSearchResponse>, IndexifyAPIError> {
    let results = state
        .repository_manager
        .search(
            &query.repository,
            &query.index,
            &query.query,
            query.k.unwrap_or(DEFAULT_SEARCH_LIMIT),
        )
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let document_fragments: Vec<DocumentFragment> = results
        .iter()
        .map(|text| DocumentFragment {
            text: text.text.text.clone(),
            metadata: text.text.metadata.clone(),
            confidence_score: text.confidence_score,
        })
        .collect();
    Ok(Json(IndexSearchResponse {
        results: document_fragments,
    }))
}

#[axum_macros::debug_handler]
async fn attribute_lookup(
    State(state): State<RepositoryEndpointState>,
    Json(query): Json<AttributeLookupRequest>,
) -> Result<Json<AttributeLookupResponse>, IndexifyAPIError> {
    let attributes = state
        .repository_manager
        .attribute_lookup(&query.repository, &query.index, query.content_id.as_ref())
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(AttributeLookupResponse {
        attributes: attributes.into_iter().map(|r| r.into()).collect(),
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
