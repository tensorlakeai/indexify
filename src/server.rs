use crate::attribute_index::AttributeIndexManager;
use crate::data_repository_manager::DataRepositoryManager;
use crate::persistence::Repository;
use crate::vector_index::VectorIndexManager;
use crate::ServerConfig;
use crate::{api::*, persistence, vectordbs, CreateWork, CreateWorkResponse};

use anyhow::Result;
use axum::extract::{Path, Query};
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
            list_repositories,
            get_repository,
            add_texts,
            index_search,
            list_extractors,
            bind_extractor,
            list_events,
            add_events,
            attribute_lookup
        ),
        components(
            schemas(CreateRepository, CreateRepositoryResponse, DataConnector,
                IndexDistance, ExtractorType, ExtractorContentType,
                SourceType, TextAddRequest, TextAdditionResponse, Text, IndexSearchResponse,
                DocumentFragment, SearchRequest, ListRepositoriesResponse, ListExtractorsResponse
            , ExtractorConfig, DataRepository, ExtractorBinding, ExtractorFilter, ExtractorBindRequest, ExtractorBindResponse,
        ListEventsResponse, EventAddRequest, EventAddResponse, Event, AttributeLookupResponse, ExtractedAttributes)
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
        let app = Router::new()
            .merge(SwaggerUi::new("/api-docs-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
            .route("/", get(root))
            .route(
                "/repository/:repository_name/extractor_bindings",
                post(bind_extractor).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/repository/:repository_name/add_texts",
                post(add_texts).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/repository/:repository_name/run_extractors",
                post(run_extractors).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/repository/:repository_name/search",
                post(index_search).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/repository/:repository_name/attributes",
                get(attribute_lookup).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/repository/:repository_name/events",
                post(add_events).with_state(repository_endpoint_state.clone()),
            )
            .route(
                "/repository/:repository_name/events",
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
                "/repository/:repository_name",
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
    let extractor_bindings = payload
        .extractors
        .clone()
        .into_iter()
        .map(|e| e.into())
        .collect();
    let data_repository = &persistence::DataRepository {
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
    Ok(Json(CreateRepositoryResponse {}))
}

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
    let data_repos = repositories.into_iter().map(|r| r.into()).collect();
    Ok(Json(ListRepositoriesResponse {
        repositories: data_repos,
    }))
}

#[utoipa::path(
    get,
    path = "/repository/{repository_name}",
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
        repository: data_repo.into(),
    }))
}

#[utoipa::path(
    post,
    path = "/repository/{repository_name}/extractor_bindings",
    request_body = ExtractorBindRequest,
    tag = "indexify",
    responses(
        (status = 200, description = "Extractor binded successfully", body = ExtractorBindResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to bind extractor to repository")
    ),
)]
#[axum_macros::debug_handler]
async fn bind_extractor(
    Path(repository_name): Path<String>,
    State(state): State<RepositoryEndpointState>,
    Json(payload): Json<ExtractorBindRequest>,
) -> Result<Json<ExtractorBindResponse>, IndexifyAPIError> {
    state
        .repository_manager
        .add_extractor_binding(&repository_name, payload.extractor_binding.into())
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
    path = "/repository/{repository_name}/add_texts",
    request_body = TextAddRequest,
    tag = "indexify",
    responses(
        (status = 200, description = "Texts were successfully added to the repository", body = TextAdditionResponse),
        (status = BAD_REQUEST, description = "Unable to add texts")
    ),
)]
#[axum_macros::debug_handler]
async fn add_texts(
    Path(repository_name): Path<String>,
    State(state): State<RepositoryEndpointState>,
    Json(payload): Json<TextAddRequest>,
) -> Result<Json<TextAdditionResponse>, IndexifyAPIError> {
    let texts = payload
        .documents
        .iter()
        .map(|d| persistence::Text::from_text(&repository_name, &d.text, d.metadata.clone()))
        .collect();
    state
        .repository_manager
        .add_texts(&repository_name, texts)
        .await
        .map_err(|e| {
            IndexifyAPIError::new(
                StatusCode::BAD_REQUEST,
                format!("failed to add text: {}", e),
            )
        })?;

    if let Err(err) = _run_extractors(&repository_name, &state.coordinator_addr.to_string()).await {
        error!("unable to run extractors: {}", err.to_string());
    }

    Ok(Json(TextAdditionResponse::default()))
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
    Path(repository_name): Path<String>,
    State(state): State<RepositoryEndpointState>,
) -> Result<Json<RunExtractorsResponse>, IndexifyAPIError> {
    _run_extractors(&repository_name, &state.coordinator_addr.to_string())
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(RunExtractorsResponse {}))
}

#[utoipa::path(
    post,
    path = "/repository/{repository_name}/events",
    request_body =  EventAddRequest,
    tag = "indexify",
    responses(
        (status = 200, description = "Events were successfully added to the repository", body = EventAddResponse),
        (status = BAD_REQUEST, description = "Unable to add event")
    ),
)]
#[axum_macros::debug_handler]
async fn add_events(
    Path(repository_name): Path<String>,
    State(state): State<RepositoryEndpointState>,
    Json(payload): Json<EventAddRequest>,
) -> Result<Json<EventAddResponse>, IndexifyAPIError> {
    let events = payload.events.iter().map(|m| m.clone().into()).collect();
    state
        .repository_manager
        .add_events(&repository_name, events)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if let Err(err) = _run_extractors(&repository_name, &state.coordinator_addr.to_string()).await {
        error!("unable to run extractors: {}", err.to_string());
    }

    Ok(Json(EventAddResponse {}))
}

#[utoipa::path(
    get,
    path = "/repository/{repository_name}/events",
    tag = "indexify",
    responses(
        (status = 200, description = "List of Events in a repository", body = ListEventsResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to list events in repository")
    ),
)]
#[axum_macros::debug_handler]
async fn list_events(
    Path(repository_name): Path<String>,
    State(state): State<RepositoryEndpointState>,
) -> Result<Json<ListEventsResponse>, IndexifyAPIError> {
    let messages = state
        .repository_manager
        .list_events(&repository_name)
        .await
        .map_err(|e| IndexifyAPIError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .iter()
        .map(|m| m.to_owned().into())
        .collect();

    Ok(Json(ListEventsResponse { messages }))
}

#[utoipa::path(
    get,
    path = "/extractors",
    tag = "indexify",
    responses(
        (status = 200, description = "List of extractors available", body = ListExtractorsResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to search index")
    ),
)]
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
    post,
    path = "/repository/{repository_name}/search",
    tag = "indexify",
    responses(
        (status = 200, description = "Index search results", body = IndexSearchResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to search index")
    ),
)]
#[axum_macros::debug_handler]
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
            text: text.text.text.clone(),
            metadata: text.text.metadata.clone(),
            confidence_score: text.confidence_score,
        })
        .collect();
    Ok(Json(IndexSearchResponse {
        results: document_fragments,
    }))
}

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
#[axum_macros::debug_handler]
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
