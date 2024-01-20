use axum::{
    extract::{Multipart, Path, Query, State},
    routing::{get, post},
    Json,
    Router,
};
use hyper::StatusCode;
use tracing::info;

use super::RepositoryEndpointState;
use crate::api::*;

const DEFAULT_SEARCH_LIMIT: u64 = 5;

pub(super) fn get_router() -> Router<RepositoryEndpointState> {
    Router::new()
        .route(
            "/repositories/:repository_name/extractor_bindings",
            post(bind_extractor),
        )
        .route("/repositories/:repository_name/indexes", get(list_indexes))
        .route("/repositories/:repository_name/add_texts", post(add_texts))
        .route("/repositories/:repository_name/content", get(list_content))
        .route(
            "/repositories/:repository_name/upload_file",
            post(upload_file),
        )
        .route("/repositories/:repository_name/search", post(index_search))
        .route(
            "/repositories/:repository_name/attributes",
            get(attribute_lookup),
        )
        .route("/repositories", post(create_repository))
        .route("/repositories", get(list_repositories))
        .route("/repositories/:repository_name", get(get_repository))
}

#[tracing::instrument]
#[axum::debug_handler]
pub async fn list_content(
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
pub async fn create_repository(
    State(state): State<RepositoryEndpointState>,
    Json(payload): Json<CreateRepository>,
) -> Result<Json<CreateRepositoryResponse>, IndexifyAPIError> {
    let data_repository = DataRepository {
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
pub async fn list_repositories(
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
pub async fn get_repository(
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
pub async fn bind_extractor(
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
pub async fn add_texts(
    Path(repository_name): Path<String>,
    State(state): State<RepositoryEndpointState>,
    Json(payload): Json<TextAddRequest>,
) -> Result<Json<TextAdditionResponse>, IndexifyAPIError> {
    let content = payload
        .documents
        .iter()
        .map(|d| Content {
            content_type: mime::TEXT_PLAIN.to_string(),
            bytes: d.text.as_bytes().to_vec(),
            labels: d.labels.clone(),
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
pub async fn list_indexes(
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
#[axum::debug_handler]
pub async fn upload_file(
    Path(repository_name): Path<String>,
    State(state): State<RepositoryEndpointState>,
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
            .repository_manager
            .upload_file(&repository_name, data, &name)
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
pub async fn index_search(
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
    path = "/repository/{repository_name}/attributes",
    tag = "indexify",
    params(AttributeLookupRequest),
    responses(
        (status = 200, description = "List of Events in a repository", body = AttributeLookupResponse),
        (status = INTERNAL_SERVER_ERROR, description = "Unable to list events in repository")
    ),
)]
#[axum::debug_handler]
pub async fn attribute_lookup(
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
