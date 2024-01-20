use axum::{
    extract::{Path, State},
    routing::{get, post},
    Extension,
    Json,
    Router,
};
use hyper::StatusCode;

use super::RepositoryEndpointState;
use crate::{api::*, caching::caches_extension::Caches, extractor_router::ExtractorRouter};

pub(super) fn get_router() -> Router<RepositoryEndpointState> {
    Router::new()
        .route("/extractors", get(list_extractors))
        .route("/extractors/extract", post(extract_content))
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
pub async fn list_extractors(
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
pub async fn extract_content(
    State(repository_endpoint): State<RepositoryEndpointState>,
    Extension(caches): Extension<Caches>,
    Json(request): Json<ExtractRequest>,
) -> Result<Json<ExtractResponse>, IndexifyAPIError> {
    let cache = caches.cache_extract_content.clone();
    let extractor_router = ExtractorRouter::new(repository_endpoint.coordinator_client.clone())
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
