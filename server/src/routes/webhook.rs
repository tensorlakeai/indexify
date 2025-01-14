use axum::{
    extract::{Query, State},
    Json,
};

use crate::{
    http_objects::{IndexifyAPIError, InkwellWebhookQueryParams},
    routes::RouteState,
};

#[utoipa::path(
    post,
    path = "/inkwell_webhook",
    tag = "operations",
    responses(
        (status = 200, description = "Post a webhook for processing"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
pub async fn receive_webhook(
    State(state): State<RouteState>,
    Query(_params): Query<InkwellWebhookQueryParams>,
) -> Result<Json<String>, IndexifyAPIError> {
    let value = format!("{}, {}", _params.job_id, _params.job_status);
    Ok(Json(value))
}
