use axum::{
    extract::{Query, State},
    Json,
};

use crate::{
    http_objects::{IndexifyAPIError, InkwellWebhookParams},
    routes::RouteState,
};

#[utoipa::path(
    post,
    path = "/inkwell_webhook",
    request_body = InkwellWebhookParams,
    tag = "operations",
    responses(
        (status = 200, description = "Post a webhook for processing"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
pub async fn receive_webhook(
    State(state): State<RouteState>,
    Json(body): Json<InkwellWebhookParams>,
) -> Result<Json<String>, IndexifyAPIError> {
    let value = format!("{}, {}", body.job_id, body.job_status);
    Ok(Json(value))
}
