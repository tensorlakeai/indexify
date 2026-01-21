use anyhow::anyhow;
use axum::{
    Json,
    extract::{Multipart, Path, Query, State},
};
use base64::{Engine, prelude::BASE64_STANDARD};
use futures::StreamExt;
use tracing::info;
use utoipa::ToSchema;

use crate::{
    blob_store::PutResult,
    http_objects::{IndexifyAPIError, ListParams},
    http_objects_v1,
    routes::{common::validate_and_submit_application, routes_state::RouteState},
    state_store::requests::{DeleteApplicationRequest, RequestPayload, StateMachineUpdateRequest},
};

#[allow(dead_code)]
#[derive(ToSchema)]
struct ApplicationCreateType {
    application: http_objects_v1::Application,
    #[schema(format = "binary")]
    code: Option<String>,
}

/// Create or update an application
#[utoipa::path(
    post,
    path = "/v1/namespaces/{namespace}/applications",
    tag = "operations",
    request_body(content_type = "multipart/form-data", content = inline(ApplicationCreateType)),
    responses(
        (status = 200, description = "create or update an application"),
        (status = INTERNAL_SERVER_ERROR, description = "unable to create or update application")
    ),
)]
pub async fn create_or_update_application(
    Path(namespace): Path<String>,
    State(state): State<RouteState>,
    mut application_code: Multipart,
) -> Result<(), IndexifyAPIError> {
    let mut application_manifest: Option<http_objects_v1::Application> = Option::None;
    let mut put_result: Option<PutResult> = None;

    let mut upgrade_requests_to_current_version: Option<bool> = None;
    while let Some(field) = application_code
        .next_field()
        .await
        .map_err(|err| IndexifyAPIError::internal_error(anyhow!(err)))?
    {
        let name = field.name();
        if let Some(name) = name {
            if name == "code" {
                info!("Found application code zip in create_or_update_application request");
                let stream = field.map(|res| res.map_err(|err| anyhow!(err)));
                let file_name = format!("{}_{}", namespace, nanoid::nanoid!());
                let result = state
                    .blob_storage
                    .get_blob_store(&namespace)
                    .put(&file_name, stream)
                    .await
                    .map_err(IndexifyAPIError::internal_error)?;
                put_result = Some(result);
            } else if name == "application" {
                let text = field
                    .text()
                    .await
                    .map_err(|e| IndexifyAPIError::bad_request(&e.to_string()))?;
                let mut json_value: serde_json::Value = serde_json::from_str(&text)?;
                json_value["namespace"] = serde_json::Value::String(namespace.clone());
                application_manifest = Some(serde_json::from_value(json_value)?);
            } else if name == "upgrade_requests_to_latest_code" {
                let text = field
                    .text()
                    .await
                    .map_err(|e| IndexifyAPIError::bad_request(&e.to_string()))?;
                upgrade_requests_to_current_version = Some(serde_json::from_str::<bool>(&text)?);
            } else if name == "code_content_type" {
                let code_content_type = field
                    .text()
                    .await
                    .map_err(|e| IndexifyAPIError::bad_request(&e.to_string()))?;
                if code_content_type != "application/zip" {
                    return Err(IndexifyAPIError::bad_request(
                        "Code content type must be application/zip",
                    ));
                }
            }
        }
    }

    let application_manifest = application_manifest.ok_or(IndexifyAPIError::bad_request(
        "application manifest is required",
    ))?;

    // Code is optional - only required if the application has functions
    let code_info = put_result
        .as_ref()
        .map(|r| (r.url.as_str(), r.sha256_hash.as_str(), r.size_bytes));

    let application = application_manifest.into_data_model(code_info)?;

    validate_and_submit_application(
        &state,
        namespace,
        application,
        upgrade_requests_to_current_version.unwrap_or(false),
    )
    .await
}

/// Delete compute graph
#[utoipa::path(
    delete,
    path = "/v1/namespaces/{namespace}/applications/{application}",
    tag = "operations",
    responses(
        (status = 200, description = "application deleted successfully"),
        (status = BAD_REQUEST, description = "unable to delete application")
    ),
)]
pub async fn delete_application(
    Path((namespace, application)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<(), IndexifyAPIError> {
    let request = RequestPayload::TombstoneApplication(DeleteApplicationRequest {
        namespace,
        name: application.clone(),
    });
    state
        .indexify_state
        .write(StateMachineUpdateRequest { payload: request })
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    info!("application deleted: {}", application);
    Ok(())
}

/// List compute graphs
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/applications",
    tag = "operations",
    params(
        ListParams
    ),
    responses(
        (status = 200, description = "lists applications", body = http_objects_v1::ApplicationsList),
        (status = INTERNAL_SERVER_ERROR, description = "internal server error")
    ),
)]
pub async fn applications(
    Path(namespace): Path<String>,
    Query(params): Query<ListParams>,
    State(state): State<RouteState>,
) -> Result<Json<http_objects_v1::ApplicationsList>, IndexifyAPIError> {
    let cursor = params
        .cursor
        .map(|c| BASE64_STANDARD.decode(c).unwrap_or_default());
    let (application, cursor) = state
        .indexify_state
        .reader()
        .list_applications(&namespace, cursor.as_deref(), params.limit)
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    let cursor = cursor.map(|c| BASE64_STANDARD.encode(c));
    Ok(Json(http_objects_v1::ApplicationsList {
        applications: application.into_iter().map(|c| c.into()).collect(),
        cursor,
    }))
}

/// Get a compute graph definition
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/applications/{application}",
    tag = "operations",
    responses(
        (status = 200, description = "application definition", body = http_objects_v1::Application),
        (status = INTERNAL_SERVER_ERROR, description = "internal server error")
    ),
)]
pub async fn get_application(
    Path((namespace, name)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<Json<http_objects_v1::Application>, IndexifyAPIError> {
    let application = state
        .indexify_state
        .reader()
        .get_application(&namespace, &name)
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    if let Some(application) = application {
        return Ok(Json(application.into()));
    }
    Err(IndexifyAPIError::not_found("Application not found"))
}
