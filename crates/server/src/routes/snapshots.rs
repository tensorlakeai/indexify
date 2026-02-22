use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    data_model::{SandboxStatus, Snapshot, SnapshotId, SnapshotStatus},
    http_objects::IndexifyAPIError,
    routes::routes_state::RouteState,
    state_store::requests::{
        DeleteSnapshotRequest,
        RequestPayload,
        SnapshotSandboxRequest,
        StateMachineUpdateRequest,
    },
    utils::get_epoch_time_in_ns,
};

/// Response after creating a snapshot
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateSnapshotResponse {
    pub snapshot_id: String,
    pub status: String,
}

/// Snapshot information returned by list/get operations
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SnapshotInfo {
    pub id: String,
    pub namespace: String,
    pub sandbox_id: String,
    pub base_image: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    pub created_at: u64,
}

impl SnapshotInfo {
    pub fn from_snapshot(snapshot: &Snapshot) -> Self {
        let (status, error) = match &snapshot.status {
            SnapshotStatus::InProgress => ("in_progress".to_string(), None),
            SnapshotStatus::Completed => ("completed".to_string(), None),
            SnapshotStatus::Failed { error } => ("failed".to_string(), Some(error.clone())),
        };
        Self {
            id: snapshot.id.get().to_string(),
            namespace: snapshot.namespace.clone(),
            sandbox_id: snapshot.sandbox_id.get().to_string(),
            base_image: snapshot.base_image.clone(),
            status,
            error,
            snapshot_uri: snapshot.snapshot_uri.clone(),
            size_bytes: snapshot.size_bytes,
            created_at: (snapshot.creation_time_ns / 1_000_000) as u64,
        }
    }
}

/// List snapshots response
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListSnapshotsResponse {
    pub snapshots: Vec<SnapshotInfo>,
}

/// Create a snapshot of a sandbox's filesystem.
///
/// This is an asynchronous operation. The snapshot will be created in the
/// background. Poll the snapshot status via GET to check for completion.
#[utoipa::path(
    post,
    path = "/v1/namespaces/{namespace}/sandboxes/{sandbox_id}/snapshot",
    tag = "snapshots",
    responses(
        (status = 202, description = "Snapshot creation initiated", body = CreateSnapshotResponse),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Sandbox not found"),
        (status = 500, description = "Internal server error")
    ),
)]
pub async fn create_snapshot(
    Path((namespace, sandbox_id)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    // Check sandbox exists and is Running
    let reader = state.indexify_state.reader();
    let sandbox = reader
        .get_sandbox(&namespace, &sandbox_id)
        .await
        .map_err(IndexifyAPIError::internal_error)?
        .ok_or_else(|| IndexifyAPIError::not_found("Sandbox not found"))?;

    match &sandbox.status {
        SandboxStatus::Running => {}
        SandboxStatus::Snapshotting { .. } => {
            return Err(IndexifyAPIError::bad_request(
                "Sandbox is already being snapshotted",
            ));
        }
        SandboxStatus::Terminated => {
            return Err(IndexifyAPIError::bad_request(
                "Cannot snapshot a terminated sandbox",
            ));
        }
        SandboxStatus::Pending { .. } => {
            return Err(IndexifyAPIError::bad_request(
                "Cannot snapshot a sandbox that is not yet running",
            ));
        }
    }

    let snapshot_id = SnapshotId::default();

    // Build the full upload URI so the dataplane can upload directly to S3.
    let base = state
        .config
        .snapshot_storage_path
        .clone()
        .unwrap_or_else(|| state.config.blob_storage.path.clone());
    if base.is_empty() {
        return Err(IndexifyAPIError::internal_error(anyhow::anyhow!(
            "snapshot storage path is not configured"
        )));
    }
    let upload_uri = format!(
        "{}/snapshots/{}/{}.tar.zst",
        base.trim_end_matches('/'),
        namespace,
        snapshot_id.get()
    );

    let snapshot = Snapshot {
        id: snapshot_id.clone(),
        namespace: namespace.clone(),
        sandbox_id: sandbox.id.clone(),
        base_image: sandbox.image.clone(),
        status: SnapshotStatus::InProgress,
        snapshot_uri: None,
        size_bytes: None,
        creation_time_ns: get_epoch_time_in_ns(),
        resources: sandbox.resources.clone(),
        entrypoint: sandbox.entrypoint.clone(),
        secret_names: sandbox.secret_names.clone(),
    };

    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::SnapshotSandbox(SnapshotSandboxRequest {
                snapshot,
                upload_uri,
            }),
        })
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    Ok((
        StatusCode::ACCEPTED,
        Json(CreateSnapshotResponse {
            snapshot_id: snapshot_id.get().to_string(),
            status: "in_progress".to_string(),
        }),
    ))
}

/// List all snapshots in a namespace
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/snapshots",
    tag = "snapshots",
    responses(
        (status = 200, description = "List of snapshots", body = ListSnapshotsResponse),
        (status = 500, description = "Internal server error")
    ),
)]
pub async fn list_snapshots(
    Path(namespace): Path<String>,
    State(state): State<RouteState>,
) -> Result<Json<ListSnapshotsResponse>, IndexifyAPIError> {
    let reader = state.indexify_state.reader();
    let snapshots = reader
        .list_snapshots(&namespace)
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    let snapshot_infos: Vec<SnapshotInfo> =
        snapshots.iter().map(SnapshotInfo::from_snapshot).collect();

    Ok(Json(ListSnapshotsResponse {
        snapshots: snapshot_infos,
    }))
}

/// Get a specific snapshot
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/snapshots/{snapshot_id}",
    tag = "snapshots",
    responses(
        (status = 200, description = "Snapshot details", body = SnapshotInfo),
        (status = 404, description = "Snapshot not found"),
        (status = 500, description = "Internal server error")
    ),
)]
pub async fn get_snapshot(
    Path((namespace, snapshot_id)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<Json<SnapshotInfo>, IndexifyAPIError> {
    let reader = state.indexify_state.reader();
    let snapshot = reader
        .get_snapshot(&namespace, &snapshot_id)
        .await
        .map_err(IndexifyAPIError::internal_error)?
        .ok_or_else(|| IndexifyAPIError::not_found("Snapshot not found"))?;

    Ok(Json(SnapshotInfo::from_snapshot(&snapshot)))
}

/// Delete a snapshot
#[utoipa::path(
    delete,
    path = "/v1/namespaces/{namespace}/snapshots/{snapshot_id}",
    tag = "snapshots",
    responses(
        (status = 200, description = "Snapshot deleted"),
        (status = 404, description = "Snapshot not found"),
        (status = 500, description = "Internal server error")
    ),
)]
pub async fn delete_snapshot(
    Path((namespace, snapshot_id)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<(), IndexifyAPIError> {
    let reader = state.indexify_state.reader();
    let snapshot = reader
        .get_snapshot(&namespace, &snapshot_id)
        .await
        .map_err(IndexifyAPIError::internal_error)?
        .ok_or_else(|| IndexifyAPIError::not_found("Snapshot not found"))?;

    // Delete the blob from storage if it exists
    if let Some(uri) = &snapshot.snapshot_uri {
        let blob_store = state.blob_storage.get_blob_store(&namespace);
        if let Err(e) = blob_store.delete_by_uri(uri).await {
            tracing::warn!(
                snapshot_id = %snapshot_id,
                uri = %uri,
                error = %e,
                "Failed to delete snapshot blob, proceeding with metadata cleanup"
            );
        }
    }

    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::DeleteSnapshot(DeleteSnapshotRequest {
                namespace,
                snapshot_id: SnapshotId::from(snapshot_id),
            }),
        })
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    Ok(())
}
