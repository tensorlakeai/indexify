use axum::{
    Json,
    extract::{Path, State},
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    data_model::{
        SandboxId,
        SandboxSnapshot,
        SandboxSnapshotBuilder,
        SandboxSnapshotConfig,
        SnapshotId,
        SnapshotStatus,
    },
    http_objects::IndexifyAPIError,
    routes::routes_state::RouteState,
    state_store::requests::{RequestPayload, StateMachineUpdateRequest},
    utils::get_epoch_time_in_ns,
};

/// Request to create a snapshot of a sandbox
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateSnapshotRequest {
    /// Time-to-live in seconds (optional, uses server default if not provided).
    /// 0 means no expiration.
    #[serde(default)]
    pub ttl_secs: Option<u64>,
    /// Optional user-defined tag for the snapshot.
    #[serde(default)]
    pub tag: Option<String>,
}

/// Response after creating a snapshot
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateSnapshotResponse {
    pub snapshot_id: String,
    pub status: String,
}

/// Snapshot information returned by list/get operations
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SnapshotInfo {
    pub snapshot_id: String,
    pub sandbox_id: String,
    pub namespace: String,
    pub status: String,
    /// When the snapshot was created (milliseconds since epoch).
    pub created_at: u64,
    /// Time-to-live in seconds (0 = no expiration).
    pub ttl_secs: u64,
    /// When the snapshot expires (milliseconds since epoch, None if no
    /// expiration).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
    /// Size of the snapshot in bytes (if known).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// Docker image reference for the snapshot.
    pub image_ref: String,
}

impl From<&SandboxSnapshot> for SnapshotInfo {
    fn from(snapshot: &SandboxSnapshot) -> Self {
        SnapshotInfo {
            snapshot_id: snapshot.id.get().to_string(),
            sandbox_id: snapshot.sandbox_id.get().to_string(),
            namespace: snapshot.namespace.clone(),
            status: snapshot.status.to_string(),
            created_at: (snapshot.created_at / 1_000_000) as u64, // Convert ns to ms
            ttl_secs: snapshot.ttl_secs,
            expires_at: snapshot.expires_at().map(|ns| (ns / 1_000_000) as u64), // Convert ns to ms
            size_bytes: snapshot.size_bytes,
            image_ref: snapshot.image_ref.clone(),
        }
    }
}

/// List of snapshots
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListSnapshotsResponse {
    pub snapshots: Vec<SnapshotInfo>,
}

/// Request to restore from a snapshot
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
pub struct RestoreSnapshotRequest {
    /// Force restore even if TTL has expired (default: false).
    #[serde(default)]
    pub force: bool,
}

/// Response after restoring from a snapshot
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RestoreSnapshotResponse {
    pub sandbox_id: String,
    pub status: String,
    /// Whether the sandbox was actually restored from the snapshot.
    /// False if TTL expired and a fresh sandbox was created instead.
    pub restored_from_snapshot: bool,
    /// Whether the snapshot's TTL had expired.
    pub ttl_expired: bool,
}

/// Create a snapshot of a sandbox.
///
/// Creates a snapshot of the sandbox's filesystem state using docker commit.
/// The snapshot can later be restored to create a new sandbox with the same
/// state.
#[utoipa::path(
    post,
    path = "/namespaces/{namespace}/sandboxes/{sandbox_id}/snapshots",
    request_body = CreateSnapshotRequest,
    responses(
        (status = 200, description = "Snapshot created successfully", body = CreateSnapshotResponse),
        (status = 404, description = "Sandbox not found"),
        (status = 409, description = "Sandbox not in Running state"),
        (status = 403, description = "Snapshot limit exceeded for namespace"),
    ),
    params(
        ("namespace" = String, Path, description = "Namespace name"),
        ("sandbox_id" = String, Path, description = "Sandbox ID"),
    )
)]
pub async fn create_snapshot(
    State(state): State<RouteState>,
    Path((namespace, sandbox_id)): Path<(String, String)>,
    Json(request): Json<CreateSnapshotRequest>,
) -> Result<Json<CreateSnapshotResponse>, IndexifyAPIError> {
    // Get the sandbox to verify it exists and is running
    let sandbox = state
        .indexify_state
        .reader()
        .get_sandbox(&namespace, &sandbox_id)
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow::anyhow!("failed to get sandbox: {}", e))
        })?
        .ok_or_else(|| IndexifyAPIError::not_found(&format!("sandbox {} not found", sandbox_id)))?;

    // Verify sandbox is in Running state
    if !matches!(sandbox.status, crate::data_model::SandboxStatus::Running) {
        return Err(IndexifyAPIError::bad_request(&format!(
            "sandbox must be in Running state, current state: {}",
            sandbox.status
        )));
    }

    // Check snapshot count limit for namespace
    let existing_snapshots = state
        .indexify_state
        .reader()
        .list_snapshots(&namespace)
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow::anyhow!("failed to list snapshots: {}", e))
        })?;

    if existing_snapshots.len() >= state.config.max_snapshots_per_namespace {
        return Err(IndexifyAPIError::forbidden(&format!(
            "snapshot limit ({}) exceeded for namespace {}",
            state.config.max_snapshots_per_namespace, namespace
        )));
    }

    // Generate snapshot ID
    let snapshot_id = SnapshotId::generate();
    let ttl_secs = request
        .ttl_secs
        .unwrap_or(state.config.default_snapshot_ttl_secs);

    // Create snapshot tag (used for Docker image tag)
    let snapshot_tag = format!("sb-{}-{}", sandbox_id, snapshot_id.get());

    // Create snapshot entity
    let snapshot = SandboxSnapshotBuilder::default()
        .id(snapshot_id.clone())
        .sandbox_id(SandboxId::from(sandbox_id.clone()))
        .namespace(namespace.clone())
        .image_ref(format!("indexify-snapshots:{}", snapshot_tag))
        .created_at(get_epoch_time_in_ns())
        .ttl_secs(ttl_secs)
        .status(SnapshotStatus::Creating)
        .sandbox_config(SandboxSnapshotConfig::from(&sandbox))
        .build()
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow::anyhow!("failed to build snapshot: {}", e))
        })?;

    // Write snapshot to state store (this will trigger snapshot creation on
    // executor)
    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::CreateSnapshot(
                crate::state_store::requests::CreateSnapshotRequest {
                    snapshot: snapshot.clone(),
                },
            ),
        })
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow::anyhow!("failed to create snapshot: {}", e))
        })?;

    Ok(Json(CreateSnapshotResponse {
        snapshot_id: snapshot.id.get().to_string(),
        status: snapshot.status.to_string(),
    }))
}

/// List snapshots for a specific sandbox.
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/sandboxes/{sandbox_id}/snapshots",
    responses(
        (status = 200, description = "Snapshots retrieved successfully", body = ListSnapshotsResponse),
    ),
    params(
        ("namespace" = String, Path, description = "Namespace name"),
        ("sandbox_id" = String, Path, description = "Sandbox ID"),
    )
)]
pub async fn list_snapshots_for_sandbox(
    State(state): State<RouteState>,
    Path((namespace, sandbox_id)): Path<(String, String)>,
) -> Result<Json<ListSnapshotsResponse>, IndexifyAPIError> {
    let snapshots = state
        .indexify_state
        .reader()
        .list_snapshots_for_sandbox(&namespace, &sandbox_id)
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow::anyhow!("failed to list snapshots: {}", e))
        })?;

    let snapshot_infos: Vec<SnapshotInfo> = snapshots.iter().map(SnapshotInfo::from).collect();

    Ok(Json(ListSnapshotsResponse {
        snapshots: snapshot_infos,
    }))
}

/// Get details of a specific snapshot.
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/snapshots/{snapshot_id}",
    responses(
        (status = 200, description = "Snapshot retrieved successfully", body = SnapshotInfo),
        (status = 404, description = "Snapshot not found"),
    ),
    params(
        ("namespace" = String, Path, description = "Namespace name"),
        ("snapshot_id" = String, Path, description = "Snapshot ID"),
    )
)]
pub async fn get_snapshot(
    State(state): State<RouteState>,
    Path((namespace, snapshot_id)): Path<(String, String)>,
) -> Result<Json<SnapshotInfo>, IndexifyAPIError> {
    let snapshot = state
        .indexify_state
        .reader()
        .get_snapshot(&namespace, &snapshot_id)
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow::anyhow!("failed to get snapshot: {}", e))
        })?
        .ok_or_else(|| {
            IndexifyAPIError::not_found(&format!("snapshot {} not found", snapshot_id))
        })?;

    Ok(Json(SnapshotInfo::from(&snapshot)))
}

/// Delete a snapshot.
#[utoipa::path(
    delete,
    path = "/namespaces/{namespace}/snapshots/{snapshot_id}",
    responses(
        (status = 200, description = "Snapshot deleted successfully"),
        (status = 404, description = "Snapshot not found"),
    ),
    params(
        ("namespace" = String, Path, description = "Namespace name"),
        ("snapshot_id" = String, Path, description = "Snapshot ID"),
    )
)]
pub async fn delete_snapshot(
    State(state): State<RouteState>,
    Path((namespace, snapshot_id)): Path<(String, String)>,
) -> Result<Json<()>, IndexifyAPIError> {
    // Verify snapshot exists
    let _snapshot = state
        .indexify_state
        .reader()
        .get_snapshot(&namespace, &snapshot_id)
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow::anyhow!("failed to get snapshot: {}", e))
        })?
        .ok_or_else(|| {
            IndexifyAPIError::not_found(&format!("snapshot {} not found", snapshot_id))
        })?;

    // Write delete request to state store
    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::DeleteSnapshot(
                crate::state_store::requests::DeleteSnapshotRequest {
                    snapshot_id: SnapshotId::from(snapshot_id),
                    namespace,
                },
            ),
        })
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow::anyhow!("failed to delete snapshot: {}", e))
        })?;

    Ok(Json(()))
}

/// Restore a sandbox from a snapshot.
///
/// If the snapshot's TTL has not expired (or force=true), creates a new sandbox
/// from the snapshot image. If the TTL has expired and force=false, creates a
/// fresh sandbox from the original configuration instead.
#[utoipa::path(
    post,
    path = "/namespaces/{namespace}/snapshots/{snapshot_id}/restore",
    request_body = RestoreSnapshotRequest,
    responses(
        (status = 200, description = "Restore initiated successfully", body = RestoreSnapshotResponse),
        (status = 404, description = "Snapshot not found"),
    ),
    params(
        ("namespace" = String, Path, description = "Namespace name"),
        ("snapshot_id" = String, Path, description = "Snapshot ID"),
    )
)]
pub async fn restore_snapshot(
    State(state): State<RouteState>,
    Path((namespace, snapshot_id)): Path<(String, String)>,
    Json(request): Json<RestoreSnapshotRequest>,
) -> Result<Json<RestoreSnapshotResponse>, IndexifyAPIError> {
    let snapshot = state
        .indexify_state
        .reader()
        .get_snapshot(&namespace, &snapshot_id)
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow::anyhow!("failed to get snapshot: {}", e))
        })?
        .ok_or_else(|| {
            IndexifyAPIError::not_found(&format!("snapshot {} not found", snapshot_id))
        })?;

    // Verify snapshot is in Active state
    if !matches!(snapshot.status, SnapshotStatus::Active) {
        return Err(IndexifyAPIError::bad_request(&format!(
            "snapshot must be in Active state, current state: {}",
            snapshot.status
        )));
    }

    // Check if TTL has expired
    let current_time = get_epoch_time_in_ns();
    let ttl_expired = snapshot.is_expired(current_time);

    // Determine whether to use snapshot or create fresh
    let (restored_from_snapshot, image) = if request.force || !ttl_expired {
        (true, snapshot.image_ref.clone())
    } else {
        (false, snapshot.sandbox_config.image.clone())
    };

    // Create new sandbox (either from snapshot or original config)
    let new_sandbox_id = SandboxId::default();

    // Convert network policy
    let network_policy = snapshot
        .sandbox_config
        .network_policy
        .as_ref()
        .map(|policy| crate::data_model::NetworkPolicy {
            allow_internet_access: policy.allow_internet_access,
            allow_out: policy.allow_out.clone(),
            deny_out: policy.deny_out.clone(),
        });

    let sandbox = crate::data_model::SandboxBuilder::default()
        .id(new_sandbox_id.clone())
        .namespace(namespace.clone())
        .image(image)
        .resources(snapshot.sandbox_config.resources.clone())
        .secret_names(snapshot.sandbox_config.secret_names.clone())
        .timeout_secs(snapshot.sandbox_config.timeout_secs)
        .entrypoint(snapshot.sandbox_config.entrypoint.clone())
        .network_policy(network_policy)
        .build()
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow::anyhow!("failed to build sandbox: {}", e))
        })?;

    // Write sandbox creation request
    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::CreateSandbox(
                crate::state_store::requests::CreateSandboxRequest { sandbox },
            ),
        })
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow::anyhow!(
                "failed to create sandbox from snapshot: {}",
                e
            ))
        })?;

    Ok(Json(RestoreSnapshotResponse {
        sandbox_id: new_sandbox_id.get().to_string(),
        status: "pending".to_string(),
        restored_from_snapshot,
        ttl_expired,
    }))
}
