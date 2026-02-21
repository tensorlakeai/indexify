use std::{sync::Arc, time::Duration};

use anyhow::Result;
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::{
    data_model::{SandboxKey, SnapshotId, SnapshotStatus},
    processor::container_scheduler::ContainerScheduler,
    state_store::{
        IndexifyState,
        in_memory_state::InMemoryState,
        requests::{
            RequestPayload,
            SchedulerUpdatePayload,
            SchedulerUpdateRequest,
            StateMachineUpdateRequest,
        },
    },
    utils::get_epoch_time_in_ns,
};

pub struct SnapshotProcessor {
    clock: u64,
}

impl SnapshotProcessor {
    pub fn new(clock: u64) -> Self {
        Self { clock }
    }

    /// Process snapshots in Creating status and send operations to executors.
    /// Returns executor state updates with snapshot operations to perform.
    pub fn process_snapshot_creations(
        &self,
        in_memory_state: &InMemoryState,
        _container_scheduler: &ContainerScheduler,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // Find all snapshots in Creating status
        for (_key, snapshot) in &in_memory_state.snapshots {
            if snapshot.status != SnapshotStatus::Creating {
                continue;
            }

            // Find the sandbox to get its container and executor
            let sandbox_key = SandboxKey::new(&snapshot.namespace, snapshot.sandbox_id.get());
            let Some(sandbox) = in_memory_state.sandboxes.get(&sandbox_key) else {
                warn!(
                    snapshot_id = %snapshot.id,
                    sandbox_id = %snapshot.sandbox_id,
                    "Sandbox not found for snapshot, skipping"
                );
                continue;
            };

            // Get the container for this sandbox
            let Some(container_id) = &sandbox.container_id else {
                warn!(
                    snapshot_id = %snapshot.id,
                    sandbox_id = %snapshot.sandbox_id,
                    "No container ID for sandbox, snapshot cannot be created"
                );
                continue;
            };

            // Get the executor running this container
            let Some(executor_id) = &sandbox.executor_id else {
                warn!(
                    snapshot_id = %snapshot.id,
                    sandbox_id = %snapshot.sandbox_id,
                    "No executor ID for sandbox, snapshot cannot be created"
                );
                continue;
            };

            // Build snapshot tag for Docker image
            let snapshot_tag = format!("sb-{}-{}", snapshot.sandbox_id.get(), snapshot.id.get());

            // Add snapshot operation to executor state update
            let snapshot_operation = proto_api::executor_api_pb::SnapshotOperation {
                operation_type: Some(
                    proto_api::executor_api_pb::SnapshotOperationType::Create.into(),
                ),
                snapshot_id: Some(snapshot.id.get().to_string()),
                container_id: Some(container_id.get().to_string()),
                snapshot_tag: Some(snapshot_tag.clone()),
                image_ref: None,
            };

            // Add to executor's snapshot operations
            update
                .snapshot_operations
                .entry(executor_id.clone())
                .or_default()
                .push(snapshot_operation);

            info!(
                snapshot_id = %snapshot.id,
                sandbox_id = %snapshot.sandbox_id,
                executor_id = %executor_id,
                container_id = %container_id,
                "Queued snapshot creation operation for executor"
            );
        }

        Ok(update)
    }

    /// Process snapshots in Deleting status and send delete operations to
    /// executors.
    pub fn process_snapshot_deletions(
        &self,
        in_memory_state: &InMemoryState,
        container_scheduler: &ContainerScheduler,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // Find all snapshots in Deleting status
        for (_key, snapshot) in &in_memory_state.snapshots {
            if snapshot.status != SnapshotStatus::Deleting {
                continue;
            }

            // For deletion, we need to find an executor to send the delete operation to.
            // We'll send it to any available executor (they all have access to the same
            // Docker daemon in a typical setup).
            // TODO: In multi-executor setups, we should send to the executor that created
            // it.

            // Add snapshot delete operation
            let snapshot_operation = proto_api::executor_api_pb::SnapshotOperation {
                operation_type: Some(
                    proto_api::executor_api_pb::SnapshotOperationType::Delete.into(),
                ),
                snapshot_id: Some(snapshot.id.get().to_string()),
                container_id: None,
                snapshot_tag: None,
                image_ref: Some(snapshot.image_ref.clone()),
            };

            // Send to first available executor
            // In a real implementation, we'd track which executor created the snapshot
            if let Some((executor_id, _)) = container_scheduler.executors.iter().next() {
                update
                    .snapshot_operations
                    .entry(executor_id.clone())
                    .or_default()
                    .push(snapshot_operation);

                info!(
                    snapshot_id = %snapshot.id,
                    image_ref = %snapshot.image_ref,
                    executor_id = %executor_id,
                    "Queued snapshot deletion operation for executor"
                );
            } else {
                warn!(
                    snapshot_id = %snapshot.id,
                    "No executors available to delete snapshot"
                );
            }
        }

        Ok(update)
    }

    /// Handle snapshot operation results from executors.
    /// Updates snapshot status based on success/failure.
    pub fn handle_snapshot_result(
        &self,
        in_memory_state: &mut InMemoryState,
        result: &proto_api::executor_api_pb::SnapshotOperationResult,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        let Some(snapshot_id_str) = &result.snapshot_id else {
            warn!("Snapshot result missing snapshot_id");
            return Ok(update);
        };

        let snapshot_id = SnapshotId::from(snapshot_id_str.as_str());

        // Find the snapshot in in-memory state
        let snapshot_key = in_memory_state
            .snapshots
            .iter()
            .find(|(_, s)| s.id == snapshot_id)
            .map(|(k, _)| k.clone());

        let Some(snapshot_key) = snapshot_key else {
            warn!(
                snapshot_id = %snapshot_id_str,
                "Snapshot not found for result, may have been deleted"
            );
            return Ok(update);
        };

        let Some(snapshot_box) = in_memory_state.snapshots.get(&snapshot_key) else {
            return Ok(update);
        };
        let mut snapshot = (**snapshot_box).clone();

        let operation_type = result.operation_type();
        let success = result.success.unwrap_or(false);

        match operation_type {
            proto_api::executor_api_pb::SnapshotOperationType::Create => {
                if success {
                    // Update snapshot to Active status
                    snapshot.status = SnapshotStatus::Active;
                    if let Some(size) = result.size_bytes {
                        snapshot.size_bytes = Some(size);
                    }

                    info!(
                        snapshot_id = %snapshot.id,
                        image_ref = ?result.image_ref,
                        size_bytes = ?snapshot.size_bytes,
                        "Snapshot creation succeeded, marked as Active"
                    );

                    // Update the snapshot in state
                    update.updated_snapshots.insert(snapshot_key, snapshot);
                } else {
                    // Creation failed - transition to Deleting to clean up
                    warn!(
                        snapshot_id = %snapshot.id,
                        error = ?result.error_message,
                        "Snapshot creation failed, marking for deletion"
                    );

                    snapshot.status = SnapshotStatus::Deleting;
                    update
                        .updated_snapshots
                        .insert(snapshot_key.clone(), snapshot);

                    // Also add a tombstone to remove it after reporting the failure
                    update.deleted_snapshots.insert(snapshot_key);
                }
            }
            proto_api::executor_api_pb::SnapshotOperationType::Delete => {
                if success {
                    info!(
                        snapshot_id = %snapshot.id,
                        "Snapshot deletion succeeded, removing from state"
                    );

                    // Remove from state completely
                    update.deleted_snapshots.insert(snapshot_key);
                } else {
                    warn!(
                        snapshot_id = %snapshot.id,
                        error = ?result.error_message,
                        "Snapshot deletion failed, will retry"
                    );
                    // Keep in Deleting status to retry
                }
            }
            _ => {
                warn!(
                    snapshot_id = %snapshot_id_str,
                    operation_type = ?operation_type,
                    "Unknown snapshot operation type in result"
                );
            }
        }

        Ok(update)
    }

    /// Clean up expired snapshots based on TTL.
    /// Returns a SchedulerUpdateRequest with snapshots marked for deletion.
    pub fn cleanup_expired_snapshots(
        &self,
        in_memory_state: &InMemoryState,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        let now = get_epoch_time_in_ns();
        // In test environments, get_epoch_time_in_ns() might return 0
        // Use the same base time as in create_test_snapshot for consistency
        let now = if now == 0 {
            1704067200u128 * 1_000_000_000
        } else {
            now
        };

        for (snapshot_key, snapshot) in &in_memory_state.snapshots {
            // Skip snapshots with no TTL (ttl_secs = 0 means no expiration)
            if snapshot.ttl_secs == 0 {
                continue;
            }

            // Skip snapshots that are already being deleted
            if snapshot.status == SnapshotStatus::Deleting {
                continue;
            }

            // Calculate age in seconds
            let age_ns = now.saturating_sub(snapshot.created_at);
            let age_secs = age_ns / 1_000_000_000;

            // Check if snapshot has expired
            if age_secs >= snapshot.ttl_secs as u128 {
                // Mark snapshot for deletion
                let mut expired_snapshot = (**snapshot).clone();
                expired_snapshot.status = SnapshotStatus::Deleting;

                update
                    .updated_snapshots
                    .insert(snapshot_key.clone(), expired_snapshot);

                info!(
                    snapshot_id = %snapshot.id,
                    age_secs = age_secs,
                    ttl_secs = snapshot.ttl_secs,
                    "Marking expired snapshot for deletion"
                );
            }
        }

        Ok(update)
    }
}

/// Background task that periodically cleans up expired snapshots.
/// Runs every hour by default.
pub async fn start_snapshot_cleanup_task(
    indexify_state: Arc<IndexifyState>,
    mut shutdown_rx: watch::Receiver<()>,
    cleanup_interval_secs: u64,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(cleanup_interval_secs));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    info!(
        interval_secs = cleanup_interval_secs,
        "Starting snapshot cleanup background task"
    );

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Err(err) = cleanup_expired_snapshots_once(&indexify_state).await {
                    error!(
                        error = %err,
                        "Error during snapshot cleanup"
                    );
                }
            }
            _ = shutdown_rx.changed() => {
                info!("Snapshot cleanup task shutting down");
                break;
            }
        }
    }
}

/// Performs a single cleanup pass for expired snapshots.
async fn cleanup_expired_snapshots_once(indexify_state: &Arc<IndexifyState>) -> Result<()> {
    let indexes_guard = indexify_state.in_memory_state.read().await;
    let clock = indexes_guard.clock;

    let snapshot_processor = SnapshotProcessor::new(clock);
    let cleanup_update = snapshot_processor.cleanup_expired_snapshots(&indexes_guard)?;

    // Only write to state machine if there are snapshots to clean up
    if !cleanup_update.updated_snapshots.is_empty() {
        drop(indexes_guard); // Release read lock before writing

        let request = StateMachineUpdateRequest {
            payload: RequestPayload::SchedulerUpdate(SchedulerUpdatePayload {
                update: Box::new(cleanup_update),
                processed_state_changes: vec![],
            }),
        };

        indexify_state.write(request).await?;

        info!("Snapshot cleanup completed, marked expired snapshots for deletion");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data_model::{
            ContainerResources,
            NetworkPolicy,
            SandboxId,
            SandboxSnapshotBuilder,
            SandboxSnapshotConfig,
            SnapshotId,
            SnapshotKey,
        },
        state_store::in_memory_state::InMemoryState,
    };

    fn create_test_snapshot(
        namespace: &str,
        snapshot_id: &str,
        sandbox_id: &str,
        ttl_secs: u64,
        status: SnapshotStatus,
        age_secs: u64,
    ) -> (SnapshotKey, crate::data_model::SandboxSnapshot) {
        let snapshot_id = SnapshotId::from(snapshot_id);
        // If get_epoch_time_in_ns() returns 0 (in test env), use a fixed base time
        let now = get_epoch_time_in_ns();
        let base_time = if now == 0 {
            // Use year 2024 as base for tests
            1704067200u128 * 1_000_000_000
        } else {
            now
        };
        let created_at = base_time.saturating_sub(age_secs as u128 * 1_000_000_000);

        let snapshot = SandboxSnapshotBuilder::default()
            .id(snapshot_id.clone())
            .sandbox_id(SandboxId::from(sandbox_id))
            .namespace(namespace.to_string())
            .image_ref(format!("indexify-snapshots:test-{}", snapshot_id.get()))
            .created_at(created_at)
            .ttl_secs(ttl_secs)
            .status(status)
            .sandbox_config(SandboxSnapshotConfig {
                image: "test:latest".to_string(),
                resources: ContainerResources {
                    cpu_ms_per_sec: 1000,
                    memory_mb: 256,
                    ephemeral_disk_mb: 1024,
                    gpu: None,
                },
                secret_names: vec![],
                timeout_secs: 300,
                entrypoint: None,
                network_policy: Some(NetworkPolicy {
                    allow_internet_access: true,
                    allow_out: vec![],
                    deny_out: vec![],
                }),
            })
            .size_bytes(Some(1024 * 1024))
            .build()
            .unwrap();

        let key = SnapshotKey::new(namespace, &snapshot.id);
        (key, snapshot)
    }

    #[test]
    fn test_cleanup_expired_snapshots_no_ttl() {
        // Snapshots with ttl_secs=0 should never be cleaned up
        let mut in_memory_state = InMemoryState::default();
        let (key, snapshot) =
            create_test_snapshot("default", "snap1", "sb1", 0, SnapshotStatus::Active, 100000);
        in_memory_state.snapshots.insert(key, Box::new(snapshot));

        let processor = SnapshotProcessor::new(1);
        let update = processor
            .cleanup_expired_snapshots(&in_memory_state)
            .unwrap();

        assert!(
            update.updated_snapshots.is_empty(),
            "Snapshots with TTL=0 should not be cleaned up"
        );
    }

    #[test]
    fn test_cleanup_expired_snapshots_not_expired() {
        // Snapshot is 10 seconds old with 1 hour TTL - should not be cleaned up
        let mut in_memory_state = InMemoryState::default();
        let (key, snapshot) =
            create_test_snapshot("default", "snap1", "sb1", 3600, SnapshotStatus::Active, 10);
        in_memory_state.snapshots.insert(key, Box::new(snapshot));

        let processor = SnapshotProcessor::new(1);
        let update = processor
            .cleanup_expired_snapshots(&in_memory_state)
            .unwrap();

        assert!(
            update.updated_snapshots.is_empty(),
            "Non-expired snapshots should not be cleaned up"
        );
    }

    #[test]
    fn test_cleanup_expired_snapshots_expired() {
        // Snapshot is 2 hours old with 1 hour TTL - should be marked for deletion
        let mut in_memory_state = InMemoryState::default();
        let (key, snapshot) = create_test_snapshot(
            "default",
            "snap1",
            "sb1",
            3600,
            SnapshotStatus::Active,
            7200,
        );

        in_memory_state
            .snapshots
            .insert(key.clone(), Box::new(snapshot));

        let processor = SnapshotProcessor::new(1);
        let update = processor
            .cleanup_expired_snapshots(&in_memory_state)
            .unwrap();

        assert_eq!(
            update.updated_snapshots.len(),
            1,
            "Expired snapshot should be marked for deletion"
        );
        assert_eq!(
            update.updated_snapshots.get(&key).unwrap().status,
            SnapshotStatus::Deleting,
            "Expired snapshot should have Deleting status"
        );
    }

    #[test]
    fn test_cleanup_expired_snapshots_already_deleting() {
        // Snapshot already in Deleting status should not be updated again
        let mut in_memory_state = InMemoryState::default();
        let (key, snapshot) = create_test_snapshot(
            "default",
            "snap1",
            "sb1",
            3600,
            SnapshotStatus::Deleting,
            7200,
        );
        in_memory_state.snapshots.insert(key, Box::new(snapshot));

        let processor = SnapshotProcessor::new(1);
        let update = processor
            .cleanup_expired_snapshots(&in_memory_state)
            .unwrap();

        assert!(
            update.updated_snapshots.is_empty(),
            "Snapshots already being deleted should not be updated"
        );
    }

    #[test]
    fn test_cleanup_multiple_snapshots_mixed() {
        // Test with multiple snapshots: some expired, some not, some with no TTL
        let mut in_memory_state = InMemoryState::default();

        // Expired snapshot
        let (key1, snapshot1) = create_test_snapshot(
            "default",
            "snap1",
            "sb1",
            3600,
            SnapshotStatus::Active,
            7200,
        );
        in_memory_state
            .snapshots
            .insert(key1.clone(), Box::new(snapshot1));

        // Not expired
        let (key2, snapshot2) =
            create_test_snapshot("default", "snap2", "sb2", 3600, SnapshotStatus::Active, 10);
        in_memory_state.snapshots.insert(key2, Box::new(snapshot2));

        // No TTL
        let (key3, snapshot3) =
            create_test_snapshot("default", "snap3", "sb3", 0, SnapshotStatus::Active, 100000);
        in_memory_state.snapshots.insert(key3, Box::new(snapshot3));

        // Already deleting
        let (key4, snapshot4) = create_test_snapshot(
            "default",
            "snap4",
            "sb4",
            3600,
            SnapshotStatus::Deleting,
            7200,
        );
        in_memory_state.snapshots.insert(key4, Box::new(snapshot4));

        let processor = SnapshotProcessor::new(1);
        let update = processor
            .cleanup_expired_snapshots(&in_memory_state)
            .unwrap();

        assert_eq!(
            update.updated_snapshots.len(),
            1,
            "Only one expired snapshot should be marked for deletion"
        );
        assert!(
            update.updated_snapshots.contains_key(&key1),
            "The expired snapshot should be in the update"
        );
    }
}
