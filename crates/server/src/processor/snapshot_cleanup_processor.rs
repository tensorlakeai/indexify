use std::collections::HashMap;

use anyhow::Result;
use tracing::info;

use crate::{
    data_model::{SnapshotKey, SnapshotStatus},
    state_store::{in_memory_state::InMemoryState, requests::SchedulerUpdateRequest},
    utils::get_epoch_time_in_ns,
};

/// Processor responsible for cleaning up expired snapshots based on TTL
pub struct SnapshotCleanupProcessor {
    /// Last time cleanup was performed (epoch nanoseconds)
    last_cleanup_time: u128,
    /// Cleanup interval in nanoseconds (default: 1 hour)
    cleanup_interval_ns: u128,
}

impl Default for SnapshotCleanupProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotCleanupProcessor {
    /// Create a new SnapshotCleanupProcessor with default 1-hour interval
    pub fn new() -> Self {
        Self {
            last_cleanup_time: 0,
            cleanup_interval_ns: 3600 * 1_000_000_000, // 1 hour in nanoseconds
        }
    }

    /// Create a SnapshotCleanupProcessor with custom cleanup interval
    pub fn with_interval_secs(interval_secs: u64) -> Self {
        Self {
            last_cleanup_time: 0,
            cleanup_interval_ns: interval_secs as u128 * 1_000_000_000,
        }
    }

    /// Check if cleanup should run based on interval
    fn should_run_cleanup(&self) -> bool {
        // If never run (last_cleanup_time == 0), should run
        if self.last_cleanup_time == 0 {
            return true;
        }

        let now = get_epoch_time_in_ns();
        now.saturating_sub(self.last_cleanup_time) >= self.cleanup_interval_ns
    }

    /// Main cleanup method - finds and marks expired snapshots for deletion
    ///
    /// This method:
    /// 1. Checks if cleanup interval has elapsed
    /// 2. Finds all snapshots with expired TTL
    /// 3. Creates deletion state changes for expired snapshots
    /// 4. Returns SchedulerUpdateRequest with deletions
    pub async fn process_cleanup(
        &mut self,
        in_memory_state: &InMemoryState,
    ) -> Result<Option<SchedulerUpdateRequest>> {
        // Check if we should run cleanup based on interval
        if !self.should_run_cleanup() {
            return Ok(None);
        }

        let now = get_epoch_time_in_ns();
        self.last_cleanup_time = now;

        // Find expired snapshots
        let expired_snapshots = self.find_expired_snapshots(in_memory_state, now);

        if expired_snapshots.is_empty() {
            return Ok(None);
        }

        info!(
            count = expired_snapshots.len(),
            "found expired snapshots for cleanup"
        );

        // Create update request with snapshot deletions
        let mut update = SchedulerUpdateRequest::default();

        // Mark snapshots as deleted
        for (key, snapshot) in expired_snapshots {
            info!(
                namespace = %snapshot.namespace,
                snapshot_id = %snapshot.id.get(),
                age_secs = (now - snapshot.created_at) / 1_000_000_000,
                ttl_secs = snapshot.ttl_secs,
                "marking expired snapshot for deletion"
            );

            update.deleted_snapshots.insert(key);
        }

        Ok(Some(update))
    }

    /// Find all snapshots that have exceeded their TTL
    fn find_expired_snapshots(
        &self,
        in_memory_state: &InMemoryState,
        now: u128,
    ) -> HashMap<SnapshotKey, Box<crate::data_model::SandboxSnapshot>> {
        let mut expired = HashMap::new();

        for (key, snapshot) in &in_memory_state.snapshots {
            // Skip snapshots with no TTL (ttl_secs = 0)
            if snapshot.ttl_secs == 0 {
                continue;
            }

            // Skip snapshots not in Active status (already being deleted or creating)
            if snapshot.status != SnapshotStatus::Active {
                continue;
            }

            // Check if snapshot has expired
            let age_secs = (now - snapshot.created_at) / 1_000_000_000;
            if age_secs >= snapshot.ttl_secs as u128 {
                expired.insert(key.clone(), snapshot.clone());
            }
        }

        expired
    }

    /// Get statistics about snapshots for monitoring
    pub fn get_snapshot_stats(&self, in_memory_state: &InMemoryState) -> SnapshotStats {
        let now = get_epoch_time_in_ns();
        self.get_snapshot_stats_at_time(in_memory_state, now)
    }

    /// Get statistics about snapshots at a specific time (for testing)
    fn get_snapshot_stats_at_time(
        &self,
        in_memory_state: &InMemoryState,
        now: u128,
    ) -> SnapshotStats {
        let mut stats = SnapshotStats::default();

        for snapshot in in_memory_state.snapshots.values() {
            // Count by status
            match snapshot.status {
                SnapshotStatus::Creating => stats.creating_count += 1,
                SnapshotStatus::Active => stats.active_count += 1,
                SnapshotStatus::Deleting => stats.deleting_count += 1,
            }

            // Count by namespace
            *stats
                .count_by_namespace
                .entry(snapshot.namespace.clone())
                .or_insert(0) += 1;

            // Track total size
            if let Some(size) = snapshot.size_bytes {
                stats.total_size_bytes += size;
            }

            // Track age
            let age_secs = (now - snapshot.created_at) / 1_000_000_000;
            if age_secs > stats.max_age_secs {
                stats.max_age_secs = age_secs;
            }

            // Count expired but not deleted yet
            if snapshot.ttl_secs > 0 && age_secs >= snapshot.ttl_secs as u128 {
                stats.expired_count += 1;
            }
        }

        stats.total_count = stats.creating_count + stats.active_count + stats.deleting_count;

        stats
    }
}

/// Statistics about snapshots for monitoring and metrics
#[derive(Debug, Default, Clone)]
pub struct SnapshotStats {
    pub total_count: usize,
    pub creating_count: usize,
    pub active_count: usize,
    pub deleting_count: usize,
    pub expired_count: usize,
    pub total_size_bytes: u64,
    pub max_age_secs: u128,
    pub count_by_namespace: HashMap<String, usize>,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mock_instant::global::MockClock;

    use super::*;
    use crate::data_model::{SandboxId, SandboxSnapshot, SandboxSnapshotBuilder, SnapshotId};

    fn create_test_snapshot_with_time(
        namespace: &str,
        ttl_secs: u64,
        age_secs: u64,
        status: SnapshotStatus,
        current_time_ns: u128,
    ) -> SandboxSnapshot {
        use crate::data_model::{ContainerResources, SandboxSnapshotConfig};

        let age_ns = age_secs as u128 * 1_000_000_000;
        let created_at = current_time_ns.saturating_sub(age_ns);

        let config = SandboxSnapshotConfig {
            image: "test:latest".to_string(),
            resources: ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            },
            secret_names: vec![],
            timeout_secs: 600,
            entrypoint: None,
            network_policy: None,
        };

        SandboxSnapshotBuilder::default()
            .id(SnapshotId::generate())
            .sandbox_id(SandboxId::default())
            .namespace(namespace.to_string())
            .image_ref("test:latest".to_string())
            .created_at(created_at)
            .ttl_secs(ttl_secs)
            .status(status)
            .sandbox_config(config)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_find_expired_snapshots() {
        // Initialize mock clock with a reasonable time (e.g., 1000 seconds)
        MockClock::set_system_time(Duration::from_secs(1000));

        let processor = SnapshotCleanupProcessor::new();
        let mut in_memory_state = InMemoryState::default();

        let now = get_epoch_time_in_ns();

        // Add snapshot with TTL that hasn't expired (age 100s, TTL 200s)
        let snapshot1 =
            create_test_snapshot_with_time("ns1", 200, 100, SnapshotStatus::Active, now);
        let key1 = SnapshotKey::new(&snapshot1.namespace, &snapshot1.id);
        in_memory_state
            .snapshots
            .insert(key1.clone(), Box::new(snapshot1.clone()));

        // Add snapshot with TTL that has expired (age 300s, TTL 200s)
        let snapshot2 =
            create_test_snapshot_with_time("ns1", 200, 300, SnapshotStatus::Active, now);
        let key2 = SnapshotKey::new(&snapshot2.namespace, &snapshot2.id);
        in_memory_state
            .snapshots
            .insert(key2.clone(), Box::new(snapshot2.clone()));

        // Add snapshot with no TTL (should never expire)
        let snapshot3 = create_test_snapshot_with_time("ns1", 0, 1000, SnapshotStatus::Active, now);
        let key3 = SnapshotKey::new(&snapshot3.namespace, &snapshot3.id);
        in_memory_state
            .snapshots
            .insert(key3.clone(), Box::new(snapshot3.clone()));

        // Add expired snapshot but in Deleting status (should be skipped)
        let snapshot4 =
            create_test_snapshot_with_time("ns1", 200, 300, SnapshotStatus::Deleting, now);
        let key4 = SnapshotKey::new(&snapshot4.namespace, &snapshot4.id);
        in_memory_state
            .snapshots
            .insert(key4.clone(), Box::new(snapshot4.clone()));

        let expired = processor.find_expired_snapshots(&in_memory_state, now);

        // Should only find snapshot2 (expired and Active)
        assert_eq!(expired.len(), 1);
        assert!(expired.contains_key(&key2));
        assert!(!expired.contains_key(&key1)); // Not expired
        assert!(!expired.contains_key(&key3)); // No TTL
        assert!(!expired.contains_key(&key4)); // Already deleting
    }

    #[tokio::test]
    async fn test_cleanup_interval() {
        // Initialize mock clock
        MockClock::set_system_time(Duration::from_secs(1000));

        let mut processor = SnapshotCleanupProcessor::with_interval_secs(10);
        let in_memory_state = InMemoryState::default();

        // First run should execute
        assert!(processor.should_run_cleanup());
        let result = processor.process_cleanup(&in_memory_state).await.unwrap();
        // Even with no snapshots, it should run
        assert!(result.is_none()); // No expired snapshots

        // Immediate second run should not execute (within interval)
        assert!(!processor.should_run_cleanup());
    }

    #[test]
    fn test_snapshot_stats() {
        // Initialize mock clock with a reasonable time (e.g., 1000 seconds)
        MockClock::set_system_time(Duration::from_secs(1000));

        let processor = SnapshotCleanupProcessor::new();
        let mut in_memory_state = InMemoryState::default();

        let now = get_epoch_time_in_ns();

        // Add various snapshots
        let snapshot1 =
            create_test_snapshot_with_time("ns1", 200, 100, SnapshotStatus::Creating, now);
        let key1 = SnapshotKey::new(&snapshot1.namespace, &snapshot1.id);
        in_memory_state.snapshots.insert(key1, Box::new(snapshot1));

        let snapshot2 =
            create_test_snapshot_with_time("ns1", 200, 150, SnapshotStatus::Active, now);
        let key2 = SnapshotKey::new(&snapshot2.namespace, &snapshot2.id);
        in_memory_state.snapshots.insert(key2, Box::new(snapshot2));

        let snapshot3 =
            create_test_snapshot_with_time("ns2", 200, 300, SnapshotStatus::Active, now);
        let key3 = SnapshotKey::new(&snapshot3.namespace, &snapshot3.id);
        in_memory_state.snapshots.insert(key3, Box::new(snapshot3));

        let stats = processor.get_snapshot_stats_at_time(&in_memory_state, now);

        assert_eq!(stats.total_count, 3);
        assert_eq!(stats.creating_count, 1);
        assert_eq!(stats.active_count, 2);
        assert_eq!(stats.deleting_count, 0);
        assert_eq!(stats.expired_count, 1); // snapshot3 is expired
        assert_eq!(stats.count_by_namespace.get("ns1"), Some(&2));
        assert_eq!(stats.count_by_namespace.get("ns2"), Some(&1));
    }
}
