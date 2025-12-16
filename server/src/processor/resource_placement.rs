//! Resource Placement Index using R-Tree for efficient spatial queries.
//!
//! This module provides O(log N + K) lookups to find pending function runs
//! that fit within an executor's available resources, replacing the previous
//! O(N) linear scan approach.
//!
//! ## Key Concepts
//!
//! - **Pending runs** are indexed as points in 4D resource space (cpu, memory,
//!   disk, gpu)
//! - **Executor capacity** is a region in that space: [0, free_resources]
//! - **Finding fits** = range query: find all points within the region
//!
//! ## Complexity
//!
//! | Operation | Complexity |
//! |-----------|------------|
//! | Add pending run | O(log N) |
//! | Remove pending run | O(log N) |
//! | Find runs for executor | O(log N + K) where K = matching runs |
//! | Update executor capacity | O(1) |

use std::collections::{HashMap, HashSet};

use rstar::{AABB, RTree, RTreeObject};
use tracing::debug;

use crate::{
    data_model::{ExecutorId, FunctionResources, FunctionURI, HostResources, filter::LabelsFilter},
    state_store::in_memory_state::FunctionRunKey,
};

/// A pending function run represented as a point in 4D resource space.
///
/// The R-Tree indexes these points, allowing efficient range queries
/// to find all runs that fit within given resource constraints.
#[derive(Clone, Debug)]
pub struct PendingRunPoint {
    /// Unique identifier for the function run
    pub run_key: FunctionRunKey,

    /// Function URI for creating allocations
    pub fn_uri: FunctionURI,

    /// Resource requirements - these form the 4D coordinates
    pub cpu_ms: u32,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub gpu_count: u32,

    /// Non-spatial constraints (filtered after spatial query)
    pub gpu_model: Option<String>,
    pub placement_constraints: LabelsFilter,

    /// Creation time for FIFO ordering
    pub created_at_ns: u64,
}

impl PendingRunPoint {
    /// Create a new pending run point from function run data
    pub fn new(
        run_key: FunctionRunKey,
        fn_uri: FunctionURI,
        resources: &FunctionResources,
        placement_constraints: LabelsFilter,
        created_at_ns: u64,
    ) -> Self {
        // Get GPU info from first config if available
        let (gpu_count, gpu_model) = resources
            .gpu_configs
            .first()
            .map(|g| (g.count, Some(g.model.clone())))
            .unwrap_or((0, None));

        Self {
            run_key,
            fn_uri,
            cpu_ms: resources.cpu_ms_per_sec,
            memory_bytes: resources.memory_mb * 1024 * 1024,
            disk_bytes: resources.ephemeral_disk_mb * 1024 * 1024,
            gpu_count,
            gpu_model,
            placement_constraints,
            created_at_ns,
        }
    }
}

impl PartialEq for PendingRunPoint {
    fn eq(&self, other: &Self) -> bool {
        self.run_key == other.run_key
    }
}

impl RTreeObject for PendingRunPoint {
    type Envelope = AABB<[i64; 4]>;

    fn envelope(&self) -> Self::Envelope {
        // Represent this point as a zero-volume bounding box
        let point = [
            self.cpu_ms as i64,
            self.memory_bytes as i64,
            self.disk_bytes as i64,
            self.gpu_count as i64,
        ];
        AABB::from_point(point)
    }
}

/// Executor capacity information for matching against pending runs
#[derive(Clone, Debug)]
pub struct ExecutorCapacity {
    /// Free resources available on this executor
    pub free_resources: HostResources,

    /// All labels on this executor (including region if present)
    pub labels: HashMap<String, String>,
}

impl ExecutorCapacity {
    pub fn new(free_resources: HostResources, labels: HashMap<String, String>) -> Self {
        Self {
            free_resources,
            labels,
        }
    }
}

/// Resource Placement Index using R-Tree for O(log N + K) queries.
///
/// This replaces the linear scan through `unallocated_function_runs` with
/// an efficient spatial index that can quickly find runs matching executor
/// capacity.
#[derive(Debug, Default)]
pub struct ResourcePlacementIndex {
    /// R-Tree indexing pending runs by resource requirements
    pending_tree: RTree<PendingRunPoint>,

    /// Reverse lookup for O(1) removal by run_key
    pending_by_key: HashMap<FunctionRunKey, PendingRunPoint>,

    /// Secondary index: runs by function URI (for function-specific queries)
    pending_by_fn_uri: HashMap<FunctionURI, HashSet<FunctionRunKey>>,

    /// Executor capacity tracking
    executor_capacity: HashMap<ExecutorId, ExecutorCapacity>,
}

impl ResourcePlacementIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a pending function run to the index. O(log N)
    pub fn add_pending_run(&mut self, point: PendingRunPoint) {
        // Skip if already present
        if self.pending_by_key.contains_key(&point.run_key) {
            debug!(
                run_key = %point.run_key,
                "run already in placement index, skipping"
            );
            return;
        }

        debug!(
            run_key = %point.run_key,
            cpu_ms = point.cpu_ms,
            memory_bytes = point.memory_bytes,
            "adding pending run to placement index"
        );

        // Add to secondary index
        self.pending_by_fn_uri
            .entry(point.fn_uri.clone())
            .or_default()
            .insert(point.run_key.clone());

        // Add to reverse lookup
        self.pending_by_key
            .insert(point.run_key.clone(), point.clone());

        // Add to R-Tree
        self.pending_tree.insert(point);
    }

    /// Remove a pending function run from the index. O(log N)
    pub fn remove_pending_run(&mut self, run_key: &FunctionRunKey) -> Option<PendingRunPoint> {
        let point = self.pending_by_key.remove(run_key)?;

        debug!(
            run_key = %run_key,
            "removing pending run from placement index"
        );

        // Remove from secondary index
        if let Some(keys) = self.pending_by_fn_uri.get_mut(&point.fn_uri) {
            keys.remove(run_key);
            if keys.is_empty() {
                self.pending_by_fn_uri.remove(&point.fn_uri);
            }
        }

        // Remove from R-Tree
        self.pending_tree.remove(&point);

        Some(point)
    }

    /// Update executor capacity. O(1)
    pub fn update_executor_capacity(
        &mut self,
        executor_id: ExecutorId,
        capacity: ExecutorCapacity,
    ) {
        debug!(
            executor_id = executor_id.get(),
            free_cpu = capacity.free_resources.cpu_ms_per_sec,
            free_memory = capacity.free_resources.memory_bytes,
            "updating executor capacity in placement index"
        );
        self.executor_capacity.insert(executor_id, capacity);
    }

    /// Remove executor from the index. O(1)
    pub fn remove_executor(&mut self, executor_id: &ExecutorId) {
        debug!(
            executor_id = executor_id.get(),
            "removing executor from placement index"
        );
        self.executor_capacity.remove(executor_id);
    }

    /// Find pending runs that fit within an executor's free resources.
    ///
    /// This is the key method - O(log N + K) instead of O(N).
    ///
    /// Returns runs sorted by creation time (FIFO) up to the limit.
    pub fn find_runs_for_executor(
        &self,
        executor_id: &ExecutorId,
        limit: usize,
    ) -> Vec<PendingRunPoint> {
        let Some(capacity) = self.executor_capacity.get(executor_id) else {
            return vec![];
        };

        self.find_runs_for_capacity(capacity, limit)
    }

    /// Find pending runs that fit within given capacity.
    ///
    /// Useful when you have capacity but haven't stored it yet.
    pub fn find_runs_for_capacity(
        &self,
        capacity: &ExecutorCapacity,
        limit: usize,
    ) -> Vec<PendingRunPoint> {
        let free = &capacity.free_resources;

        // Build the query box: [0,0,0,0] to [free_cpu, free_mem, free_disk, free_gpu]
        let max_gpu = free.gpu.as_ref().map(|g| g.count as i64).unwrap_or(0);
        let query_box = AABB::from_corners(
            [0, 0, 0, 0],
            [
                free.cpu_ms_per_sec as i64,
                free.memory_bytes as i64,
                free.disk_bytes as i64,
                max_gpu,
            ],
        );

        debug!(
            free_cpu = free.cpu_ms_per_sec,
            free_memory = free.memory_bytes,
            free_disk = free.disk_bytes,
            free_gpu = max_gpu,
            total_pending = self.pending_by_key.len(),
            "querying R-Tree for matching runs"
        );

        // R-Tree range query - O(log N + K)
        let mut results: Vec<_> = self
            .pending_tree
            .locate_in_envelope(&query_box)
            .filter(|point| self.matches_constraints(point, capacity))
            .cloned()
            .collect();

        debug!(matches_found = results.len(), "R-Tree query completed");

        // Sort by creation time (FIFO - oldest first)
        results.sort_by_key(|p| p.created_at_ns);

        // Return up to limit
        results.truncate(limit);
        results
    }

    /// Check non-spatial constraints (labels, GPU model, region)
    fn matches_constraints(&self, point: &PendingRunPoint, capacity: &ExecutorCapacity) -> bool {
        // Check placement constraints against executor labels
        if !point.placement_constraints.matches(&capacity.labels) {
            return false;
        }

        // Check GPU model if required
        if let Some(required_model) = &point.gpu_model {
            match &capacity.free_resources.gpu {
                Some(gpu) if &gpu.model != required_model => return false,
                None => return false,
                _ => {}
            }
        }

        true
    }

    /// Get pending runs for a specific function. O(1) lookup + O(K) iteration.
    #[cfg(test)]
    pub fn get_runs_for_function(&self, fn_uri: &FunctionURI) -> Vec<FunctionRunKey> {
        self.pending_by_fn_uri
            .get(fn_uri)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Check if a run is pending. O(1)
    #[cfg(test)]
    pub fn is_pending(&self, run_key: &FunctionRunKey) -> bool {
        self.pending_by_key.contains_key(run_key)
    }

    /// Total count of pending runs. O(1)
    pub fn pending_count(&self) -> usize {
        self.pending_by_key.len()
    }

    /// Count of tracked executors. O(1)
    pub fn executor_count(&self) -> usize {
        self.executor_capacity.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_fn_uri(name: &str) -> FunctionURI {
        FunctionURI {
            namespace: "test".to_string(),
            application: "app".to_string(),
            function: name.to_string(),
            version: "v1".to_string(),
        }
    }

    fn make_run_key(id: &str) -> FunctionRunKey {
        FunctionRunKey::new("test", "app", "req1", id)
    }

    fn make_point(
        id: &str,
        cpu_ms: u32,
        memory_mb: u64,
        disk_mb: u64,
        gpu_count: u32,
    ) -> PendingRunPoint {
        PendingRunPoint {
            run_key: make_run_key(id),
            fn_uri: make_fn_uri("func1"),
            cpu_ms,
            memory_bytes: memory_mb * 1024 * 1024,
            disk_bytes: disk_mb * 1024 * 1024,
            gpu_count,
            gpu_model: None,
            placement_constraints: LabelsFilter::default(),
            created_at_ns: 0,
        }
    }

    fn make_capacity(
        cpu_ms: u32,
        memory_mb: u64,
        disk_mb: u64,
        gpu_count: u32,
    ) -> ExecutorCapacity {
        ExecutorCapacity {
            free_resources: HostResources {
                cpu_ms_per_sec: cpu_ms,
                memory_bytes: memory_mb * 1024 * 1024,
                disk_bytes: disk_mb * 1024 * 1024,
                gpu: if gpu_count > 0 {
                    Some(crate::data_model::GPUResources {
                        count: gpu_count,
                        model: "A100".to_string(),
                    })
                } else {
                    None
                },
            },
            labels: HashMap::new(),
        }
    }

    #[test]
    fn test_add_and_remove_pending_run() {
        let mut index = ResourcePlacementIndex::new();

        let point = make_point("call1", 1000, 2048, 1024, 0);
        index.add_pending_run(point.clone());

        assert_eq!(index.pending_count(), 1);
        assert!(index.is_pending(&make_run_key("call1")));

        let removed = index.remove_pending_run(&make_run_key("call1"));
        assert!(removed.is_some());
        assert_eq!(index.pending_count(), 0);
        assert!(!index.is_pending(&make_run_key("call1")));
    }

    #[test]
    fn test_find_runs_for_executor_basic() {
        let mut index = ResourcePlacementIndex::new();

        // Add some pending runs with different resource requirements
        index.add_pending_run(make_point("small", 500, 1024, 512, 0));
        index.add_pending_run(make_point("medium", 1000, 2048, 1024, 0));
        index.add_pending_run(make_point("large", 2000, 4096, 2048, 0));
        index.add_pending_run(make_point("huge", 4000, 8192, 4096, 0));

        // Create executor with medium capacity
        let executor_id = ExecutorId::new("exec1".to_string());
        let capacity = make_capacity(2000, 4096, 2048, 0);
        index.update_executor_capacity(executor_id.clone(), capacity);

        // Should find small, medium, large (all fit in 2000 CPU, 4GB mem, 2GB disk)
        let matches = index.find_runs_for_executor(&executor_id, 100);
        assert_eq!(matches.len(), 3);

        // "huge" should NOT match (needs 4000 CPU, 8GB mem)
        let match_ids: Vec<_> = matches.iter().map(|p| p.run_key.to_string()).collect();
        assert!(!match_ids.iter().any(|id| id.contains("huge")));
    }

    #[test]
    fn test_find_runs_respects_limit() {
        let mut index = ResourcePlacementIndex::new();

        // Add many small runs
        for i in 0..100 {
            let mut point = make_point(&format!("call{}", i), 100, 256, 128, 0);
            point.created_at_ns = i as u64; // Ensure ordering
            index.add_pending_run(point);
        }

        let executor_id = ExecutorId::new("exec1".to_string());
        let capacity = make_capacity(1000, 8192, 4096, 0);
        index.update_executor_capacity(executor_id.clone(), capacity);

        // Request only 10
        let matches = index.find_runs_for_executor(&executor_id, 10);
        assert_eq!(matches.len(), 10);

        // Should be FIFO ordered (oldest first)
        assert!(matches[0].created_at_ns < matches[9].created_at_ns);
    }

    #[test]
    fn test_runs_by_function() {
        let mut index = ResourcePlacementIndex::new();

        let mut point1 = make_point("call1", 1000, 2048, 1024, 0);
        point1.fn_uri = make_fn_uri("func_a");

        let mut point2 = make_point("call2", 1000, 2048, 1024, 0);
        point2.fn_uri = make_fn_uri("func_a");

        let mut point3 = make_point("call3", 1000, 2048, 1024, 0);
        point3.fn_uri = make_fn_uri("func_b");

        index.add_pending_run(point1);
        index.add_pending_run(point2);
        index.add_pending_run(point3);

        let func_a_runs = index.get_runs_for_function(&make_fn_uri("func_a"));
        assert_eq!(func_a_runs.len(), 2);

        let func_b_runs = index.get_runs_for_function(&make_fn_uri("func_b"));
        assert_eq!(func_b_runs.len(), 1);
    }

    #[test]
    fn test_gpu_matching() {
        let mut index = ResourcePlacementIndex::new();

        // Run that needs GPU
        let mut gpu_run = make_point("gpu_call", 1000, 2048, 1024, 1);
        gpu_run.gpu_model = Some("A100".to_string());
        index.add_pending_run(gpu_run);

        // Run that doesn't need GPU
        let cpu_run = make_point("cpu_call", 1000, 2048, 1024, 0);
        index.add_pending_run(cpu_run);

        // Executor without GPU
        let executor_no_gpu = ExecutorId::new("no_gpu".to_string());
        let capacity_no_gpu = make_capacity(2000, 4096, 2048, 0);
        index.update_executor_capacity(executor_no_gpu.clone(), capacity_no_gpu);

        // Should only find CPU run
        let matches = index.find_runs_for_executor(&executor_no_gpu, 100);
        assert_eq!(matches.len(), 1);
        assert!(matches[0].run_key.to_string().contains("cpu_call"));

        // Executor with GPU
        let executor_with_gpu = ExecutorId::new("with_gpu".to_string());
        let capacity_with_gpu = make_capacity(2000, 4096, 2048, 1);
        index.update_executor_capacity(executor_with_gpu.clone(), capacity_with_gpu);

        // Should find both runs
        let matches = index.find_runs_for_executor(&executor_with_gpu, 100);
        assert_eq!(matches.len(), 2);
    }

    #[test]
    fn test_duplicate_add_is_idempotent() {
        let mut index = ResourcePlacementIndex::new();

        let point = make_point("call1", 1000, 2048, 1024, 0);
        index.add_pending_run(point.clone());
        index.add_pending_run(point.clone());
        index.add_pending_run(point);

        assert_eq!(index.pending_count(), 1);
    }

    #[test]
    fn test_remove_nonexistent_returns_none() {
        let mut index = ResourcePlacementIndex::new();
        let result = index.remove_pending_run(&make_run_key("nonexistent"));
        assert!(result.is_none());
    }
}
