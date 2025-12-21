use std::collections::{HashMap, HashSet};

use rstar::{AABB, RTree, RTreeObject};
use tracing::debug;

use crate::data_model::{
    ExecutorId,
    FunctionExecutorId,
    FunctionResources,
    FunctionRunKey,
    FunctionURI,
    GPUResources,
    HostResources,
    filter::LabelsFilter,
};

#[derive(Clone, Debug)]
pub struct PendingRunPoint {
    pub key: FunctionRunKey,

    pub fn_uri: FunctionURI,

    pub cpu_ms: u32,
    pub memory_bytes: u64,
    pub disk_bytes: u64,

    pub gpu: Option<GPUResources>,

    pub placement_constraints: LabelsFilter,

    pub created_at_ns: u64,
}

impl PendingRunPoint {
    pub fn new(
        run_key: FunctionRunKey,
        fn_uri: FunctionURI,
        resources: &FunctionResources,
        placement_constraints: LabelsFilter,
        created_at_ns: u64,
    ) -> Self {
        Self {
            key: run_key,
            fn_uri,
            cpu_ms: resources.cpu_ms_per_sec,
            memory_bytes: resources.memory_mb * 1024 * 1024,
            disk_bytes: resources.ephemeral_disk_mb * 1024 * 1024,
            gpu: resources.gpu.clone(),
            placement_constraints,
            created_at_ns,
        }
    }

    fn gpu_count(&self) -> u32 {
        self.gpu.as_ref().map(|g| g.count).unwrap_or(0)
    }
}

impl PartialEq for PendingRunPoint {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
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
            self.gpu_count() as i64,
        ];
        AABB::from_point(point)
    }
}

/// Tracked executor state for resource matching
#[derive(Clone, Debug)]
struct TrackedExecutor {
    free_resources: HostResources,
    labels: HashMap<String, String>,
}

/// Tracked Function Executor for autoscaling
#[derive(Clone, Debug)]
pub struct TrackedFE {
    pub fe_id: FunctionExecutorId,
    pub fn_uri: FunctionURI,
    pub executor_id: ExecutorId,
    pub allocation_count: u32,
}

impl TrackedFE {
    pub fn is_idle(&self) -> bool {
        self.allocation_count == 0
    }
}

/// Key for FE grouping: (executor, function)
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ExecutorFunctionKey {
    pub executor_id: ExecutorId,
    pub fn_uri: FunctionURI,
}

#[derive(Debug, Default)]
pub struct ResourcePlacementIndex {
    pending_tree: RTree<PendingRunPoint>,

    pending_by_key: HashMap<FunctionRunKey, PendingRunPoint>,

    pending_by_fn_uri: HashMap<FunctionURI, HashSet<FunctionRunKey>>,

    executors: HashMap<ExecutorId, TrackedExecutor>,

    fes_by_id: HashMap<FunctionExecutorId, TrackedFE>,
    fes_by_executor_function: HashMap<ExecutorFunctionKey, HashSet<FunctionExecutorId>>,
    fes_by_fn_uri: HashMap<FunctionURI, HashSet<FunctionExecutorId>>,
    idle_fes_by_executor: HashMap<ExecutorId, HashSet<FunctionExecutorId>>,
}

impl ResourcePlacementIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_pending_run(&mut self, point: PendingRunPoint) {
        if self.pending_by_key.contains_key(&point.key) {
            debug!(
                run_key = %point.key,
                "run already in placement index, skipping"
            );
            return;
        }

        debug!(
            run_key = %point.key,
            cpu_ms = point.cpu_ms,
            memory_bytes = point.memory_bytes,
            "adding pending run to placement index"
        );

        // Add to secondary index
        self.pending_by_fn_uri
            .entry(point.fn_uri.clone())
            .or_default()
            .insert(point.key.clone());

        // Add to reverse lookup
        self.pending_by_key.insert(point.key.clone(), point.clone());

        // Add to R-Tree
        self.pending_tree.insert(point);
    }

    pub fn remove_pending_run(&mut self, run_key: &FunctionRunKey) -> Option<PendingRunPoint> {
        let point = self.pending_by_key.remove(run_key)?;

        debug!(
            run_key = %run_key,
            "removing pending run from placement index"
        );

        if let Some(keys) = self.pending_by_fn_uri.get_mut(&point.fn_uri) {
            keys.remove(run_key);
            if keys.is_empty() {
                self.pending_by_fn_uri.remove(&point.fn_uri);
            }
        }

        self.pending_tree.remove(&point);

        Some(point)
    }

    pub fn update_executor(
        &mut self,
        executor_id: ExecutorId,
        free_resources: HostResources,
        labels: HashMap<String, String>,
    ) {
        debug!(
            executor_id = executor_id.get(),
            free_cpu = free_resources.cpu_ms_per_sec,
            free_memory = free_resources.memory_bytes,
            "updating executor capacity in placement index"
        );
        self.executors.insert(
            executor_id,
            TrackedExecutor {
                free_resources,
                labels,
            },
        );
    }

    pub fn remove_executor(&mut self, executor_id: &ExecutorId) {
        debug!(
            executor_id = executor_id.get(),
            "removing executor from placement index"
        );
        self.executors.remove(executor_id);

        // Remove all FEs for this executor from tracking
        // Collect FE IDs to remove (can't modify while iterating)
        let fe_ids_to_remove: Vec<_> = self
            .fes_by_id
            .iter()
            .filter(|(_, fe)| &fe.executor_id == executor_id)
            .map(|(id, _)| id.clone())
            .collect();

        for fe_id in fe_ids_to_remove {
            self.remove_fe(&fe_id);
        }

        // Also clean up the idle tracking for this executor
        self.idle_fes_by_executor.remove(executor_id);
    }

    pub fn find_runs_for_executor(
        &self,
        executor_id: &ExecutorId,
        limit: usize,
    ) -> Vec<PendingRunPoint> {
        let Some(executor) = self.executors.get(executor_id) else {
            return vec![];
        };

        let free = &executor.free_resources;
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

        let mut results: Vec<_> = self
            .pending_tree
            .locate_in_envelope(&query_box)
            .filter(|point| self.matches_constraints(point, executor))
            .cloned()
            .collect();

        debug!(matches_found = results.len(), "R-Tree query completed");

        results.sort_by_key(|p| p.created_at_ns);
        results.truncate(limit);
        results
    }

    fn matches_constraints(&self, point: &PendingRunPoint, executor: &TrackedExecutor) -> bool {
        if !point.placement_constraints.matches(&executor.labels) {
            return false;
        }

        if let Some(required_gpu) = &point.gpu {
            let Some(executor_gpu) = &executor.free_resources.gpu else {
                return false;
            };

            if required_gpu.model != executor_gpu.model || required_gpu.count > executor_gpu.count {
                return false;
            }
        }

        true
    }

    /// Get pending runs for a specific function. O(1) lookup + O(K) iteration.
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

    pub fn pending_count(&self) -> usize {
        self.pending_by_key.len()
    }

    pub fn has_pending_runs(&self) -> bool {
        !self.pending_by_key.is_empty()
    }

    pub fn pending_run_keys(&self) -> impl Iterator<Item = &FunctionRunKey> {
        self.pending_by_key.keys()
    }

    pub fn executor_count(&self) -> usize {
        self.executors.len()
    }

    pub fn add_fe(&mut self, fe: TrackedFE) {
        let key = ExecutorFunctionKey {
            executor_id: fe.executor_id.clone(),
            fn_uri: fe.fn_uri.clone(),
        };
        self.fes_by_executor_function
            .entry(key)
            .or_default()
            .insert(fe.fe_id.clone());
        // Track globally by function URI (for min_fe_count)
        self.fes_by_fn_uri
            .entry(fe.fn_uri.clone())
            .or_default()
            .insert(fe.fe_id.clone());
        if fe.is_idle() {
            self.idle_fes_by_executor
                .entry(fe.executor_id.clone())
                .or_default()
                .insert(fe.fe_id.clone());
        }
        self.fes_by_id.insert(fe.fe_id.clone(), fe);
    }

    pub fn remove_fe(&mut self, fe_id: &FunctionExecutorId) {
        let Some(fe) = self.fes_by_id.remove(fe_id) else {
            return;
        };
        let key = ExecutorFunctionKey {
            executor_id: fe.executor_id.clone(),
            fn_uri: fe.fn_uri.clone(),
        };
        if let Some(fes) = self.fes_by_executor_function.get_mut(&key) {
            fes.remove(fe_id);
            if fes.is_empty() {
                self.fes_by_executor_function.remove(&key);
            }
        }
        // Clean up global function URI index
        if let Some(fes) = self.fes_by_fn_uri.get_mut(&fe.fn_uri) {
            fes.remove(fe_id);
            if fes.is_empty() {
                self.fes_by_fn_uri.remove(&fe.fn_uri);
            }
        }
        if let Some(idle) = self.idle_fes_by_executor.get_mut(&fe.executor_id) {
            idle.remove(fe_id);
            if idle.is_empty() {
                self.idle_fes_by_executor.remove(&fe.executor_id);
            }
        }
    }

    pub fn update_fe_allocation_count(&mut self, fe_id: &FunctionExecutorId, count: u32) {
        let Some(fe) = self.fes_by_id.get_mut(fe_id) else {
            return;
        };
        let was_idle = fe.is_idle();
        fe.allocation_count = count;
        let is_idle = fe.is_idle();
        if was_idle && !is_idle {
            if let Some(idle) = self.idle_fes_by_executor.get_mut(&fe.executor_id) {
                idle.remove(fe_id);
            }
        } else if !was_idle && is_idle {
            self.idle_fes_by_executor
                .entry(fe.executor_id.clone())
                .or_default()
                .insert(fe_id.clone());
        }
    }

    pub fn fe_count_for_function(&self, executor_id: &ExecutorId, fn_uri: &FunctionURI) -> usize {
        let key = ExecutorFunctionKey {
            executor_id: executor_id.clone(),
            fn_uri: fn_uri.clone(),
        };
        self.fes_by_executor_function
            .get(&key)
            .map(|s| s.len())
            .unwrap_or(0)
    }

    pub fn total_fe_count_for_function(&self, fn_uri: &FunctionURI) -> usize {
        self.fes_by_fn_uri.get(fn_uri).map(|s| s.len()).unwrap_or(0)
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
            key: make_run_key(id),
            fn_uri: make_fn_uri("func1"),
            cpu_ms,
            memory_bytes: memory_mb * 1024 * 1024,
            disk_bytes: disk_mb * 1024 * 1024,
            gpu: if gpu_count > 0 {
                Some(GPUResources {
                    count: gpu_count,
                    model: "A100".to_string(),
                })
            } else {
                None
            },
            placement_constraints: LabelsFilter::default(),
            created_at_ns: 0,
        }
    }

    fn make_resources(cpu_ms: u32, memory_mb: u64, disk_mb: u64, gpu_count: u32) -> HostResources {
        HostResources {
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
        index.update_executor(
            executor_id.clone(),
            make_resources(2000, 4096, 2048, 0),
            HashMap::new(),
        );

        // Should find small, medium, large (all fit in 2000 CPU, 4GB mem, 2GB disk)
        let matches = index.find_runs_for_executor(&executor_id, 100);
        assert_eq!(matches.len(), 3);

        // "huge" should NOT match (needs 4000 CPU, 8GB mem)
        let match_ids: Vec<_> = matches.iter().map(|p| p.key.to_string()).collect();
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
        index.update_executor(
            executor_id.clone(),
            make_resources(1000, 8192, 4096, 0),
            HashMap::new(),
        );

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
        let gpu_run = make_point("gpu_call", 1000, 2048, 1024, 1);
        index.add_pending_run(gpu_run);

        // Run that doesn't need GPU
        let cpu_run = make_point("cpu_call", 1000, 2048, 1024, 0);
        index.add_pending_run(cpu_run);

        // Executor without GPU
        let executor_no_gpu = ExecutorId::new("no_gpu".to_string());
        index.update_executor(
            executor_no_gpu.clone(),
            make_resources(2000, 4096, 2048, 0),
            HashMap::new(),
        );

        // Should only find CPU run
        let matches = index.find_runs_for_executor(&executor_no_gpu, 100);
        assert_eq!(matches.len(), 1);
        assert!(matches[0].key.to_string().contains("cpu_call"));

        // Executor with GPU
        let executor_with_gpu = ExecutorId::new("with_gpu".to_string());
        index.update_executor(
            executor_with_gpu.clone(),
            make_resources(2000, 4096, 2048, 1),
            HashMap::new(),
        );

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
