use std::collections::HashMap;

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
}

impl PartialEq for PendingRunPoint {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
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
    executor_id: ExecutorId,
    fn_uri: FunctionURI,
}

/// Composite key for sorted pending runs index.
/// Sorted by (cpu_ms, created_at_ns, run_key) for efficient range queries.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct PendingRunSortKey {
    cpu_ms: u32,
    created_at_ns: u64,
    key: FunctionRunKey,
}

/// Resource placement index using imbl collections for O(1) clone
/// (copy-on-write).
///
/// Uses sorted index by CPU for range queries - most runs are CPU-constrained,
/// so filtering by CPU first then checking other dimensions is efficient.
#[derive(Clone, Debug, Default)]
pub struct ResourcePlacementIndex {
    // Primary index: sorted by CPU for range queries
    // Key: (cpu_ms, created_at_ns, run_key) for unique ordering
    pending_by_cpu: imbl::OrdMap<PendingRunSortKey, PendingRunPoint>,

    // Secondary index for O(1) lookup by key
    pending_by_key: imbl::HashMap<FunctionRunKey, PendingRunSortKey>,

    // Secondary index: runs by function URI
    pending_by_fn_uri: imbl::HashMap<FunctionURI, imbl::HashSet<FunctionRunKey>>,

    // Executor tracking
    executors: imbl::HashMap<ExecutorId, TrackedExecutor>,

    // FE tracking for autoscaling
    fes_by_id: imbl::HashMap<FunctionExecutorId, TrackedFE>,
    fes_by_executor_function: imbl::HashMap<ExecutorFunctionKey, imbl::HashSet<FunctionExecutorId>>,
    fes_by_fn_uri: imbl::HashMap<FunctionURI, imbl::HashSet<FunctionExecutorId>>,
    idle_fes_by_executor: imbl::HashMap<ExecutorId, imbl::HashSet<FunctionExecutorId>>,
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

        let sort_key = PendingRunSortKey {
            cpu_ms: point.cpu_ms,
            created_at_ns: point.created_at_ns,
            key: point.key.clone(),
        };

        // Add to secondary index by function
        self.pending_by_fn_uri
            .entry(point.fn_uri.clone())
            .or_default()
            .insert(point.key.clone());

        // Add to key lookup
        self.pending_by_key
            .insert(point.key.clone(), sort_key.clone());

        // Add to sorted index
        self.pending_by_cpu.insert(sort_key, point);
    }

    pub fn remove_pending_run(&mut self, run_key: &FunctionRunKey) -> Option<PendingRunPoint> {
        let sort_key = self.pending_by_key.remove(run_key)?;
        let point = self.pending_by_cpu.remove(&sort_key)?;

        debug!(
            run_key = %run_key,
            "removing pending run from placement index"
        );

        if let Some(mut keys) = self.pending_by_fn_uri.remove(&point.fn_uri) {
            keys.remove(run_key);
            if !keys.is_empty() {
                self.pending_by_fn_uri.insert(point.fn_uri.clone(), keys);
            }
        }

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
        let fe_ids_to_remove: Vec<_> = self
            .fes_by_id
            .iter()
            .filter(|(_, fe)| &fe.executor_id == executor_id)
            .map(|(id, _)| id.clone())
            .collect();

        for fe_id in fe_ids_to_remove {
            self.remove_fe(&fe_id);
        }

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

        debug!(
            free_cpu = free.cpu_ms_per_sec,
            free_memory = free.memory_bytes,
            free_disk = free.disk_bytes,
            total_pending = self.pending_by_key.len(),
            "querying index for matching runs"
        );

        // Create upper bound key for range query
        // We want all runs with cpu_ms <= free.cpu_ms_per_sec
        let upper_bound = PendingRunSortKey {
            cpu_ms: free.cpu_ms_per_sec + 1, // exclusive upper bound
            created_at_ns: 0,
            key: FunctionRunKey::new("", "", "", ""),
        };

        // Range query: get all runs with CPU <= capacity, then filter by other
        // dimensions
        let mut results: Vec<_> = self
            .pending_by_cpu
            .range(..upper_bound)
            .map(|(_, point)| point)
            .filter(|point| self.matches_constraints(point, executor))
            .cloned()
            .collect();

        debug!(matches_found = results.len(), "index query completed");

        // Already sorted by (cpu_ms, created_at_ns) due to OrdMap ordering
        // Just need to truncate
        results.truncate(limit);
        results
    }

    fn matches_constraints(&self, point: &PendingRunPoint, executor: &TrackedExecutor) -> bool {
        let free = &executor.free_resources;

        // Check memory
        if point.memory_bytes > free.memory_bytes {
            return false;
        }

        // Check disk
        if point.disk_bytes > free.disk_bytes {
            return false;
        }

        // Check placement constraints
        if !point.placement_constraints.matches(&executor.labels) {
            return false;
        }

        // Check GPU requirements
        if let Some(required_gpu) = &point.gpu {
            let Some(executor_gpu) = &free.gpu else {
                return false;
            };

            if required_gpu.model != executor_gpu.model || required_gpu.count > executor_gpu.count {
                return false;
            }
        }

        true
    }

    pub fn get_runs_for_function(&self, fn_uri: &FunctionURI) -> Vec<FunctionRunKey> {
        self.pending_by_fn_uri
            .get(fn_uri)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

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

        if let Some(mut fes) = self.fes_by_executor_function.remove(&key) {
            fes.remove(fe_id);
            if !fes.is_empty() {
                self.fes_by_executor_function.insert(key, fes);
            }
        }

        if let Some(mut fes) = self.fes_by_fn_uri.remove(&fe.fn_uri) {
            fes.remove(fe_id);
            if !fes.is_empty() {
                self.fes_by_fn_uri.insert(fe.fn_uri.clone(), fes);
            }
        }

        if let Some(mut idle) = self.idle_fes_by_executor.remove(&fe.executor_id) {
            idle.remove(fe_id);
            if !idle.is_empty() {
                self.idle_fes_by_executor
                    .insert(fe.executor_id.clone(), idle);
            }
        }
    }

    pub fn update_fe_allocation_count(&mut self, fe_id: &FunctionExecutorId, count: u32) {
        let Some(fe) = self.fes_by_id.get(fe_id).cloned() else {
            return;
        };
        let was_idle = fe.is_idle();

        // Update the FE
        let mut updated_fe = fe.clone();
        updated_fe.allocation_count = count;
        let is_idle = updated_fe.is_idle();
        self.fes_by_id.insert(fe_id.clone(), updated_fe);

        // Update idle tracking
        if was_idle && !is_idle {
            if let Some(mut idle) = self.idle_fes_by_executor.remove(&fe.executor_id) {
                idle.remove(fe_id);
                if !idle.is_empty() {
                    self.idle_fes_by_executor
                        .insert(fe.executor_id.clone(), idle);
                }
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

    #[test]
    fn test_clone_is_independent() {
        let mut index = ResourcePlacementIndex::new();
        index.add_pending_run(make_point("call1", 1000, 2048, 1024, 0));

        // Clone the index
        let mut cloned = index.clone();

        // Modify original
        index.add_pending_run(make_point("call2", 1000, 2048, 1024, 0));

        // Clone should NOT see the new run (COW semantics)
        assert_eq!(index.pending_count(), 2);
        assert_eq!(cloned.pending_count(), 1);

        // Modify clone
        cloned.remove_pending_run(&make_run_key("call1"));

        // Original should still have call1
        assert!(index.is_pending(&make_run_key("call1")));
        assert!(!cloned.is_pending(&make_run_key("call1")));
    }
}
