use std::collections::{HashMap, HashSet};

use rand::seq::SliceRandom;

use crate::{
    data_model::{ExecutorId, ExecutorMetadata, Function, FunctionResources},
    processor::container_scheduler::ContainerScheduler,
    scheduler::executor_class::ExecutorClass,
};

/// Per-pass constraint feasibility cache.
///
/// Caches the result of constraint checks keyed by (workload, executor class).
/// If one executor of a class fails a constraint check, all executors of the
/// same class will also fail — avoiding redundant evaluation.
#[derive(Debug, Default)]
pub struct FeasibilityCache {
    cache: HashMap<(WorkloadKey, ExecutorClass), bool>,
}

impl FeasibilityCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Look up whether a workload is feasible on a given executor class.
    pub fn get(&self, workload: &WorkloadKey, class: &ExecutorClass) -> Option<bool> {
        self.cache.get(&(workload.clone(), class.clone())).copied()
    }

    /// Record the feasibility result for a workload on an executor class.
    pub fn insert(&mut self, workload: WorkloadKey, class: ExecutorClass, feasible: bool) {
        self.cache.insert((workload, class), feasible);
    }
}

/// Identifies a workload for constraint-checking purposes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WorkloadKey {
    Function {
        namespace: String,
        application: String,
        function_name: String,
    },
    Sandbox {
        namespace: String,
    },
}

/// Result of executor selection.
#[derive(Debug, Clone)]
pub struct PlacementResult {
    /// The selected executor, if any.
    pub executor_id: Option<ExecutorId>,
    /// Executor classes that passed constraint checks.
    pub eligible_classes: HashSet<ExecutorClass>,
}

/// Compute which executor classes are eligible (pass constraint checks) and
/// eligible for a given workload. Used by the BlockedWorkTracker to record
/// accurate class information when placement wasn't attempted.
pub fn compute_eligible_classes(
    scheduler: &ContainerScheduler,
    cache: &mut FeasibilityCache,
    workload_key: &WorkloadKey,
    namespace: &str,
    application: &str,
    function: Option<&Function>,
) -> HashSet<ExecutorClass> {
    let mut eligible = HashSet::new();
    for (class, executor_ids) in &scheduler.executors_by_class {
        if let Some(executor_id) = executor_ids.iter().next() {
            let feasible = match cache.get(workload_key, class) {
                Some(cached) => cached,
                None => {
                    if let Some(executor) = scheduler.executors.get(executor_id) {
                        let is_feasible = check_constraints(
                            scheduler,
                            executor,
                            namespace,
                            application,
                            function,
                        );
                        cache.insert(workload_key.clone(), class.clone(), is_feasible);
                        is_feasible
                    } else {
                        false
                    }
                }
            };
            if feasible {
                eligible.insert(class.clone());
            }
        }
    }
    eligible
}

/// Select the best executor for a workload.
///
/// Reads from immutable `ContainerScheduler` snapshot.
/// - `limit`: controls how many feasible candidates to collect before choosing
///   the best (2 for normal allocation via power-of-two-choices, `usize::MAX`
///   for vacuum scans).
///
/// The selection process:
/// 1. Range scan `executors_by_free_memory` for executors with sufficient
///    memory
/// 2. Randomize start position (power-of-two-choices style)
/// 3. For each executor: a. Skip if tombstoned b. Look up ExecutorClass, check
///    feasibility cache c. On cache miss: run `executor_matches_constraints()`,
///    cache result d. Check free resources (executor_states updated inline by
///    register_container) e. If feasible: add to candidates f. Stop after
///    `limit` feasible candidates
/// 4. Return best by free memory (bin-packing)
pub fn select_executor(
    scheduler: &ContainerScheduler,
    cache: &mut FeasibilityCache,
    workload_key: &WorkloadKey,
    namespace: &str,
    application: &str,
    function: Option<&Function>,
    resources: &FunctionResources,
    limit: usize,
) -> PlacementResult {
    let mut result = PlacementResult {
        executor_id: None,
        eligible_classes: HashSet::new(),
    };

    let min_memory_bytes = resources.memory_mb * 1024 * 1024;

    // Range scan: all executors with >= min_memory_bytes free memory in the
    // snapshot. Note: some may have less effective free memory due to
    // in-flight placements within this batch.
    let range_start = (min_memory_bytes, ExecutorId::default());
    let candidates_from_index: Vec<(u64, ExecutorId)> = scheduler
        .executors_by_free_memory
        .range(range_start..)
        .cloned()
        .collect();

    if candidates_from_index.is_empty() {
        // No executor has enough free memory, but still compute feasibility
        // per class so the caller gets proper class info instead of escaped.
        result.eligible_classes = compute_eligible_classes(
            scheduler,
            cache,
            workload_key,
            namespace,
            application,
            function,
        );
        return result;
    }

    // Randomize the order for power-of-two-choices style selection
    let mut shuffled = candidates_from_index;
    let mut rng = rand::rng();
    shuffled.shuffle(&mut rng);

    let mut feasible_candidates: Vec<(ExecutorId, u64)> = Vec::new();

    for (snapshot_free_memory, executor_id) in &shuffled {
        // Look up executor metadata
        let Some(executor) = scheduler.executors.get(executor_id) else {
            continue;
        };

        // Skip tombstoned executors
        if executor.tombstoned {
            continue;
        }

        // Look up or compute the executor class
        let class = scheduler
            .executor_classes
            .get(executor_id)
            .cloned()
            .unwrap_or_else(|| ExecutorClass::from_executor(executor));

        // Check feasibility cache
        let feasible = match cache.get(workload_key, &class) {
            Some(cached) => cached,
            None => {
                // Cache miss — evaluate constraints
                let is_feasible =
                    check_constraints(scheduler, executor, namespace, application, function);
                cache.insert(workload_key.clone(), class.clone(), is_feasible);
                is_feasible
            }
        };

        if !feasible {
            continue;
        }

        result.eligible_classes.insert(class);

        // Check free memory (register_container already updates executor_states
        // inline, so the snapshot value is current within this batch)
        if *snapshot_free_memory < min_memory_bytes {
            continue;
        }

        // Check free CPU/disk
        let Some(executor_state) = scheduler.executor_states.get(executor_id) else {
            continue;
        };
        if executor_state.free_resources.cpu_ms_per_sec < resources.cpu_ms_per_sec {
            continue;
        }
        if executor_state.free_resources.disk_bytes < resources.ephemeral_disk_mb * 1024 * 1024 {
            continue;
        }
        // GPU check against snapshot values (context doesn't track GPU)
        if executor_state
            .free_resources
            .can_handle_function_resources(resources)
            .is_err()
        {
            continue;
        }

        feasible_candidates.push((executor_id.clone(), *snapshot_free_memory));

        if feasible_candidates.len() >= limit {
            break;
        }
    }

    // Select the candidate with the most free memory for load spreading.
    if let Some((best_id, _)) = feasible_candidates
        .iter()
        .max_by_key(|(_, free_mem)| *free_mem)
    {
        result.executor_id = Some(best_id.clone());
    }

    result
}

/// Check if an executor matches the workload's constraints.
///
/// Delegates to `ContainerScheduler::executor_matches_constraints` which checks
/// tombstone, allowlist, and placement constraints.
fn check_constraints(
    scheduler: &ContainerScheduler,
    executor: &ExecutorMetadata,
    namespace: &str,
    application: &str,
    function: Option<&Function>,
) -> bool {
    scheduler.executor_matches_constraints(executor, namespace, application, function)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feasibility_cache() {
        let mut cache = FeasibilityCache::new();
        let workload = WorkloadKey::Sandbox {
            namespace: "ns".to_string(),
        };
        let class = ExecutorClass {
            labels: Default::default(),
            allowlist: None,
        };

        assert_eq!(cache.get(&workload, &class), None);

        cache.insert(workload.clone(), class.clone(), true);
        assert_eq!(cache.get(&workload, &class), Some(true));

        cache.insert(workload.clone(), class.clone(), false);
        assert_eq!(cache.get(&workload, &class), Some(false));
    }

    #[test]
    fn test_workload_key_equality() {
        let k1 = WorkloadKey::Function {
            namespace: "ns".to_string(),
            application: "app".to_string(),
            function_name: "fn1".to_string(),
        };
        let k2 = WorkloadKey::Function {
            namespace: "ns".to_string(),
            application: "app".to_string(),
            function_name: "fn1".to_string(),
        };
        let k3 = WorkloadKey::Sandbox {
            namespace: "ns".to_string(),
        };

        assert_eq!(k1, k2);
        assert_ne!(k1, k3);
    }

    #[test]
    fn test_placement_result_default_no_executor() {
        let result = PlacementResult {
            executor_id: None,
            eligible_classes: HashSet::new(),
        };
        assert!(result.executor_id.is_none());
    }
}
