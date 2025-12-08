use std::hash::{Hash, Hasher};

use tracing::debug;

use crate::{
    data_model::{ExecutorMetadata, FunctionURI},
    state_store::in_memory_state::FunctionRunKey,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorClass {
    pub cpu_ms: u32,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    pub gpu_count: u32,
    pub gpu_model: Option<String>,
    pub region: Option<String>,
}

impl Hash for ExecutorClass {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.cpu_ms.hash(state);
        self.memory_bytes.hash(state);
        self.disk_bytes.hash(state);
        self.gpu_count.hash(state);
        self.gpu_model.hash(state);
        self.region.hash(state);
    }
}

impl ExecutorClass {
    pub fn from_executor(executor: &ExecutorMetadata) -> Self {
        let resources = &executor.host_resources;
        let gpu_count = resources.gpu.as_ref().map(|g| g.count).unwrap_or(0);
        let gpu_model = resources.gpu.as_ref().map(|g| g.model.clone());
        let region = executor.labels.get("region").cloned();

        ExecutorClass {
            cpu_ms: resources.cpu_ms_per_sec,
            memory_bytes: resources.memory_bytes,
            disk_bytes: resources.disk_bytes,
            gpu_count,
            gpu_model,
            region,
        }
    }

    /// Check if this executor class can handle the given function run
    /// requirements. Returns true if executor has >= resources than the
    /// function run needs.
    pub fn can_handle(&self, requirements: &FunctionRunRequirements) -> bool {
        self.cpu_ms >= requirements.cpu_ms &&
            self.memory_bytes >= requirements.memory_bytes &&
            self.disk_bytes >= requirements.disk_bytes &&
            self.gpu_count >= requirements.gpu_count &&
            (requirements.gpu_model.is_none() || self.gpu_model == requirements.gpu_model) &&
            (requirements.region.is_none() || self.region == requirements.region)
    }
}

/// Function run requirements - what resources a function run needs.
pub type FunctionRunRequirements = ExecutorClass;

/// Index for blocked function runs by ExecutorClass and FunctionURI.
///
/// Function runs are assigned to ALL executor classes that can handle them.
/// When an executor frees capacity, O(1) lookup by its class.
/// When an FE frees capacity, O(1) lookup by function.
#[derive(Debug, Clone, Default)]
pub struct BlockedRunsIndex {
    /// Function runs assigned to executor classes (O(1) lookup when executor
    /// frees capacity). A function run can be in MULTIPLE class buckets.
    /// Using HashSet prevents duplicates and enables O(1) removal.
    pub blocked_by_class: im::HashMap<ExecutorClass, im::HashSet<FunctionRunKey>>,

    /// Function runs indexed by FunctionURI for O(1) lookup when FE frees
    /// capacity. When an FE completes an allocation, we can directly find
    /// blocked runs for that same function without scanning all blocked
    /// runs. Using HashSet prevents duplicates and enables O(1) removal.
    pub blocked_by_fn_uri: im::HashMap<FunctionURI, im::HashSet<FunctionRunKey>>,

    /// Function runs indexed by their requirements.
    /// O(1) insert/remove, O(K) scan when new executor joins (K = distinct
    /// requirement types). This replaces the old `unassigned` Vector for
    /// better performance.
    pub blocked_by_requirements: im::HashMap<FunctionRunRequirements, im::HashSet<FunctionRunKey>>,

    /// Reverse index: function run → its requirements (for O(1) removal).
    pub requirements_by_run: im::HashMap<FunctionRunKey, FunctionRunRequirements>,

    /// Reverse index: function run → which classes it's assigned to (for O(K)
    /// removal).
    pub classes_by_run: im::HashMap<FunctionRunKey, im::HashSet<ExecutorClass>>,

    /// Known executor classes.
    pub known_classes: im::HashSet<ExecutorClass>,

    /// FunctionURI for each run (needed to create allocation).
    pub fn_uri_by_run: im::HashMap<FunctionRunKey, FunctionURI>,
}

impl BlockedRunsIndex {
    /// Block a function run - assign to ALL executor classes that can handle
    /// it.
    ///
    /// All runs are also indexed by requirements for O(K) lookup when new
    /// executors join.
    pub fn block(
        &mut self,
        run_key: FunctionRunKey,
        fn_uri: FunctionURI,
        requirements: FunctionRunRequirements,
    ) {
        // Skip if already blocked
        if self.fn_uri_by_run.contains_key(&run_key) {
            debug!(
                run_key = ?run_key,
                "function run already blocked, skipping"
            );
            return;
        }

        debug!(
            run_key = ?run_key,
            known_classes = self.known_classes.len(),
            "blocking function run"
        );

        // Add to all indexes
        self.fn_uri_by_run.insert(run_key.clone(), fn_uri.clone());

        self.blocked_by_fn_uri
            .entry(fn_uri)
            .or_default()
            .insert(run_key.clone());

        // Always add to requirements index - O(1)
        self.blocked_by_requirements
            .entry(requirements.clone())
            .or_default()
            .insert(run_key.clone());
        self.requirements_by_run
            .insert(run_key.clone(), requirements.clone());

        // Find all classes that can handle this function run
        let eligible_classes: Vec<_> = self
            .known_classes
            .iter()
            .filter(|class| class.can_handle(&requirements))
            .cloned()
            .collect();

        if !eligible_classes.is_empty() {
            // Add to ALL eligible class buckets - HashSet ensures no duplicates
            debug!(
                run_key = ?run_key,
                num_classes = eligible_classes.len(),
                "adding to eligible class buckets"
            );
            for class in &eligible_classes {
                self.blocked_by_class
                    .entry(class.clone())
                    .or_default()
                    .insert(run_key.clone());
            }
            self.classes_by_run
                .insert(run_key, eligible_classes.into_iter().collect());
        } else {
            debug!(
                run_key = ?run_key,
                "no eligible classes yet"
            );
        }
    }

    /// Register a new executor class (when new executor type joins).
    ///
    /// Scans requirement keys (O(K) where K = distinct requirement types) and
    /// assigns matching runs to this class.
    pub fn register_executor_class(&mut self, new_class: ExecutorClass) {
        debug!(
            cpu_ms = new_class.cpu_ms,
            memory_bytes = new_class.memory_bytes,
            region = ?new_class.region,
            requirement_types = self.blocked_by_requirements.len(),
            "registering executor class"
        );

        if self.known_classes.contains(&new_class) {
            return;
        }

        self.known_classes.insert(new_class.clone());

        // O(K) scan of requirement keys, not O(N) runs
        let mut assigned_count = 0;
        for (requirements, run_keys) in self.blocked_by_requirements.iter() {
            if new_class.can_handle(requirements) {
                // This new class can handle these runs - add them to its bucket
                for run_key in run_keys.iter() {
                    // Add to the new class's bucket
                    self.blocked_by_class
                        .entry(new_class.clone())
                        .or_default()
                        .insert(run_key.clone());

                    // Update the reverse index
                    self.classes_by_run
                        .entry(run_key.clone())
                        .or_default()
                        .insert(new_class.clone());

                    assigned_count += 1;
                }
            }
        }

        debug!(
            assigned = assigned_count,
            "finished registering executor class"
        );
    }

    /// Get function runs waiting for the given executor class - O(1) lookup.
    pub fn get_runs_for_class(&self, class: &ExecutorClass) -> Vec<FunctionRunKey> {
        self.blocked_by_class
            .get(class)
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Remove a function run from all indexes (when allocated or cancelled).
    ///
    /// Returns the FunctionURI if the function run was found.
    /// Now O(K) where K = number of executor classes the function run was in,
    /// instead of O(N) where N = total function runs in each class.
    pub fn unblock(&mut self, run_key: &FunctionRunKey) -> Option<FunctionURI> {
        // Remove from all class buckets - O(1) per class thanks to HashSet
        if let Some(classes) = self.classes_by_run.remove(run_key) {
            for class in classes {
                if let Some(runs) = self.blocked_by_class.get_mut(&class) {
                    runs.remove(run_key);
                }
            }
        }

        // Remove from requirements index - O(1) thanks to reverse lookup
        if let Some(requirements) = self.requirements_by_run.remove(run_key) &&
            let Some(runs) = self.blocked_by_requirements.get_mut(&requirements)
        {
            runs.remove(run_key);
        }

        // Remove from FunctionURI index - O(1) thanks to HashSet
        let fn_uri = self.fn_uri_by_run.remove(run_key);
        if let Some(ref uri) = fn_uri &&
            let Some(runs) = self.blocked_by_fn_uri.get_mut(uri)
        {
            runs.remove(run_key);
        }

        fn_uri
    }

    /// Get blocked runs for a specific function. O(1) lookup.
    /// Use this when an FE frees capacity to find runs for that same function.
    /// Returns up to `limit` runs to avoid allocating large vectors.
    pub fn get_runs_for_function(&self, fn_uri: &FunctionURI, limit: usize) -> Vec<FunctionRunKey> {
        self.blocked_by_fn_uri
            .get(fn_uri)
            .map(|v| v.iter().take(limit).cloned().collect())
            .unwrap_or_default()
    }

    /// Check if there are any blocked runs for a function. O(1).
    pub fn has_blocked_for_function(&self, fn_uri: &FunctionURI) -> bool {
        self.blocked_by_fn_uri
            .get(fn_uri)
            .is_some_and(|v| !v.is_empty())
    }

    /// Check if a function run is blocked. O(1).
    #[cfg(test)]
    pub fn is_blocked(&self, run_key: &FunctionRunKey) -> bool {
        self.requirements_by_run.contains_key(run_key)
    }

    /// Get total count of blocked function runs. O(1).
    pub fn total_blocked(&self) -> usize {
        self.requirements_by_run.len()
    }

    /// Get count of unassigned function runs (no matching executor type). O(1).
    /// A run is unassigned if it has requirements but is not in any class
    /// bucket.
    pub fn unassigned_count(&self) -> usize {
        // Total blocked - assigned = unassigned
        // classes_by_run tracks runs that have at least one matching class
        let assigned = self.classes_by_run.len();
        let total = self.requirements_by_run.len();
        total.saturating_sub(assigned)
    }

    /// Get demand by class (for autoscaler).
    pub fn demand_by_class(&self) -> Vec<(ExecutorClass, usize)> {
        self.blocked_by_class
            .iter()
            .map(|(class, runs)| (class.clone(), runs.len()))
            .collect()
    }

    /// Get known executor classes.
    pub fn known_classes(&self) -> impl Iterator<Item = &ExecutorClass> {
        self.known_classes.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_store::in_memory_state::test_helpers::make_function_run_key;

    fn make_executor_class(cpu: u32, mem: u64, region: Option<&str>) -> ExecutorClass {
        ExecutorClass {
            cpu_ms: cpu,
            memory_bytes: mem,
            disk_bytes: 0,
            gpu_count: 0,
            gpu_model: None,
            region: region.map(|s| s.to_string()),
        }
    }

    fn make_run_requirements(cpu: u32, mem: u64, region: Option<&str>) -> FunctionRunRequirements {
        FunctionRunRequirements {
            cpu_ms: cpu,
            memory_bytes: mem,
            disk_bytes: 0,
            gpu_count: 0,
            gpu_model: None,
            region: region.map(|s| s.to_string()),
        }
    }

    fn make_fn_uri(name: &str) -> FunctionURI {
        FunctionURI {
            namespace: "test".to_string(),
            application: "app".to_string(),
            function: name.to_string(),
            version: "v1".to_string(),
        }
    }

    #[test]
    fn test_run_goes_to_unassigned_when_no_executor_class() {
        let mut index = BlockedRunsIndex::default();

        let run_key = make_function_run_key("test", "app", "req1", "call1");
        let fn_uri = make_fn_uri("func1");
        let requirements = make_run_requirements(1000, 2_000_000_000, Some("us-west"));

        index.block(run_key.clone(), fn_uri, requirements);

        assert_eq!(index.unassigned_count(), 1);
        assert_eq!(index.total_blocked(), 1);
        assert!(index.is_blocked(&run_key));
    }

    #[test]
    fn test_run_assigned_to_matching_class() {
        let mut index = BlockedRunsIndex::default();

        // Register executor class first
        let class = make_executor_class(2000, 4_000_000_000, Some("us-west"));
        index.register_executor_class(class.clone());

        // Now block a function run
        let run_key = make_function_run_key("test", "app", "req1", "call1");
        let fn_uri = make_fn_uri("func1");
        let requirements = make_run_requirements(1000, 2_000_000_000, Some("us-west"));

        index.block(run_key.clone(), fn_uri, requirements);

        assert_eq!(index.unassigned_count(), 0);
        assert_eq!(index.get_runs_for_class(&class).len(), 1);
    }

    #[test]
    fn test_run_assigned_to_multiple_classes() {
        let mut index = BlockedRunsIndex::default();

        // Register two executor classes
        let small_class = make_executor_class(2000, 4_000_000_000, Some("us-west"));
        let large_class = make_executor_class(4000, 8_000_000_000, Some("us-west"));
        index.register_executor_class(small_class.clone());
        index.register_executor_class(large_class.clone());

        // Block a small function run - should go to both classes
        let run_key = make_function_run_key("test", "app", "req1", "call1");
        let fn_uri = make_fn_uri("func1");
        let requirements = make_run_requirements(1000, 2_000_000_000, Some("us-west"));

        index.block(run_key.clone(), fn_uri, requirements);

        assert_eq!(index.get_runs_for_class(&small_class).len(), 1);
        assert_eq!(index.get_runs_for_class(&large_class).len(), 1);
    }

    #[test]
    fn test_unblock_removes_from_all_classes() {
        let mut index = BlockedRunsIndex::default();

        let small_class = make_executor_class(2000, 4_000_000_000, Some("us-west"));
        let large_class = make_executor_class(4000, 8_000_000_000, Some("us-west"));
        index.register_executor_class(small_class.clone());
        index.register_executor_class(large_class.clone());

        let run_key = make_function_run_key("test", "app", "req1", "call1");
        let fn_uri = make_fn_uri("func1");
        let requirements = make_run_requirements(1000, 2_000_000_000, Some("us-west"));

        index.block(run_key.clone(), fn_uri, requirements);

        // Unblock
        let removed_fn_uri = index.unblock(&run_key);
        assert!(removed_fn_uri.is_some());

        // Should be removed from both classes
        assert_eq!(index.get_runs_for_class(&small_class).len(), 0);
        assert_eq!(index.get_runs_for_class(&large_class).len(), 0);
        assert!(!index.is_blocked(&run_key));
    }

    #[test]
    fn test_new_executor_class_picks_up_unassigned_runs() {
        let mut index = BlockedRunsIndex::default();

        // Block function run with no matching class
        let run_key = make_function_run_key("test", "app", "req1", "call1");
        let fn_uri = make_fn_uri("func1");
        let requirements = make_run_requirements(1000, 2_000_000_000, Some("us-west"));

        index.block(run_key.clone(), fn_uri, requirements);
        assert_eq!(index.unassigned_count(), 1);

        // Now register a matching class
        let class = make_executor_class(2000, 4_000_000_000, Some("us-west"));
        index.register_executor_class(class.clone());

        // Function run should move from unassigned to the class bucket
        assert_eq!(index.unassigned_count(), 0);
        assert_eq!(index.get_runs_for_class(&class).len(), 1);
    }

    #[test]
    fn test_region_mismatch_not_eligible() {
        let mut index = BlockedRunsIndex::default();

        let class = make_executor_class(2000, 4_000_000_000, Some("us-east"));
        index.register_executor_class(class.clone());

        let run_key = make_function_run_key("test", "app", "req1", "call1");
        let fn_uri = make_fn_uri("func1");
        let requirements = make_run_requirements(1000, 2_000_000_000, Some("us-west")); // Different region

        index.block(run_key.clone(), fn_uri, requirements);

        // Should be unassigned because region doesn't match
        assert_eq!(index.unassigned_count(), 1);
        assert_eq!(index.get_runs_for_class(&class).len(), 0);
    }
}
