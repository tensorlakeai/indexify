use std::collections::{HashMap, HashSet};

use tokio::sync::RwLock;

/// Tracks which executors watch which function call IDs, and vice versa,
/// to allow efficient incremental synchronization.
pub struct ExecutorWatches {
    // function_call_id -> set(executor_id)
    requests: RwLock<HashMap<String, HashSet<String>>>,
    // executor_id -> set(function_call_id)
    executors: RwLock<HashMap<String, HashSet<String>>>,
}

impl ExecutorWatches {
    pub fn new() -> Self {
        Self {
            requests: RwLock::new(HashMap::new()),
            executors: RwLock::new(HashMap::new()),
        }
    }

    /// Synchronize the set of function call IDs watched by an executor.
    /// Applies only the delta between the previously watched set and the new
    /// set.
    pub async fn sync_watches(&self, executor_id: String, new_function_call_ids: HashSet<String>) {
        let mut requests_guard = self.requests.write().await;
        let mut executors_guard = self.executors.write().await;

        let old_function_call_ids = executors_guard
            .get(&executor_id)
            .cloned()
            .unwrap_or_default();

        let to_add: HashSet<String> = new_function_call_ids
            .difference(&old_function_call_ids)
            .cloned()
            .collect();
        let to_remove: HashSet<String> = old_function_call_ids
            .difference(&new_function_call_ids)
            .cloned()
            .collect();

        // Apply removals first
        if !to_remove.is_empty() {
            if let Some(fc_set) = executors_guard.get_mut(&executor_id) {
                for fc_id in &to_remove {
                    fc_set.remove(fc_id);
                }
                if fc_set.is_empty() {
                    executors_guard.remove(&executor_id);
                }
            }

            for fc_id in &to_remove {
                if let Some(ex_set) = requests_guard.get_mut(fc_id) {
                    ex_set.remove(&executor_id);
                    if ex_set.is_empty() {
                        requests_guard.remove(fc_id);
                    }
                }
            }
        }

        // Apply additions
        if !to_add.is_empty() {
            let entry = executors_guard
                .entry(executor_id.clone())
                .or_insert_with(HashSet::new);
            for fc_id in &to_add {
                entry.insert(fc_id.clone());
                requests_guard
                    .entry(fc_id.clone())
                    .or_insert_with(HashSet::new)
                    .insert(executor_id.clone());
            }
        }
    }

    /// Remove all watches for a given executor.
    pub async fn remove_executor(&self, executor_id: &str) {
        let mut requests_guard = self.requests.write().await;
        let mut executors_guard = self.executors.write().await;

        if let Some(fc_ids) = executors_guard.remove(executor_id) {
            for fc_id in fc_ids {
                if let Some(executors) = requests_guard.get_mut(&fc_id) {
                    executors.remove(executor_id);
                    if executors.is_empty() {
                        requests_guard.remove(&fc_id);
                    }
                }
            }
        }
    }

    // Get the set of executors and the function call ids that are watched
    // the input is the list of function call ids that just changed.
    // We return all the function call ids of the executors for which any
    // function call id changed.
    pub async fn impacted_executors(&self, fn_call_ids: HashSet<String>) -> HashSet<String> {
        let requests_guard = self.requests.read().await;
        let executors_guard = self.executors.read().await;

        let mut impacted_executors: HashSet<String> = HashSet::new();
        for fc_id in fn_call_ids.iter() {
            if let Some(executors) = requests_guard.get(fc_id) {
                for ex in executors {
                    // Include executor only if it currently has any watches
                    if let Some(watches) = executors_guard.get(ex) {
                        if !watches.is_empty() {
                            impacted_executors.insert(ex.clone());
                        }
                    }
                }
            }
        }

        impacted_executors
    }

    pub async fn get_watches(&self, executor_id: &str) -> HashSet<String> {
        let executors_guard = self.executors.read().await;
        executors_guard
            .get(executor_id)
            .cloned()
            .unwrap_or_default()
    }
}
