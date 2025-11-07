use std::collections::{HashMap, HashSet};

use tokio::sync::RwLock;

use crate::data_model::FunctionRunStatus;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ExecutorWatch {
    pub namespace: String,
    pub application: String,
    pub request_id: String,
    pub function_call_id: String,
}

/// Tracks which executors watch which function call IDs, and vice versa,
/// to allow efficient incremental synchronization.
pub struct ExecutorWatches {
    // function_call_id -> set(executor_id)
    requests: RwLock<HashMap<ExecutorWatch, HashSet<String>>>,
    // executor_id -> set(function_call_id)
    executors: RwLock<HashMap<String, HashSet<ExecutorWatch>>>,
}

impl Default for ExecutorWatches {
    fn default() -> Self {
        Self::new()
    }
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
    pub async fn sync_watches(
        &self,
        executor_id: String,
        new_function_call_ids: HashSet<ExecutorWatch>,
    ) {
        let mut requests_guard = self.requests.write().await;
        let mut executors_guard = self.executors.write().await;

        let old_function_call_ids = executors_guard
            .get(&executor_id)
            .cloned()
            .unwrap_or_default();

        let to_add: HashSet<ExecutorWatch> = new_function_call_ids
            .difference(&old_function_call_ids)
            .cloned()
            .collect();
        let to_remove: HashSet<ExecutorWatch> = old_function_call_ids
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

    // Get the set of executors that are impacted by the given scheduler update.
    // This looks at which function runs were updated in the request and finds
    // executors that have watches on those function call IDs.
    pub async fn impacted_executors(
        &self,
        updated_function_runs: &HashMap<String, HashSet<crate::data_model::FunctionCallId>>,
        updated_request_states: &HashMap<String, crate::data_model::RequestCtx>,
    ) -> HashSet<String> {
        // Build the set of ExecutorWatch objects for all updated function runs
        let mut possible_watches = HashSet::new();
        for (ctx_key, function_run_ids) in updated_function_runs {
            let Some(ctx) = updated_request_states.get(ctx_key) else {
                continue;
            };
            for function_call_id in function_run_ids {
                if let Some(function_run) = ctx.function_runs.get(function_call_id) &&
                    function_run.status != FunctionRunStatus::Completed
                {
                    continue;
                }
                possible_watches.insert(ExecutorWatch {
                    namespace: ctx.namespace.clone(),
                    application: ctx.application_name.clone(),
                    request_id: ctx.request_id.clone(),
                    function_call_id: function_call_id.0.clone(),
                });
            }
        }

        let requests_guard = self.requests.read().await;
        let executors_guard = self.executors.read().await;

        let mut impacted_executors: HashSet<String> = HashSet::new();
        for possible_watch in possible_watches.iter() {
            if let Some(executors) = requests_guard.get(possible_watch) {
                for ex in executors {
                    // Include executor only if it currently has any watches
                    if let Some(watches) = executors_guard.get(ex) &&
                        !watches.is_empty()
                    {
                        impacted_executors.insert(ex.clone());
                    }
                }
            }
        }

        impacted_executors
    }

    pub async fn get_watches(&self, executor_id: &str) -> HashSet<ExecutorWatch> {
        let executors_guard = self.executors.read().await;
        executors_guard
            .get(executor_id)
            .cloned()
            .unwrap_or_default()
    }
}
