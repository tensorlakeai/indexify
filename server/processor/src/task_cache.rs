use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use data_model::{
    CacheKey,
    NodeOutput,
    Task,
    TaskOutcome,
    AllocationOutputIngestedEvent,
    TaskOutputsIngestionStatus,
    TaskStatus,
};
use state_store::{
    in_memory_state::{InMemoryState, UnallocatedTaskId},
    requests::SchedulerUpdateRequest,
    IndexifyState,
};
use tracing::{debug, span};

pub struct CacheState {
    indexify_state: Arc<IndexifyState>,
    outputs_ingested: usize,
    cache_checks: usize,
    cache_hits: usize,
    map: HashMap<String, HashMap<(CacheKey, String), NodeOutput>>,
}

pub struct TaskCache {
    mutex: Mutex<CacheState>,
}

impl TaskCache {
    pub fn new(indexify_state: Arc<IndexifyState>) -> Self {
        Self {
            mutex: Mutex::new(CacheState {
                indexify_state,
                outputs_ingested: 0,
                cache_checks: 0,
                cache_hits: 0,
                map: HashMap::new(),
            }),
        }
    }

    pub fn handle_task_outputs(
        &self,
        event: &AllocationOutputIngestedEvent,
        indexes: Arc<RwLock<InMemoryState>>,
    ) {
        let _span = span!(tracing::Level::DEBUG, "cache_write").entered();

        let mut state = self.mutex.lock().unwrap();
        state.outputs_ingested += 1;

        let indexes = indexes.read().unwrap();

        let Some(task) = indexes.tasks.get(&Task::key_from(
            &event.namespace,
            &event.compute_graph,
            &event.invocation_id,
            &event.compute_fn,
            &event.task_id.to_string(),
        )) else {
            return;
        };
        let Some(cache_key) = &task.cache_key else {
            return;
        };

        let input_hash = task.input.sha256_hash.clone();

        let Ok(Some(node_output)) = state
            .indexify_state
            .reader()
            .get_node_output_by_key(&event.node_output_key)
        else {
            return;
        };

        let namespace = event.namespace.clone();

        debug!("Caching the output of {namespace}/{cache_key:?}/{input_hash}");

        state
            .map
            .entry(namespace)
            .or_default()
            .insert((cache_key.clone(), input_hash.clone()), node_output);
    }

    pub fn try_allocate(&self, indexes: Arc<RwLock<InMemoryState>>) -> SchedulerUpdateRequest {
        let _span = span!(tracing::Level::DEBUG, "cache_check").entered();

        let mut state = self.mutex.lock().unwrap();

        let mut to_remove: Vec<UnallocatedTaskId> = Vec::new();
        let mut result = SchedulerUpdateRequest::default();

        let mut indexes = indexes.write().unwrap();

        for task_id in &indexes.unallocated_tasks {
            debug!(task = ?task_id);
            state.cache_checks += 1;

            let Some(task) = indexes.tasks.get(&task_id.task_key) else {
                continue;
            };

            let Some(cache_key) = &task.cache_key else {
                continue;
            };

            debug!(namespace = task.namespace);
            debug!(task_key = ?cache_key);

            let Some(submap) = state.map.get(&task.namespace) else {
                debug!("No cache namespace entry found");
                continue;
            };

            let submap_key = (cache_key.clone(), task.input.sha256_hash.clone());

            let Some(outputs) = submap.get(&submap_key) else {
                debug!("No cache entry found");
                continue;
            };

            // TODO: We need to handle the case where the previous
            // outputs have been deleted.  It's unclear that there's a
            // great place to do that, though (at least, not without a
            // race condition).  We should probably migrate towards
            // holding our knowledge of the blobstore state as part of
            // our state, allowing Indexify to manage it explicitly.

            debug!("Cache entry found; ingesting previous outputs");
            to_remove.push(task_id.clone());
            let mut task = *(task.clone());
            task.status = TaskStatus::Completed;
            task.outcome = TaskOutcome::Success;
            task.output_status = TaskOutputsIngestionStatus::Ingested;
            result
                .cached_task_outputs
                .insert(task.id.clone(), outputs.clone());
            result.updated_tasks.insert(task.id.clone(), task);
            state.cache_hits += 1;
        }

        for task_id in &to_remove {
            indexes.unallocated_tasks.remove(task_id);
        }

        result
    }
}
