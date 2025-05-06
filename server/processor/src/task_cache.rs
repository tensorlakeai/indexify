use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use data_model::{
    CacheKey,
    DataPayload,
    NodeOutput,
    OutputPayload,
    Task,
    TaskOutcome,
    TaskOutputsIngestedEvent,
    TaskOutputsIngestionStatus,
    TaskStatus,
};
use state_store::{
    in_memory_state::{InMemoryState, UnallocatedTaskId},
    requests::{CachedTaskOutput, SchedulerUpdateRequest},
    IndexifyState,
};
use tracing::{debug, span};

pub struct CacheState {
    indexify_state: Arc<IndexifyState>,
    outputs_ingested: usize,
    cache_checks: usize,
    cache_hits: usize,
    map: HashMap<String, HashMap<(CacheKey, String), Vec<NodeOutput>>>,
}

pub struct TaskCache {
    mutex: Mutex<CacheState>,
}

fn task_input_hash(task: &Task, indexify_state: &IndexifyState) -> Option<String> {
    if task.invocation_id == task.input_node_output_key.split("|").last().unwrap_or("") {
        // This is an invocation task; we look up its input hash via the invocation
        // payload.
        indexify_state
            .reader()
            .invocation_payload(
                &task.namespace,
                &task.compute_graph_name,
                &task.invocation_id,
            )
            .ok()
            .map(|ip| ip.payload.sha256_hash)
    } else {
        // Otherwise, we look up the input hash via the task's input_node_output_key.
        indexify_state
            .reader()
            .fn_output_payload_by_key(&task.input_node_output_key)
            .ok()
            .and_then(|no| match no.payload {
                OutputPayload::Fn(DataPayload {
                    sha256_hash: input_hash,
                    ..
                }) => Some(input_hash),
                _ => None,
            })
    }
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
        event: &TaskOutputsIngestedEvent,
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

        let Some(input_hash) = task_input_hash(&task, &mut state.indexify_state) else {
            return;
        };

        let Ok(outputs) = state
            .indexify_state
            .reader()
            .get_task_outputs(&task.namespace, &task.id.to_string())
        else {
            return;
        };

        let namespace = event.namespace.clone();

        debug!("Caching the output of {namespace}/{cache_key:?}/{input_hash}");

        for node_output in &outputs {
            if let OutputPayload::Fn(DataPayload { sha256_hash, .. }) = &node_output.payload {
                debug!(output_hash = sha256_hash);
            }
        }

        state
            .map
            .entry(namespace)
            .or_default()
            .insert((cache_key.clone(), input_hash.clone()), outputs);
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

            let Some(input_hash) = task_input_hash(&task, &mut state.indexify_state) else {
                continue;
            };

            debug!(namespace = task.namespace);
            debug!(task_key = ?cache_key);
            debug!(input_hash = ?input_hash);

            let Some(submap) = state.map.get(&task.namespace) else {
                debug!("No cache namespace entry found");
                continue;
            };

            let submap_key = (cache_key.clone(), input_hash);

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
            result.cached_task_outputs.insert(
                task.id.clone(),
                CachedTaskOutput {
                    task: task.clone(),
                    node_outputs: outputs.clone(),
                },
            );
            result.updated_tasks.insert(task.id.clone(), task);
            state.cache_hits += 1;
        }

        for task_id in &to_remove {
            indexes.unallocated_tasks.remove(task_id);
        }

        result
    }
}
