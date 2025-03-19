use std::{collections::HashMap, vec};

use anyhow::{anyhow, Result};
use data_model::{
    Allocation,
    AllocationBuilder,
    ChangeType,
    ComputeGraphVersion,
    ExecutorId,
    ExecutorMetadata,
    Node,
    Task,
    TaskId,
    TaskStatus,
};
use im::Vector;
use itertools::Itertools;
use rand::seq::SliceRandom;
use state_store::{
    in_memory_state::{InMemoryState, UnallocatedTaskId},
    requests::SchedulerUpdateRequest,
};
use tracing::{debug, error, info, span};

pub struct FilteredExecutors {
    pub executors: Vec<ExecutorId>,
}

pub struct TaskPlacementResult {
    pub new_allocations: Vec<Allocation>,
    pub updated_tasks: HashMap<TaskId, Task>,
}

// Maximum number of allocations per executor.
//
// In the future, this should be a dynamic value based on:
// - function concurrency configuration
// - function batching configuration
// - function timeout configuration
const MAX_ALLOCATIONS_PER_FN_EXECUTOR: usize = 20;

pub struct TaskAllocationProcessor {}

impl TaskAllocationProcessor {
    pub fn new() -> Self {
        Self {}
    }
}
impl TaskAllocationProcessor {
    pub fn invoke(
        &self,
        change: &ChangeType,
        indexes: &mut Box<InMemoryState>,
    ) -> Result<SchedulerUpdateRequest> {
        match change {
            ChangeType::ExecutorAdded(_) | ChangeType::ExecutorRemoved(_) => {
                let task_allocation_results = self.allocate(indexes)?;
                return Ok(SchedulerUpdateRequest {
                    new_allocations: task_allocation_results.new_allocations,
                    updated_tasks: task_allocation_results.updated_tasks,
                    ..Default::default()
                });
            }
            ChangeType::HandleAbandonedAllocations => {
                // Get all executor IDs of executors that haven't registered.
                let missing_executor_ids: Vec<String> = indexes
                    .allocations_by_fn
                    .keys()
                    .filter(|id| !indexes.executors.contains_key(&**id))
                    .cloned()
                    .collect();

                if missing_executor_ids.is_empty() {
                    info!("no abandoned allocations to handle");
                    return Ok(SchedulerUpdateRequest::default());
                }

                self.deregister_executors(missing_executor_ids, indexes)
            }
            ChangeType::TombStoneExecutor(ev) => {
                self.deregister_executors(vec![ev.executor_id.get().to_string()], indexes)
            }
            _ => {
                error!("unhandled change type: {:?}", change);
                return Err(anyhow!("unhandled change type"));
            }
        }
    }

    pub fn deregister_executors(
        &self,
        executor_ids: Vec<String>,
        indexes: &mut Box<InMemoryState>,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest {
            remove_executors: executor_ids
                .iter()
                .map(|id| ExecutorId::new(id.clone()))
                .collect(),
            ..Default::default()
        };

        // Get all allocations for the executors that are being deregistered.
        let allocations = indexes
            .allocations_by_fn
            .iter()
            .filter(|(executor_id, _)| executor_ids.contains(executor_id))
            .flat_map(|(_, allocations)| allocations.values().cloned())
            .flatten()
            .collect_vec();

        // Remove the allocations from the store.
        update.remove_allocations = allocations.clone().iter().map(|a| *a.clone()).collect();

        // Remove the executors from the indexes.
        indexes
            .executors
            .retain(|executor_id, _| !executor_ids.contains(executor_id));

        // Remove the allocations from the indexes.
        indexes
            .allocations_by_fn
            .retain(|executor_id, _| !executor_ids.contains(executor_id));

        // Mark all tasks being unallocated as pending.
        for allocation in allocations {
            let task = indexes.tasks.get(&allocation.task_key());
            if let Some(task) = task.cloned() {
                let mut task = *task;
                task.status = TaskStatus::Pending;
                indexes.tasks.insert(task.key(), Box::new(task.clone()));
                update.updated_tasks.insert(task.id.clone(), task);
            } else {
                error!(
                    "task of allocation not found in indexes: {}",
                    allocation.task_key(),
                );
            }
        }

        // Immediately attempt to reallocate tasks that were unallocated due to executor
        // deregistration.
        {
            let placement_result = self.allocate_tasks(
                update
                    .updated_tasks
                    .iter()
                    .map(|(_, t)| Box::new(t.clone()))
                    .collect(),
                indexes,
            )?;

            update
                .new_allocations
                .extend(placement_result.new_allocations);
            update.updated_tasks.extend(placement_result.updated_tasks);
        }

        return Ok(update);
    }

    pub fn allocate(&self, indexes: &mut Box<InMemoryState>) -> Result<TaskPlacementResult> {
        let unallocated_task_ids = indexes.unallocated_tasks.clone();
        let mut tasks = Vec::new();
        for unallocated_task_id in &unallocated_task_ids {
            if let Some(task) = indexes.tasks.get(&unallocated_task_id.task_key) {
                tasks.push(task.clone());
            } else {
                error!(
                    task_key=%unallocated_task_id.task_key,
                    "task not found in indexes for unallocated task"
                );
            }
        }
        if tasks.is_empty() {
            return Ok(TaskPlacementResult {
                new_allocations: vec![],
                updated_tasks: HashMap::new(),
            });
        }
        self.allocate_tasks(tasks, indexes)
    }

    pub fn allocate_tasks(
        &self,
        tasks: Vec<Box<Task>>,
        indexes: &mut Box<InMemoryState>,
    ) -> Result<TaskPlacementResult> {
        let mut allocations = Vec::new();
        let mut updated_tasks: HashMap<TaskId, Task> = HashMap::new();

        if indexes.executors.is_empty() {
            info!("no executors available for task allocation");
            return Ok(TaskPlacementResult {
                new_allocations: vec![],
                updated_tasks,
            });
        }

        for mut task in tasks {
            let span = span!(
                tracing::Level::DEBUG,
                "allocate_task",
                task_id = task.id.to_string(),
                namespace = task.namespace,
                compute_graph = task.compute_graph_name,
                compute_fn = task.compute_fn_name,
                invocation_id = task.invocation_id
            );
            let _enter = span.enter();
            if task.outcome.is_terminal() {
                error!("task: {} already completed, skipping", task.id);
                continue;
            }

            debug!("attempting to allocate task {:?} ", task.id);

            // get executors with allocation capacity
            let executors = indexes
                .executors
                .iter()
                .filter(|(k, _)| {
                    let all_allocations = indexes.allocations_by_fn.get(*k);
                    let allocations_for_fn = all_allocations.map_or(0, |allocs| {
                        allocs.get(&task.fn_uri()).unwrap_or(&Vector::new()).len()
                    });
                    allocations_for_fn < MAX_ALLOCATIONS_PER_FN_EXECUTOR
                })
                .map(|(_, v)| v)
                .collect_vec();

            // terminate allocating early if no executors available
            if executors.is_empty() {
                debug!("no executors with capacity available for task");
                break;
            }

            match self.allocate_task(&task, indexes, &executors) {
                Ok(Some(allocation)) => {
                    info!(
                        executor_id = &allocation.executor_id.get(),
                        task_id = &task.id.to_string(),
                        namespace = &task.namespace,
                        compute_graph = &task.compute_graph_name,
                        compute_fn = &task.compute_fn_name,
                        invocation_id = &task.invocation_id,
                        "allocated task"
                    );
                    allocations.push(allocation.clone());
                    task.status = TaskStatus::Running;
                    indexes
                        .allocations_by_fn
                        .entry(allocation.executor_id.to_string())
                        .or_default()
                        .entry(task.fn_uri())
                        .or_default()
                        .push_back(Box::new(allocation.clone()));
                    indexes.tasks.insert(task.key(), task.clone());
                    indexes
                        .unallocated_tasks
                        .remove(&UnallocatedTaskId::new(&task));
                    updated_tasks.insert(task.id.clone(), *task.clone());
                }
                Ok(None) => {
                    debug!(
                        task_id = task.id.to_string(),
                        invocation_id = task.invocation_id.to_string(),
                        namespace = task.namespace,
                        compute_graph = task.compute_graph_name,
                        compute_fn = task.compute_fn_name,
                        "no executors available for task"
                    );
                }
                Err(err) => {
                    error!("failed to allocate task, skipping: {:?}", err);
                }
            }
        }
        Ok(TaskPlacementResult {
            new_allocations: allocations,
            updated_tasks,
        })
    }

    fn allocate_task(
        &self,
        task: &Task,
        indexes: &Box<InMemoryState>,
        executors: &Vec<&Box<ExecutorMetadata>>,
    ) -> Result<Option<Allocation>> {
        let compute_graph_version = indexes
            .compute_graph_versions
            .get(&task.key_compute_graph_version())
            .ok_or(anyhow!("compute graph not found"))?
            .clone();
        let compute_fn = compute_graph_version
            .nodes
            .get(&task.compute_fn_name)
            .ok_or(anyhow!("compute fn not found"))?;

        let filtered_executors =
            self.filter_executors(&compute_graph_version, &compute_fn, executors)?;

        let executor_id = filtered_executors.executors.choose(&mut rand::thread_rng());
        if let Some(executor_id) = executor_id {
            info!("assigning task {:?} to executor {:?}", task.id, executor_id);
            let allocation = AllocationBuilder::default()
                .namespace(task.namespace.clone())
                .compute_graph(task.compute_graph_name.clone())
                .compute_fn(task.compute_fn_name.clone())
                .invocation_id(task.invocation_id.clone())
                .task_id(task.id.clone())
                .executor_id(executor_id.clone())
                .build()?;
            return Ok(Some(allocation));
        }
        Ok(None)
    }

    fn filter_executors(
        &self,
        compute_graph: &ComputeGraphVersion,
        node: &Node,
        executors: &Vec<&Box<ExecutorMetadata>>,
    ) -> Result<FilteredExecutors> {
        let mut filtered_executors = vec![];

        for executor in executors.iter() {
            match executor.function_allowlist {
                Some(ref allowlist) => {
                    for func_uri in allowlist {
                        if func_matches(func_uri, compute_graph, node) {
                            filtered_executors.push(executor.id.clone());
                            break;
                        }
                    }
                }
                None => {
                    filtered_executors.push(executor.id.clone());
                }
            }
        }
        Ok(FilteredExecutors {
            executors: filtered_executors,
        })
    }
}

fn func_matches(
    func_uri: &data_model::FunctionURI,
    compute_graph: &ComputeGraphVersion,
    node: &Node,
) -> bool {
    func_uri.compute_fn_name.eq(node.name()) &&
        func_uri
            .compute_graph_name
            .eq(&compute_graph.compute_graph_name) &&
        func_uri.version.as_ref().unwrap_or(&compute_graph.version) == &compute_graph.version &&
        func_uri.namespace.eq(&compute_graph.namespace)
}
