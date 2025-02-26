use std::{ops::Deref, sync::Arc, vec};

use anyhow::{anyhow, Result};
use data_model::{
    Allocation,
    AllocationBuilder,
    ChangeType,
    ComputeGraphVersion,
    ExecutorId,
    Node,
    Task,
    TaskStatus,
};
use itertools::Itertools;
use rand::seq::SliceRandom;
use state_store::{
    in_memory_state::InMemoryState,
    requests::{ReductionTasks, SchedulerUpdateRequest},
};
use tracing::{error, info, span};

pub struct FilteredExecutors {
    pub executors: Vec<ExecutorId>,
}

pub struct TaskPlacementResult {
    pub new_allocations: Vec<Allocation>,
    pub remove_allocations: Vec<Allocation>,
    pub updated_tasks: Vec<Task>,
}

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
                let task_allocation_results = self.schedule_unplaced_tasks(indexes)?;
                return Ok(SchedulerUpdateRequest {
                    new_allocations: task_allocation_results.new_allocations,
                    remove_allocations: task_allocation_results.remove_allocations,
                    updated_tasks: task_allocation_results.updated_tasks,
                    updated_invocations_states: vec![],
                    reduction_tasks: ReductionTasks::default(),
                    remove_executors: vec![],
                });
            }
            ChangeType::TombStoneExecutor(ev) => {
                let mut updated_tasks = Vec::new();
                let mut remove_allocations = Vec::new();
                let allocations = indexes.allocations_by_executor.get(ev.executor_id.get());
                if let Some(allocations) = allocations {
                    remove_allocations.extend(allocations.iter().map(|a| a.clone()));
                    for allocation in allocations {
                        let task = indexes.tasks.get(&allocation.task_key());
                        if let Some(task) = task.cloned() {
                            let mut task = task.clone().deref().clone();
                            task.status = TaskStatus::Pending;
                            updated_tasks.push(task);
                        } else {
                            error!(
                                "task of allocation not found in indexes: {}",
                                allocation.task_key(),
                            );
                        }
                    }
                }

                return Ok(SchedulerUpdateRequest {
                    new_allocations: vec![],
                    remove_allocations,
                    updated_tasks,
                    updated_invocations_states: vec![],
                    reduction_tasks: ReductionTasks::default(),
                    remove_executors: vec![ev.executor_id.clone()],
                });
            }
            _ => {
                error!("unhandled change type: {:?}", change);
                return Err(anyhow!("unhandled change type"));
            }
        }
    }

    pub fn schedule_unplaced_tasks(
        &self,
        indexes: &mut Box<InMemoryState>,
    ) -> Result<TaskPlacementResult> {
        let unallocated_task_ids = indexes.unallocated_tasks.keys().cloned().collect_vec();
        let mut tasks = Vec::new();
        for task_id in &unallocated_task_ids {
            if let Some(task) = indexes.tasks.get(task_id) {
                tasks.push(task.clone());
            } else {
                error!("task not found in indexes: {}", task_id);
            }
        }
        self.schedule_tasks(tasks, indexes)
    }

    pub fn schedule_tasks(
        &self,
        tasks: Vec<Arc<Task>>,
        indexes: &mut Box<InMemoryState>,
    ) -> Result<TaskPlacementResult> {
        let mut allocations = Vec::new();
        let mut updated_tasks: Vec<Task> = Vec::new();
        for task in tasks {
            let span = span!(
                tracing::Level::INFO,
                "allocate_task",
                task_id = task.id.to_string(),
                namespace = task.namespace,
                compute_graph = task.compute_graph_name,
                compute_fn = task.compute_fn_name,
                invocation_id = task.invocation_id
            );
            let _enter = span.enter();
            let mut task = task.clone().deref().clone();
            if task.outcome.is_terminal() {
                error!("task: {} already completed, skipping", task.id);
                continue;
            }
            info!("allocate task {:?} ", task.id);
            match self.allocate_task(&task, indexes) {
                Ok(Some(allocation)) => {
                    allocations.push(allocation.clone());
                    task.status = TaskStatus::Running;
                    indexes
                        .allocations_by_executor
                        .entry(allocation.executor_id.to_string())
                        .or_default()
                        .push_back(allocation);
                    indexes.tasks.insert(task.key(), Arc::new(task.clone()));
                }
                Ok(None) => {
                    info!("no executors available for task {:?}", task.id);
                }
                Err(err) => {
                    error!("failed to allocate task, skipping: {:?}", err);
                }
            }

            // always updated to tasks that were just created
            updated_tasks.push(task);
        }
        Ok(TaskPlacementResult {
            new_allocations: allocations,
            remove_allocations: vec![],
            updated_tasks,
        })
    }

    fn allocate_task(
        &self,
        task: &Task,
        indexes: &Box<InMemoryState>,
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
            self.filter_executors(&compute_graph_version, &compute_fn, indexes)?;
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
        indexes: &Box<InMemoryState>,
    ) -> Result<FilteredExecutors> {
        let mut filtered_executors = vec![];

        let executors = indexes.executors.values().collect_vec();

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
