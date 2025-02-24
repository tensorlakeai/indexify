use std::{
    sync::{Arc, RwLock},
    vec,
};

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
    TaskStatus,
};
use rand::seq::SliceRandom;
use state_store::{
    in_memory_state::InMemoryState,
    requests::SchedulerUpdateRequest,
    IndexifyState,
};
use tracing::{error, info, span};

pub struct FilteredExecutors {
    pub executors: Vec<ExecutorId>,
}

pub struct TaskPlacementResult {
    pub new_allocations: Vec<Allocation>,
    pub remove_allocations: Vec<Allocation>,
    pub tasks: Vec<Task>,
}

pub struct TaskAllocationProcessor {
    indexify_state: Arc<IndexifyState>,
    executors: Arc<RwLock<Vec<ExecutorMetadata>>>,
}

impl TaskAllocationProcessor {
    pub fn new(indexify_state: Arc<IndexifyState>) -> Self {
        let executors = Arc::new(RwLock::new(Vec::new()));
        Self {
            indexify_state,
            executors,
        }
    }
}
impl TaskAllocationProcessor {
    pub fn invoke(
        &self,
        change: &ChangeType,
        indexes: InMemoryState,
    ) -> Result<SchedulerUpdateRequest> {
        match change {
            ChangeType::ExecutorAdded(_ev) => {
                self.refresh_executors()?;
                let task_allocation_results = self.schedule_unplaced_tasks()?;
                return Ok(SchedulerUpdateRequest {
                    new_allocations: task_allocation_results.new_allocations,
                    remove_allocations: vec![],
                    updated_tasks: task_allocation_results.tasks,
                    updated_invocations_states: vec![],
                    new_reduction_tasks: vec![],
                    processed_reduction_tasks: vec![],
                });
            }
            ChangeType::ExecutorRemoved(ev) => {
                let mut updated_tasks = Vec::new();
                let mut remove_allocations = Vec::new();
                let allocations = indexes.allocations_by_executor.get(ev.executor_id.get());
                if let Some(allocations) = allocations {
                    remove_allocations.extend(allocations.clone());
                    for allocation in allocations {
                        let task = indexes.tasks.get(&allocation.task_id.to_string());
                        if let Some(mut task) = task.cloned() {
                            task.status = TaskStatus::Pending;
                            updated_tasks.push(task.clone());
                        }
                    }
                }
                return Ok(SchedulerUpdateRequest {
                    new_allocations: vec![],
                    remove_allocations,
                    updated_tasks,
                    updated_invocations_states: vec![],
                    new_reduction_tasks: vec![],
                    processed_reduction_tasks: vec![],
                });
            }
            ChangeType::TaskCreated(ev) => {
                let result = self.schedule_tasks(vec![ev.task.clone()])?;
                return Ok(SchedulerUpdateRequest {
                    new_allocations: result.new_allocations,
                    remove_allocations: result.remove_allocations,
                    updated_tasks: result.tasks,
                    updated_invocations_states: vec![],
                    new_reduction_tasks: vec![],
                    processed_reduction_tasks: vec![],
                });
            }
            _ => {
                error!("unhandled change type: {:?}", change);
                return Err(anyhow!("unhandled change type"));
            }
        }
    }

    pub fn schedule_unplaced_tasks(&self) -> Result<TaskPlacementResult> {
        let tasks = self.indexify_state.reader().unallocated_tasks()?;
        self.schedule_tasks(tasks)
    }

    pub fn refresh_executors(&self) -> Result<()> {
        let all_executors = self.indexify_state.reader().get_all_executors()?;
        let mut executors = self.executors.write().unwrap();
        *executors = all_executors;
        Ok(())
    }

    pub fn schedule_tasks(&self, tasks: Vec<Task>) -> Result<TaskPlacementResult> {
        let mut allocations = Vec::new();
        let mut updated_tasks = Vec::new();
        for mut task in tasks {
            let span = span!(
                tracing::Level::INFO,
                "allocate_task",
                task_id = task.id.to_string(),
                namespace = task.namespace,
                compute_graph = task.compute_graph_name,
                compute_fn = task.compute_fn_name,
            );
            let _enter = span.enter();
            if task.outcome.is_terminal() {
                error!("task: {} already completed, skipping", task.id);
                continue;
            }
            info!("allocate task {:?} ", task.id);
            match self.allocate_task(&mut task) {
                Ok(Some(new_allocations)) => {
                    allocations.push(new_allocations);
                    task.status = TaskStatus::Running;
                    updated_tasks.push(task.clone());
                }
                Ok(None) => {
                    info!("no executors available for task {:?}", task.id);
                }
                Err(err) => {
                    error!("failed to allocate task, skipping: {:?}", err);
                }
            }
        }
        Ok(TaskPlacementResult {
            new_allocations: allocations,
            remove_allocations: vec![],
            tasks: updated_tasks,
        })
    }

    fn allocate_task(&self, task: &mut Task) -> Result<Option<Allocation>> {
        let compute_graph_version = self
            .indexify_state
            .reader()
            .get_compute_graph_version(
                &task.namespace,
                &task.compute_graph_name,
                &task.graph_version,
            )?
            .ok_or(anyhow!("compute graph not found"))?;
        let compute_fn = compute_graph_version
            .nodes
            .get(&task.compute_fn_name)
            .ok_or(anyhow!("compute fn not found"))?;
        let filtered_executors = self.filter_executors(&compute_graph_version, &compute_fn)?;
        let executor_id = filtered_executors.executors.choose(&mut rand::thread_rng());
        if let Some(executor_id) = executor_id {
            info!("assigning task {:?} to executor {:?}", task.id, executor_id);
            let allocation = AllocationBuilder::default()
                .namespace(task.namespace.clone())
                .compute_graph(task.compute_graph_name.clone())
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
    ) -> Result<FilteredExecutors> {
        let mut filtered_executors = vec![];

        let executors = self.executors.read().unwrap();

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
