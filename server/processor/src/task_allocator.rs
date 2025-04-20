use std::sync::{atomic::Ordering, Arc};

use anyhow::{anyhow, Result};
use data_model::{
    Allocation,
    AllocationBuilder,
    ChangeType,
    ExecutorId,
    FunctionExecutor,
    FunctionExecutorId,
    FunctionURI,
    Task,
};
use state_store::{
    in_memory_state::InMemoryState,
    requests::{FunctionExecutorIdWithExecutionId, SchedulerUpdateRequest},
    IndexifyState,
};
use tracing::{error, trace};

pub struct FilteredExecutors {
    pub executors: Vec<ExecutorId>,
}

#[derive(Debug, Clone)]
// Define a struct to represent a candidate executor for allocation
pub struct ExecutorCandidate {
    pub executor_id: ExecutorId,
    pub function_executor_id: Option<FunctionExecutorId>, // None if needs to be created
    pub function_uri: FunctionURI,
    pub allocation_count: usize, // Number of allocations for this function executor
}

pub struct TaskAllocator {
    indexify_state: Arc<IndexifyState>,
    in_memory_state: Box<InMemoryState>,
}

impl TaskAllocator {
    pub fn new(indexify_state: Arc<IndexifyState>, in_memory_state: Box<InMemoryState>) -> Self {
        Self {
            indexify_state,
            in_memory_state,
        }
    }
}
impl TaskAllocator {
    #[tracing::instrument(skip(self, change))]
    pub fn invoke(&mut self, change: &ChangeType) -> Result<SchedulerUpdateRequest> {
        match change {
            ChangeType::ExecutorUpserted(ev) => {
                let mut update = self.reconcile_executor_state(&ev.executor_id)?;
                update.extend(self.allocate()?);
                return Ok(update);
            }
            ChangeType::ExecutorRemoved(_) => {
                // TODO: FIXME WE DONT NEED THIS ANYMORE. FUTURE RELEASES OF SERVER NEEDS TO
                // REMOVE THIS
                let update = self.allocate()?;
                return Ok(update);
            }
            ChangeType::TombStoneExecutor(ev) => self.deregister_executor(&ev.executor_id),
            _ => {
                error!("unhandled change type: {:?}", change);
                return Err(anyhow!("unhandled change type"));
            }
        }
    }

    pub fn reconcile_executor_state(
        &mut self,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        trace!(
            "reconciling executor state for executor {}",
            executor_id.get(),
        );

        update.extend(
            self.in_memory_state
                .prune_function_executors(&executor_id)?,
        );
        update.extend(self.allocate()?);
        return Ok(update);
    }

    #[tracing::instrument(skip(self, executor_id))]
    fn deregister_executor(&mut self, executor_id: &ExecutorId) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        update.remove_executors.push(executor_id.clone());
        update.extend(self.in_memory_state.remove_executor(executor_id)?);
        update.extend(self.allocate()?);

        return Ok(update);
    }

    #[tracing::instrument(skip(self))]
    pub fn allocate(&mut self) -> Result<SchedulerUpdateRequest> {
        let unallocated_task_ids = self.in_memory_state.unallocated_tasks.clone();
        let mut tasks = Vec::new();
        for unallocated_task_id in &unallocated_task_ids {
            if let Some(task) = self
                .in_memory_state
                .tasks
                .get(&unallocated_task_id.task_key)
            {
                tasks.push(task.clone());
            } else {
                error!(
                    task_key = unallocated_task_id.task_key,
                    "task not found in indexes for unallocated task"
                );
            }
        }
        self.allocate_tasks(tasks)
    }

    pub fn allocate_tasks(&mut self, tasks: Vec<Box<Task>>) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        // 1. See if there are any function executors that are available which are under
        //    capacity
        // 2. If there are no function executors available, find nodes where we can
        //    create a new one
        // 3. If there are no nodes where we can create a new one, return NOOP
        // 4. Create a new function executor on a node that has capacity
        // 5. Assign the tasks to the function executor
        // 6. Return the update
        let server_clock = self
            .indexify_state
            .last_state_change_id
            .load(Ordering::Relaxed);
        for task in tasks {
            let function_executors = self.in_memory_state.candidate_function_executors(&task);
            if function_executors.is_empty() {
                self.in_memory_state.vacuum_function_executors();
                let candidate_nodes = self.in_memory_state.candidate_executors(&task)?;
                if candidate_nodes.is_empty() {
                    continue;
                }
                let mut candidate_node = candidate_nodes.first().unwrap().clone();
                let function_executor = self.in_memory_state.create_function_executor(
                    &mut candidate_node,
                    &task,
                    server_clock,
                )?;
                update.new_function_executors.push(function_executor);
            }
            if let Some(function_executor) = function_executors.first() {
                let allocation = self.assign_tasks(&task, &function_executor)?;
                update.new_allocations.push(allocation);
            }
        }

        Ok(update)
    }

    pub fn vacuum_function_executors(&mut self) -> Result<Vec<FunctionExecutorIdWithExecutionId>> {
        let mut function_executors = Vec::new();
        for (fe_id, fe) in self.in_memory_state.function_executors.iter() {
            if self
                .in_memory_state
                .tasks_by_function_uri
                .get(&fe.into())
                .unwrap_or(&im::HashSet::new())
                .is_empty()
            {
                function_executors.push(FunctionExecutorIdWithExecutionId {
                    function_executor_id: fe_id.clone(),
                    executor_id: fe.executor_id.clone(),
                });
            }
        }
        Ok(function_executors)
    }

    pub fn assign_tasks(
        &mut self,
        task: &Box<Task>,
        function_executor: &Box<FunctionExecutor>,
    ) -> Result<Allocation> {
        let allocation = AllocationBuilder::default()
            .namespace(task.namespace.clone())
            .compute_graph(task.compute_graph_name.clone())
            .compute_fn(task.compute_fn_name.clone())
            .invocation_id(task.invocation_id.clone())
            .task_id(task.id.clone())
            .executor_id(function_executor.executor_id.clone())
            .function_executor_id(function_executor.id.clone())
            .build()?;

        Ok(allocation)
    }
}
