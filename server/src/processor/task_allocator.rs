use anyhow::Result;
use tracing::{debug, info, info_span, warn};

use crate::{
    data_model::{AllocationBuilder, Task, TaskOutcome, TaskStatus},
    processor::function_executor_manager::FunctionExecutorManager,
    state_store::{
        self,
        in_memory_state::InMemoryState,
        requests::{RequestPayload, SchedulerUpdateRequest},
    },
};

pub struct TaskAllocationProcessor<'a> {
    clock: u64,
    fe_manager: &'a FunctionExecutorManager,
}

impl<'a> TaskAllocationProcessor<'a> {
    pub fn new(clock: u64, fe_manager: &'a FunctionExecutorManager) -> Self {
        Self { clock, fe_manager }
    }

    /// Allocate attempts to allocate unallocated tasks to function executors.
    #[tracing::instrument(skip(self, in_memory_state))]
    pub fn allocate(&self, in_memory_state: &mut InMemoryState) -> Result<SchedulerUpdateRequest> {
        // Step 1: Fetch unallocated tasks
        let tasks = in_memory_state.unallocated_tasks();
        debug!("found {} unallocated tasks to process", tasks.len());

        // Step 2: Allocate tasks
        let mut update = SchedulerUpdateRequest::default();

        for task in tasks {
            match self.create_allocation(in_memory_state, &task) {
                Ok(allocation_update) => {
                    update.extend(allocation_update);
                }
                Err(err) => {
                    // Check if this is a state store error we can handle gracefully
                    if let Some(state_store_error) =
                        err.downcast_ref::<state_store::in_memory_state::Error>()
                    {
                        warn!(
                            task_id = task.id.get(),
                            namespace = task.namespace,
                            graph = task.compute_graph_name,
                            graph_version = state_store_error.version(),
                            "fn" = state_store_error.function_name(),
                            error = %state_store_error,
                            "Unable to allocate task"
                        );
                        continue;
                    }
                    // For any other error, return it
                    return Err(err);
                }
            }
        }

        Ok(update)
    }

    fn create_allocation(
        &self,
        in_memory_state: &mut InMemoryState,
        task: &Task,
    ) -> Result<SchedulerUpdateRequest> {
        let span = info_span!(
            "create_allocation",
            namespace = task.namespace,
            task_id = task.id.get(),
            invocation_id = task.invocation_id,
            graph = task.compute_graph_name,
            "fn" = task.compute_fn_name,
            graph_version = task.graph_version.to_string(),
        );
        let _guard = span.enter();

        let mut update = SchedulerUpdateRequest::default();

        // Use FunctionExecutorManager to handle function executor selection/creation
        let (selected_target, fe_update) = self
            .fe_manager
            .select_or_create_function_executor(in_memory_state, task)?;
        update.extend(fe_update);

        let Some(target) = selected_target else {
            return Ok(update);
        };
        let mut updated_task = task.clone();
        updated_task.status = TaskStatus::Running;
        let allocation = AllocationBuilder::default()
            .namespace(task.namespace.clone())
            .compute_graph(task.compute_graph_name.clone())
            .compute_fn(task.compute_fn_name.clone())
            .invocation_id(task.invocation_id.clone())
            .task_id(task.id.clone())
            .target(target)
            .attempt_number(updated_task.attempt_number)
            .outcome(TaskOutcome::Unknown)
            .build()?;

        info!(allocation_id = allocation.id, "created allocation");
        update
            .updated_tasks
            .insert(updated_task.id.clone(), updated_task.clone());
        update.new_allocations.push(allocation);
        in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate(Box::new(update.clone())),
            "task_allocator",
        )?;
        Ok(update)
    }
}
