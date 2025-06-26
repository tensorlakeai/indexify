use std::{
    ops::DerefMut,
    sync::{Arc, RwLock},
    vec,
};

use anyhow::{anyhow, Result};
use data_model::{
    AllocationBuilder,
    ChangeType,
    ExecutorId,
    ExecutorMetadata,
    ExecutorServerMetadata,
    FunctionExecutor,
    FunctionExecutorBuilder,
    FunctionExecutorServerMetadata,
    FunctionExecutorState,
    FunctionExecutorTerminationReason,
    FunctionResources,
    GraphInvocationCtx,
    Task,
    TaskOutcome,
    TaskStatus,
};
use rand::seq::IndexedRandom;
use state_store::{
    in_memory_state::InMemoryState,
    requests::{RequestPayload, SchedulerUpdateRequest},
};
use tracing::{debug, error, info, info_span, warn};

struct TaskAllocationProcessor<'a> {
    in_memory_state: &'a mut InMemoryState,
    clock: u64,

    // Maximum number of allocations per executor.
    //
    // In the future, this should be a dynamic value based on:
    // - function concurrency configuration
    // - function batching configuration
    // - function timeout configuration
    queue_size: u32,
}

#[tracing::instrument(skip(in_memory_state, clock, change))]
pub fn invoke(
    in_memory_state: Arc<RwLock<InMemoryState>>,
    clock: u64,
    change: &ChangeType,
    queue_size: u32,
) -> Result<SchedulerUpdateRequest> {
    let mut in_memory_state = in_memory_state.write().unwrap();

    let mut task_allocator = TaskAllocationProcessor {
        in_memory_state: &mut in_memory_state.deref_mut(),
        clock,
        queue_size,
    };

    task_allocator.invoke(change)
}

/// Allocate attempts to allocate unallocated tasks to function executors.
/// It first runs a vacuum phase to clean up any stale function executors.
#[tracing::instrument(skip(in_memory_state, clock))]
pub fn allocate(
    in_memory_state: Arc<RwLock<InMemoryState>>,
    clock: u64,
    queue_size: u32,
) -> Result<SchedulerUpdateRequest> {
    let mut in_memory_state = in_memory_state.write().unwrap();

    let mut task_allocator = TaskAllocationProcessor {
        in_memory_state: &mut in_memory_state.deref_mut(),
        clock,
        queue_size,
    };

    task_allocator.allocate()
}

impl<'a> TaskAllocationProcessor<'a> {
    #[tracing::instrument(skip(self, change))]
    fn invoke(&mut self, change: &ChangeType) -> Result<SchedulerUpdateRequest> {
        match change {
            ChangeType::ExecutorUpserted(ev) => {
                let mut update = self.reconcile_executor_state(&ev.executor_id)?;
                update.extend(self.allocate()?);
                return Ok(update);
            }
            ChangeType::ExecutorRemoved(_) => {
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

    /// Allocate attempts to allocate unallocated tasks to function executors.
    /// It first runs a vacuum phase to clean up any stale function executors.
    #[tracing::instrument(skip(self))]
    fn allocate(&mut self) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // Step 1: Fetch unallocated tasks
        let tasks = self.in_memory_state.unallocated_tasks();
        debug!("found {} unallocated tasks to process", tasks.len());

        // Step 3: Allocate tasks
        update.extend(self.allocate_tasks(tasks)?);

        Ok(update)
    }

    #[tracing::instrument(skip(self, tasks))]
    fn allocate_tasks(&mut self, tasks: Vec<Box<Task>>) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // Step 1: Process tasks
        for task in tasks {
            update.extend(self.create_allocation(&task)?);
        }
        Ok(update)
    }

    // Vacuum phase - returns scheduler update for cleanup actions
    #[tracing::instrument(skip(self))]
    fn vacuum(&self, fe_resource: &FunctionResources) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        let function_executors_to_mark = self
            .in_memory_state
            .vacuum_function_executors(fe_resource)?;

        info!(
            "vacuum phase identified {} function executors to mark for termination",
            function_executors_to_mark.len(),
        );

        // Mark FEs for termination (change desired state to Terminated)
        // but don't actually remove them - reconciliation will handle that
        for fe in &function_executors_to_mark {
            let mut update_fe = fe.clone();
            update_fe.desired_state = FunctionExecutorState::Terminated;
            update.new_function_executors.push(*update_fe);

            info!(
                fn_executor_id = fe.function_executor.id.get(),
                executor_id = fe.executor_id.get(),
                "Marked function executor {} on executor {} for termination",
                fe.function_executor.id.get(),
                fe.executor_id.get()
            );
        }
        Ok(update)
    }

    fn create_function_executor(&mut self, task: &Task) -> Result<SchedulerUpdateRequest> {
        let span = info_span!(
            "create_function_executor",
            namespace = task.namespace,
            invocation_id = task.invocation_id,
            task_id = task.id.get(),
            graph = task.compute_graph_name,
            "fn" = task.compute_fn_name,
            graph_version = task.graph_version.to_string(),
        );
        let _guard = span.enter();

        let mut update = SchedulerUpdateRequest::default();
        let mut candidates = self.in_memory_state.candidate_executors(task)?;
        if candidates.is_empty() {
            info!("no candidates found for task, running vacuum");
            let func_resource = self.in_memory_state.function_resources_for_task(&task)?;
            let vacuum_update = self.vacuum(&func_resource)?;
            update.extend(vacuum_update);
            self.in_memory_state.update_state(
                self.clock,
                &RequestPayload::SchedulerUpdate(Box::new(update.clone())),
                "task_allocator",
            )?;
            candidates = self.in_memory_state.candidate_executors(task)?;
        }
        info!(
            "found {} candidates for creating function executor",
            candidates.len()
        );

        let Some(mut candidate) = candidates.choose(&mut rand::rng()).cloned() else {
            return Ok(update);
        };
        let executor_id = candidate.executor_id.clone();
        let node_resources = self
            .in_memory_state
            .get_function_resources_by_uri(
                &task.namespace,
                &task.compute_graph_name,
                &task.compute_fn_name,
                &task.graph_version,
            )
            .ok_or(anyhow!("failed to get function executor resources"))?;

        let fe_resources = candidate
            .free_resources
            .consume_function_resources(&node_resources)?;
        // Consume resources from executor
        update
            .updated_executor_resources
            .insert(executor_id.clone(), candidate.free_resources.clone());

        // Create a new function executor
        let function_executor = FunctionExecutorBuilder::default()
            .namespace(task.namespace.clone())
            .compute_graph_name(task.compute_graph_name.clone())
            .compute_fn_name(task.compute_fn_name.clone())
            .version(task.graph_version.clone())
            .state(FunctionExecutorState::Unknown)
            .resources(fe_resources.clone())
            .termination_reason(FunctionExecutorTerminationReason::Unknown)
            .build()?;

        info!(
            executor_id = executor_id.get(),
            fn_executor_id = function_executor.id.get(),
            "created function executor"
        );

        // Create with current timestamp for last_allocation_at
        let fe_server_metadata = FunctionExecutorServerMetadata::new(
            executor_id.clone(),
            function_executor,
            FunctionExecutorState::Running, // Start with Running state
        );
        update.new_function_executors.push(fe_server_metadata);

        self.in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate(Box::new(update.clone())),
            "task_allocator",
        )?;
        Ok(update)
    }

    fn create_allocation(&mut self, task: &Task) -> Result<SchedulerUpdateRequest> {
        let span = info_span!(
            "delete_compute_graph",
            namespace = task.namespace,
            task_id = task.id.get(),
            invocation_id = task.invocation_id,
            graph = task.compute_graph_name,
            "fn" = task.compute_fn_name,
            graph_version = task.graph_version.to_string(),
        );
        let _guard = span.enter();

        let mut update = SchedulerUpdateRequest::default();
        let mut function_executors = self
            .in_memory_state
            .candidate_function_executors(task, self.queue_size)?;
        if function_executors.function_executors.is_empty() &&
            function_executors.num_pending_function_executors == 0
        {
            info!("no function executors found for task, creating one");
            let fe_update = self.create_function_executor(task)?;
            update.extend(fe_update);
            self.in_memory_state.update_state(
                self.clock,
                &RequestPayload::SchedulerUpdate(Box::new(update.clone())),
                "task_allocator",
            )?;
            function_executors = self
                .in_memory_state
                .candidate_function_executors(task, self.queue_size)?;
        }
        info!(
            num_function_executors = function_executors.function_executors.len(),
            "found function executors for task",
        );

        let Some(candidate) = function_executors
            .function_executors
            .choose(&mut rand::rng())
        else {
            return Ok(update);
        };
        let fe_id = candidate.function_executor.id.clone();
        let mut updated_task = task.clone();
        updated_task.status = TaskStatus::Running;
        let allocation = AllocationBuilder::default()
            .namespace(task.namespace.clone())
            .compute_graph(task.compute_graph_name.clone())
            .compute_fn(task.compute_fn_name.clone())
            .invocation_id(task.invocation_id.clone())
            .task_id(task.id.clone())
            .executor_id(candidate.executor_id.clone())
            .function_executor_id(fe_id.clone())
            .attempt_number(updated_task.attempt_number)
            .outcome(TaskOutcome::Unknown)
            .build()?;

        info!(allocation_id = allocation.id, "created allocation");
        update
            .updated_tasks
            .insert(updated_task.id.clone(), updated_task.clone());
        update.new_allocations.push(allocation);
        self.in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate(Box::new(update.clone())),
            "task_allocator",
        )?;
        Ok(update)
    }

    #[tracing::instrument(skip(self, executor_id))]
    fn reconcile_executor_state(
        &mut self,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        let executor = self
            .in_memory_state
            .executors
            .get(&executor_id)
            .ok_or(anyhow!("executor not found"))?
            .clone();
        debug!(
            "reconciling executor state for executor {} - {:#?}",
            executor_id.get(),
            executor
        );

        // Reconcile function executors
        update.extend(self.reconcile_function_executors(&executor)?);

        return Ok(update);
    }

    #[tracing::instrument(skip(self, executor))]
    fn reconcile_function_executors(
        &mut self,
        executor: &ExecutorMetadata,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        // Get the function executors from the indexes
        let mut executor_server_metadata = self
            .in_memory_state
            .executor_states
            .get(&executor.id)
            .unwrap()
            .clone();
        let server_function_executors = executor_server_metadata.function_executors.clone();
        let mut function_executors_to_remove = Vec::new();
        let mut new_function_executors = Vec::new();

        let fes_exist_only_in_executor = executor
            .function_executors
            .iter()
            .filter(|(fe_id, _fe)| !server_function_executors.contains_key(fe_id))
            .map(|(_fe_id, fe)| fe.clone())
            .collect::<Vec<_>>();
        let fes_exist_only_in_server = server_function_executors
            .iter()
            .filter(|(fe_id, _fe)| !executor.function_executors.contains_key(fe_id))
            .map(|(_fe_id, fe)| fe.clone())
            .collect::<Vec<_>>();
        for fe in fes_exist_only_in_server {
            if fe.desired_state == FunctionExecutorState::Terminated {
                function_executors_to_remove.push(fe.function_executor.clone());
            }
        }
        for fe in fes_exist_only_in_executor {
            if fe.state != FunctionExecutorState::Terminated &&
                executor_server_metadata
                    .free_resources
                    .can_handle_fe_resources(&fe.resources)
                    .is_ok()
            {
                new_function_executors.push(FunctionExecutorServerMetadata::new(
                    executor.id.clone(),
                    fe.clone(),
                    fe.state,
                ));
                executor_server_metadata
                    .free_resources
                    .consume_fe_resources(&fe.resources)?;
                update.updated_executor_resources.insert(
                    executor.id.clone(),
                    executor_server_metadata.free_resources.clone(),
                );
            }
        }
        for (executor_fe_id, executor_fe) in &executor.function_executors {
            if let Some(server_fe) = server_function_executors.get(&executor_fe_id) {
                if executor_fe.state == FunctionExecutorState::Terminated {
                    function_executors_to_remove.push(executor_fe.clone());
                    continue;
                }
                if server_fe.desired_state == FunctionExecutorState::Terminated {
                    continue;
                }
                if executor_fe.state != server_fe.desired_state {
                    let mut server_fe_clone = server_fe.clone();
                    server_fe_clone.function_executor.state = executor_fe.state;
                    new_function_executors.push(*server_fe_clone);
                }
            }
        }
        update.new_function_executors.extend(new_function_executors);

        update.extend(self.remove_function_executors(
            &mut executor_server_metadata,
            &function_executors_to_remove,
        )?);

        self.in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate(Box::new(update.clone())),
            "task_allocator",
        )?;

        Ok(update)
    }

    #[tracing::instrument(skip(self, executor_server_metadata, function_executors_to_remove))]
    fn remove_function_executors(
        &mut self,
        executor_server_metadata: &mut ExecutorServerMetadata,
        function_executors_to_remove: &Vec<FunctionExecutor>,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        let span = info_span!(
            "remove_function_executors",
            executor_id = executor_server_metadata.executor_id.get(),
            num_function_executors = function_executors_to_remove.len(),
        );
        let _guard = span.enter();

        if function_executors_to_remove.is_empty() {
            return Ok(update);
        }

        let mut failed_tasks = 0;
        // Handle allocations for FEs to be removed and update tasks
        for fe in function_executors_to_remove {
            info!(
                namespace = fe.namespace,
                compute_graph = fe.compute_graph_name,
                compute_fn = fe.compute_fn_name,
                fn_executor_id = fe.id.get(),
                "removing function executor from executor",
            );

            let allocs = self
                .in_memory_state
                .allocations_by_executor
                .get(&executor_server_metadata.executor_id)
                .and_then(|allocs_be_fe| allocs_be_fe.get(&fe.id))
                .cloned()
                .unwrap_or_default();
            for alloc in allocs {
                let Some(mut task) = self.in_memory_state.tasks.get(&alloc.task_key()).cloned()
                else {
                    continue;
                };
                if task.status == TaskStatus::Pending || task.status == TaskStatus::Completed {
                    continue;
                }
                if fe.termination_reason == FunctionExecutorTerminationReason::CustomerCodeError {
                    task.status = TaskStatus::Completed;
                    task.outcome = TaskOutcome::Failure;
                    failed_tasks += 1;
                } else if fe.termination_reason == FunctionExecutorTerminationReason::PlatformError ||
                    fe.termination_reason == FunctionExecutorTerminationReason::Unknown ||
                    fe.termination_reason ==
                        FunctionExecutorTerminationReason::DesiredStateRemoved
                {
                    task.status = TaskStatus::Pending;
                    task.attempt_number = task.attempt_number + 1;
                }
                update.updated_tasks.insert(task.id.clone(), *task.clone());
                let invocation_ctx_key = GraphInvocationCtx::key_from(
                    &task.namespace,
                    &task.compute_graph_name,
                    &task.invocation_id,
                );
                if let Some(invocation_ctx) = self
                    .in_memory_state
                    .invocation_ctx
                    .get(&invocation_ctx_key)
                    .cloned()
                {
                    if task.status == TaskStatus::Completed {
                        let mut invocation_ctx = invocation_ctx.clone();
                        invocation_ctx.completed = true;
                        invocation_ctx.outcome = task.outcome.into();
                        invocation_ctx.failure_reason = task.failure_reason.into();
                        update.updated_invocations_states.push(*invocation_ctx);
                    }
                }
            }
        }

        info!(
            num_failed_allocations = failed_tasks,
            "failed allocations on executor due to function executor terminations",
        );

        // Add function executors to remove list
        update
            .remove_function_executors
            .entry(executor_server_metadata.executor_id.clone())
            .or_default()
            .extend(function_executors_to_remove.iter().map(|fe| fe.id.clone()));

        for fe in function_executors_to_remove {
            if let Some(fe_resource_claim) = executor_server_metadata.resource_claims.get(&fe.id) {
                executor_server_metadata
                    .free_resources
                    .free(fe_resource_claim)?;
                update.updated_executor_resources.insert(
                    executor_server_metadata.executor_id.clone(),
                    executor_server_metadata.free_resources.clone(),
                );
            } else {
                error!(
                    fn_executor_id = fe.id.get(),
                    "resources not freed: function executor is not claiming resources on executor",
                );
            }
        }
        Ok(update)
    }

    #[tracing::instrument(skip(self, executor_id))]
    fn deregister_executor(&mut self, executor_id: &ExecutorId) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest {
            remove_executors: vec![executor_id.clone()],
            ..Default::default()
        };

        let Some(mut executor_server_metadata) = self
            .in_memory_state
            .executor_states
            .get(executor_id)
            .cloned()
        else {
            error!(
                executor_id = executor_id.get(),
                "executor {} not found while de-registering executor",
                executor_id.get()
            );
            return Ok(update);
        };

        // Get all function executors to remove
        let function_executors_to_remove = &executor_server_metadata
            .function_executors
            .values()
            .map(|fe| fe.function_executor.clone())
            .collect::<Vec<_>>();

        update.extend(self.remove_function_executors(
            &mut executor_server_metadata,
            &function_executors_to_remove,
        )?);

        self.in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate(Box::new(update.clone())),
            "task_allocator",
        )?;

        let allocation_update = self.allocate()?;
        update.extend(allocation_update);
        return Ok(update);
    }
}
