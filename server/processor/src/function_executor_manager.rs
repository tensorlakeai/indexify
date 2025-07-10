use anyhow::{anyhow, Result};
use data_model::{
    AllocationTarget,
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
    TaskStatus,
};
use if_chain::if_chain;
use rand::seq::IndexedRandom;
use state_store::{
    in_memory_state::InMemoryState,
    requests::{RequestPayload, SchedulerUpdateRequest},
};
use tracing::{debug, error, info, info_span};

use crate::{targets, task_policy::TaskRetryPolicy};

pub struct FunctionExecutorManager {
    clock: u64,
    queue_size: u32,
}

/// Implements the policy around function executors: when to create
/// them, when to terminate them, and the selection of function
/// executors appropriate to a given task.
impl FunctionExecutorManager {
    pub fn new(clock: u64, queue_size: u32) -> Self {
        Self { clock, queue_size }
    }

    /// Vacuum phase - identifies function executors that should be terminated
    /// Returns scheduler update for cleanup actions
    #[tracing::instrument(skip(self, in_memory_state))]
    fn vacuum(
        &self,
        in_memory_state: &mut InMemoryState,
        fe_resource: &FunctionResources,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        let function_executors_to_mark =
            in_memory_state.vacuum_function_executors_candidates(fe_resource)?;

        debug!(
            target: targets::SCHEDULER,
            "vacuum phase identified {} function executors to mark for termination",
            function_executors_to_mark.len(),
        );

        // Mark FEs for termination (change desired state to Terminated)
        // but don't actually remove them - reconciliation will handle that
        for fe in &function_executors_to_mark {
            let mut update_fe = fe.clone();
            update_fe.desired_state = FunctionExecutorState::Terminated {
                reason: FunctionExecutorTerminationReason::DesiredStateRemoved,
                failed_alloc_ids: Vec::new(),
            };
            update.new_function_executors.push(*update_fe);

            info!(
                target: targets::SCHEDULER,
                fn_executor_id = fe.function_executor.id.get(),
                executor_id = fe.executor_id.get(),
                "Marked function executor {} on executor {} for termination",
                fe.function_executor.id.get(),
                fe.executor_id.get()
            );
        }
        Ok(update)
    }

    /// Creates a new function executor for the given task
    fn create_function_executor(
        &self,
        in_memory_state: &mut InMemoryState,
        task: &Task,
    ) -> Result<SchedulerUpdateRequest> {
        let span = info_span!(
            target: targets::SCHEDULER,
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
        let mut candidates = in_memory_state.candidate_executors(task)?;
        if candidates.is_empty() {
            debug!(target: targets::SCHEDULER, "no candidates found for task, running vacuum");
            let fe_resource = in_memory_state.fe_resource_for_task(&task)?;
            let vacuum_update = self.vacuum(in_memory_state, &fe_resource)?;
            update.extend(vacuum_update);
            in_memory_state.update_state(
                self.clock,
                &RequestPayload::SchedulerUpdate(Box::new(update.clone())),
                "function_executor_manager",
            )?;
            candidates = in_memory_state.candidate_executors(task)?;
        }
        debug!(
            target: targets::SCHEDULER,
            "found {} candidates for creating function executor",
            candidates.len()
        );

        let Some(mut candidate) = candidates.choose(&mut rand::rng()).cloned() else {
            return Ok(update);
        };
        let executor_id = candidate.executor_id.clone();
        let node_resources = in_memory_state
            .get_fe_resources_by_uri(
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
            .build()?;

        info!(
            target: targets::SCHEDULER,
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

        in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate(Box::new(update.clone())),
            "function_executor_manager",
        )?;
        Ok(update)
    }

    /// Reconciles function executor state between executor and server
    #[tracing::instrument(skip(self, in_memory_state, executor))]
    fn reconcile_function_executors(
        &self,
        in_memory_state: &mut InMemoryState,
        executor: &ExecutorMetadata,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        // Get the function executors from the indexes
        let mut executor_server_metadata = in_memory_state
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
            if matches!(fe.desired_state, FunctionExecutorState::Terminated { .. }) {
                function_executors_to_remove.push(fe.function_executor.clone());
            }
        }
        for fe in fes_exist_only_in_executor {
            if !matches!(fe.state, FunctionExecutorState::Terminated { .. }) &&
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
                if matches!(executor_fe.state, FunctionExecutorState::Terminated { .. }) {
                    function_executors_to_remove.push(executor_fe.clone());
                    continue;
                }
                if matches!(
                    server_fe.desired_state,
                    FunctionExecutorState::Terminated { .. }
                ) {
                    continue;
                }
                if executor_fe.state != server_fe.desired_state {
                    let mut server_fe_clone = server_fe.clone();
                    server_fe_clone.function_executor.state = executor_fe.state.clone();
                    new_function_executors.push(*server_fe_clone);
                }
            }
        }
        update.new_function_executors.extend(new_function_executors);

        update.extend(self.remove_function_executors(
            in_memory_state,
            &mut executor_server_metadata,
            &function_executors_to_remove,
        )?);

        in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate(Box::new(update.clone())),
            "function_executor_manager",
        )?;

        Ok(update)
    }

    /// Removes function executors and handles associated task cleanup
    #[tracing::instrument(skip(
        self,
        in_memory_state,
        executor_server_metadata,
        function_executors_to_remove
    ))]
    fn remove_function_executors(
        &self,
        in_memory_state: &mut InMemoryState,
        executor_server_metadata: &mut ExecutorServerMetadata,
        function_executors_to_remove: &Vec<FunctionExecutor>,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        let span = info_span!(
            target: targets::SCHEDULER,
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
                target: targets::SCHEDULER,
                namespace = fe.namespace,
                graph = fe.compute_graph_name,
                fn = fe.compute_fn_name,
                executor_id = fe.id.get(),
                fe_state = ?fe.state,
                "Removing function executor from executor",
            );

            let allocs = in_memory_state
                .allocations_by_executor
                .get(&executor_server_metadata.executor_id)
                .and_then(|allocs_be_fe| allocs_be_fe.get(&fe.id))
                .cloned()
                .unwrap_or_default();
            for alloc in allocs {
                let Some(mut task) = in_memory_state.tasks.get(&alloc.task_key()).cloned() else {
                    continue;
                };
                if task.status == TaskStatus::Pending || task.status == TaskStatus::Completed {
                    continue;
                }

                if_chain! {
                        if let Ok(compute_graph_version) = in_memory_state.get_existing_compute_graph_version(&task);
                        if let FunctionExecutorState::Terminated { reason: termination_reason, failed_alloc_ids: blame_allocs } = &fe.state;
                then {
                            let task_failure_reason = (*termination_reason).into();

                            TaskRetryPolicy::handle_function_executor_termination(
                                &mut task,
                                task_failure_reason,
                                blame_allocs,
                                &alloc.id,
                                &compute_graph_version,
                            );

                            // Count failed tasks for logging
                            if task.status == TaskStatus::Completed {
                                failed_tasks += 1;
                            }
                        }
                else {
                            // Could not get compute graph version, or function executor not terminated; set task to pending
                            task.status = TaskStatus::Pending;
                        }
                    }
                update.updated_tasks.insert(task.id.clone(), *task.clone());
                let invocation_ctx_key = GraphInvocationCtx::key_from(
                    &task.namespace,
                    &task.compute_graph_name,
                    &task.invocation_id,
                );
                if let Some(invocation_ctx) = in_memory_state
                    .invocation_ctx
                    .get(&invocation_ctx_key)
                    .cloned()
                {
                    if task.status == TaskStatus::Completed {
                        let mut invocation_ctx = invocation_ctx.clone();
                        invocation_ctx.completed = true;
                        invocation_ctx.outcome = task.outcome.into();
                        update.updated_invocations_states.push(*invocation_ctx);
                    }
                }
            }
        }

        info!(
            target: targets::SCHEDULER,
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
                    target: targets::SCHEDULER,
                    fn_executor_id = fe.id.get(),
                    "resources not freed: function executor is not claiming resources on executor",
                );
            }
        }
        Ok(update)
    }

    /// Removes all function executors from an executor when it's being
    /// deregistered
    fn remove_all_function_executors_for_executor(
        &self,
        in_memory_state: &mut InMemoryState,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let Some(mut executor_server_metadata) =
            in_memory_state.executor_states.get(executor_id).cloned()
        else {
            error!(
                target: targets::SCHEDULER,
                executor_id = executor_id.get(),
                "executor {} not found while removing function executors",
                executor_id.get()
            );
            return Ok(SchedulerUpdateRequest::default());
        };

        // Get all function executors to remove
        let function_executors_to_remove = &executor_server_metadata
            .function_executors
            .values()
            .map(|fe| {
                let mut fec = fe.function_executor.clone();
                if !matches!(fec.state, FunctionExecutorState::Terminated { .. }) {
                    fec.state = FunctionExecutorState::Terminated {
                        reason: FunctionExecutorTerminationReason::ExecutorRemoved,
                        failed_alloc_ids: Vec::new(),
                    };
                }
                fec
            })
            .collect::<Vec<_>>();

        self.remove_function_executors(
            in_memory_state,
            &mut executor_server_metadata,
            &function_executors_to_remove,
        )
    }

    /// Selects or creates a function executor for the given task
    /// Returns the allocation target and any scheduler updates
    #[tracing::instrument(skip(self, in_memory_state, task))]
    pub fn select_or_create_function_executor(
        &self,
        in_memory_state: &mut InMemoryState,
        task: &Task,
    ) -> Result<(Option<AllocationTarget>, SchedulerUpdateRequest)> {
        let mut update = SchedulerUpdateRequest::default();
        let mut function_executors =
            in_memory_state.candidate_function_executors(task, self.queue_size)?;

        // If no function executors are available, create one
        if function_executors.function_executors.is_empty() &&
            function_executors.num_pending_function_executors == 0
        {
            debug!(target: targets::SCHEDULER, "no function executors found for task, creating one");
            let fe_update = self.create_function_executor(in_memory_state, task)?;
            update.extend(fe_update);
            in_memory_state.update_state(
                self.clock,
                &RequestPayload::SchedulerUpdate(Box::new(update.clone())),
                "function_executor_manager",
            )?;
            function_executors =
                in_memory_state.candidate_function_executors(task, self.queue_size)?;
        }

        debug!(
            target: targets::SCHEDULER,
            num_function_executors = function_executors.function_executors.len(),
            num_pending_function_executors = function_executors.num_pending_function_executors,
            "found function executors for task",
        );

        // Select a function executor using the current policy (random selection)
        let selected_fe = function_executors
            .function_executors
            .choose(&mut rand::rng())
            .map(|fe| {
                AllocationTarget::new(fe.executor_id.clone(), fe.function_executor.id.clone())
            });

        Ok((selected_fe, update))
    }

    /// Completely deregisters an executor and handles all associated cleanup
    /// Returns scheduler update that includes executor removal and function
    /// executor cleanup
    #[tracing::instrument(skip(self, in_memory_state, executor_id))]
    pub fn deregister_executor(
        &self,
        in_memory_state: &mut InMemoryState,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest {
            remove_executors: vec![executor_id.clone()],
            ..Default::default()
        };

        // Remove all function executors for this executor
        update
            .extend(self.remove_all_function_executors_for_executor(in_memory_state, executor_id)?);

        in_memory_state.update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate(Box::new(update.clone())),
            "function_executor_manager",
        )?;

        Ok(update)
    }

    /// Reconciles executor state when an executor is upserted
    /// Returns scheduler update that includes function executor reconciliation
    #[tracing::instrument(skip(self, in_memory_state, executor_id))]
    pub fn reconcile_executor_state(
        &self,
        in_memory_state: &mut InMemoryState,
        executor_id: &ExecutorId,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        let executor = in_memory_state
            .executors
            .get(&executor_id)
            .ok_or(anyhow!("executor not found"))?
            .clone();

        tracing::debug!(
            target: targets::SCHEDULER,
            executor_id = executor_id.get(),
            ?executor,
            "reconciling executor state for executor",
        );

        // Reconcile function executors
        update.extend(self.reconcile_function_executors(in_memory_state, &executor)?);

        Ok(update)
    }
}
