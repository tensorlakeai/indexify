use std::{
    sync::{Arc, RwLock},
    vec,
};

use anyhow::{anyhow, Result};
use data_model::{
    Allocation,
    AllocationBuilder,
    ChangeType,
    ExecutorId,
    ExecutorMetadata,
    FunctionExecutor,
    FunctionExecutorBuilder,
    FunctionExecutorServerMetadata,
    FunctionExecutorState,
    FunctionExecutorStatus,
    GraphInvocationOutcome,
    Task,
    TaskOutcome,
    TaskStatus,
};
use im::HashMap;
use itertools::Itertools;
use rand::seq::IndexedRandom;
use state_store::{
    in_memory_state::InMemoryState,
    requests::{FunctionExecutorIdWithExecutionId, RequestPayload, SchedulerUpdateRequest},
};
use tracing::{debug, error, info, warn};

// Maximum number of allocations per executor.
//
// In the future, this should be a dynamic value based on:
// - function concurrency configuration
// - function batching configuration
// - function timeout configuration
const MAX_ALLOCATIONS_PER_FN_EXECUTOR: usize = 20;

pub struct TaskAllocationProcessor {
    in_memory_state: Arc<RwLock<InMemoryState>>,
    clock: u64,
}

impl TaskAllocationProcessor {
    pub fn new(in_memory_state: Arc<RwLock<InMemoryState>>, clock: u64) -> Self {
        Self {
            in_memory_state,
            clock,
        }
    }

    #[tracing::instrument(skip(self, change))]
    pub fn invoke(&mut self, change: &ChangeType) -> Result<SchedulerUpdateRequest> {
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
    pub fn allocate(&mut self) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // Step 1: Fetch unallocated tasks
        let tasks = self.in_memory_state.read().unwrap().unallocated_tasks();
        debug!("found {} unallocated tasks to process", tasks.len());

        // Step 3: Allocate tasks
        update.extend(self.allocate_tasks(tasks)?);

        Ok(update)
    }

    #[tracing::instrument(skip(self, tasks))]
    pub fn allocate_tasks(&self, tasks: Vec<Box<Task>>) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // Step 1: Process tasks
        for task in tasks {
            update.extend(self.create_allocation(&task)?);
        }
        Ok(update)
    }

    // Vacuum phase - returns scheduler update for cleanup actions
    #[tracing::instrument(skip(self))]
    fn vacuum(&self) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        let function_executors_to_mark = self
            .in_memory_state
            .read()
            .unwrap()
            .vacuum_function_executors_candidates()?;
        debug!(
            "vacuum phase identified {} function executors to mark for termination",
            function_executors_to_mark.len()
        );

        // Mark FEs for termination (change desired state to Terminated)
        // but don't actually remove them - reconciliation will handle that
        for fe in &function_executors_to_mark {
            let mut update_fe = fe.clone();
            update_fe.desired_state = FunctionExecutorState::Terminated;
            update.new_function_executors.push(*update_fe);

            info!(
                "Marked function executor {} on executor {} for termination",
                fe.function_executor.id.get(),
                fe.executor_id.get()
            );
        }
        Ok(update)
    }

    fn create_function_executor(&self, task: &Task) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        let mut candidates = self
            .in_memory_state
            .read()
            .unwrap()
            .candidate_executors(task)?;
        if candidates.is_empty() {
            info!(
                invocation_id = task.invocation_id,
                compute_graph = task.compute_graph_name,
                compute_fn = task.compute_fn_name,
                version = task.graph_version.to_string(),
                "no candidates found for task, running vacuum"
            );
            let vacuum_update = self.vacuum()?;
            update.extend(vacuum_update);
            self.in_memory_state.write().unwrap().update_state(
                self.clock,
                &RequestPayload::SchedulerUpdate(Box::new(update.clone())),
            )?;
            candidates = self
                .in_memory_state
                .read()
                .unwrap()
                .candidate_executors(task)?;
        }
        info!(
            invocation_id = task.invocation_id,
            compute_graph = task.compute_graph_name,
            compute_fn = task.compute_fn_name,
            version = task.graph_version.to_string(),
            "found {} candidates for creating function executor",
            candidates.len()
        );

        let Some(candidate) = candidates.choose(&mut rand::rng()) else {
            return Ok(update);
        };
        let executor_id = candidate.id.clone();
        // Create a new function executor
        let function_executor = FunctionExecutorBuilder::default()
            .namespace(task.namespace.clone())
            .compute_graph_name(task.compute_graph_name.clone())
            .compute_fn_name(task.compute_fn_name.clone())
            .version(task.graph_version.clone())
            .status(FunctionExecutorStatus::Unknown)
            .build()?;

        info!(
            invocation_id = task.invocation_id,
            compute_graph = task.compute_graph_name,
            compute_fn = task.compute_fn_name,
            version = task.graph_version.to_string(),
            executor_id = executor_id.get(),
            function_executor = function_executor.id.get(),
            "created function executor"
        );
        // Create with current timestamp for last_allocation_at
        let fe_server_metadata = FunctionExecutorServerMetadata::new(
            executor_id.clone(),
            function_executor,
            FunctionExecutorState::Running, // Start with Running state
        );
        update.new_function_executors.push(fe_server_metadata);
        update
            .updated_executors
            .insert(executor_id.clone(), *candidate.clone());
        self.in_memory_state.write().unwrap().update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate(Box::new(update.clone())),
        )?;
        Ok(update)
    }

    fn create_allocation(&self, task: &Task) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        let mut function_executors = self
            .in_memory_state
            .read()
            .unwrap()
            .candidate_function_executors(task, MAX_ALLOCATIONS_PER_FN_EXECUTOR)?;
        if function_executors.is_empty() {
            info!(
                invocation_id = task.invocation_id,
                compute_graph = task.compute_graph_name,
                compute_fn = task.compute_fn_name,
                version = task.graph_version.to_string(),
                "no function executors found for task, creating one"
            );
            let fe_update = self.create_function_executor(task)?;
            update.extend(fe_update);
            self.in_memory_state.write().unwrap().update_state(
                self.clock,
                &RequestPayload::SchedulerUpdate(Box::new(update.clone())),
            )?;
            function_executors = self
                .in_memory_state
                .read()
                .unwrap()
                .candidate_function_executors(task, MAX_ALLOCATIONS_PER_FN_EXECUTOR)?;
        }
        info!(
            invocation_id = task.invocation_id,
            compute_graph = task.compute_graph_name,
            compute_fn = task.compute_fn_name,
            version = task.graph_version.to_string(),
            "found {} function executors for task",
            function_executors.len()
        );

        let Some(candidate) = function_executors.choose(&mut rand::rng()) else {
            return Ok(update);
        };
        let fe_id = candidate.function_executor.id.clone();
        let allocation = AllocationBuilder::default()
            .namespace(task.namespace.clone())
            .compute_graph(task.compute_graph_name.clone())
            .compute_fn(task.compute_fn_name.clone())
            .invocation_id(task.invocation_id.clone())
            .task_id(task.id.clone())
            .executor_id(candidate.executor_id.clone())
            .function_executor_id(fe_id.clone())
            .build()?;

        info!(
            invocation_id = task.invocation_id,
            compute_graph = task.compute_graph_name,
            compute_fn = task.compute_fn_name,
            version = task.graph_version.to_string(),
            allocation = allocation.id,
            "created allocation"
        );
        let mut updated_task = task.clone();
        updated_task.status = TaskStatus::Running;
        update
            .updated_tasks
            .insert(updated_task.id.clone(), updated_task.clone());
        update.new_allocations.push(allocation);
        self.in_memory_state.write().unwrap().update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate(Box::new(update.clone())),
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
            .read()
            .unwrap()
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
        &self,
        executor: &ExecutorMetadata,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        // Get the function executors from the indexes
        let function_executors_in_indexes = self
            .in_memory_state
            .read()
            .unwrap()
            .function_executors_by_executor
            .get(&executor.id)
            .cloned()
            .unwrap_or_default();
        // Step 1: Identify and remove FEs that should be removed
        // Cases when we should remove:
        // 1. FE in our indexes has desired_state=Running but doesn't exist in
        //    executor's valid list
        // 2. FE in our indexes has desired_state=Terminated (marked by vacuum phase)
        // 3. FE in executor has status mapping to Terminated state
        // Note: We should never remove a FE that is in Pending state in our indexes if
        // not present in executor's list, since it may still be creating.
        let function_executors_to_remove = function_executors_in_indexes
            .iter()
            .filter_map(|(indexed_fe_id, indexed_fe)| {
                // Case 1: If our indexed FE is marked as Terminated, remove it
                if indexed_fe.desired_state == FunctionExecutorState::Terminated {
                    debug!(
                        "Removing function executor {} that was marked for termination",
                        indexed_fe_id.get()
                    );
                    return Some(indexed_fe.function_executor.clone());
                }

                // Case 2: Check if it exists in executor's list
                if let Some(executor_fe) = executor.function_executors.get(indexed_fe_id) {
                    // It exists in executor's list, check if its state is Terminated
                    if executor_fe.status.as_state() == FunctionExecutorState::Terminated {
                        debug!(
                            "Removing function executor {} that is in Terminated state in executor",
                            indexed_fe_id.get()
                        );
                        return Some(indexed_fe.function_executor.clone());
                    }
                }
                // Otherwise keep it
                None
            })
            .collect_vec();
        if !function_executors_to_remove.is_empty() {
            debug!(
                "Executor {} has {} function executors to be removed",
                executor.id.get(),
                function_executors_to_remove.len()
            );
            update.extend(
                self.remove_function_executors(&executor.id, &function_executors_to_remove)?,
            );
            self.in_memory_state.write().unwrap().update_state(
                self.clock,
                &RequestPayload::SchedulerUpdate(Box::new(update.clone())),
            )?;
        }

        // Consider both Running and Pending function executors from the executor as
        // valid

        let mut active_function_executors = HashMap::new();
        let mut stale_function_executors = HashMap::new();
        for (fe_id, fe) in executor.function_executors.iter() {
            let state = fe.status.as_state();
            if state == FunctionExecutorState::Running || state == FunctionExecutorState::Pending {
                active_function_executors.insert(fe_id.clone(), fe.clone());
            } else {
                stale_function_executors.insert(fe_id.clone(), fe.clone());
            }
        }

        // Step 2: Update existing FEs and add new ones
        for (fe_id, fe) in active_function_executors.into_iter() {
            // Check if this FE already exists in our indexes
            if let Some(indexed_fe) = function_executors_in_indexes.get(&fe_id) {
                // FE exists in our indexes - check if we need to update its state
                let executor_state = fe.status.as_state();

                if indexed_fe.desired_state != executor_state {
                    // Update state to match executor's state
                    debug!(
                        "Updating function executor {} state from {:?} to {:?}",
                        fe_id.get(),
                        indexed_fe.desired_state,
                        executor_state
                    );

                    // Create updated metadata
                    let updated_fe_metadata = FunctionExecutorServerMetadata::new(
                        executor.id.clone(),
                        fe.clone(),
                        executor_state,
                    );

                    // Add to update
                    update
                        .new_function_executors
                        .push(updated_fe_metadata.clone());
                }
            } else {
                // This FE exists in the executor but not in our indexes - add it
                debug!(
                    "Adding existing function executor {} from executor {} to indexes",
                    fe_id.get(),
                    executor.id.get()
                );

                // Create a new FunctionExecutorMetadata
                let fe_metadata = FunctionExecutorServerMetadata::new(
                    executor.id.clone(),
                    fe.clone(),
                    fe.status.as_state(),
                );

                // Add to update
                update.new_function_executors.push(fe_metadata.clone());
                let node_resources = self
                    .in_memory_state
                    .read()
                    .unwrap()
                    .get_fe_resources(&fe_metadata.function_executor);
                if let Some(node_resources) = node_resources {
                    let mut executor = executor.clone();
                    executor.host_resources.consume(&node_resources)?;
                    update
                        .updated_executors
                        .insert(executor.id.clone(), executor);
                }
            }
        }

        update.extend(self.remove_function_executors(
            &executor.id,
            &stale_function_executors.values().cloned().collect_vec(),
        )?);

        self.in_memory_state.write().unwrap().update_state(
            self.clock,
            &RequestPayload::SchedulerUpdate(Box::new(update.clone())),
        )?;

        Ok(update)
    }

    #[tracing::instrument(skip(self, executor_id))]
    fn remove_function_executors(
        &self,
        executor_id: &ExecutorId,
        function_executors_to_remove: &Vec<FunctionExecutor>,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        if function_executors_to_remove.is_empty() {
            return Ok(update);
        }

        debug!(
            "Removing {} function executors from executor {}",
            function_executors_to_remove.len(),
            executor_id.get()
        );

        // Handle allocations for FEs to be removed and update tasks
        let allocations_to_remove: Vec<Allocation> = if let Some(allocations_by_fe) = self
            .in_memory_state
            .read()
            .unwrap()
            .allocations_by_executor
            .get(executor_id)
        {
            function_executors_to_remove
                .iter()
                .filter_map(|fe| allocations_by_fe.get(&fe.id))
                .flat_map(|allocations| allocations.iter().map(|alloc| *alloc.clone()))
                .collect()
        } else {
            vec![]
        };

        debug!(
            "Found {} allocations to remove for function executors being removed",
            allocations_to_remove.len()
        );

        // Process each function executor being removed
        for fe in function_executors_to_remove {
            // Get the function executor metadata to check its status
            let fe_status = self
                .in_memory_state
                .read()
                .unwrap()
                .executors
                .get(executor_id)
                .map(|em| em.function_executors.clone())
                .map(|fe_map| fe_map.get(&fe.id).cloned())
                .flatten()
                .map(|fe| fe.status.clone())
                .unwrap_or(FunctionExecutorStatus::Unknown);

            let is_startup_failure = match fe_status {
                FunctionExecutorStatus::StartupFailedCustomerError |
                FunctionExecutorStatus::StartupFailedPlatformError => true,
                _ => false,
            };

            if is_startup_failure {
                let mut invocations = self
                    .in_memory_state
                    .read()
                    .unwrap()
                    .get_invocations_by_compute_graph_version(
                        &fe.namespace,
                        &fe.compute_graph_name,
                        &fe.version,
                    )
                    .clone();
                for invocation in invocations.iter_mut() {
                    invocation.completed = true;
                    invocation.outcome = GraphInvocationOutcome::Failure;

                    self.in_memory_state
                        .write()
                        .unwrap()
                        .invocation_ctx
                        .insert(invocation.key(), invocation.clone());
                    update.updated_invocations_states.push(*invocation.clone());
                    let tasks = self
                        .in_memory_state
                        .read()
                        .unwrap()
                        .get_tasks_by_invocation(
                            &invocation.namespace,
                            &invocation.compute_graph_name,
                            &invocation.invocation_id,
                        )
                        .clone();

                    for task in tasks.iter() {
                        let mut task = *task.clone();
                        task.status = TaskStatus::Completed;
                        task.outcome = TaskOutcome::Failure;

                        debug!(
                            "Marking task {} as failed due to function executor failing to start: {:?}",
                            task.id, fe_status,
                        );

                        // Update task in memory
                        self.in_memory_state
                            .write()
                            .unwrap()
                            .tasks
                            .insert(task.key(), Box::new(task.clone()));

                        // Add to update
                        update.updated_tasks.insert(task.id.clone(), task);
                    }
                }
            } else {
                // Process allocations for this specific function executor
                let allocations_by_fe = self
                    .in_memory_state
                    .read()
                    .unwrap()
                    .allocations_by_executor
                    .get(executor_id)
                    .cloned();
                if let Some(allocations_by_fe) = allocations_by_fe {
                    if let Some(allocations) = allocations_by_fe.get(&fe.id) {
                        for allocation in allocations {
                            let task = self
                                .in_memory_state
                                .read()
                                .unwrap()
                                .tasks
                                .get(&allocation.task_key())
                                .cloned();

                            if let Some(task) = task {
                                let mut task = *task.clone();
                                task.status = TaskStatus::Pending;

                                debug!(
                                    "Marking task {} as pending due to function executor removal: {:?}",
                                    task.id, fe_status,
                                );

                                // Update task in memory
                                self.in_memory_state
                                    .write()
                                    .unwrap()
                                    .tasks
                                    .insert(task.key(), Box::new(task.clone()));

                                // Add to update
                                update.updated_tasks.insert(task.id.clone(), task);
                            } else {
                                error!(
                                    "Task of allocation not found in indexes: {}",
                                    allocation.task_key(),
                                );
                            }
                        }
                    }
                }
            }
        }

        // Add allocations to remove list
        update.remove_allocations = allocations_to_remove;

        // Add function executors to remove list
        update.remove_function_executors = function_executors_to_remove
            .iter()
            .map(|fe| FunctionExecutorIdWithExecutionId::new(fe.id.clone(), executor_id.clone()))
            .collect();

        for fe in function_executors_to_remove {
            let executor = self
                .in_memory_state
                .read()
                .unwrap()
                .executors
                .get(executor_id)
                .cloned();
            if let Some(mut executor) = executor {
                let fe_resources = self.in_memory_state.read().unwrap().get_fe_resources(&fe);
                if let Some(fe_resources) = fe_resources {
                    if let Err(err) = executor.host_resources.free(&fe_resources) {
                        error!(
                            "failed to free resources for function executor {} in executor {}: {}",
                            fe.id.get(),
                            executor_id.get(),
                            err
                        );
                    }
                    update
                        .updated_executors
                        .insert(executor_id.clone(), *executor.clone());
                    self.in_memory_state.write().unwrap().update_state(
                        self.clock,
                        &RequestPayload::SchedulerUpdate(Box::new(update.clone())),
                    )?;
                }
            }
        }

        // Remove the function executors from the indexes
        self.in_memory_state
            .write()
            .unwrap()
            .function_executors_by_executor
            .entry(executor_id.clone())
            .and_modify(|fe_mapping| {
                fe_mapping.retain(|fe_id, _fe| {
                    !function_executors_to_remove
                        .iter()
                        .any(|fe_remove| fe_remove.id == *fe_id)
                });
            });

        // Now that we've removed the function executors and updated tasks,
        // we need to immediately attempt to allocate only tasks that were marked as
        // pending
        if !update.updated_tasks.is_empty() {
            let tasks_to_allocate = update
                .updated_tasks
                .iter()
                .filter(|(_, t)| t.status == TaskStatus::Pending)
                .map(|(_, t)| Box::new(t.clone()))
                .collect_vec();

            if !tasks_to_allocate.is_empty() {
                debug!(
                "Attempting to reallocate {} tasks that were unallocated due to function executor removal",
                tasks_to_allocate.len()
            );

                let allocation_update = self.allocate_tasks(tasks_to_allocate)?;
                update.extend(allocation_update);
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

        // Get all function executors to remove
        let function_executors_to_remove = self
            .in_memory_state
            .read()
            .unwrap()
            .function_executors_by_executor
            .get(executor_id)
            .map(|fes| {
                fes.values()
                    .map(|fe| fe.function_executor.clone())
                    .collect::<Vec<_>>()
            });

        if let Some(function_executors) = function_executors_to_remove {
            update.extend(self.remove_function_executors(executor_id, &function_executors)?);
        }

        return Ok(update);
    }
}
