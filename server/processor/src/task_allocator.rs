use std::vec;

use anyhow::{anyhow, Result};
use data_model::{
    Allocation,
    AllocationBuilder,
    ChangeType,
    ExecutorId,
    ExecutorMetadata,
    FunctionExecutor,
    FunctionExecutorId,
    FunctionExecutorStatus,
    FunctionURI,
    Task,
    TaskStatus,
};
use itertools::Itertools;
use rand::seq::SliceRandom;
use state_store::{
    in_memory_state::{InMemoryState, UnallocatedTaskId},
    requests::{FunctionExecutorIdWithExecutionId, SchedulerUpdateRequest},
};
use tracing::{debug, error, info, span, trace};

pub struct FilteredExecutors {
    pub executors: Vec<ExecutorId>,
}

// Maximum number of allocations per executor.
//
// In the future, this should be a dynamic value based on:
// - function concurrency configuration
// - function batching configuration
// - function timeout configuration
const MAX_ALLOCATIONS_PER_FN_EXECUTOR: usize = 20;

#[derive(Debug, Clone)]
// Define a struct to represent a candidate executor for allocation
pub struct ExecutorCandidate {
    pub executor_id: ExecutorId,
    pub function_executor_id: Option<FunctionExecutorId>, // None if needs to be created
    pub function_uri: FunctionURI,
    pub allocation_count: usize, // Number of allocations for this function executor
}

pub struct TaskAllocationProcessor {}

impl TaskAllocationProcessor {
    pub fn new() -> Self {
        Self {}
    }
}
impl TaskAllocationProcessor {
    #[tracing::instrument(skip(self, change, indexes))]
    pub fn invoke(
        &self,
        change: &ChangeType,
        indexes: &mut Box<InMemoryState>,
    ) -> Result<SchedulerUpdateRequest> {
        match change {
            ChangeType::ExecutorUpserted(ev) => {
                let mut update = self.reconcile_executor_state(&ev.executor_id, indexes)?;
                update.extend(self.allocate(indexes)?);
                return Ok(update);
            }
            ChangeType::ExecutorRemoved(_) => {
                let update = self.allocate(indexes)?;
                return Ok(update);
            }
            ChangeType::TombStoneExecutor(ev) => self.deregister_executor(&ev.executor_id, indexes),
            _ => {
                error!("unhandled change type: {:?}", change);
                return Err(anyhow!("unhandled change type"));
            }
        }
    }

    // Updated allocate_tasks to use the new approach
    #[tracing::instrument(skip(self, tasks, indexes))]
    pub fn allocate_tasks(
        &self,
        tasks: Vec<Box<Task>>,
        indexes: &mut Box<InMemoryState>,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

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

            match self.allocate_task(&task, indexes) {
                Ok(Some((allocation, function_executor))) => {
                    info!(
                        task_id = &task.id.to_string(),
                        namespace = &task.namespace,
                        compute_graph = &task.compute_graph_name,
                        compute_fn = &task.compute_fn_name,
                        invocation_id = &task.invocation_id,
                        executor_id = &allocation.executor_id.get(),
                        function_executor_id = &allocation.function_executor_id.get(),
                        "allocated task"
                    );

                    // Record the allocation
                    {
                        update.new_allocations.push(allocation.clone());

                        indexes
                            .allocations_by_executor
                            .entry(allocation.executor_id.clone())
                            .or_default()
                            .entry(allocation.function_executor_id.clone())
                            .or_default()
                            .push_back(Box::new(allocation.clone()));
                    }

                    // Record new function executor
                    if let Some(function_executor) = function_executor {
                        update
                            .new_function_executors
                            .push(function_executor.clone());

                        indexes
                            .function_executors_by_executor
                            .entry(allocation.executor_id.clone())
                            .or_default()
                            .entry(allocation.function_executor_id.clone())
                            .or_insert_with(|| Box::new(function_executor));
                    }
                    // Record task status update
                    {
                        task.status = TaskStatus::Running;
                        update.updated_tasks.insert(task.id.clone(), *task.clone());

                        indexes.tasks.insert(task.key(), task.clone());
                        indexes
                            .unallocated_tasks
                            .remove(&UnallocatedTaskId::new(&task));
                    }
                }
                Ok(None) => {
                    debug!(
                        task_id = task.id.to_string(),
                        namespace = task.namespace,
                        compute_graph = task.compute_graph_name,
                        compute_fn = task.compute_fn_name,
                        invocation_id = task.invocation_id.to_string(),
                        "no executors available for task"
                    );
                }
                Err(err) => {
                    error!(
                        task_id = task.id.to_string(),
                        namespace = task.namespace,
                        compute_graph = task.compute_graph_name,
                        compute_fn = task.compute_fn_name,
                        compute_graph_version = task.graph_version.0,
                        invocation_id = task.invocation_id.to_string(),
                        "failed to allocate task, skipping: {:?}",
                        err
                    );
                }
            }
        }

        Ok(update)
    }

    #[tracing::instrument(skip(self, executor_id, indexes))]
    pub fn reconcile_executor_state(
        &self,
        executor_id: &ExecutorId,
        indexes: &mut Box<InMemoryState>,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        let executor = indexes
            .executors
            .get(&executor_id)
            .ok_or(anyhow!("executor not found"))?
            .clone();

        trace!(
            "reconciling executor state for executor {} - {:#?}",
            executor_id.get(),
            executor
        );

        // Reconcile the function executors with the allowlist.
        update.extend(self.reconcile_allowlist(&executor, indexes)?);

        // Reconcile function executors
        update.extend(self.reconcile_function_executors(&executor, indexes)?);

        return Ok(update);
    }

    #[tracing::instrument(skip(self, executor, indexes))]
    fn reconcile_allowlist(
        &self,
        executor: &ExecutorMetadata,
        indexes: &mut Box<InMemoryState>,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        if executor.development_mode {
            return Ok(update);
        }
        // Reconcile the function executors with the allowlist.
        if let Some(functions) = &executor.function_allowlist {
            let function_executor_ids_without_allowlist = executor
                .function_executors
                .iter()
                .filter_map(|(_id, fe)| {
                    if !functions.iter().any(|f| fe.matches_fn_uri(f)) {
                        // this function executor is not allowlisted
                        Some(fe.id.clone())
                    } else {
                        None
                    }
                })
                .collect_vec();

            if !function_executor_ids_without_allowlist.is_empty() {
                info!(
                    "executor {} has function executors not allowlisted: {}",
                    executor.id.get(),
                    function_executor_ids_without_allowlist.len()
                );
            }

            update.extend(self.remove_function_executors(
                &executor.id,
                &function_executor_ids_without_allowlist,
                indexes,
            )?);
        }

        Ok(update)
    }

    #[tracing::instrument(skip(self, executor, indexes))]
    fn reconcile_function_executors(
        &self,
        executor: &ExecutorMetadata,
        indexes: &mut Box<InMemoryState>,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // TODO: handle function executor statuses

        // Get the function executors from the indexes
        let function_executors_in_indexes = indexes
            .function_executors_by_executor
            .get(&executor.id)
            .cloned()
            .unwrap_or_default();

        // Find function executor IDs in the indexes that don't match any in the
        // executor
        let function_executor_ids_to_remove = function_executors_in_indexes
            .iter()
            .filter_map(|(indexed_fe_id, indexed_fe)| {
                // Check if there's a direct ID match in the executor's function executors
                let id_match_exists = executor.function_executors.contains_key(indexed_fe_id);

                if id_match_exists {
                    // Direct ID match found, keep it
                    None
                } else {
                    // Temporary handle not versioned function executors
                    // Not versioned function executors are those that start with "not_versioned/".
                    // Which are the ones created by the host executor using the task stream as
                    // opposed to the get_desired_state stream.
                    let not_versioned_match_exists =
                        executor.function_executors.iter().any(|(fe_id, fe)| {
                            if fe_id.get().starts_with("not_versioned/") {
                                // Compare function URIs
                                indexed_fe.matches(&fe)
                            } else {
                                false
                            }
                        });

                    if not_versioned_match_exists {
                        // Match found with a not_versioned function executor, keep it
                        None
                    } else {
                        // No match found, should remove
                        Some(indexed_fe_id.clone())
                    }
                }
            })
            .collect_vec();

        if !function_executor_ids_to_remove.is_empty() {
            trace!(
                "executor {} has function executors ({}) in indexes to be removed",
                executor.id.get(),
                function_executor_ids_to_remove.len()
            );

            update.extend(self.remove_function_executors(
                &executor.id,
                &function_executor_ids_to_remove,
                indexes,
            )?);
        }

        Ok(update)
    }

    #[tracing::instrument(skip(self, executor_id, indexes))]
    fn remove_function_executors(
        &self,
        executor_id: &ExecutorId,
        function_executor_ids_to_remove: &[FunctionExecutorId],
        indexes: &mut Box<InMemoryState>,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // Remove the function executors from the indexes
        indexes
            .function_executors_by_executor
            .entry(executor_id.clone())
            .and_modify(|fe_mapping| {
                fe_mapping.retain(|fe_id, _fe| {
                    !function_executor_ids_to_remove
                        .iter()
                        .any(|fe_id_remove| fe_id_remove == fe_id)
                });
            });

        // Get the inner map for the executor_id
        let allocations_to_remove =
            if let Some(allocations_by_fe) = indexes.allocations_by_executor.get(executor_id) {
                function_executor_ids_to_remove
                    .iter()
                    .filter_map(|fe_id| allocations_by_fe.get(&fe_id))
                    .flat_map(|allocations| allocations.iter().map(|alloc| *alloc.clone()))
                    .collect_vec()
            } else {
                vec![]
            };

        // Mark all tasks being unallocated as pending.
        for allocation in allocations_to_remove.clone() {
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

        // Remove the allocations from the store.
        update.remove_allocations = allocations_to_remove.clone();
        update.remove_function_executors = function_executor_ids_to_remove
            .iter()
            .map(|fe_id| FunctionExecutorIdWithExecutionId::new(fe_id.clone(), executor_id.clone()))
            .collect_vec();

        // Immediately attempt to reallocate tasks that were unallocated due to function
        // executor removal.
        {
            let allocation_update = self.allocate_tasks(
                update
                    .updated_tasks
                    .iter()
                    .map(|(_, t)| Box::new(t.clone()))
                    .collect(),
                indexes,
            )?;

            update.extend(allocation_update);
        }

        return Ok(update);
    }

    #[tracing::instrument(skip(self, executor_id, indexes))]
    fn deregister_executor(
        &self,
        executor_id: &ExecutorId,
        indexes: &mut Box<InMemoryState>,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest {
            remove_executors: vec![executor_id.clone()],
            ..Default::default()
        };

        // Get all function executor ids to remove
        let function_executor_ids_to_remove = indexes
            .allocations_by_executor
            .get(executor_id)
            .map(|a| a.keys().cloned().collect_vec());

        if let Some(function_executor_ids) = function_executor_ids_to_remove {
            update.extend(self.remove_function_executors(
                executor_id,
                &function_executor_ids,
                indexes,
            )?);
        }

        return Ok(update);
    }

    #[tracing::instrument(skip(self, indexes))]
    pub fn allocate(&self, indexes: &mut Box<InMemoryState>) -> Result<SchedulerUpdateRequest> {
        let unallocated_task_ids = indexes.unallocated_tasks.clone();
        let mut tasks = Vec::new();
        for unallocated_task_id in &unallocated_task_ids {
            if let Some(task) = indexes.tasks.get(&unallocated_task_id.task_key) {
                tasks.push(task.clone());
            } else {
                error!(
                    task_key = unallocated_task_id.task_key,
                    "task not found in indexes for unallocated task"
                );
            }
        }
        self.allocate_tasks(tasks, indexes)
    }

    // Get available executors considering dev mode and allowlists
    #[tracing::instrument(skip(self, task, indexes))]
    #[tracing::instrument(skip(self, task, indexes))]
    fn get_executor_candidates(
        &self,
        task: &Task,
        indexes: &InMemoryState,
    ) -> Vec<ExecutorCandidate> {
        let fn_uri = task.function_uri();
        let mut candidates = Vec::new();

        for (executor_id, executor) in indexes.executors.iter() {
            if executor.tombstoned {
                continue;
            }

            // Skip if this executor can't handle this task due to allowlist
            if !executor.development_mode &&
                executor
                    .function_allowlist
                    .as_ref()
                    .map_or(false, |allowlist| {
                        !allowlist.iter().any(|f| f.matches_task(task))
                    })
            {
                trace!(
                    "executor not allowlisted for function {} - {:#?}",
                    fn_uri,
                    executor,
                );
                continue;
            }

            // Check existing function executors for a match
            let matching_fe = indexes
                .function_executors_by_executor
                .get(executor_id)
                .and_then(|executors| {
                    executors
                        .iter()
                        .find(|(_, fe)| fe.matches_task(task))
                        .map(|(id, _)| id.clone())
                });

            // Get the allocation count specifically for the function executor we're
            // considering If no matching function executor exists, allocation
            // count is 0
            let allocation_count = matching_fe
                .as_ref()
                .and_then(|fe_id| {
                    indexes
                        .allocations_by_executor
                        .get(executor_id)
                        .and_then(|alloc_map| alloc_map.get(fe_id))
                        .map(|allocs| allocs.len())
                })
                .unwrap_or(0);

            // Check if the specific function executor is at capacity
            if allocation_count >= MAX_ALLOCATIONS_PER_FN_EXECUTOR {
                trace!(
                    "executor {} skipped due to function executor at capacity (allocations: {})",
                    executor.id.get(),
                    allocation_count
                );
                continue;
            }

            candidates.push(ExecutorCandidate {
                executor_id: executor_id.clone(),
                function_executor_id: matching_fe,
                function_uri: fn_uri.clone(),
                allocation_count,
            });
        }

        candidates
    }

    #[tracing::instrument(skip(self, candidates))]
    fn select_executor(&self, candidates: &[ExecutorCandidate]) -> Option<ExecutorCandidate> {
        if candidates.is_empty() {
            return None;
        }

        // Create an array of indices and shuffle them
        let mut indices: Vec<usize> = (0..candidates.len()).collect();
        indices.shuffle(&mut rand::thread_rng());

        // Use these indices to iterate through candidates in a random order
        // ensuring that we select the one with the least allocation count
        // without relying on the order of candidates.
        indices
            .into_iter()
            .map(|i| &candidates[i])
            .min_by_key(|candidate| candidate.allocation_count)
            .cloned()
    }

    // Ensure function executor exists (or create it)
    #[tracing::instrument(skip(self, candidate))]
    fn ensure_function_executor(
        &self,
        candidate: &ExecutorCandidate,
    ) -> Result<Option<FunctionExecutor>> {
        // If function executor already exists, return its ID
        if candidate.function_executor_id.is_some() {
            return Ok(None);
        }

        // Otherwise, we need to create a new one (for dev mode)
        let function_executor_id = FunctionExecutorId::default();

        let function_executor = FunctionExecutor {
            id: function_executor_id.clone(),
            executor_id: candidate.executor_id.clone(),
            namespace: candidate.function_uri.namespace.clone(),
            compute_graph_name: candidate.function_uri.compute_graph_name.clone(),
            compute_fn_name: candidate.function_uri.compute_fn_name.clone(),
            version: candidate.function_uri.version.clone().ok_or(anyhow!(
                "function uri version is required for function executor creation"
            ))?,
            status: FunctionExecutorStatus::Idle,
        };

        trace!(
            "creating new function executor: {:?} for task {:?}",
            function_executor_id,
            candidate.function_uri
        );

        return Ok(Some(function_executor));
    }

    // Refactored allocate_task method
    #[tracing::instrument(skip(self, task, indexes))]
    fn allocate_task(
        &self,
        task: &Task,
        indexes: &mut Box<InMemoryState>,
    ) -> Result<Option<(Allocation, Option<FunctionExecutor>)>> {
        // Step 1: Get candidates
        let candidates = self.get_executor_candidates(task, indexes);

        // Step 2: Select best candidate
        let candidate = match self.select_executor(&candidates) {
            Some(c) => c,
            None => {
                trace!("No suitable executor candidates available");
                return Ok(None);
            }
        };

        trace!(
            "available executor candidates: {:#?} - picked: {:#?}",
            candidates
                .iter()
                .map(|c| format!("{} - {}", c.executor_id.get(), c.allocation_count))
                .collect::<Vec<_>>(),
            candidate,
        );

        // Step 3: Ensure function executor exists
        let function_executor = self.ensure_function_executor(&candidate)?;

        let function_executor_id = match function_executor {
            Some(ref fe) => fe.id.clone(),
            None => match candidate.function_executor_id {
                Some(ref fe_id) => fe_id.clone(),
                None => {
                    return Err(anyhow!("No function executor ID available"));
                }
            },
        };

        // Step 4: Create allocation
        let allocation = AllocationBuilder::default()
            .namespace(task.namespace.clone())
            .compute_graph(task.compute_graph_name.clone())
            .compute_fn(task.compute_fn_name.clone())
            .invocation_id(task.invocation_id.clone())
            .task_id(task.id.clone())
            .executor_id(candidate.executor_id.clone())
            .function_executor_id(function_executor_id)
            .build()?;

        Ok(Some((allocation, function_executor)))
    }
}
