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
use state_store::{
    in_memory_state::InMemoryState,
    requests::{FunctionExecutorIdWithExecutionId, SchedulerUpdateRequest},
};
use tracing::{error, info, trace};

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
    in_memory_state: Box<InMemoryState>,
}

impl TaskAllocator {
    pub fn new(in_memory_state: Box<InMemoryState>) -> Self {
        Self { in_memory_state }
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

        let executor = self
            .in_memory_state
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
        update.extend(self.reconcile_allowlist(&executor)?);

        // Reconcile function executors
        update.extend(self.reconcile_function_executors(&executor)?);

        return Ok(update);
    }

    #[tracing::instrument(skip(self, executor))]
    fn reconcile_allowlist(
        &mut self,
        executor: &ExecutorMetadata,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        if executor.development_mode {
            return Ok(update);
        }

        // Reconcile the function executors with the allowlist.

        let remove_all = executor
            .function_allowlist
            .as_ref()
            .map_or(true, |functions| functions.is_empty());

        if remove_all {
            // Remove all if the allowlist is empty or not present.
            let all_function_executor_ids: Vec<_> = executor
                .function_executors
                .iter()
                .map(|(_id, fe)| fe.id.clone())
                .collect();

            if !all_function_executor_ids.is_empty() {
                info!(
                    "executor {} has {} allowlist, removing all {} function executors",
                    executor.id.get(),
                    if executor.function_allowlist.is_some() {
                        "empty"
                    } else {
                        "no"
                    },
                    all_function_executor_ids.len()
                );
            }

            update
                .extend(self.remove_function_executors(&executor.id, &all_function_executor_ids)?);
        } else {
            // Has non-empty allowlist - remove only non-allowlisted executors
            let function_executor_ids_without_allowlist = executor
                .function_executors
                .iter()
                .filter_map(|(_id, fe)| {
                    if !executor
                        .function_allowlist
                        .as_ref()
                        .unwrap()
                        .iter()
                        .any(|f| f.matches_function_executor(fe))
                    {
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
            )?);
        }

        Ok(update)
    }

    #[tracing::instrument(skip(self, executor))]
    fn reconcile_function_executors(
        &mut self,
        executor: &ExecutorMetadata,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // TODO: handle function executor statuses

        // Get the function executors from the indexes
        let function_executors_in_indexes = self
            .in_memory_state
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

            update.extend(
                self.remove_function_executors(&executor.id, &function_executor_ids_to_remove)?,
            );
        }

        Ok(update)
    }

    #[tracing::instrument(skip(self, executor_id))]
    fn remove_function_executors(
        &mut self,
        executor_id: &ExecutorId,
        function_executor_ids_to_remove: &[FunctionExecutorId],
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // Remove the function executors from the indexes
        self.in_memory_state
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
        let allocations_to_remove = if let Some(allocations_by_fe) = self
            .in_memory_state
            .allocations_by_executor
            .get(executor_id)
        {
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
            let task = self.in_memory_state.tasks.get(&allocation.task_key());
            if let Some(task) = task.cloned() {
                let mut task = *task;
                task.status = TaskStatus::Pending;
                self.in_memory_state
                    .tasks
                    .insert(task.key(), Box::new(task.clone()));
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
            )?;

            update.extend(allocation_update);
        }

        return Ok(update);
    }

    #[tracing::instrument(skip(self, executor_id))]
    fn deregister_executor(&mut self, executor_id: &ExecutorId) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest {
            remove_executors: vec![executor_id.clone()],
            ..Default::default()
        };

        // Get all function executor ids to remove
        let function_executor_ids_to_remove = self
            .in_memory_state
            .allocations_by_executor
            .get(executor_id)
            .map(|a| a.keys().cloned().collect_vec());

        if let Some(function_executor_ids) = function_executor_ids_to_remove {
            update.extend(self.remove_function_executors(executor_id, &function_executor_ids)?);
        }

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
        for task in tasks {
            let function_executors = self.in_memory_state.candidate_function_executors(&task);
            if function_executors.is_empty() {
                self.in_memory_state.vacuum_function_executors();
                let candidate_nodes = self.in_memory_state.candidate_executors(&task)?;
                if candidate_nodes.is_empty() {
                    continue;
                }
                let candidate_node = candidate_nodes.first().unwrap();
                let function_executor = self.create_function_executor(candidate_node, &task)?;
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

    pub fn create_function_executor(
        &mut self,
        executor: &Box<ExecutorMetadata>,
        task: &Task,
    ) -> Result<FunctionExecutor> {
        let function_executor_id = FunctionExecutorId::default();
        let function_executor = FunctionExecutor {
            id: function_executor_id.clone(),
            executor_id: executor.id.clone(),
            namespace: task.namespace.clone(),
            compute_graph_name: task.compute_graph_name.clone(),
            compute_fn_name: task.compute_fn_name.clone(),
            version: task.graph_version.clone(),
            status: FunctionExecutorStatus::Idle,
        };
        Ok(function_executor)
    }

    pub fn assign_tasks(
        &mut self,
        task: &Box<Task>,
        function_executor: &FunctionExecutor,
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
