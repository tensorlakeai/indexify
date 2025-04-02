use std::{collections::HashMap, vec};

use anyhow::{anyhow, Result};
use data_model::{
    Allocation,
    AllocationBuilder,
    ChangeType,
    ExecutorId,
    FunctionExecutor,
    FunctionExecutorId,
    FunctionExecutorStatus,
    Task,
    TaskId,
    TaskStatus,
};
use itertools::Itertools;
use rand::seq::IndexedRandom;
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
    #[tracing::instrument(skip(self, change, indexes))]
    pub fn invoke(
        &self,
        change: &ChangeType,
        indexes: &mut Box<InMemoryState>,
    ) -> Result<SchedulerUpdateRequest> {
        match change {
            ChangeType::ExecutorUpserted(ev) => {
                let task_allocation_results =
                    self.reconcile_executor_state(&ev.executor_id, indexes)?;
                return Ok(SchedulerUpdateRequest {
                    new_allocations: task_allocation_results.new_allocations,
                    updated_tasks: task_allocation_results.updated_tasks,
                    ..Default::default()
                });
            }
            ChangeType::ExecutorRemoved(_) => {
                let task_allocation_results = self.allocate(indexes)?;
                return Ok(SchedulerUpdateRequest {
                    new_allocations: task_allocation_results.new_allocations,
                    updated_tasks: task_allocation_results.updated_tasks,
                    ..Default::default()
                });
            }
            ChangeType::TombStoneExecutor(ev) => self.deregister_executor(&ev.executor_id, indexes),
            _ => {
                error!("unhandled change type: {:?}", change);
                return Err(anyhow!("unhandled change type"));
            }
        }
    }

    #[tracing::instrument(skip(self, executor_id, indexes))]
    pub fn reconcile_executor_state(
        &self,
        executor_id: &ExecutorId,
        indexes: &mut Box<InMemoryState>,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest {
            ..Default::default()
        };

        let executor = indexes
            .executors
            .get(&executor_id)
            .ok_or(anyhow!("executor not found"))?;

        if executor.development_mode {
            // no need to reconcile, all function executors are allowed
            return Ok(update);
        }

        // Reconcile the function executors with the allowlist.
        if let Some(functions) = &executor.function_allowlist {
            let function_executor_ids_without_allowlist = executor
                .function_executors
                .iter()
                .filter_map(|(_id, fe)| {
                    if !functions.iter().any(|f| fe.matches(f)) {
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
                    executor_id.get(),
                    function_executor_ids_without_allowlist.len()
                );
            }

            update.extend(self.remove_function_executors(
                executor_id,
                &function_executor_ids_without_allowlist,
                indexes,
            )?);
        }

        // TODO: for get_desired_state, manage the lifecycle of function executors so
        // that       1- we remove any function executors that are unhealthy or
        // non-existent       2- we reallocate any tasks on a function executor
        // being removed

        return Ok(update);
    }

    #[tracing::instrument(skip(self, executor_id, indexes))]
    pub fn remove_function_executors(
        &self,
        executor_id: &ExecutorId,
        function_executor_ids_to_remove: &[FunctionExecutorId],
        indexes: &mut Box<InMemoryState>,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest {
            remove_executors: vec![executor_id.clone()],
            ..Default::default()
        };

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

        //remove_function_executors.iter().any(|fe| &fe.id == fe_id)

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

            // Remove the allocations from the store.
            update.remove_allocations = allocations_to_remove.clone();
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

    #[tracing::instrument(skip(self, executor_id, indexes))]
    pub fn deregister_executor(
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
    pub fn allocate(&self, indexes: &mut Box<InMemoryState>) -> Result<TaskPlacementResult> {
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
        if tasks.is_empty() {
            return Ok(TaskPlacementResult {
                new_allocations: vec![],
                updated_tasks: HashMap::new(),
            });
        }
        self.allocate_tasks(tasks, indexes)
    }

    // TODO: For dev executors, we need to ensure that the function executor is
    // passed in the SchedulerUpdateRequest
    fn get_valid_function_executors(
        &self,
        task: &Task,
        indexes: &mut InMemoryState,
    ) -> Vec<Box<FunctionExecutor>> {
        let fn_uri = task.function_uri();
        let mut valid_function_executors = Vec::new();

        for (executor_id, executor) in indexes.executors.iter() {
            if executor.tombstoned {
                continue;
            }

            // Get or create the function executors map for this executor
            let function_executors = indexes
                .function_executors_by_executor
                .entry(executor_id.clone())
                .or_insert_with(im::HashMap::new);

            // For dev executors, we need to ensure that the function executor is created in
            // the indexes
            if executor.development_mode {
                let matching_executor_exists =
                    function_executors.values().any(|fe| fe.matches(&fn_uri));

                // Create a new function executor if no matching one exists
                if !matching_executor_exists {
                    // Create a new FunctionExecutor with a new ID
                    let function_executor_id = FunctionExecutorId::default();
                    let function_executor = Box::new(FunctionExecutor {
                        id: function_executor_id.clone(),
                        executor_id: executor_id.clone(),
                        namespace: fn_uri.namespace.clone(),
                        compute_graph_name: fn_uri.compute_graph_name.clone(),
                        compute_fn_name: fn_uri.compute_fn_name.clone(),
                        version: fn_uri.version.clone().unwrap_or_default(),
                        status: FunctionExecutorStatus::Idle,
                    });

                    // Insert the new function executor
                    function_executors.insert(function_executor_id.clone(), function_executor);

                    // Ensure there's an empty allocations vector for this function executor
                    let allocations = indexes
                        .allocations_by_executor
                        .entry(executor_id.clone())
                        .or_insert_with(im::HashMap::new);

                    allocations.insert(function_executor_id, im::Vector::new());
                }
            }

            // Find all matching function executors with capacity
            for (fe_id, function_executor) in function_executors.iter() {
                if function_executor.matches(&fn_uri) {
                    // Get the current allocation count
                    let allocation_count = indexes
                        .allocations_by_executor
                        .get(executor_id)
                        .and_then(|allocations| allocations.get(fe_id))
                        .map_or(0, |v| v.len());

                    // Add to valid executors if below allocation limit
                    if allocation_count < MAX_ALLOCATIONS_PER_FN_EXECUTOR {
                        valid_function_executors.push(function_executor.clone());
                    }
                }
            }
        }

        valid_function_executors
    }

    #[tracing::instrument(skip(self, tasks, indexes))]
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
            let function_executors = self.get_valid_function_executors(&task, indexes);

            // terminate allocating early if no function executors available
            if function_executors.is_empty() {
                debug!("no function executors with capacity available for task");
                break;
            }

            match self.allocate_task(&task, &function_executors, indexes) {
                Ok(Some(allocation)) => {
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
                    allocations.push(allocation.clone());
                    task.status = TaskStatus::Running;
                    indexes
                        .allocations_by_executor
                        .entry(allocation.executor_id.clone())
                        .or_default()
                        .entry(allocation.function_executor_id.clone())
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
        Ok(TaskPlacementResult {
            new_allocations: allocations,
            updated_tasks,
        })
    }

    #[tracing::instrument(skip(self, task, _indexes, function_executors))]
    fn allocate_task(
        &self,
        task: &Task,
        function_executors: &Vec<Box<FunctionExecutor>>,
        _indexes: &Box<InMemoryState>,
    ) -> Result<Option<Allocation>> {
        let function_executor = function_executors.choose(&mut rand::thread_rng());

        if let Some(function_executor) = function_executor {
            info!(
                "assigning task {:?} to executor {}/{:?}",
                task.id, function_executor.executor_id, function_executor.id
            );
            let allocation = AllocationBuilder::default()
                .namespace(task.namespace.clone())
                .compute_graph(task.compute_graph_name.clone())
                .compute_fn(task.compute_fn_name.clone())
                .invocation_id(task.invocation_id.clone())
                .task_id(task.id.clone())
                .executor_id(function_executor.executor_id.clone())
                .function_executor_id(function_executor.id.clone())
                .build()?;
            return Ok(Some(allocation));
        }
        Ok(None)
    }
}
