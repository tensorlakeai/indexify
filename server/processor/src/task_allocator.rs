use std::{
    time::{Duration, SystemTime},
    vec,
};

use anyhow::{anyhow, Result};
use data_model::{
    Allocation,
    AllocationBuilder,
    ChangeType,
    ComputeGraph,
    ExecutorId,
    ExecutorMetadata,
    FunctionExecutor,
    FunctionExecutorId,
    FunctionExecutorServerMetadata,
    FunctionExecutorState,
    FunctionExecutorStatus,
    GraphVersion,
    Task,
    TaskStatus,
};
use im::HashMap;
use itertools::Itertools;
use state_store::{
    in_memory_state::InMemoryState,
    requests::{
        FunctionExecutorIdWithExecutionId,
        SchedulerUpdateRequest,
        StateMachineUpdateRequest,
    },
};
use tracing::{debug, error, info, span, warn};

// Maximum number of allocations per executor.
//
// In the future, this should be a dynamic value based on:
// - function concurrency configuration
// - function batching configuration
// - function timeout configuration
const MAX_ALLOCATIONS_PER_FN_EXECUTOR: usize = 20;

// Maximum number of function executors allowed for each function on each
// executor.
//
// In the future, this should be a dynamic value based on:
// - function max concurrent function executors configuration
// - the QoS of the system / invocation
const MAX_FUNCTION_EXECUTORS_PER_FUNCTION_PER_EXECUTOR: usize = 1;

// Timeout for idle function executors (20 minutes)
// 20 minutes is used in order to support 15min synthetic tests
// keeping the function executors warm.
//
// In the future, this should be a dynamic value based on:
// - function idle timeout configuration
const IDLE_TIMEOUT_MS: u64 = 20 * 60 * 1000;

// Define the capacity threshold to trigger new FE creation (95%)
// This represents the percentage of capacity that triggers new FE creation.
//
// In the future, this should be a dynamic value based on:
// - Estimated pending tasks for this function based on pending invocations
// - Historical invocation creation trends
// - Task priority or QoS requirements
// - Startup time of new function executors
const CAPACITY_THRESHOLD: f64 = 0.95;

#[derive(Debug, Clone)]
struct ExecutorCandidate {
    executor_id: ExecutorId,
    function_executor_id: Option<FunctionExecutorId>, // None if needs to be created
    allocation_count: usize,                          /* Number of allocations for this function
                                                       * executor */
    is_dev_executor: bool, // Flag to indicate if this is a dev executor
    needs_creation: bool,  // Flag to indicate if this is a creation candidate
}

pub struct TaskAllocationProcessor {
    in_memory_state: Box<InMemoryState>,
}

impl TaskAllocationProcessor {
    pub fn new(in_memory_state: Box<InMemoryState>) -> Self {
        Self { in_memory_state }
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

        // Step 1: Run vacuum phase
        let vacuum_update = self.vacuum_phase()?;
        self.in_memory_state.update_state(
            self.in_memory_state.clock,
            &StateMachineUpdateRequest {
                payload: state_store::requests::RequestPayload::SchedulerUpdate(Box::new(
                    vacuum_update.clone(),
                )),
                processed_state_changes: vec![],
            },
        )?;
        update.extend(vacuum_update);

        // Step 2: Fetch unallocated tasks
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

        debug!("found {} unallocated tasks to process", tasks.len());

        // Step 3: Allocate tasks
        update.extend(self.allocate_tasks(tasks)?);

        Ok(update)
    }

    #[tracing::instrument(skip(self, tasks))]
    pub fn allocate_tasks(&mut self, tasks: Vec<Box<Task>>) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // Step 1: Process tasks
        for task in tasks {
            match self.process_task(&task) {
                Ok((Some(allocation), new_function_executors)) => {
                    let mut task_update = SchedulerUpdateRequest::default();
                    debug!(
                        task_id = task.id.to_string(),
                        "task {} allocated to function executor {}",
                        task.id,
                        allocation.function_executor_id.get(),
                    );

                    // We have a successful allocation

                    // 1. Add new function executors
                    for fe_meta in &new_function_executors {
                        // Add to update
                        task_update.new_function_executors.push(fe_meta.clone());
                    }

                    // 2. Add allocation
                    task_update.new_allocations.push(allocation.clone());

                    // 3. Create and update task with Running status
                    let mut updated_task = *task.clone();
                    updated_task.status = TaskStatus::Running;

                    task_update
                        .updated_tasks
                        .insert(updated_task.id.clone(), updated_task.clone());

                    self.in_memory_state.update_state(
                        self.in_memory_state.clock,
                        &StateMachineUpdateRequest {
                            payload: state_store::requests::RequestPayload::SchedulerUpdate(
                                Box::new(task_update.clone()),
                            ),
                            processed_state_changes: vec![],
                        },
                    )?;

                    update.extend(task_update);
                }
                Ok((None, new_function_executors)) => {
                    let mut task_update = SchedulerUpdateRequest::default();
                    if !new_function_executors.is_empty() {
                        debug!(
                            task_id = task.id.to_string(),
                            "task {} created {} function executors",
                            task.id,
                            new_function_executors.len(),
                        );

                        // Add new function executors to update and in-memory state
                        for fe_metadata in &new_function_executors {
                            // Add to update
                            task_update.new_function_executors.push(fe_metadata.clone());

                            self.in_memory_state.update_state(
                                self.in_memory_state.clock,
                                &StateMachineUpdateRequest {
                                    payload: state_store::requests::RequestPayload::SchedulerUpdate(
                                        Box::new(task_update.clone()),
                                    ),
                                    processed_state_changes: vec![],
                                },
                            )?;
                        }
                    } else {
                        debug!(
                            task_id = task.id.to_string(),
                            "task {} could not be allocated and could not create any function executors",
                            task.id,
                        );
                    }
                    update.extend(task_update);
                }
                Err(err) => {
                    error!("Error processing task {}: {:?}", task.id, err);
                }
            }
        }

        Ok(update)
    }

    // Process a single task - handling both allocation and FE creation
    // Process a single task - handling both allocation and FE creation
    #[tracing::instrument(skip(self, task))]
    fn process_task(
        &self,
        task: &Task,
    ) -> Result<(Option<Allocation>, Vec<FunctionExecutorServerMetadata>)> {
        let span = span!(
            tracing::Level::DEBUG,
            "process_task",
            task_id = task.id.to_string(),
            namespace = task.namespace,
            compute_graph = task.compute_graph_name,
            compute_fn = task.compute_fn_name,
            invocation_id = task.invocation_id
        );
        let _enter = span.enter();

        if task.outcome.is_terminal() {
            error!(
                namespace = task.namespace,
                compute_graph = task.compute_graph_name,
                compute_fn = task.compute_fn_name,
                invocation_id = task.invocation_id,
                "task already completed, skipping"
            );
            return Ok((None, Vec::new()));
        }

        debug!("attempting to allocate task {:?} ", task.id);

        // Function executor allocation phase - get unified sorted candidates
        let candidates = self.function_executor_allocation_phase(task)?;

        // Variables to track the results
        let mut allocation = None;
        let mut new_function_executors = Vec::new();

        if candidates.is_empty() {
            info!(
                namespace = task.namespace,
                compute_graph = task.compute_graph_name,
                compute_fn = task.compute_fn_name,
                invocation_id = task.invocation_id,
                task_id = task.id.to_string(),
                "no suitable candidates available for task"
            );
            return Ok((None, Vec::new()));
        }

        // Step 1: Create all FEs that need creation
        let creation_candidates: Vec<&ExecutorCandidate> =
            candidates.iter().filter(|c| c.needs_creation).collect();

        // Create all needed FEs
        let mut new_fe_candidates = Vec::new();
        for creation_candidate in creation_candidates {
            let fe = self.create_function_executor_metadata(task, &creation_candidate.executor_id);
            new_fe_candidates.push((creation_candidate.executor_id.clone(), fe.clone()));
            new_function_executors.push(fe);
        }

        // Step 2: Try to allocate to existing FE first (non-creation candidates)
        let existing_candidates: Vec<&ExecutorCandidate> =
            candidates.iter().filter(|c| !c.needs_creation).collect();

        if !existing_candidates.is_empty() {
            // Get the best existing candidate (should be the first one due to sorting)
            let best_candidate = existing_candidates[0];

            // Try to allocate to this existing FE
            match self.create_allocation(task, best_candidate) {
                Ok(alloc) => {
                    allocation = Some(alloc);

                    debug!(
                        "Allocated task {} to existing function executor {}",
                        task.id,
                        best_candidate.function_executor_id.as_ref().unwrap().get()
                    );
                }
                Err(err) => {
                    error!("Failed to create allocation to existing FE: {:?}", err);
                }
            }
        }

        // Step 3: If we couldn't allocate to an existing FE, try with newly created
        // ones
        if allocation.is_none() && !new_fe_candidates.is_empty() {
            for (executor_id, fe_metadata) in &new_fe_candidates {
                // Create a candidate with the new FE ID for allocation
                let new_fe_candidate = ExecutorCandidate {
                    executor_id: executor_id.clone(),
                    function_executor_id: Some(fe_metadata.function_executor.id.clone()),
                    allocation_count: 0,
                    is_dev_executor: false, // This value doesn't matter for allocation
                    needs_creation: false,  // We're treating it as already created now
                };

                // Try to allocate to this new FE
                match self.create_allocation(task, &new_fe_candidate) {
                    Ok(alloc) => {
                        allocation = Some(alloc);

                        debug!(
                            "Allocated task {} to newly created function executor {}",
                            task.id,
                            fe_metadata.function_executor.id.get()
                        );

                        // Successfully allocated to this FE, no need to try others
                        break;
                    }
                    Err(err) => {
                        // This is expected since the FE isn't actually running yet
                        debug!(
                            "Could not immediately allocate to new function executor {}: {:?}",
                            fe_metadata.function_executor.id.get(),
                            err
                        );
                        // Continue to try other newly created FEs
                    }
                }
            }
        }

        if allocation.is_none() && new_function_executors.is_empty() {
            info!(
                namespace = task.namespace,
                compute_graph = task.compute_graph_name,
                compute_fn = task.compute_fn_name,
                invocation_id = task.invocation_id,
                task_id = task.id.to_string(),
                "could not allocate task or create any function executors"
            );
        }

        Ok((allocation, new_function_executors))
    }

    // Vacuum phase - returns scheduler update for cleanup actions
    #[tracing::instrument(skip(self))]
    fn vacuum_phase(&mut self) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();
        let function_executors_to_mark = self.identify_executors_to_remove()?;
        debug!(
            "vacuum phase identified {} function executors to mark for termination",
            function_executors_to_mark.len()
        );

        // Mark FEs for termination (change desired state to Terminated)
        // but don't actually remove them - reconciliation will handle that
        for (executor_id, fe_id) in &function_executors_to_mark {
            // Get the existing FE metadata
            if let Some(fe_metadata) = self
                .in_memory_state
                .function_executors_by_executor
                .get(executor_id)
                .and_then(|fe_map| fe_map.get(fe_id))
            {
                // Only update if not already terminated
                if fe_metadata.desired_state != FunctionExecutorState::Terminated {
                    // Create updated metadata with Terminated state
                    let updated_fe_metadata = FunctionExecutorServerMetadata::new(
                        executor_id.clone(),
                        fe_metadata.function_executor.clone(),
                        FunctionExecutorState::Terminated,
                        fe_metadata.last_allocation_at,
                    );

                    // Add to update
                    update
                        .new_function_executors
                        .push(updated_fe_metadata.clone());

                    debug!(
                        "Marked function executor {} on executor {} for termination",
                        fe_id.get(),
                        executor_id.get()
                    );
                }
            }
        }

        Ok(update)
    }

    // Identify function executors that should be removed in vacuum phase
    #[tracing::instrument(skip(self))]
    fn identify_executors_to_remove(&self) -> Result<Vec<(ExecutorId, FunctionExecutorId)>> {
        let mut function_executors_to_remove = Vec::new();

        // For each executor in the system
        for (executor_id, executor) in &self.in_memory_state.executors {
            if executor.tombstoned {
                continue;
            }

            // Get function executors for this executor from our in-memory state
            let function_executors = self
                .in_memory_state
                .function_executors_by_executor
                .get(executor_id)
                .cloned()
                .unwrap_or_default();

            // Process each function executor based on allowlist and version status
            for (fe_id, fe_metadata) in function_executors.iter() {
                let fe = &fe_metadata.function_executor;

                // Check if this FE is in the executor's allowlist
                let allowlist_entry = executor
                    .function_allowlist
                    .as_ref()
                    .and_then(|allowlist| allowlist.iter().find(|f| fe.matches_fn_uri(f)));

                // IDLE TIMEOUT CHECK:
                // Check if function executor has been idle for more than the timeout period
                // (20min) regardless of dev mode or allowlist status
                if let Some(last_allocation_time) = fe_metadata.last_allocation_at {
                    let timeout = Duration::from_millis(IDLE_TIMEOUT_MS);

                    // TODO: Add a jitter to the timeout

                    if let Ok(idle_duration) =
                        SystemTime::now().duration_since(last_allocation_time)
                    {
                        if idle_duration > timeout {
                            debug!(
                            "Removing idle timed-out function executor {} from executor {}, idle for {:?}",
                            fe_id.get(), executor_id.get(), idle_duration
                        );
                            function_executors_to_remove.push((executor_id.clone(), fe_id.clone()));
                            continue; // Skip further checks since we're
                                      // removing this FE
                        }
                    }
                }

                // VERSION CHECK:
                // Check if the FE is using an outdated version
                if let Some(latest_version) = self.get_latest_function_version(fe) {
                    if fe.version != latest_version {
                        // Handle the case of an allowlist explicitly specifying the older version
                        if let Some(allowlist_entry) = allowlist_entry {
                            if let Some(allowlist_version) = &allowlist_entry.version {
                                if allowlist_version == &fe.version {
                                    // Allowlist explicitly specifies this older version - keep it
                                    // but warn
                                    warn!(
                                    "Function executor {} on executor {} is using outdated version {} (latest is {}), but is explicitly allowlisted with this version",
                                    fe_id.get(), executor_id.get(), fe.version, latest_version
                                );
                                    continue; // Skip further checks, we're
                                              // keeping this FE
                                }
                            }
                        }

                        // Check if this FE has any pending invocations with its current version
                        let has_pending_invocations = self.has_pending_invocations(&fe_metadata);

                        if !has_pending_invocations {
                            // Can remove this outdated FE since it has no active invocations,
                            // regardless of dev mode or allowlist status
                            debug!(
                            "Removing outdated function executor {} from executor {} (version {} < latest {})",
                            fe_id.get(), executor_id.get(), fe.version, latest_version
                        );
                            function_executors_to_remove.push((executor_id.clone(), fe_id.clone()));
                        }
                    }
                } else {
                    // No latest version found - this could be a new function or a missing compute
                    // graph
                    warn!(
                        "No latest version found for function executor {} on executor {} - all {:#?} - {}",
                        fe_id.get(),
                        executor_id.get(),
                        self.in_memory_state.compute_graphs, ComputeGraph::key_from(&fe.namespace, &fe.compute_fn_name),
                    );
                }
            }
        }

        Ok(function_executors_to_remove)
    }

    // Helper function to get the latest version of a function
    // This would need access to compute graph versions
    fn get_latest_function_version(&self, fe: &FunctionExecutor) -> Option<GraphVersion> {
        self.in_memory_state
            .compute_graphs
            .get(&ComputeGraph::key_from(
                &fe.namespace,
                &fe.compute_graph_name,
            ))
            .map(|cg| cg.version.clone())
    }

    // Helper function to check if an FE has any pending invocations with its
    // version
    fn has_pending_invocations(&self, fe_meta: &FunctionExecutorServerMetadata) -> bool {
        // Check if there are any allocations for this FE with pending tasks
        if let Some(allocations_by_fe) = self
            .in_memory_state
            .allocations_by_executor
            .get(&fe_meta.executor_id)
        {
            if let Some(allocations) = allocations_by_fe.get(&fe_meta.function_executor.id) {
                for allocation in allocations {
                    if let Some(task) = self.in_memory_state.tasks.get(&allocation.task_key()) {
                        if !task.outcome.is_terminal() {
                            return true;
                        }
                    }
                }
            }
        }

        false
    }

    // Function executor allocation phase - returns candidates for existing and
    // to-be-created FEs
    #[tracing::instrument(skip(self, task))]
    fn function_executor_allocation_phase(&self, task: &Task) -> Result<Vec<ExecutorCandidate>> {
        let mut candidates = Vec::new();

        // For each executor in the system
        for (executor_id, executor) in &self.in_memory_state.executors {
            if executor.tombstoned {
                continue;
            }

            // Check if this executor can handle this task according to allowlist
            let is_allowlisted = executor
                .function_allowlist
                .as_ref()
                .map_or(false, |allowlist| {
                    allowlist.iter().any(|f| f.matches_task(task))
                });

            // Skip if not allowlisted and not in development mode
            if !is_allowlisted && !executor.development_mode {
                debug!(
                    "executor not allowlisted for function {} - {:#?}",
                    task.function_uri(),
                    executor,
                );
                continue;
            }

            // Get existing function executors for this executor
            let function_executors = self
                .in_memory_state
                .function_executors_by_executor
                .get(executor_id)
                .cloned()
                .unwrap_or_default();

            // Check for an existing matching function executor
            let matching_fe_metadata = function_executors
                .iter()
                .find(|(_, fe_metadata)| fe_metadata.function_executor.matches_task(task))
                .map(|(id, metadata)| (id.clone(), metadata.clone()));

            match matching_fe_metadata {
                Some((fe_id, fe_metadata)) => {
                    // Check if this FE is in Running state - skip if not
                    if fe_metadata.desired_state != FunctionExecutorState::Running {
                        debug!(
                    "function executor {} is not in Running state (current state: {:?}), skipping",
                    fe_id.get(),
                    fe_metadata.desired_state
                );
                        continue;
                    }

                    // Check if this FE has capacity
                    let allocation_count = self.get_allocation_count(executor_id, &fe_id);
                    let capacity_percentage =
                        allocation_count as f64 / MAX_ALLOCATIONS_PER_FN_EXECUTOR as f64;

                    // Add as a candidate
                    let candidate = ExecutorCandidate {
                        executor_id: executor_id.clone(),
                        function_executor_id: Some(fe_id),
                        allocation_count,
                        is_dev_executor: executor.development_mode,
                        needs_creation: false,
                    };

                    candidates.push(candidate);

                    // If this FE is nearing capacity, also consider creating a new one
                    if capacity_percentage >= CAPACITY_THRESHOLD {
                        // Count existing FEs for this function (ignoring version)
                        let existing_fe_count = function_executors
                            .iter()
                            .filter(|(_, fe_metadata)| {
                                let fe = &fe_metadata.function_executor;
                                fe.namespace == task.namespace &&
                                    fe.compute_graph_name == task.compute_graph_name &&
                                    fe.compute_fn_name == task.compute_fn_name
                            })
                            .count();

                        // Check if we can create a new FE
                        if existing_fe_count < MAX_FUNCTION_EXECUTORS_PER_FUNCTION_PER_EXECUTOR {
                            // Also add a creation candidate
                            let creation_candidate = ExecutorCandidate {
                                executor_id: executor_id.clone(),
                                function_executor_id: None, // Will be created if selected
                                allocation_count: 0,        // New FE has no allocations
                                is_dev_executor: executor.development_mode,
                                needs_creation: true,
                            };

                            candidates.push(creation_candidate);
                        }
                    }
                }
                None => {
                    // No matching FE found - see if we can create one

                    // Count existing FEs for this function (ignoring version)
                    let existing_fe_count = function_executors
                        .iter()
                        .filter(|(_, fe_metadata)| {
                            let fe = &fe_metadata.function_executor;
                            fe.namespace == task.namespace &&
                                fe.compute_graph_name == task.compute_graph_name &&
                                fe.compute_fn_name == task.compute_fn_name
                        })
                        .count();

                    // Check if we can create a new FE
                    if existing_fe_count < MAX_FUNCTION_EXECUTORS_PER_FUNCTION_PER_EXECUTOR {
                        // Add as candidate that needs creation
                        let candidate = ExecutorCandidate {
                            executor_id: executor_id.clone(),
                            function_executor_id: None, // Will be created if selected
                            allocation_count: 0,        // New FE has no allocations
                            is_dev_executor: executor.development_mode,
                            needs_creation: true,
                        };

                        candidates.push(candidate);
                    }
                }
            }
        }

        // Sort candidates by priority:
        // 1. First, prefer existing FEs (needs_creation = false)
        // 2. Then by dev mode (prefer non-dev)
        // 3. Then by allocation count (prefer fewer allocations)
        candidates.sort_by(|a, b| {
            // First sort by creation status (existing FEs first)
            let creation_cmp = a.needs_creation.cmp(&b.needs_creation);
            if creation_cmp != std::cmp::Ordering::Equal {
                return creation_cmp;
            }

            // Then sort by dev mode (non-dev first)
            let dev_cmp = a.is_dev_executor.cmp(&b.is_dev_executor);
            if dev_cmp != std::cmp::Ordering::Equal {
                return dev_cmp;
            }

            // Then sort by allocation count (fewer allocations first)
            a.allocation_count.cmp(&b.allocation_count)
        });

        Ok(candidates)
    }

    // Helper function to get allocation count for a function executor
    fn get_allocation_count(&self, executor_id: &ExecutorId, fe_id: &FunctionExecutorId) -> usize {
        self.in_memory_state
            .allocations_by_executor
            .get(executor_id)
            .and_then(|alloc_map| alloc_map.get(fe_id))
            .map(|allocs| allocs.len())
            .unwrap_or(0)
    }

    // Helper function to create a function executor metadata
    fn create_function_executor_metadata(
        &self,
        task: &Task,
        executor_id: &ExecutorId,
    ) -> FunctionExecutorServerMetadata {
        // Create a new function executor
        let function_executor_id = FunctionExecutorId::default();

        let function_executor = FunctionExecutor {
            id: function_executor_id,
            namespace: task.namespace.clone(),
            compute_graph_name: task.compute_graph_name.clone(),
            compute_fn_name: task.compute_fn_name.clone(),
            version: task.graph_version.clone(),
            status: FunctionExecutorStatus::Unknown,
        };
        // Create with current timestamp for last_allocation_at
        FunctionExecutorServerMetadata::new(
            executor_id.clone(),
            function_executor,
            FunctionExecutorState::Pending, // Start with Pending state
            None,
        )
    }

    // Helper function to create an allocation
    #[tracing::instrument(skip(self, task, candidate))]
    fn create_allocation(&self, task: &Task, candidate: &ExecutorCandidate) -> Result<Allocation> {
        let fe_id = match &candidate.function_executor_id {
            Some(id) => id.clone(),
            None => return Err(anyhow!("No function executor ID available for allocation")),
        };

        let allocation = AllocationBuilder::default()
            .namespace(task.namespace.clone())
            .compute_graph(task.compute_graph_name.clone())
            .compute_fn(task.compute_fn_name.clone())
            .invocation_id(task.invocation_id.clone())
            .task_id(task.id.clone())
            .executor_id(candidate.executor_id.clone())
            .function_executor_id(fe_id)
            .build()?;

        Ok(allocation)
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
                        .any(|f| fe.matches_fn_uri(f))
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

        // Consider both Running and Pending function executors from the executor as
        // valid
        let valid_function_executors = executor
            .function_executors
            .iter()
            .filter(|(_, fe)| {
                let state = fe.status.as_state();
                state == FunctionExecutorState::Running || state == FunctionExecutorState::Pending
            })
            .collect::<HashMap<_, _>>();

        // Get the function executors from the indexes
        let function_executors_in_indexes = self
            .in_memory_state
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
        let function_executor_ids_to_remove = function_executors_in_indexes
        .iter()
        .filter_map(|(indexed_fe_id, indexed_fe)| {
            // Case 1: If our indexed FE is marked as Terminated, remove it
            if indexed_fe.desired_state == FunctionExecutorState::Terminated {
                debug!(
                    "Removing function executor {} that was marked for termination",
                    indexed_fe_id.get()
                );
                return Some(indexed_fe_id.clone());
            }

            // Case 2: Check if it exists in executor's list
            let executor_fe = valid_function_executors.get(indexed_fe_id);

            if let Some(executor_fe) = executor_fe {
                // It exists in executor's list, check if its state is Terminated
                if executor_fe.status.as_state() == FunctionExecutorState::Terminated {
                    debug!(
                        "Removing function executor {} that is in Terminated state in executor",
                        indexed_fe_id.get()
                    );
                    return Some(indexed_fe_id.clone());
                }
                // Otherwise keep it
                None
            } else {
                // Not in executor's list
                // Special case: If it's in Pending state in our indexes, keep it
                if indexed_fe.desired_state == FunctionExecutorState::Pending {
                    debug!(
                        "Keeping pending function executor {} not yet in executor's list",
                        indexed_fe_id.get()
                    );
                    None
                } else if indexed_fe.desired_state == FunctionExecutorState::Running {
                    // If it's in Running state in our indexes but not in executor's list, remove it
                    debug!(
                        "Removing function executor {} that is Running in our indexes but not in executor's list",
                        indexed_fe_id.get()
                    );
                    Some(indexed_fe_id.clone())
                } else {
                    // For any other state, keep it for now
                    None
                }
            }
        })
        .collect_vec();

        if !function_executor_ids_to_remove.is_empty() {
            debug!(
                "Executor {} has {} function executors to be removed",
                executor.id.get(),
                function_executor_ids_to_remove.len()
            );

            update.extend(
                self.remove_function_executors(&executor.id, &function_executor_ids_to_remove)?,
            );
        }

        // Step 2: Update existing FEs and add new ones
        for (fe_id, fe) in valid_function_executors.into_iter() {
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
                        indexed_fe.last_allocation_at,
                    );

                    // Add to update
                    update
                        .new_function_executors
                        .push(updated_fe_metadata.clone());

                    // Update in-memory state
                    self.in_memory_state
                        .function_executors_by_executor
                        .entry(executor.id.clone())
                        .or_default()
                        .entry(fe_id.clone())
                        .and_modify(|existing| {
                            **existing = updated_fe_metadata;
                        });
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
                    None, // Last allocation time unknown
                );

                // Add to update
                update.new_function_executors.push(fe_metadata.clone());

                // Add to in-memory state
                self.in_memory_state
                    .function_executors_by_executor
                    .entry(executor.id.clone())
                    .or_default()
                    .entry(fe_id.clone())
                    .or_insert_with(|| Box::new(fe_metadata));
            }
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

        if function_executor_ids_to_remove.is_empty() {
            return Ok(update);
        }

        debug!(
            "Removing {} function executors from executor {}",
            function_executor_ids_to_remove.len(),
            executor_id.get()
        );

        // Handle allocations for FEs to be removed and update tasks
        let allocations_to_remove: Vec<Allocation> = if let Some(allocations_by_fe) = self
            .in_memory_state
            .allocations_by_executor
            .get(executor_id)
        {
            function_executor_ids_to_remove
                .iter()
                .filter_map(|fe_id| allocations_by_fe.get(fe_id))
                .flat_map(|allocations| allocations.iter().map(|alloc| *alloc.clone()))
                .collect()
        } else {
            vec![]
        };

        debug!(
            "Found {} allocations to remove for function executors being removed",
            allocations_to_remove.len()
        );

        // Mark all tasks being unallocated as pending
        for allocation in &allocations_to_remove {
            if let Some(task) = self.in_memory_state.tasks.get(&allocation.task_key()) {
                let mut task = *task.clone();
                task.status = TaskStatus::Pending;

                debug!(
                    "Marking task {} as pending due to function executor removal",
                    task.id
                );

                self.in_memory_state
                    .tasks
                    .insert(task.key(), Box::new(task.clone()));
                update.updated_tasks.insert(task.id.clone(), task);
            } else {
                error!(
                    "Task of allocation not found in indexes: {}",
                    allocation.task_key(),
                );
            }
        }

        // Add allocations to remove list
        update.remove_allocations = allocations_to_remove;

        // Add function executors to remove list
        update.remove_function_executors = function_executor_ids_to_remove
            .iter()
            .map(|fe_id| FunctionExecutorIdWithExecutionId::new(fe_id.clone(), executor_id.clone()))
            .collect();

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

        // Now that we've removed the function executors and updated tasks,
        // we need to immediately attempt to allocate the tasks that were just marked as
        // pending
        if !update.updated_tasks.is_empty() {
            let tasks_to_allocate = update
                .updated_tasks
                .iter()
                .map(|(_, t)| Box::new(t.clone()))
                .collect_vec();

            debug!(
                "Attempting to reallocate {} tasks that were unallocated due to function executor removal",
                tasks_to_allocate.len()
            );

            let allocation_update = self.allocate_tasks(tasks_to_allocate)?;
            update.extend(allocation_update);
        }

        Ok(update)
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
}
