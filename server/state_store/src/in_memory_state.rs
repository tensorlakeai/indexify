use std::{cmp::Ordering, sync::Arc};

use anyhow::Result;
use data_model::{
    Allocation,
    ComputeGraph,
    ComputeGraphVersion,
    ExecutorMetadata,
    GraphInvocationCtx,
    ReduceTask,
    Task,
    TaskStatus,
};
use indexify_utils::{get_elapsed_time, TimeUnit};
use metrics::low_latency_boundaries;
use opentelemetry::{
    metrics::{Histogram, ObservableGauge},
    KeyValue,
};
use tokio::sync::RwLock;
use tracing::{error, warn};

use crate::{
    requests::{RequestPayload, StateMachineUpdateRequest},
    scanner::StateReader,
    state_machine::IndexifyObjectsColumns,
};

/// UnallocatedTaskId is a unique identifier for a task that has not been
/// allocated to an executor. It is used to order tasks in the unallocated_tasks
/// queue.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UnallocatedTaskId {
    pub creation_time_ns: u128,
    pub task_key: String,
}

impl UnallocatedTaskId {
    pub fn new(task: &Task) -> Self {
        Self {
            creation_time_ns: task.creation_time_ns,
            task_key: task.key(),
        }
    }
}

impl PartialOrd for UnallocatedTaskId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for UnallocatedTaskId {
    fn cmp(&self, other: &Self) -> Ordering {
        // First, compare creation times
        match self.creation_time_ns.cmp(&other.creation_time_ns) {
            Ordering::Equal => {
                // If creation times are equal, compare task keys
                self.task_key.cmp(&other.task_key)
            }
            time_ordering => time_ordering,
        }
    }
}

pub struct InMemoryState {
    pub namespaces: im::HashMap<String, [u8; 0]>,

    // Namespace|CG Name -> ComputeGraph
    pub compute_graphs: im::HashMap<String, Box<ComputeGraph>>,

    // Namespace|CG Name|Version -> ComputeGraph
    pub compute_graph_versions: im::OrdMap<String, Box<ComputeGraphVersion>>,

    // ExecutorId -> ExecutorMetadata
    pub executors: im::HashMap<String, Box<ExecutorMetadata>>,

    // Executor ID -> (FN URI -> List of Allocation IDs)
    pub allocations_by_fn: im::HashMap<String, im::HashMap<String, im::Vector<Box<Allocation>>>>,

    // TaskKey -> Task
    pub unallocated_tasks: im::OrdSet<UnallocatedTaskId>,

    // Task Key -> Task
    pub tasks: im::OrdMap<String, Box<Task>>,

    // Queued Reduction Tasks
    pub queued_reduction_tasks: im::OrdMap<String, Box<ReduceTask>>,

    // Invocation Ctx
    pub invocation_ctx: im::OrdMap<String, Box<GraphInvocationCtx>>,

    // Histogram metrics for task latency measurements for direct recording
    task_pending_latency: Histogram<f64>,
    task_running_latency: Histogram<f64>,
    task_completion_latency: Histogram<f64>,
}

/// InMemoryMetrics manages observable metrics for the InMemoryState
pub struct InMemoryMetrics {
    pub unallocated_tasks_gauge: ObservableGauge<u64>,
    pub active_tasks_gauge: ObservableGauge<u64>,
    pub active_invocations_gauge: ObservableGauge<u64>,
    pub active_allocations_gauge: ObservableGauge<u64>,
    pub max_invocation_age_gauge: ObservableGauge<f64>,
    pub max_task_age_gauge: ObservableGauge<f64>,
}

impl InMemoryMetrics {
    pub fn new(state: Arc<RwLock<InMemoryState>>) -> Self {
        let meter = opentelemetry::global::meter("state_store");

        // Create observable gauges with callbacks that clone needed data
        let unallocated_tasks_gauge = {
            let state_clone = state.clone();
            meter
                .u64_observable_gauge("un_allocated_tasks")
                .with_description("Number of unallocated tasks, reported from in_memory_state")
                .with_callback(move |observer| {
                    // Use a block scope to ensure the lock is dropped automatically
                    {
                        if let Ok(state) = state_clone.try_read() {
                            let task_count = state.unallocated_tasks.len() as u64;
                            // Lock is automatically dropped at the end of this block
                            observer.observe(task_count, &[]);
                        } else {
                            warn!("Failed to acquire read lock for unallocated_tasks metric");
                        }
                    }
                })
                .build()
        };

        let active_tasks_gauge = {
            let state_clone = state.clone();
            meter
                .u64_observable_gauge("active_tasks")
                .with_description("Number of active tasks, reported from in_memory_state")
                .with_callback(move |observer| {
                    if let Ok(state) = state_clone.try_read() {
                        let task_count = state.tasks.len() as u64;
                        // Lock is automatically dropped at the end of this block
                        observer.observe(task_count, &[]);
                    } else {
                        warn!("Failed to acquire read lock for active_tasks metric");
                    }
                })
                .build()
        };

        let active_invocations_gauge = {
            let state_clone = state.clone();
            meter
                .u64_observable_gauge("active_invocations_gauge")
                .with_description("Number of active invocations, reported from in_memory_state")
                .with_callback(move |observer| {
                    if let Ok(state) = state_clone.try_read() {
                        let invocation_count = state.invocation_ctx.len() as u64;
                        // Lock is automatically dropped at the end of this block
                        observer.observe(invocation_count, &[]);
                    } else {
                        warn!("Failed to acquire read lock for active_invocations metric");
                    }
                })
                .build()
        };

        let active_allocations_gauge = {
            let state_clone = state.clone();
            meter
                .u64_observable_gauge("active_allocations_gauge")
                .with_description("Number of active allocations, reported from in_memory_state")
                .with_callback(move |observer| {
                    // Clone data within a minimal scope to auto-drop the lock immediately
                    let allocations_by_fn_clone = {
                        if let Ok(state) = state_clone.try_read() {
                            state.allocations_by_fn.clone()
                        } else {
                            warn!("Failed to acquire read lock for active_allocations metric");
                            im::HashMap::new()
                        }
                    };

                    // Process the cloned data outside the lock scope
                    for (executor_id, fn_map) in allocations_by_fn_clone.iter() {
                        for (fn_uri, allocations) in fn_map.iter() {
                            observer.observe(
                                allocations.len() as u64,
                                &[
                                    KeyValue::new("executor_id", executor_id.clone()),
                                    KeyValue::new("fn_uri", fn_uri.clone()),
                                ],
                            );
                        }
                    }
                })
                .build()
        };

        // Add max invocation age metric
        let max_invocation_age_gauge = {
            let state_clone = state.clone();
            meter
                .f64_observable_gauge("max_invocation_age")
                .with_unit("s")
                .with_description("Maximum age of any non-completed invocation in seconds")
                .with_callback(move |observer| {
                    // Clone data within a minimal scope to auto-drop the lock immediately
                    let invocation_ctx = {
                        if let Ok(state) = state_clone.try_read() {
                            state.invocation_ctx.clone()
                        } else {
                            warn!("Failed to acquire read lock for invocation_ctx metric");
                            im::OrdMap::new()
                        }
                    };
                    let max_age = {
                        // Find the oldest non-completed invocation
                        if !invocation_ctx.is_empty() {
                            invocation_ctx
                                .values()
                                .filter(|inv| !inv.completed)
                                .map(|inv| {
                                    get_elapsed_time(inv.created_at.into(), TimeUnit::Milliseconds)
                                })
                                .max_by(|a, b| {
                                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                                })
                                .unwrap_or(0.0) // Default to 0 if no
                                                // non-completed invocations
                        } else {
                            0.0 // Default to 0 if no invocations
                        }
                    };

                    // Always report the max age (which may be 0)
                    observer.observe(max_age, &[]);
                })
                .build()
        };

        // Add max task age metric
        let max_task_age_gauge = {
            let state_clone = state.clone();
            meter
                .f64_observable_gauge("max_task_age")
                .with_unit("s")
                .with_description("Maximum age of any non-terminal task in seconds")
                .with_callback(move |observer| {
                    // Clone data within a minimal scope to auto-drop the lock immediately
                    let tasks = {
                        if let Ok(state) = state_clone.try_read() {
                            state.tasks.clone()
                        } else {
                            warn!("Failed to acquire read lock for tasks metric");
                            im::OrdMap::new()
                        }
                    };

                    let max_age = {
                        // Find the oldest non-terminal task
                        if !tasks.is_empty() {
                            tasks
                                .values()
                                .filter(|task| !task.is_terminal())
                                .map(|task| {
                                    get_elapsed_time(task.creation_time_ns, TimeUnit::Nanoseconds)
                                })
                                .max_by(|a, b| {
                                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                                })
                                .unwrap_or(0.0) // Default to 0 if no
                                                // non-terminal tasks
                        } else {
                            0.0 // Default to 0 if no tasks
                        }
                    };

                    // Always report the max age (which may be 0)
                    observer.observe(max_age, &[]);
                })
                .build()
        };

        Self {
            unallocated_tasks_gauge,
            active_tasks_gauge,
            active_invocations_gauge,
            active_allocations_gauge,
            max_invocation_age_gauge,
            max_task_age_gauge,
        }
    }
}

impl InMemoryState {
    pub fn new(reader: StateReader) -> Result<Self> {
        let meter = opentelemetry::global::meter("state_store");

        // Create histogram metrics for task latency measurements
        let task_pending_latency = meter
            .f64_histogram("task_pending_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time tasks spend from creation to running")
            .build();

        let task_running_latency = meter
            .f64_histogram("task_running_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time tasks spend from running to completion")
            .build();

        let task_completion_latency = meter
            .f64_histogram("task_completion_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time tasks spend from creation to completion")
            .build();

        // Creating Namespaces
        let mut namespaces = im::HashMap::new();
        let mut compute_graphs = im::HashMap::new();
        {
            let all_ns = reader.get_all_namespaces()?;
            for ns in &all_ns {
                // Creating Namespaces
                namespaces.insert(ns.name.clone(), [0; 0]);

                // Creating Compute Graphs and Versions
                let cgs = reader.list_compute_graphs(&ns.name, None, None)?.0;
                for cg in cgs {
                    compute_graphs.insert(cg.key(), Box::new(cg));
                }
            }
        }

        let mut compute_graph_versions = im::OrdMap::new();
        {
            let all_cg_versions: Vec<(String, ComputeGraphVersion)> =
                reader.get_all_rows_from_cf(IndexifyObjectsColumns::ComputeGraphVersions)?;
            for (id, cg) in all_cg_versions {
                compute_graph_versions.insert(id, Box::new(cg));
            }
        }

        // Creating Allocated Tasks By Function by Executor
        let mut allocations_by_fn: im::HashMap<
            String,
            im::HashMap<String, im::Vector<Box<Allocation>>>,
        > = im::HashMap::new();
        {
            let (allocations, _) = reader.get_rows_from_cf_with_limits::<Allocation>(
                &[],
                None,
                IndexifyObjectsColumns::Allocations,
                None,
            )?;
            for allocation in allocations {
                allocations_by_fn
                    .entry(allocation.executor_id.get().to_string())
                    .or_default()
                    .entry(allocation.fn_uri())
                    .or_default()
                    .push_back(Box::new(allocation));
            }
        }

        let mut invocation_ctx = im::OrdMap::new();
        {
            let all_graph_invocation_ctx: Vec<(String, GraphInvocationCtx)> =
                reader.get_all_rows_from_cf(IndexifyObjectsColumns::GraphInvocationCtx)?;
            for (_id, ctx) in all_graph_invocation_ctx {
                // Do not cache completed invocations
                if ctx.completed {
                    continue;
                }
                invocation_ctx.insert(ctx.key(), Box::new(ctx));
            }
        }

        // Creating Tasks
        let mut tasks = im::OrdMap::new();
        let mut unallocated_tasks = im::OrdSet::new();
        {
            let all_tasks: Vec<(String, Task)> =
                reader.get_all_rows_from_cf(IndexifyObjectsColumns::Tasks)?;
            for (_id, task) in all_tasks {
                // Do not cache tasks for completed invocations
                if invocation_ctx
                    .get(&GraphInvocationCtx::key_from(
                        &task.namespace,
                        &task.compute_graph_name,
                        &task.invocation_id,
                    ))
                    .is_none()
                {
                    continue;
                }

                if task.is_terminal() {
                    continue;
                }
                if task.status == TaskStatus::Pending {
                    unallocated_tasks.insert(UnallocatedTaskId::new(&task));
                }
                tasks.insert(task.key(), Box::new(task));
            }
        }

        let mut queued_reduction_tasks = im::OrdMap::new();
        {
            let all_reduction_tasks: Vec<(String, ReduceTask)> =
                reader.get_all_rows_from_cf(IndexifyObjectsColumns::ReductionTasks)?;
            for (_id, task) in all_reduction_tasks {
                // Do not cache reduction tasks for completed invocations
                if invocation_ctx
                    .get(&GraphInvocationCtx::key_from(
                        &task.namespace,
                        &task.compute_graph_name,
                        &task.invocation_id,
                    ))
                    .is_none()
                {
                    continue;
                }
                queued_reduction_tasks.insert(task.key(), Box::new(task));
            }
        }

        let in_memory_state = Self {
            namespaces,
            compute_graphs,
            compute_graph_versions,
            executors: im::HashMap::new(),
            tasks,
            unallocated_tasks,
            invocation_ctx,
            queued_reduction_tasks,
            allocations_by_fn,
            task_pending_latency,
            task_running_latency,
            task_completion_latency,
        };

        Ok(in_memory_state)
    }

    pub fn get_tasks_by_fn(
        &self,
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
        compute_fn: &str,
    ) -> Vec<Box<Task>> {
        let key = Task::key_prefix_for_fn(namespace, compute_graph, invocation_id, compute_fn);
        self.tasks
            .range(key.clone()..)
            .take_while(|(k, _v)| k.starts_with(&key))
            .map(|(_, v)| v.clone())
            .collect()
    }

    pub fn active_tasks_for_executor(&self, executor_id: &str) -> Vec<Box<Task>> {
        let mut tasks = vec![];
        if let Some(allocations_by_fn) = self.allocations_by_fn.get(executor_id) {
            for allocations in allocations_by_fn.values() {
                for allocation in allocations {
                    tasks.push(self.tasks.get(&allocation.task_key()).unwrap().clone());
                }
            }
        }
        tasks
    }

    pub fn update_state(
        &mut self,
        state_machine_update_request: &StateMachineUpdateRequest,
    ) -> Result<()> {
        match &state_machine_update_request.payload {
            RequestPayload::InvokeComputeGraph(req) => {
                self.invocation_ctx
                    .insert(req.ctx.key(), Box::new(req.ctx.clone()));
            }
            RequestPayload::IngestTaskOutputs(req) => {
                let allocation_id = Allocation::id(
                    &req.executor_id,
                    &req.task.id,
                    &req.namespace,
                    &req.compute_graph,
                    &req.compute_fn,
                    &req.invocation_id,
                );

                // update task
                self.tasks
                    .insert(req.task.key(), Box::new(req.task.clone()));

                // Remove the allocation
                {
                    self.allocations_by_fn
                        .entry(req.executor_id.get().to_string())
                        .and_modify(|allocation_map| {
                            allocation_map
                                .entry(req.task.fn_uri())
                                .and_modify(|allocations| {
                                    if let Some(index) =
                                        allocations.iter().position(|a| a.id == allocation_id)
                                    {
                                        let allocation = &allocations[index];
                                        // Record metrics
                                        self.task_running_latency.record(
                                            get_elapsed_time(
                                                allocation.created_at,
                                                TimeUnit::Milliseconds,
                                            ),
                                            &[KeyValue::new(
                                                "outcome",
                                                req.task.outcome.to_string(),
                                            )],
                                        );

                                        // Remove the allocation
                                        allocations.remove(index);
                                    }
                                });

                            // Remove the function if no allocations left
                            allocation_map.retain(|_, f| !f.is_empty());
                        });
                }

                // Record metrics
                self.task_completion_latency.record(
                    get_elapsed_time(req.task.creation_time_ns, TimeUnit::Nanoseconds),
                    &[KeyValue::new("outcome", req.task.outcome.to_string())],
                );
            }
            RequestPayload::CreateNameSpace(req) => {
                self.namespaces.insert(req.name.clone(), [0; 0]);
            }
            RequestPayload::CreateOrUpdateComputeGraph(req) => {
                self.compute_graphs
                    .insert(req.compute_graph.key(), Box::new(req.compute_graph.clone()));
                self.compute_graph_versions.insert(
                    req.compute_graph.into_version().key(),
                    Box::new(req.compute_graph.into_version()),
                );

                // FIXME - we should set this in the API and not here, so that these things are
                // not set in the state store
                if req.upgrade_tasks_to_current_version {
                    let mut tasks_to_update = vec![];
                    let key_prefix =
                        Task::key_prefix_for_compute_graph(&req.namespace, &req.compute_graph.name);
                    self.tasks
                        .range(key_prefix.clone()..)
                        .into_iter()
                        .take_while(|(k, _v)| k.starts_with(&key_prefix))
                        .for_each(|(_k, v)| {
                            let mut task = v.clone();
                            task.graph_version = req.compute_graph.into_version().version;
                            // Update the image uri and secret names to the latest version
                            if let Some(node) = req.compute_graph.nodes.get(&task.compute_fn_name) {
                                if let Some(image_uri) = node.image_uri() {
                                    task.image_uri = Some(image_uri.clone());
                                }
                                if let Some(secret_names) = node.secret_names() {
                                    task.secret_names = Some(secret_names.clone());
                                }
                            }
                            tasks_to_update.push(task);
                        });
                    let mut invocation_ctx_to_update = vec![];
                    self.invocation_ctx
                        .range(key_prefix.clone()..)
                        .into_iter()
                        .take_while(|(k, _v)| k.starts_with(&key_prefix))
                        .for_each(|(_k, v)| {
                            let mut ctx = v.clone();
                            ctx.graph_version = req.compute_graph.into_version().version;
                            invocation_ctx_to_update.push(ctx);
                        });

                    for task in tasks_to_update {
                        self.tasks.insert(task.key(), task);
                    }
                    for ctx in invocation_ctx_to_update {
                        self.invocation_ctx.insert(ctx.key(), ctx);
                    }
                }
            }
            RequestPayload::DeleteInvocationRequest(req) => {
                self.delete_invocation(&req.namespace, &req.compute_graph, &req.invocation_id);
            }
            RequestPayload::DeleteComputeGraphRequest(req) => {
                let key = ComputeGraph::key_from(&req.namespace, &req.name);
                let key_prefix = ComputeGraph::key_prefix_from(&req.namespace, &req.name);
                self.compute_graphs.remove(&key);
                let keys_to_remove = self
                    .compute_graph_versions
                    .range(key_prefix.clone()..)
                    .into_iter()
                    .take_while(|(k, _v)| k.starts_with(&key_prefix))
                    .map(|(k, _v)| k.clone())
                    .collect::<Vec<String>>();
                for k in keys_to_remove {
                    self.compute_graph_versions.remove(&k);
                }
                let keys_to_remove = self
                    .invocation_ctx
                    .range(key_prefix.clone()..)
                    .into_iter()
                    .take_while(|(k, _v)| k.starts_with(&key_prefix))
                    .map(|(k, _v)| k.clone())
                    .collect::<Vec<String>>();
                let mut invocations_to_remove = Vec::new();
                for k in keys_to_remove {
                    invocations_to_remove.push(k);
                }
                for k in invocations_to_remove {
                    self.delete_invocation(&req.namespace, &req.name, &k);
                }
            }
            RequestPayload::SchedulerUpdate(req) => {
                for (_, task) in &req.updated_tasks {
                    if task.status == TaskStatus::Pending {
                        self.unallocated_tasks.insert(UnallocatedTaskId::new(&task));
                    } else {
                        self.unallocated_tasks
                            .remove(&UnallocatedTaskId::new(&task));
                    }
                    self.tasks.insert(task.key(), Box::new(task.clone()));
                }
                for task in &req.reduction_tasks.new_reduction_tasks {
                    self.queued_reduction_tasks
                        .insert(task.key(), Box::new(task.clone()));
                }
                for task in &req.reduction_tasks.processed_reduction_tasks {
                    self.queued_reduction_tasks.remove(task);
                }
                for invocation_ctx in &req.updated_invocations_states {
                    self.invocation_ctx
                        .insert(invocation_ctx.key(), Box::new(invocation_ctx.clone()));
                    // Remove tasks for invocation ctx if completed
                    if invocation_ctx.completed {
                        let key_prefix = Task::key_prefix_for_invocation(
                            &invocation_ctx.namespace,
                            &invocation_ctx.compute_graph_name,
                            &invocation_ctx.invocation_id,
                        );
                        self.invocation_ctx.remove(&key_prefix);
                        let tasks_to_remove = self
                            .tasks
                            .range(key_prefix.clone()..)
                            .into_iter()
                            .take_while(|(k, _v)| k.starts_with(&key_prefix))
                            .map(|(_k, v)| v.clone())
                            .collect::<Vec<_>>();

                        self.delete_tasks(tasks_to_remove);
                        // TODO: delete cached queued reduction tasks
                    }
                }

                // Note: We don't need to process req.remove_allocations here, as they are
                // already removed when removing an executor.

                for allocation in &req.new_allocations {
                    if let Some(task) = self.tasks.get(&allocation.task_key()) {
                        self.unallocated_tasks
                            .remove(&UnallocatedTaskId::new(&task));

                        self.allocations_by_fn
                            .entry(allocation.executor_id.get().to_string())
                            .or_default()
                            .entry(allocation.fn_uri())
                            .or_default()
                            .push_back(Box::new(allocation.clone()));

                        // Record metrics
                        self.task_pending_latency.record(
                            get_elapsed_time(task.creation_time_ns, TimeUnit::Nanoseconds),
                            &[],
                        );
                    } else {
                        error!(
                            namespace = &allocation.namespace,
                            compute_graph = &allocation.compute_graph,
                            invocation_id = &allocation.invocation_id,
                            task_id = allocation.task_id.get(),
                            "task not found for new allocation"
                        );
                    }
                }

                for executor_id in &req.remove_executors {
                    self.executors.remove(executor_id.get());
                    self.allocations_by_fn
                        .remove(&executor_id.get().to_string());
                }
            }
            RequestPayload::UpsertExecutor(req) => {
                self.executors.insert(
                    req.executor.id.get().to_string(),
                    Box::new(req.executor.clone()),
                );
            }
            RequestPayload::DeregisterExecutor(req) => {
                let executor = self.executors.get_mut(&req.executor_id.get().to_string());
                if let Some(executor) = executor {
                    executor.tombstoned = true;
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub fn next_reduction_task(
        &self,
        ns: &str,
        cg: &str,
        inv: &str,
        c_fn: &str,
    ) -> Option<ReduceTask> {
        let key_prefix = ReduceTask::key_prefix_from(ns, cg, inv, c_fn);
        self.queued_reduction_tasks
            .range(key_prefix.clone()..)
            .take_while(|(k, _v)| k.starts_with(&key_prefix))
            .next()
            .map(|(_, v)| *v.clone())
    }

    pub fn delete_tasks(&mut self, tasks: Vec<Box<Task>>) {
        for task in tasks.iter() {
            self.tasks.remove(&task.key());
            self.unallocated_tasks
                .remove(&UnallocatedTaskId::new(&task));
        }

        for (_executor, allocations_by_fn) in self.allocations_by_fn.iter_mut() {
            for (_fn_uri, allocations) in allocations_by_fn.iter_mut() {
                allocations.retain(|allocation| !tasks.iter().any(|t| t.id == allocation.task_id));
            }
        }
    }

    pub fn delete_invocation(&mut self, namespace: &str, compute_graph: &str, invocation_id: &str) {
        // Remove tasks
        let key_prefix =
            Task::key_prefix_for_invocation(&namespace, &compute_graph, &invocation_id);
        let mut tasks_to_remove = Vec::new();
        self.tasks
            .range(key_prefix.clone()..)
            .into_iter()
            .take_while(|(k, _v)| k.starts_with(&key_prefix))
            .for_each(|(k, _v)| {
                tasks_to_remove.push(k.clone());
            });
        for task in tasks_to_remove {
            self.tasks.remove(&task);
        }

        // Remove invocation ctx
        self.invocation_ctx.remove(&key_prefix);

        // Remove allocations
        for (_executor, allocations_by_fn) in self.allocations_by_fn.iter_mut() {
            for (_fn_uri, allocations) in allocations_by_fn.iter_mut() {
                allocations.retain(|allocation| allocation.invocation_id != invocation_id);
            }
        }

        // Remove unallocated tasks
        let mut unallocated_tasks_to_remove = Vec::new();
        let unallocated_tasks_clone = self.unallocated_tasks.clone();
        for unallocated_task_id in unallocated_tasks_clone.iter() {
            let task_key = unallocated_task_id.task_key.clone();
            if task_key.starts_with(&key_prefix) {
                unallocated_tasks_to_remove.push(unallocated_task_id);
            }
        }
        for k in unallocated_tasks_to_remove {
            self.unallocated_tasks.remove(k);
        }

        // Remove queued reduction tasks
        let mut queued_reduction_tasks_to_remove = Vec::new();
        for (k, _v) in self.queued_reduction_tasks.iter() {
            if k.starts_with(&key_prefix) {
                queued_reduction_tasks_to_remove.push(k.clone());
            }
        }
        for k in queued_reduction_tasks_to_remove {
            self.queued_reduction_tasks.remove(&k);
        }
    }

    pub fn clone(&self) -> Box<Self> {
        Box::new(InMemoryState {
            namespaces: self.namespaces.clone(),
            compute_graphs: self.compute_graphs.clone(),
            compute_graph_versions: self.compute_graph_versions.clone(),
            executors: self.executors.clone(),
            tasks: self.tasks.clone(),
            unallocated_tasks: self.unallocated_tasks.clone(),
            invocation_ctx: self.invocation_ctx.clone(),
            queued_reduction_tasks: self.queued_reduction_tasks.clone(),
            allocations_by_fn: self.allocations_by_fn.clone(),
            task_pending_latency: self.task_pending_latency.clone(),
            task_running_latency: self.task_running_latency.clone(),
            task_completion_latency: self.task_completion_latency.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::in_memory_state::UnallocatedTaskId;

    #[test]
    fn test_unallocated_task_id_ordering() {
        {
            let task1 = UnallocatedTaskId {
                creation_time_ns: 100,
                task_key: "task1".to_string(),
            };
            let task2 = UnallocatedTaskId {
                creation_time_ns: 200,
                task_key: "task1".to_string(),
            };
            let task3 = UnallocatedTaskId {
                creation_time_ns: 300,
                task_key: "task1".to_string(),
            };
            let task4 = UnallocatedTaskId {
                creation_time_ns: 400,
                task_key: "task1".to_string(),
            };
            let task5 = UnallocatedTaskId {
                creation_time_ns: 1000,
                task_key: "task1".to_string(),
            };

            assert!(task1 < task2);
            assert!(task2 < task3);
            assert!(task3 < task4);
            assert!(task3 < task5);
        }

        {
            let task1 = UnallocatedTaskId {
                creation_time_ns: 100,
                task_key: "task1".to_string(),
            };
            let task2 = UnallocatedTaskId {
                creation_time_ns: 100,
                task_key: "task2".to_string(),
            };
            let task3 = UnallocatedTaskId {
                creation_time_ns: 100,
                task_key: "task3".to_string(),
            };
            let task4 = UnallocatedTaskId {
                creation_time_ns: 100,
                task_key: "task4".to_string(),
            };

            assert!(task1 < task2);
            assert!(task2 < task3);
            assert!(task3 < task4);
        }

        // test that task key is only used as a tie breaker.
        {
            let task1 = UnallocatedTaskId {
                creation_time_ns: 400,
                task_key: "task1".to_string(),
            };
            let task2 = UnallocatedTaskId {
                creation_time_ns: 300,
                task_key: "task2".to_string(),
            };
            let task3 = UnallocatedTaskId {
                creation_time_ns: 200,
                task_key: "task3".to_string(),
            };
            let task4 = UnallocatedTaskId {
                creation_time_ns: 100,
                task_key: "task4".to_string(),
            };

            assert!(task4 < task3);
            assert!(task3 < task2);
            assert!(task2 < task1);
        }
    }
}
