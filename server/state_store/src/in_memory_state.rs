use std::{cmp::Ordering, collections::HashSet, sync::Arc};

use anyhow::{anyhow, Result};
use data_model::{
    Allocation,
    ComputeGraph,
    ComputeGraphVersion,
    DataPayload,
    ExecutorId,
    ExecutorMetadata,
    ExecutorServerMetadata,
    FunctionExecutor,
    FunctionExecutorId,
    FunctionExecutorServerMetadata,
    FunctionExecutorState,
    FunctionURI,
    GraphInvocationCtx,
    GraphVersion,
    NodeResources,
    NodeRetryPolicy,
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
use tracing::{debug, error, warn};

use crate::{
    requests::RequestPayload,
    scanner::StateReader,
    state_machine::IndexifyObjectsColumns,
};

#[derive(Debug, Clone)]
pub struct DesiredStateTask {
    pub task: Box<Task>,
    pub allocation_id: String,
    pub timeout_ms: u32,
    pub retry_policy: NodeRetryPolicy,
}

pub struct DesiredStateFunctionExecutor {
    pub function_executor: Box<FunctionExecutorServerMetadata>,
    pub resources: NodeResources,
    pub image_uri: String,
    pub secret_names: std::vec::Vec<String>,
    pub customer_code_timeout_ms: u32,
    pub code_payload: DataPayload,
    pub task_timeout_ms: u32,
}

pub struct DesiredExecutorState {
    pub function_executors: std::vec::Vec<Box<DesiredStateFunctionExecutor>>,
    pub task_allocations:
        std::collections::HashMap<FunctionExecutorId, Box<std::vec::Vec<DesiredStateTask>>>,
    pub clock: u64,
}

/// UnallocatedTaskId is a unique identifier for a task that has not been
/// allocated to an executor. It is used to order tasks in the unallocated_tasks
/// queue.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UnallocatedTaskId {
    pub task_creation_time_ns: u128,
    pub task_key: String,
}

impl UnallocatedTaskId {
    pub fn new(task: &Task) -> Self {
        Self {
            task_creation_time_ns: task.creation_time_ns,
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
        match self.task_creation_time_ns.cmp(&other.task_creation_time_ns) {
            Ordering::Equal => {
                // If creation times are equal, compare task keys
                self.task_key.cmp(&other.task_key)
            }
            time_ordering => time_ordering,
        }
    }
}

pub struct CandidateFunctionExecutors {
    pub function_executors: Vec<Box<FunctionExecutorServerMetadata>>,
    pub num_pending_function_executors: usize,
}

pub struct InMemoryState {
    // clock is the value of the state_id this in-memory state is at.
    pub clock: u64,

    pub namespaces: im::HashMap<String, [u8; 0]>,

    // Namespace|CG Name -> ComputeGraph
    pub compute_graphs: im::HashMap<String, Box<ComputeGraph>>,

    // Namespace|CG Name|Version -> ComputeGraph
    pub compute_graph_versions: im::OrdMap<String, Box<ComputeGraphVersion>>,

    // ExecutorId -> ExecutorMetadata
    // This is the metadata that executor is sending us, not the **Desired** state
    // from the perspective of the state store.
    pub executors: im::HashMap<ExecutorId, Box<ExecutorMetadata>>,

    // ExecutorId -> (FE ID -> List of Function Executors)
    pub executor_states: im::HashMap<ExecutorId, Box<ExecutorServerMetadata>>,

    pub function_executors_by_fn_uri:
        im::HashMap<FunctionURI, im::HashSet<Box<FunctionExecutorServerMetadata>>>,

    // ExecutorId -> (FE ID -> List of Allocations)
    pub allocations_by_executor:
        im::HashMap<ExecutorId, im::HashMap<FunctionExecutorId, im::Vector<Box<Allocation>>>>,

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
                .u64_observable_gauge("unallocated_tasks")
                .with_description("Number of unallocated tasks, reported from in_memory_state")
                .with_callback(move |observer| {
                    // Use a block scope to ensure the lock is dropped automatically
                    {
                        if let Ok(state) = state_clone.try_read() {
                            let task_count = state.unallocated_tasks.len() as u64;
                            // Lock is automatically dropped at the end of this block
                            observer.observe(task_count, &[]);
                        } else {
                            debug!("Failed to acquire read lock for unallocated_tasks metric");
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
                        let task_count = state
                            .tasks
                            .iter()
                            // Filter out terminal tasks since they stick around until their
                            // invocation is completed.
                            .filter(|(_k, task)| !task.is_terminal())
                            .count() as u64;
                        // Lock is automatically dropped at the end of this block
                        observer.observe(task_count, &[]);
                    } else {
                        debug!("Failed to acquire read lock for active_tasks metric");
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
                        debug!("Failed to acquire read lock for active_invocations metric");
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
                    let allocations_by_executor = {
                        if let Ok(state) = state_clone.try_read() {
                            Some(state.allocations_by_executor.clone())
                        } else {
                            debug!("Failed to acquire read lock for active_allocations metric");
                            None
                        }
                    };

                    match allocations_by_executor {
                        Some(allocations_by_executor) => {
                            // Process the cloned data outside the lock scope
                            for (executor_id, fn_map) in allocations_by_executor.iter() {
                                for (_, allocations) in fn_map.iter() {
                                    if allocations.is_empty() {
                                        continue;
                                    }
                                    observer.observe(
                                        allocations.len() as u64,
                                        &[
                                            KeyValue::new(
                                                "executor_id",
                                                executor_id.get().to_string(),
                                            ),
                                            KeyValue::new(
                                                "fn_uri",
                                                // Use the first allocation's function URI if
                                                // available
                                                // or fallback to the function executor's URI if
                                                // available
                                                allocations
                                                    .iter()
                                                    .next()
                                                    .map(|a| a.fn_uri())
                                                    .unwrap_or("unknown".to_string()),
                                            ),
                                        ],
                                    );
                                }
                            }
                        }
                        None => {}
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
                            Some(state.invocation_ctx.clone())
                        } else {
                            debug!("Failed to acquire read lock for invocation_ctx metric");
                            None
                        }
                    };

                    let max_age = match invocation_ctx {
                        Some(invocation_ctx) => {
                            // Find the oldest non-completed invocation
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
                        }
                        None => 0.0,
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
                            Some(state.tasks.clone())
                        } else {
                            debug!("Failed to acquire read lock for tasks metric");
                            None
                        }
                    };

                    let max_age = match tasks {
                        Some(tasks) => {
                            // Find the oldest non-terminal task
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
                        }
                        None => 0.0,
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
    pub fn new(clock: u64, reader: StateReader) -> Result<Self> {
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
        let mut allocations_by_executor: im::HashMap<
            ExecutorId,
            im::HashMap<FunctionExecutorId, im::Vector<Box<Allocation>>>,
        > = im::HashMap::new();
        {
            let (allocations, _) = reader.get_rows_from_cf_with_limits::<Allocation>(
                &[],
                None,
                IndexifyObjectsColumns::Allocations,
                None,
            )?;
            for allocation in allocations {
                allocations_by_executor
                    .entry(allocation.executor_id.clone())
                    .or_default()
                    .entry(allocation.function_executor_id.clone())
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
            clock,
            namespaces,
            compute_graphs,
            compute_graph_versions,
            executors: im::HashMap::new(),
            tasks,
            unallocated_tasks,
            invocation_ctx,
            queued_reduction_tasks,
            allocations_by_executor,
            // function executors by executor are not known at startup
            executor_states: im::HashMap::new(),
            function_executors_by_fn_uri: im::HashMap::new(),
            // metrics
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

    pub fn get_tasks_by_invocation(
        &self,
        namespace: &str,
        compute_graph: &str,
        invocation_id: &str,
    ) -> Vec<Box<Task>> {
        let key = Task::key_prefix_for_invocation(namespace, compute_graph, invocation_id);
        self.tasks
            .range(key.clone()..)
            .take_while(|(k, _v)| k.starts_with(&key))
            .map(|(_, v)| v.clone())
            .collect()
    }

    pub fn get_invocations_by_compute_graph_version(
        &self,
        namespace: &str,
        compute_graph: &str,
        compute_graph_version: &GraphVersion,
    ) -> Vec<Box<GraphInvocationCtx>> {
        let key_prefix = GraphInvocationCtx::key_prefix_for_compute_graph(namespace, compute_graph);
        self.invocation_ctx
            .range(key_prefix.clone()..)
            .take_while(|(k, _v)| k.starts_with(&key_prefix))
            .take_while(|(_k, v)| v.graph_version == *compute_graph_version)
            .map(|(_, v)| v.clone())
            .collect()
    }

    pub fn active_tasks_for_executor(&self, executor_id: &ExecutorId) -> Vec<Box<Task>> {
        let mut tasks = vec![];
        if let Some(allocations_by_fn) = self.allocations_by_executor.get(executor_id) {
            for allocations in allocations_by_fn.values() {
                for allocation in allocations {
                    if let Some(task) = self.tasks.get(&allocation.task_key()) {
                        tasks.push(task.clone());
                    } else {
                        error!(
                            executor_id = executor_id.get(),
                            task_key = allocation.task_key(),
                            namespace = allocation.namespace,
                            compute_graph = allocation.compute_graph,
                            compute_fn = allocation.compute_fn,
                            invocation_id = allocation.invocation_id,
                            "task not found for allocation"
                        );
                    }
                }
            }
        }
        tasks
    }

    pub fn update_state(
        &mut self,
        new_clock: u64,
        state_machine_update_request: &RequestPayload,
    ) -> Result<HashSet<ExecutorId>> {
        // keep track of what clock we are at for this update state
        self.clock = new_clock;

        // Collect all executors that are being changed to notify them.
        let mut changed_executors = HashSet::new();

        match state_machine_update_request {
            RequestPayload::InvokeComputeGraph(req) => {
                self.invocation_ctx
                    .insert(req.ctx.key(), Box::new(req.ctx.clone()));
            }
            RequestPayload::IngestTaskOutputs(req) => {
                // Update task
                {
                    let invocation_ctx_key = GraphInvocationCtx::key_from(
                        &req.task.namespace,
                        &req.task.compute_graph_name,
                        &req.task.invocation_id,
                    );
                    // Only update tasks for running invocations, this can happen if
                    // the invocation failed with pending allocations on executors.
                    if self.invocation_ctx.get(&invocation_ctx_key).is_some() {
                        self.tasks
                            .insert(req.task.key(), Box::new(req.task.clone()));
                    }
                }

                // Remove the allocation
                {
                    self.allocations_by_executor
                        .entry(req.executor_id.clone())
                        .and_modify(|allocation_map| {
                            // TODO: This can be optimized by keeping a new index of task_id to FE,
                            //       we should measure the overhead.
                            allocation_map.iter_mut().for_each(|(_, allocations)| {
                                if let Some(index) =
                                    allocations.iter().position(|a| a.id == req.allocation_id)
                                {
                                    let allocation = &allocations[index];
                                    // Record metrics
                                    self.task_running_latency.record(
                                        get_elapsed_time(
                                            allocation.created_at,
                                            TimeUnit::Milliseconds,
                                        ),
                                        &[KeyValue::new("outcome", req.task.outcome.to_string())],
                                    );

                                    // Remove the allocation
                                    allocations.remove(index);
                                }
                            });

                            // Remove the function if no allocations left
                            allocation_map.retain(|_, f| !f.is_empty());
                        });

                    // Executor's allocation is removed
                    changed_executors.insert(req.executor_id.clone());
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
                    // Update tasks
                    {
                        let mut tasks_to_update = vec![];
                        let tasks_key_prefix = Task::key_prefix_for_compute_graph(
                            &req.namespace,
                            &req.compute_graph.name,
                        );
                        self.tasks
                            .range(tasks_key_prefix.clone()..)
                            .into_iter()
                            .take_while(|(k, _v)| k.starts_with(&tasks_key_prefix))
                            .for_each(|(_k, v)| {
                                let mut task = v.clone();
                                task.graph_version = req.compute_graph.into_version().version;
                                tasks_to_update.push(task);
                            });

                        for task in tasks_to_update {
                            self.tasks.insert(task.key(), task);
                        }
                    }

                    // Update invocation contexts
                    {
                        let mut invocation_ctx_to_update = vec![];
                        let invocation_ctx_key_prefix =
                            GraphInvocationCtx::key_prefix_for_compute_graph(
                                &req.namespace,
                                &req.compute_graph.name,
                            );
                        self.invocation_ctx
                            .range(invocation_ctx_key_prefix.clone()..)
                            .into_iter()
                            .take_while(|(k, _v)| k.starts_with(&invocation_ctx_key_prefix))
                            .for_each(|(_k, v)| {
                                let mut ctx = v.clone();
                                ctx.graph_version = req.compute_graph.into_version().version;
                                invocation_ctx_to_update.push(ctx);
                            });

                        for ctx in invocation_ctx_to_update {
                            self.invocation_ctx.insert(ctx.key(), ctx);
                        }
                    }
                }
            }
            RequestPayload::DeleteInvocationRequest(req) => {
                self.delete_invocation(&req.namespace, &req.compute_graph, &req.invocation_id);
            }
            RequestPayload::DeleteComputeGraphRequest(req) => {
                // Remove compute graph
                let key = ComputeGraph::key_from(&req.namespace, &req.name);
                self.compute_graphs.remove(&key);

                // Remove compute graph versions
                {
                    let version_key_prefix =
                        ComputeGraphVersion::key_prefix_from(&req.namespace, &req.name);
                    let keys_to_remove = self
                        .compute_graph_versions
                        .range(version_key_prefix.clone()..)
                        .into_iter()
                        .take_while(|(k, _v)| k.starts_with(&version_key_prefix))
                        .map(|(k, _v)| k.clone())
                        .collect::<Vec<String>>();
                    for k in keys_to_remove {
                        self.compute_graph_versions.remove(&k);
                    }
                }

                // Remove invocation contexts
                {
                    let invocation_key_prefix =
                        GraphInvocationCtx::key_prefix_for_compute_graph(&req.namespace, &req.name);
                    let invocations_to_remove = self
                        .invocation_ctx
                        .range(invocation_key_prefix.clone()..)
                        .into_iter()
                        .take_while(|(k, _v)| k.starts_with(&invocation_key_prefix))
                        .map(|(k, _v)| k.clone())
                        .collect::<Vec<String>>();
                    for k in invocations_to_remove {
                        self.delete_invocation(&req.namespace, &req.name, &k);
                    }
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
                    // Remove tasks for invocation ctx if completed
                    if invocation_ctx.completed {
                        self.delete_invocation(
                            &invocation_ctx.namespace,
                            &invocation_ctx.compute_graph_name,
                            &invocation_ctx.invocation_id,
                        );
                    } else {
                        self.invocation_ctx
                            .insert(invocation_ctx.key(), Box::new(invocation_ctx.clone()));
                    }
                }

                for fe_meta in req.new_function_executors.clone() {
                    let Some(executor_state) = self.executor_states.get_mut(&fe_meta.executor_id)
                    else {
                        error!(
                            executor_id = fe_meta.executor_id.get(),
                            "executor not found for new function executor"
                        );
                        continue;
                    };
                    executor_state
                        .function_executors
                        .entry(fe_meta.function_executor.id.clone())
                        .or_insert(Box::new(fe_meta.clone()));

                    let fn_uri = FunctionURI::from(fe_meta.clone());
                    self.function_executors_by_fn_uri
                        .entry(fn_uri)
                        .or_default()
                        .insert(Box::new(fe_meta.clone()));

                    // Executor has a new function executor
                    changed_executors.insert(fe_meta.executor_id.clone());
                }

                for allocation in &req.remove_allocations {
                    self.allocations_by_executor
                        .get_mut(&allocation.executor_id)
                        .map(|allocation_map| {
                            allocation_map
                                .get_mut(&allocation.function_executor_id)
                                .map(|allocations| {
                                    if let Some(index) =
                                        allocations.iter().position(|a| a.id == allocation.id)
                                    {
                                        allocations.remove(index);
                                    }
                                });
                        });

                    // Executor has a removed allocation
                    changed_executors.insert(allocation.executor_id.clone());
                }

                for allocation in &req.new_allocations {
                    if let Some(task) = self.tasks.get(&allocation.task_key()) {
                        self.unallocated_tasks
                            .remove(&UnallocatedTaskId::new(&task));

                        self.allocations_by_executor
                            .entry(allocation.executor_id.clone())
                            .or_default()
                            .entry(allocation.function_executor_id.clone())
                            .or_default()
                            .push_back(Box::new(allocation.clone()));

                        // Record metrics
                        self.task_pending_latency.record(
                            get_elapsed_time(task.creation_time_ns, TimeUnit::Nanoseconds),
                            &[],
                        );

                        // Executor has a new allocation
                        changed_executors.insert(allocation.executor_id.clone());
                    } else {
                        error!(
                            namespace = &allocation.namespace,
                            compute_graph = &allocation.compute_graph,
                            compute_fn = &allocation.compute_fn,
                            executor_id = allocation.executor_id.get(),
                            invocation_id = &allocation.invocation_id,
                            task_id = allocation.task_id.get(),
                            "task not found for new allocation"
                        );
                    }
                }

                for function_executor in &req.remove_function_executors {
                    self.allocations_by_executor
                        .get_mut(&function_executor.executor_id)
                        .and_then(|allocation_map| {
                            allocation_map.remove(&function_executor.function_executor_id)
                        });
                    let fe = self
                        .executor_states
                        .get_mut(&function_executor.executor_id)
                        .and_then(|executor_state| {
                            executor_state
                                .function_executors
                                .remove(&function_executor.function_executor_id)
                        });

                    if let Some(fe) = fe {
                        let fn_uri = FunctionURI::from(fe.clone());
                        self.function_executors_by_fn_uri
                            .get_mut(&fn_uri)
                            .and_then(|fe_set| fe_set.remove(&fe));
                    }
                    changed_executors.insert(function_executor.executor_id.clone());
                }

                for executor_id in &req.remove_executors {
                    self.executors.remove(executor_id);
                    self.allocations_by_executor.remove(executor_id);
                    self.executor_states.remove(executor_id);

                    // Executor is removed
                    changed_executors.insert(executor_id.clone());
                }

                for (executor_id, host_resources) in &req.updated_executor_resources {
                    if let Some(executor) = self.executor_states.get_mut(executor_id) {
                        executor.free_resources = host_resources.clone();
                    }
                    // } else {
                    //     self.executor_states.insert(
                    //         executor_id.clone(),
                    //         Box::new(ExecutorServerMetadata {
                    //             executor_id: executor_id.clone(),
                    //             function_executors: im::HashMap::new(),
                    //             free_resources: host_resources.clone(),
                    //         }),
                    //     );
                    // }
                }
            }
            RequestPayload::UpsertExecutor(req) => {
                self.executors
                    .insert(req.executor.id.clone(), Box::new(req.executor.clone()));
                if self.executor_states.get(&req.executor.id).is_none() {
                    self.executor_states.insert(
                        req.executor.id.clone(),
                        Box::new(ExecutorServerMetadata {
                            executor_id: req.executor.id.clone(),
                            function_executors: im::HashMap::new(),
                            free_resources: req.executor.host_resources.clone(),
                        }),
                    );
                }
            }
            RequestPayload::DeregisterExecutor(req) => {
                let executor = self.executors.get_mut(&req.executor_id);
                if let Some(executor) = executor {
                    executor.tombstoned = true;
                }
            }
            _ => {}
        }

        Ok(changed_executors)
    }

    pub fn candidate_executors(&self, task: &Task) -> Result<Vec<Box<ExecutorMetadata>>> {
        let compute_graph = self
            .compute_graph_versions
            .get(&ComputeGraphVersion::key_from(
                &task.namespace,
                &task.compute_graph_name,
                &task.graph_version,
            ))
            .ok_or(anyhow!("Compute graph version not found"))?;
        let compute_fn = compute_graph
            .nodes
            .get(&task.compute_fn_name)
            .ok_or(anyhow!("Compute function not found"))?;
        let mut candidates = Vec::new();
        for (_, executor_state) in &self.executor_states {
            let Some(executor) = self.executors.get(&executor_state.executor_id) else {
                error!(
                    executor_id = executor_state.executor_id.get(),
                    "executor not found for candidate executors but was found in executor_states"
                );
                continue;
            };
            if executor.tombstoned || !executor.is_task_allowed(task) {
                continue;
            }
            if executor_state
                .free_resources
                .can_handle(&compute_fn.resources())
            {
                let mut candidate = executor.clone();
                candidate.host_resources.consume(&compute_fn.resources())?;
                candidates.push(candidate);
            }
        }
        Ok(candidates)
    }

    pub fn candidate_function_executors(
        &self,
        task: &Task,
        capacity_threshold: usize,
    ) -> Result<CandidateFunctionExecutors> {
        let mut candidates = Vec::new();
        let fn_uri = FunctionURI::from(task);
        let function_executors = self.function_executors_by_fn_uri.get(&fn_uri);
        let mut num_pending_function_executors = 0;
        if let Some(function_executors) = function_executors {
            for function_executor in function_executors.iter() {
                if function_executor.function_executor.state == FunctionExecutorState::Pending ||
                    function_executor.function_executor.state == FunctionExecutorState::Unknown
                {
                    num_pending_function_executors += 1;
                }
                if function_executor.desired_state == FunctionExecutorState::Terminated ||
                    function_executor.function_executor.state == FunctionExecutorState::Running
                {
                    continue;
                }
                // FIXME - Create a reverse index of fe_id -> # active allocations
                let allocation_count = self
                    .allocations_by_executor
                    .get(&function_executor.executor_id)
                    .and_then(|alloc_map| alloc_map.get(&function_executor.function_executor.id))
                    .map(|allocs| allocs.len())
                    .unwrap_or(0);
                if allocation_count < capacity_threshold {
                    candidates.push(function_executor.clone());
                }
            }
        }
        Ok(CandidateFunctionExecutors {
            function_executors: candidates,
            num_pending_function_executors,
        })
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

        for (_executor, allocations_by_fe) in self.allocations_by_executor.iter_mut() {
            for (_fe_id, allocations) in allocations_by_fe.iter_mut() {
                allocations.retain(|allocation| !tasks.iter().any(|t| t.id == allocation.task_id));
            }
        }
    }

    pub fn get_fe_resources(&self, fe: &FunctionExecutor) -> Option<NodeResources> {
        let cg_version = self
            .compute_graph_versions
            .get(&ComputeGraphVersion::key_from(
                &fe.namespace,
                &fe.compute_graph_name,
                &fe.version,
            ))
            .cloned()?;
        cg_version
            .nodes
            .get(&fe.compute_fn_name)
            .map(|node| node.resources())
    }

    pub fn delete_invocation(&mut self, namespace: &str, compute_graph: &str, invocation_id: &str) {
        // Remove invocation ctx
        self.invocation_ctx.remove(&GraphInvocationCtx::key_from(
            namespace,
            compute_graph,
            invocation_id,
        ));

        // Remove tasks
        let key_prefix =
            Task::key_prefix_for_invocation(&namespace, &compute_graph, &invocation_id);
        let mut tasks_to_remove = Vec::new();
        self.tasks
            .range(key_prefix.clone()..)
            .into_iter()
            .take_while(|(k, _v)| k.starts_with(&key_prefix))
            .for_each(|(_k, v)| {
                tasks_to_remove.push(v.clone());
            });
        self.delete_tasks(tasks_to_remove);

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

    pub fn unallocated_tasks(&self) -> Vec<Box<Task>> {
        let unallocated_task_ids = self
            .unallocated_tasks
            .iter()
            .map(|task| task.task_key.clone())
            .collect::<Vec<_>>();
        let mut tasks = Vec::new();
        for task_id in unallocated_task_ids {
            if let Some(task) = self.tasks.get(&task_id) {
                tasks.push(task.clone());
            } else {
                error!(task_key = task_id, "task not found for unallocated task");
            }
        }
        tasks
    }

    #[tracing::instrument(skip(self))]
    pub fn vacuum_function_executors_candidates(
        &self,
    ) -> Result<Vec<Box<FunctionExecutorServerMetadata>>> {
        let mut function_executors_to_remove: Vec<Box<FunctionExecutorServerMetadata>> = Vec::new();

        // For each executor in the system
        for (executor_id, executor) in &self.executors {
            if executor.tombstoned {
                continue;
            }

            // Get function executors for this executor from our in-memory state
            let function_executors = self
                .executor_states
                .get(executor_id)
                .cloned()
                .map(|executor_state| {
                    executor_state
                        .function_executors
                        .values()
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            // Process each function executor based on allowlist and version status
            for fe_metadata in function_executors.iter() {
                // Skip if the FE is already marked for termination
                if fe_metadata.desired_state == FunctionExecutorState::Terminated {
                    continue;
                }

                let fe = &fe_metadata.function_executor;
                let Some(executor) = self.executors.get(executor_id) else {
                    // If the executor is not found, we can remove the FE
                    // since it's not running any more
                    function_executors_to_remove.push(fe_metadata.clone());
                    continue;
                };

                let Some(latest_cg_version) = self
                    .compute_graphs
                    .get(&ComputeGraph::key_from(
                        &fe.namespace,
                        &fe.compute_graph_name,
                    ))
                    .map(|cg| cg.version.clone())
                else {
                    // If the compute graph is not found, we can remove the FE
                    // since it's not running any more
                    function_executors_to_remove.push(fe_metadata.clone());
                    continue;
                };

                // Check if this FE has any pending invocations with its current version
                let has_pending_invocations = self.has_pending_invocations(&fe_metadata);

                if !has_pending_invocations {
                    // Can remove this outdated FE since it has no active invocations,
                    // and running on an outdated version of the allowed compute graph
                    let mut found_allowlist_match = false;
                    if let Some(allowlist) = executor.function_allowlist.as_ref() {
                        for allowlist_entry in allowlist.iter() {
                            if allowlist_entry.matches_function_executor(fe) &&
                                fe.version == latest_cg_version
                            {
                                // This FE is allowed and up to date, so we don't need to remove it
                                found_allowlist_match = true;
                            }
                        }
                    }
                    if found_allowlist_match {
                        continue;
                    }
                    debug!(
                    "Removing outdated function executor {} from executor {} (version {} < latest {})",
                    fe.id.get(), executor_id.get(), fe.version, latest_cg_version
                );
                    function_executors_to_remove.push(fe_metadata.clone());
                }
            }
        }

        Ok(function_executors_to_remove)
    }

    fn has_pending_invocations(&self, fe_meta: &FunctionExecutorServerMetadata) -> bool {
        // Check if there are any allocations for this FE with pending tasks
        if let Some(allocations_by_fe) = self.allocations_by_executor.get(&fe_meta.executor_id) {
            if let Some(allocations) = allocations_by_fe.get(&fe_meta.function_executor.id) {
                for allocation in allocations {
                    if let Some(task) = self.tasks.get(&allocation.task_key()) {
                        if !task.outcome.is_terminal() {
                            return true;
                        }
                    }
                }
            }
        }

        false
    }

    pub fn desired_state(&self, executor_id: &ExecutorId) -> DesiredExecutorState {
        let active_function_executors = self
            .executor_states
            .get(executor_id)
            .cloned()
            .map(|executor_state| executor_state.function_executors.clone())
            .unwrap_or_default()
            .values()
            .filter(|fe_meta| fe_meta.function_executor.state != FunctionExecutorState::Terminated)
            .map(|fe_meta| fe_meta.clone())
            .collect::<Vec<_>>();

        let mut function_executors = Vec::new();
        let mut task_allocations = std::collections::HashMap::new();
        for fe_meta in active_function_executors.iter() {
            let fe = &fe_meta.function_executor;
            let Some(cg_version) = self
                .compute_graph_versions
                .get(&ComputeGraphVersion::key_from(
                    &fe.namespace,
                    &fe.compute_graph_name,
                    &fe.version,
                ))
                .cloned()
            else {
                continue;
            };
            let Some(cg_node) = cg_version.nodes.get(&fe.compute_fn_name) else {
                continue;
            };
            function_executors.push(Box::new(DesiredStateFunctionExecutor {
                function_executor: fe_meta.clone(),
                resources: cg_node.resources(),
                image_uri: cg_node.image_uri().unwrap_or_default(),
                secret_names: cg_node.secret_names(),
                customer_code_timeout_ms: cg_node.timeout().0,
                code_payload: DataPayload {
                    path: cg_version.code.path.clone(),
                    size: cg_version.code.size,
                    sha256_hash: cg_version.code.sha256_hash.clone(),
                },
                task_timeout_ms: cg_node.timeout().0,
            }));

            let allocations = self
                .allocations_by_executor
                .get(executor_id)
                .and_then(|allocations| allocations.get(&fe_meta.function_executor.id))
                .unwrap_or(&im::Vector::new())
                .clone();
            let mut desired_state_tasks = std::vec::Vec::new();
            for allocation in allocations.iter() {
                let Some(task) = self.tasks.get(&allocation.task_key()) else {
                    error!(
                        task_key = allocation.task_key(),
                        task_id = allocation.task_id.get(),
                        namespace = allocation.namespace,
                        compute_graph = allocation.compute_graph,
                        compute_fn = allocation.compute_fn,
                        invocation_id = allocation.invocation_id,
                        "task not found for allocation, shouldn't happen"
                    );
                    continue;
                };
                let desired_state_task = DesiredStateTask {
                    task: task.clone(),
                    allocation_id: allocation.id.clone(),
                    timeout_ms: cg_node.timeout().0,
                    retry_policy: cg_node.retry_policy(),
                };
                desired_state_tasks.push(desired_state_task);
            }
            task_allocations.insert(
                fe_meta.function_executor.id.clone(),
                Box::new(desired_state_tasks),
            );
        }

        DesiredExecutorState {
            function_executors,
            task_allocations,
            clock: self.clock,
        }
    }

    pub fn clone(&self) -> Arc<std::sync::RwLock<Self>> {
        Arc::new(std::sync::RwLock::new(InMemoryState {
            clock: self.clock,
            namespaces: self.namespaces.clone(),
            compute_graphs: self.compute_graphs.clone(),
            compute_graph_versions: self.compute_graph_versions.clone(),
            executors: self.executors.clone(),
            tasks: self.tasks.clone(),
            unallocated_tasks: self.unallocated_tasks.clone(),
            invocation_ctx: self.invocation_ctx.clone(),
            queued_reduction_tasks: self.queued_reduction_tasks.clone(),
            allocations_by_executor: self.allocations_by_executor.clone(),
            executor_states: self.executor_states.clone(),
            function_executors_by_fn_uri: self.function_executors_by_fn_uri.clone(),
            // metrics
            task_pending_latency: self.task_pending_latency.clone(),
            task_running_latency: self.task_running_latency.clone(),
            task_completion_latency: self.task_completion_latency.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::in_memory_state::UnallocatedTaskId;

    #[test]
    fn test_unallocated_task_id_ordering() {
        {
            let task1 = UnallocatedTaskId {
                task_creation_time_ns: 100,
                task_key: "task1".to_string(),
            };
            let task2 = UnallocatedTaskId {
                task_creation_time_ns: 200,
                task_key: "task1".to_string(),
            };
            let task3 = UnallocatedTaskId {
                task_creation_time_ns: 300,
                task_key: "task1".to_string(),
            };
            let task4 = UnallocatedTaskId {
                task_creation_time_ns: 400,
                task_key: "task1".to_string(),
            };
            let task5 = UnallocatedTaskId {
                task_creation_time_ns: 1000,
                task_key: "task1".to_string(),
            };

            assert!(task1 < task2);
            assert!(task2 < task3);
            assert!(task3 < task4);
            assert!(task3 < task5);
        }

        {
            let task1 = UnallocatedTaskId {
                task_creation_time_ns: 100,
                task_key: "task1".to_string(),
            };
            let task2 = UnallocatedTaskId {
                task_creation_time_ns: 100,
                task_key: "task2".to_string(),
            };
            let task3 = UnallocatedTaskId {
                task_creation_time_ns: 100,
                task_key: "task3".to_string(),
            };
            let task4 = UnallocatedTaskId {
                task_creation_time_ns: 100,
                task_key: "task4".to_string(),
            };

            assert!(task1 < task2);
            assert!(task2 < task3);
            assert!(task3 < task4);
        }

        // test that task key is only used as a tie breaker.
        {
            let task1 = UnallocatedTaskId {
                task_creation_time_ns: 400,
                task_key: "task1".to_string(),
            };
            let task2 = UnallocatedTaskId {
                task_creation_time_ns: 300,
                task_key: "task2".to_string(),
            };
            let task3 = UnallocatedTaskId {
                task_creation_time_ns: 200,
                task_key: "task3".to_string(),
            };
            let task4 = UnallocatedTaskId {
                task_creation_time_ns: 100,
                task_key: "task4".to_string(),
            };

            assert!(task4 < task3);
            assert!(task3 < task2);
            assert!(task2 < task1);
        }
    }
}
