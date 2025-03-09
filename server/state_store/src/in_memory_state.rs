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
use metrics::{low_latency_boundaries, StateStoreMetrics, Timer};
use opentelemetry::{
    metrics::{Gauge, Histogram},
    KeyValue,
};
use tracing::error;

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

    // Executor Id -> List of Task IDs
    pub allocations_by_executor: im::HashMap<String, im::Vector<Box<Allocation>>>,

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

    state_store_metrics: Arc<StateStoreMetrics>,

    unallocated_tasks_gauge: Gauge<u64>,
    active_tasks_gauge: Gauge<u64>,
    active_invocations_gauge: Gauge<u64>,
    active_allocations_gauge: Gauge<u64>,
    active_allocations_by_fn_gauge: Gauge<u64>,
    task_pending_latency: Histogram<f64>,
    task_running_latency: Histogram<f64>,
    task_completion_latency: Histogram<f64>,
}

impl InMemoryState {
    pub fn new(reader: StateReader, state_store_metrics: Arc<StateStoreMetrics>) -> Result<Self> {
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
                    compute_graphs.insert(format!("{}|{}", ns.name, cg.name), Box::new(cg));
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

        // Creating Executors
        let mut executors = im::HashMap::new();
        {
            let all_executors = reader.get_all_executors()?;
            for executor in &all_executors {
                executors.insert(executor.id.get().to_string(), Box::new(executor.clone()));
            }
        }

        // Creating Allocated Tasks By Executor
        let mut allocations_by_executor: im::HashMap<String, im::Vector<Box<Allocation>>> =
            im::HashMap::new();
        {
            let (allocations, _) = reader.get_rows_from_cf_with_limits::<Allocation>(
                &[],
                None,
                IndexifyObjectsColumns::Allocations,
                None,
            )?;
            for allocation in allocations {
                allocations_by_executor
                    .entry(allocation.executor_id.get().to_string())
                    .or_default()
                    .push_back(Box::new(allocation));
            }
        }

        // Creating Allocated Tasks By Function
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

        let meter = opentelemetry::global::meter("state_store");

        let unallocated_tasks_gauge = meter
            .u64_gauge("un_allocated_tasks")
            .with_description("Number of unallocated tasks, reported from in_memory_state")
            .build();
        let active_tasks_gauge = meter
            .u64_gauge("active_tasks")
            .with_description("Number of active tasks, reported from in_memory_state")
            .build();
        let active_invocations_gauge = meter
            .u64_gauge("active_invocations_gauge")
            .with_description("Number of active tasks, reported from in_memory_state")
            .build();
        let active_allocations_gauge = meter
            .u64_gauge("active_allocations_gauge")
            .with_description("Number of active tasks, reported from in_memory_state")
            .build();
        let active_allocations_by_fn_gauge = meter
            .u64_gauge("active_allocations_by_fn_gauge")
            .with_description(
                "Number of active allocations by function, reported from in_memory_state",
            )
            .build();
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

        let in_memory_state = Self {
            namespaces,
            compute_graphs,
            compute_graph_versions,
            executors,
            allocations_by_executor,
            tasks,
            unallocated_tasks,
            invocation_ctx,
            queued_reduction_tasks,
            state_store_metrics,
            unallocated_tasks_gauge,
            active_tasks_gauge,
            active_invocations_gauge,
            active_allocations_gauge,
            task_pending_latency,
            task_running_latency,
            task_completion_latency,
            allocations_by_fn,
            active_allocations_by_fn_gauge,
        };
        in_memory_state.emit_metrics();

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
        for (_, allocations) in self.allocations_by_fn.get(executor_id).unwrap() {
            for allocation in allocations {
                tasks.push(self.tasks.get(&allocation.task_key()).unwrap().clone());
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
                    req.executor_id.get(),
                    &req.task.id.to_string(),
                    &req.namespace,
                    &req.compute_graph,
                    &req.compute_fn,
                    &req.invocation_id,
                );

                // update task
                self.tasks
                    .insert(req.task.key(), Box::new(req.task.clone()));

                // remove allocation
                for (_executor, allocations) in self.allocations_by_executor.iter_mut() {
                    if let Some(index) = allocations.iter().position(|a| a.id == allocation_id) {
                        let allocation = &allocations[index];
                        self.task_running_latency.record(
                            get_elapsed_time(allocation.created_at, TimeUnit::Milliseconds),
                            &[KeyValue::new("outcome", req.task.outcome.to_string())],
                        );

                        // Remove the allocation
                        allocations.remove(index);
                        break;
                    }
                }

                self.task_completion_latency.record(
                    get_elapsed_time(req.task.creation_time_ns, TimeUnit::Nanoseconds),
                    &[KeyValue::new("outcome", req.task.outcome.to_string())],
                );

                self.allocations_by_fn
                    .entry(req.executor_id.get().to_string())
                    .or_default()
                    .entry(req.task.fn_uri())
                    .or_default()
                    .retain(|a| a.id != allocation_id);
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
                        Task::keys_for_compute_graph(&req.namespace, &req.compute_graph.name);
                    self.tasks
                        .range(key_prefix.clone()..)
                        .into_iter()
                        .take_while(|(k, _v)| k.starts_with(&key_prefix))
                        .for_each(|(_k, v)| {
                            let mut task = v.clone();
                            task.graph_version = req.compute_graph.into_version().version;
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
                self.compute_graphs.remove(&key);
                let keys_to_remove = self
                    .compute_graph_versions
                    .range(key.clone()..)
                    .into_iter()
                    .take_while(|(k, _v)| k.starts_with(&key))
                    .map(|(k, _v)| k.clone())
                    .collect::<Vec<String>>();
                for k in keys_to_remove {
                    self.compute_graph_versions.remove(&k);
                }
                let keys_to_remove = self
                    .invocation_ctx
                    .range(key.clone()..)
                    .into_iter()
                    .take_while(|(k, _v)| k.starts_with(&key))
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
                for task in &req.updated_tasks {
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
                        let key = Task::key_prefix_for_invocation(
                            &invocation_ctx.namespace,
                            &invocation_ctx.compute_graph_name,
                            &invocation_ctx.invocation_id,
                        );
                        self.invocation_ctx.remove(&key);
                        let tasks_to_remove = self
                            .tasks
                            .range(key.clone()..)
                            .into_iter()
                            .take_while(|(k, _v)| k.starts_with(&key))
                            .map(|(_k, v)| v.clone())
                            .collect::<Vec<_>>();

                        self.delete_tasks(tasks_to_remove);
                        // TODO: delete cached queued reduction tasks
                    }
                }
                for allocation in &req.new_allocations {
                    if let Some(task) = self.tasks.get(&allocation.task_key()) {
                        self.allocations_by_executor
                            .entry(allocation.executor_id.get().to_string())
                            .or_default()
                            .push_back(Box::new(allocation.clone()));
                        self.unallocated_tasks
                            .remove(&UnallocatedTaskId::new(&task));

                        self.allocations_by_fn
                            .entry(allocation.executor_id.get().to_string())
                            .or_default()
                            .entry(allocation.fn_uri())
                            .or_default()
                            .push_back(Box::new(allocation.clone()));

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
                for allocation in &req.remove_allocations {
                    self.allocations_by_executor
                        .entry(allocation.executor_id.get().to_string())
                        .or_default()
                        .retain(|a| a.task_id != allocation.task_id);
                    self.allocations_by_fn
                        .entry(allocation.executor_id.get().to_string())
                        .or_default()
                        .entry(allocation.fn_uri())
                        .or_default()
                        .retain(|a| a.id != allocation.id);
                }
                for executor_id in &req.remove_executors {
                    self.active_allocations_gauge
                        .record(0, &[KeyValue::new("executor_id", executor_id.to_string())]);
                    self.active_allocations_by_fn_gauge
                        .record(0, &[KeyValue::new("executor_id", executor_id.to_string())]);
                    self.executors.remove(executor_id.get());
                    self.allocations_by_executor
                        .remove(&executor_id.get().to_string());
                    self.allocations_by_fn
                        .remove(&executor_id.get().to_string());
                }
            }
            RequestPayload::RegisterExecutor(req) => {
                self.executors.insert(
                    req.executor.id.get().to_string(),
                    Box::new(req.executor.clone()),
                );
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
        let key = format!("{}|{}|{}|{}", ns, cg, inv, c_fn);
        self.queued_reduction_tasks
            .range(key.clone()..)
            .take_while(|(k, _v)| k.starts_with(&key))
            .next()
            .map(|(_, v)| *v.clone())
    }

    pub fn emit_metrics(&self) {
        let kvs = &[KeyValue::new("op", "state_store_metrics_write")];
        let _timer = Timer::start_with_labels(&self.state_store_metrics.state_metrics_write, kvs);
        self.unallocated_tasks_gauge.record(
            self.unallocated_tasks.len() as u64,
            &[KeyValue::new("global", "unallocated_tasks")],
        );
        self.active_tasks_gauge.record(
            self.tasks.len() as u64,
            &[KeyValue::new("global", "active_tasks")],
        );
        self.active_invocations_gauge.record(
            self.invocation_ctx.len() as u64,
            &[KeyValue::new("global", "active_invocations")],
        );
        for (executor_id, allocations) in &self.allocations_by_executor {
            self.active_allocations_gauge.record(
                allocations.len() as u64,
                &[KeyValue::new("executor_id", executor_id.to_string())],
            );
        }

        for (executor_id, allocations_by_fn) in &self.allocations_by_fn {
            for (fn_uri, allocations) in allocations_by_fn {
                self.active_allocations_by_fn_gauge.record(
                    allocations.len() as u64,
                    &[
                        KeyValue::new("executor_id", executor_id.to_string()),
                        KeyValue::new("fn_uri", fn_uri.to_string()),
                    ],
                );
            }
        }
    }

    pub fn delete_tasks(&mut self, tasks: Vec<Box<Task>>) {
        for task in tasks.iter() {
            self.tasks.remove(&task.key());
            self.unallocated_tasks
                .remove(&UnallocatedTaskId::new(&task));
        }

        for (_executor, allocations) in self.allocations_by_executor.iter_mut() {
            allocations.retain(|allocation| !tasks.iter().any(|t| t.id == allocation.task_id));
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
        for (_executor, allocations) in self.allocations_by_executor.iter_mut() {
            allocations.retain(|allocation| allocation.invocation_id != invocation_id);
        }
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
            allocations_by_executor: self.allocations_by_executor.clone(),
            tasks: self.tasks.clone(),
            unallocated_tasks: self.unallocated_tasks.clone(),
            invocation_ctx: self.invocation_ctx.clone(),
            queued_reduction_tasks: self.queued_reduction_tasks.clone(),
            state_store_metrics: self.state_store_metrics.clone(),
            unallocated_tasks_gauge: self.unallocated_tasks_gauge.clone(),
            active_tasks_gauge: self.active_tasks_gauge.clone(),
            active_invocations_gauge: self.active_invocations_gauge.clone(),
            active_allocations_gauge: self.active_allocations_gauge.clone(),
            task_pending_latency: self.task_pending_latency.clone(),
            task_running_latency: self.task_running_latency.clone(),
            task_completion_latency: self.task_completion_latency.clone(),
            allocations_by_fn: self.allocations_by_fn.clone(),
            active_allocations_by_fn_gauge: self.active_allocations_by_fn_gauge.clone(),
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
