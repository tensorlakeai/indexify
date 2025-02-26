use std::sync::Arc;

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
use metrics::{StateStoreMetrics, Timer};
use opentelemetry::{metrics::Gauge, KeyValue};

use crate::{
    requests::{RequestPayload, StateMachineUpdateRequest},
    scanner::StateReader,
    state_machine::IndexifyObjectsColumns,
};

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

    // NS|CG|Fn|Inv -> Task
    pub unallocated_tasks: im::OrdMap<String, [u8; 0]>,

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

        // Creating Tasks
        let mut tasks = im::OrdMap::new();
        let mut unallocated_tasks = im::OrdMap::new();
        {
            let all_tasks: Vec<(String, Task)> =
                reader.get_all_rows_from_cf(IndexifyObjectsColumns::Tasks)?;
            for (_id, task) in all_tasks {
                if task.is_terminal() {
                    continue;
                }
                if task.status == TaskStatus::Pending {
                    unallocated_tasks.insert(task.key(), [0; 0]);
                }
                tasks.insert(task.key(), Box::new(task));
            }
        }

        let mut invocation_ctx = im::OrdMap::new();
        {
            let all_graph_invocation_ctx: Vec<(String, GraphInvocationCtx)> =
                reader.get_all_rows_from_cf(IndexifyObjectsColumns::GraphInvocationCtx)?;
            for (id, ctx) in all_graph_invocation_ctx {
                if ctx.completed {
                    continue;
                }
                invocation_ctx.insert(id, Box::new(ctx));
            }
        }

        let mut queued_reduction_tasks = im::OrdMap::new();
        {
            let all_reduction_tasks: Vec<(String, ReduceTask)> =
                reader.get_all_rows_from_cf(IndexifyObjectsColumns::ReductionTasks)?;
            for (_id, task) in all_reduction_tasks {
                queued_reduction_tasks.insert(task.key(), Box::new(task));
            }
        }

        let unallocated_tasks_gauge = opentelemetry::global::meter("state_store")
            .u64_gauge("un_allocated_tasks")
            .with_description("Number of unallocated tasks, reported from in_memory_state")
            .build();

        let active_tasks_gauge = opentelemetry::global::meter("state_store")
            .u64_gauge("active_tasks")
            .with_description("Number of active tasks, reported from in_memory_state")
            .build();

        let active_invocations_gauge = opentelemetry::global::meter("state_store")
            .u64_gauge("active_invocations_gauge")
            .with_description("Number of active tasks, reported from in_memory_state")
            .build();
        let active_allocations_gauge = opentelemetry::global::meter("state_store")
            .u64_gauge("active_allocations_gauge")
            .with_description("Number of active tasks, reported from in_memory_state")
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

    pub fn active_tasks_for_executor(&self, executor_id: &str, limit: usize) -> Vec<Box<Task>> {
        self.allocations_by_executor
            .get(executor_id)
            .map(|allocations| {
                allocations
                    .iter()
                    .take(limit)
                    .filter_map(|allocation| self.tasks.get(&allocation.task_key()))
                    .map(|task| task.clone())
                    .collect()
            })
            .unwrap_or_default()
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
                let allocation_key = Allocation::id(
                    req.executor_id.get(),
                    &req.task.id.to_string(),
                    &req.namespace,
                    &req.compute_graph,
                    &req.compute_fn,
                    &req.invocation_id,
                );
                self.allocations_by_executor
                    .entry(req.executor_id.get().to_string())
                    .or_default()
                    .retain(|a| a.id != allocation_key);
                self.tasks
                    .insert(req.task.key(), Box::new(req.task.clone()));
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
                for k in keys_to_remove {
                    self.invocation_ctx.remove(&k);
                }
            }
            RequestPayload::SchedulerUpdate(req) => {
                for task in &req.updated_tasks {
                    if task.status == TaskStatus::Pending {
                        self.unallocated_tasks.insert(task.key(), [0; 0]);
                    } else {
                        self.unallocated_tasks.remove(&task.key());
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
                        let keys_to_remove = self
                            .tasks
                            .range(key.clone()..)
                            .into_iter()
                            .take_while(|(k, _v)| k.starts_with(&key))
                            .map(|(k, _v)| k.clone())
                            .collect::<Vec<String>>();
                        for k in keys_to_remove {
                            self.tasks.remove(&k);
                        }
                    }
                }
                for allocation in &req.new_allocations {
                    self.allocations_by_executor
                        .entry(allocation.executor_id.get().to_string())
                        .or_default()
                        .push_back(Box::new(allocation.clone()));
                    self.unallocated_tasks
                        .remove(&allocation.task_id.to_string());
                }
                for allocation in &req.remove_allocations {
                    self.allocations_by_executor
                        .entry(allocation.executor_id.get().to_string())
                        .or_default()
                        .retain(|a| a.task_id != allocation.task_id);
                }
                for executor_id in &req.remove_executors {
                    self.executors.remove(executor_id.get());
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
        })
    }
}
