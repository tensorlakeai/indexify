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

use crate::{
    requests::{RequestPayload, StateMachineUpdateRequest},
    scanner::StateReader,
    state_machine::IndexifyObjectsColumns,
};

pub struct InMemoryState {
    pub namespaces: im::HashMap<String, [u8; 0]>,

    // Namespace|CG Name -> ComputeGraph
    pub compute_graphs: im::HashMap<String, Arc<ComputeGraph>>,

    // Namespace|CG Name|Version -> ComputeGraph
    pub compute_graph_versions: im::OrdMap<String, Arc<ComputeGraphVersion>>,

    // ExecutorId -> ExecutorMetadata
    pub executors: im::HashMap<String, Arc<ExecutorMetadata>>,

    // Executor Id -> List of Task IDs
    pub allocations_by_executor: im::HashMap<String, im::Vector<Arc<Allocation>>>,

    // NS|CG|Fn|Inv -> Task
    pub unallocated_tasks: im::OrdMap<String, [u8; 0]>,

    // Task Key -> Task
    pub tasks: im::OrdMap<String, Arc<Task>>,

    // Queued Reduction Tasks
    pub queued_reduction_tasks: im::OrdMap<String, Arc<ReduceTask>>,

    // Invocation Ctx
    pub invocation_ctx: im::OrdMap<String, Arc<GraphInvocationCtx>>,
}

impl InMemoryState {
    pub fn new(reader: StateReader) -> Result<Self> {
        // Creating Namespaces
        let mut namespaces = im::HashMap::new();
        let all_ns = reader.get_all_namespaces()?;
        for ns in &all_ns {
            namespaces.insert(ns.name.clone(), [0; 0]);
        }

        // Creating Compute Graphs and Versions
        let mut compute_graphs = im::HashMap::new();
        for ns in &all_ns {
            let cgs = reader.list_compute_graphs(&ns.name, None, None)?.0;
            for cg in cgs {
                compute_graphs.insert(format!("{}|{}", ns.name, cg.name), Arc::new(cg));
            }
        }
        let mut compute_graph_versions = im::OrdMap::new();
        let all_cg_versions: Vec<(String, ComputeGraphVersion)> =
            reader.get_all_rows_from_cf(IndexifyObjectsColumns::ComputeGraphVersions)?;
        for (id, cg) in all_cg_versions {
            compute_graph_versions.insert(id, Arc::new(cg));
        }

        // Creating Executors
        let all_executors = reader.get_all_executors()?;
        let mut executors = im::HashMap::new();
        for executor in &all_executors {
            executors.insert(executor.id.get().to_string(), Arc::new(executor.clone()));
        }

        // Creating Allocated Tasks By Executor
        let mut allocations_by_executor: im::HashMap<String, im::Vector<Arc<Allocation>>> =
            im::HashMap::new();
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
                .push_back(Arc::new(allocation));
        }

        // Creating Tasks
        let all_tasks: Vec<(String, Task)> =
            reader.get_all_rows_from_cf(IndexifyObjectsColumns::Tasks)?;
        let mut tasks = im::OrdMap::new();
        let mut unallocated_tasks = im::OrdMap::new();
        for (_id, task) in all_tasks {
            if task.is_terminal() {
                continue;
            }
            tasks.insert(task.key(), Arc::new(task.clone()));
            if task.status == TaskStatus::Pending {
                unallocated_tasks.insert(task.key(), [0; 0]);
            }
        }

        let mut invocation_ctx = im::OrdMap::new();
        let all_graph_invocation_ctx: Vec<(String, GraphInvocationCtx)> =
            reader.get_all_rows_from_cf(IndexifyObjectsColumns::GraphInvocationCtx)?;
        for (id, ctx) in all_graph_invocation_ctx {
            if ctx.completed {
                continue;
            }
            invocation_ctx.insert(id, Arc::new(ctx));
        }
        let mut queued_reduction_tasks = im::OrdMap::new();
        let all_reduction_tasks: Vec<(String, ReduceTask)> =
            reader.get_all_rows_from_cf(IndexifyObjectsColumns::ReductionTasks)?;
        for (_id, task) in all_reduction_tasks {
            queued_reduction_tasks.insert(task.key(), Arc::new(task));
        }
        Ok(Self {
            namespaces,
            compute_graphs,
            compute_graph_versions,
            executors,
            allocations_by_executor,
            tasks,
            unallocated_tasks,
            invocation_ctx,
            queued_reduction_tasks,
        })
    }

    pub fn active_tasks_for_executor(&self, executor_id: &str, limit: usize) -> Vec<Task> {
        self.allocations_by_executor
            .get(executor_id)
            .map(|allocations| {
                allocations
                    .iter()
                    .take(limit)
                    .filter_map(|allocation| self.tasks.get(&allocation.task_key()))
                    .map(|task| (task).as_ref().clone())
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
                    .insert(req.ctx.key(), Arc::new(req.ctx.clone()));
            }
            RequestPayload::IngestTaskOutputs(req) => {
                let allocation_key = Allocation::key(
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
                self.tasks.remove(&req.task.id.to_string());
            }
            RequestPayload::CreateNameSpace(req) => {
                self.namespaces.insert(req.name.clone(), [0; 0]);
            }
            RequestPayload::CreateOrUpdateComputeGraph(req) => {
                self.compute_graphs
                    .insert(req.compute_graph.key(), Arc::new(req.compute_graph.clone()));
                self.compute_graph_versions.insert(
                    req.compute_graph.into_version().key(),
                    Arc::new(req.compute_graph.into_version().clone()),
                );
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
                    self.tasks.insert(task.key(), Arc::new(task.clone()));
                }
                for task in &req.reduction_tasks.new_reduction_tasks {
                    self.queued_reduction_tasks
                        .insert(task.key(), Arc::new(task.clone()));
                }
                for task in &req.reduction_tasks.processed_reduction_tasks {
                    self.queued_reduction_tasks.remove(task);
                }
                for invocation_ctx in &req.updated_invocations_states {
                    self.invocation_ctx
                        .insert(invocation_ctx.key(), Arc::new(invocation_ctx.clone()));
                }
                for allocation in &req.new_allocations {
                    self.allocations_by_executor
                        .entry(allocation.executor_id.get().to_string())
                        .or_default()
                        .push_back(Arc::new(allocation.clone()));
                    self.unallocated_tasks
                        .remove(&allocation.task_id.to_string());
                }
                for allocation in &req.remove_allocations {
                    self.allocations_by_executor
                        .entry(allocation.executor_id.get().to_string())
                        .or_default()
                        .retain(|a| a.task_id != allocation.task_id);
                    self.unallocated_tasks
                        .insert(allocation.task_id.to_string(), [0; 0]);
                }
            }
            RequestPayload::RegisterExecutor(req) => {
                self.executors.insert(
                    req.executor.id.get().to_string(),
                    Arc::new(req.executor.clone()),
                );
            }
            RequestPayload::DeregisterExecutor(req) => {
                self.executors.remove(req.executor_id.get());
            }
            _ => {}
        }
        Ok(())
    }

    pub fn get_in_memory_state(&self) -> Arc<Self> {
        Arc::new(InMemoryState {
            namespaces: self.namespaces.clone(),
            compute_graphs: self.compute_graphs.clone(),
            compute_graph_versions: self.compute_graph_versions.clone(),
            executors: self.executors.clone(),
            allocations_by_executor: self.allocations_by_executor.clone(),
            tasks: self.tasks.clone(),
            unallocated_tasks: self.unallocated_tasks.clone(),
            invocation_ctx: self.invocation_ctx.clone(),
            queued_reduction_tasks: self.queued_reduction_tasks.clone(),
        })
    }
}
