use anyhow::Result;
use data_model::{
    ComputeGraph,
    ComputeGraphVersion,
    ExecutorMetadata,
    GraphInvocationCtx,
    ReduceTask,
    Task,
};
use im::HashSet;

use crate::{
    requests::{RequestPayload, StateMachineUpdateRequest},
    scanner::StateReader,
    state_machine::IndexifyObjectsColumns,
};

pub struct InMemoryState {
    pub namespaces: im::HashMap<String, [u8; 0]>,

    // Namespace|CG Name -> ComputeGraph
    pub compute_graphs: im::HashMap<String, ComputeGraph>,

    // Namespace|CG Name|Version -> ComputeGraph
    pub compute_graph_versions: im::HashMap<String, ComputeGraphVersion>,

    // ExecutorId -> ExecutorMetadata
    pub executors: im::HashMap<String, ExecutorMetadata>,

    // Executor Id -> List of Task IDs
    pub allocated_tasks: im::HashMap<String, HashSet<String>>,

    // Task Key -> Task
    pub tasks: im::OrdMap<String, Task>,

    // Queued Reduction Tasks
    pub queued_reduction_tasks: im::OrdMap<String, ReduceTask>,

    // Task Keys
    pub unallocated_tasks: im::HashMap<String, [u8; 0]>,

    // Invocation Ctx
    pub invocation_ctx: im::OrdMap<String, GraphInvocationCtx>,
}

impl InMemoryState {
    pub fn new(reader: &StateReader) -> Result<Self> {
        let mut namespaces = im::HashMap::new();
        let all_ns = reader.get_all_namespaces()?;
        for ns in &all_ns {
            namespaces.insert(ns.name.clone(), [0; 0]);
        }
        let mut compute_graphs = im::HashMap::new();
        for ns in &all_ns {
            let cgs = reader.list_compute_graphs(&ns.name, None, None)?.0;
            for cg in cgs {
                compute_graphs.insert(format!("{}|{}", ns.name, cg.name), cg);
            }
        }
        let mut compute_graph_versions = im::HashMap::new();
        let all_cg_versions: Vec<(String, ComputeGraphVersion)> =
            reader.get_all_rows_from_cf(IndexifyObjectsColumns::ComputeGraphVersions)?;
        for (id, cg) in all_cg_versions {
            compute_graph_versions.insert(id, cg);
        }
        let all_executors = reader.get_all_executors()?;
        let mut executors = im::HashMap::new();
        let mut allocated_tasks = im::HashMap::new();
        for executor in &all_executors {
            executors.insert(executor.id.get().to_string(), executor.clone());
            let executor_allocated_tasks = reader.get_allocated_tasks(&executor.id)?;
            allocated_tasks.insert(
                executor.id.get().to_string(),
                executor_allocated_tasks
                    .into_iter()
                    .map(|t| t.id.to_string())
                    .collect(),
            );
        }

        let all_tasks: Vec<(String, Task)> =
            reader.get_all_rows_from_cf(IndexifyObjectsColumns::Tasks)?;
        let mut tasks = im::OrdMap::new();
        for (_id, task) in all_tasks {
            tasks.insert(task.key(), task);
        }
        let all_unallocated_tasks: Vec<(String, [u8; 0])> =
            reader.get_all_rows_from_cf(IndexifyObjectsColumns::UnallocatedTasks)?;
        let mut unallocated_tasks = im::HashMap::new();
        for (id, task) in all_unallocated_tasks {
            unallocated_tasks.insert(id, task);
        }
        let mut invocation_ctx = im::OrdMap::new();
        let all_graph_invocation_ctx: Vec<(String, GraphInvocationCtx)> =
            reader.get_all_rows_from_cf(IndexifyObjectsColumns::GraphInvocationCtx)?;
        for (id, ctx) in all_graph_invocation_ctx {
            invocation_ctx.insert(id, ctx);
        }
        let mut queued_reduction_tasks = im::OrdMap::new();
        let all_reduction_tasks: Vec<(String, ReduceTask)> =
            reader.get_all_rows_from_cf(IndexifyObjectsColumns::ReductionTasks)?;
        for (_id, task) in all_reduction_tasks {
            queued_reduction_tasks.insert(task.key(), task);
        }
        Ok(Self {
            namespaces,
            compute_graphs,
            compute_graph_versions,
            executors,
            allocated_tasks,
            tasks,
            unallocated_tasks,
            invocation_ctx,
            queued_reduction_tasks,
        })
    }

    pub fn update_state(
        &mut self,
        state_machine_update_request: &StateMachineUpdateRequest,
    ) -> Result<()> {
        match &state_machine_update_request.payload {
            RequestPayload::CreateNameSpace(req) => {
                self.namespaces.insert(req.name.clone(), [0; 0]);
            }
            RequestPayload::CreateOrUpdateComputeGraph(req) => {
                self.compute_graphs.insert(
                    format!("{}|{}", req.namespace, req.compute_graph.name),
                    req.compute_graph.clone(),
                );
                self.compute_graph_versions.insert(
                    format!(
                        "{}|{}|{}",
                        req.namespace, req.compute_graph.name, &req.compute_graph.version.0,
                    ),
                    req.compute_graph.into_version().clone(),
                );
            }
            RequestPayload::DeleteComputeGraphRequest(req) => {
                self.compute_graphs
                    .remove(&format!("{}|{}", req.namespace, req.name));
                self.compute_graph_versions
                    .remove(&format!("{}|{}", req.namespace, req.name));
                let key = format!("{}|{}", req.namespace, req.name);
                let mut graph_ctx_to_remove = vec![];
                for (k, _v) in self.invocation_ctx.range(key.clone()..key.clone()) {
                    graph_ctx_to_remove.push(k.clone());
                }
                for k in graph_ctx_to_remove {
                    self.invocation_ctx.remove(&k);
                }
            }
            RequestPayload::FinalizeTask(req) => {
                self.allocated_tasks
                    .entry(req.executor_id.get().to_string())
                    .or_default()
                    .remove(&req.task_id.to_string());
                //self.invocation_ctx = self.invocation_ctx.update(req.invocation_ctx.key(), req.invocation_ctx.clone());
            }
            RequestPayload::TaskAllocationProcessorUpdate(req) => {
                for allocation in &req.allocations {
                    self.allocated_tasks
                        .entry(allocation.executor.get().to_string())
                        .or_default()
                        .insert(allocation.task.id.to_string());
                }
                // for unplaced_task in &req.unplaced_task_keys {
                //     self.unallocated_tasks
                //         .insert(unplaced_task.to_string(), [0; 0]);
                // }
            }
            // RequestPayload::TaskCreatorUpdate(req) => {
            //     for task in &req.task_requests {
            //         self.tasks.insert(task.key(), task.clone());
            //     }
            //     for task in &req.reduction_tasks.new_reduction_tasks {
            //         self.queued_reduction_tasks.insert(task.key(), task.clone());
            //     }
            //     for task in &req.reduction_tasks.processed_reduction_tasks {
            //         self.queued_reduction_tasks.remove(task);
            //     }
            //     if let Some(updated_invocation_ctx) = &req.invocation_ctx {
            //         self.invocation_ctx = self.invocation_ctx.update(updated_invocation_ctx.key(), updated_invocation_ctx.clone());
                    
            //     }
            // }
            _ => {}
        }
        Ok(())
    }

    pub fn next_queued_task(
        &self,
        ns: &str,
        cg: &str,
        inv: &str,
        c_fn: &str,
    ) -> Option<ReduceTask> {
        let key = format!("{}|{}|{}|{}", ns, cg, inv, c_fn);
        self.queued_reduction_tasks
            .range(key.clone()..key.clone())
            .next()
            .map(|(_, v)| v.clone())
    }

    pub fn get_in_memory_state(&self) -> Self {
        InMemoryState {
            namespaces: self.namespaces.clone(),
            compute_graphs: self.compute_graphs.clone(),
            compute_graph_versions: self.compute_graph_versions.clone(),
            executors: self.executors.clone(),
            allocated_tasks: self.allocated_tasks.clone(),
            tasks: self.tasks.clone(),
            unallocated_tasks: self.unallocated_tasks.clone(),
            invocation_ctx: self.invocation_ctx.clone(),
            queued_reduction_tasks: self.queued_reduction_tasks.clone(),
        }
    }
}