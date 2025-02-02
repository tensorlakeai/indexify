use anyhow::Result;
use data_model::{ComputeGraph, ExecutorMetadata, Task};
use im::HashSet;

use crate::{
    requests::{RequestPayload, StateMachineUpdateRequest},
    scanner::StateReader,
    state_machine::IndexifyObjectsColumns,
};

pub struct InMemoryState {
    namespaces: im::HashMap<String, [u8; 0]>,

    // Namespace|CG Name -> ComputeGraph
    compute_graphs: im::HashMap<String, ComputeGraph>,

    // ExecutorId -> ExecutorMetadata
    executors: im::HashMap<String, ExecutorMetadata>,

    // Executor Id -> List of Task IDs
    allocated_tasks: im::HashMap<String, HashSet<String>>,
    // Task ID -> Task
    tasks: im::HashMap<String, Task>,

    // Task Keys
    unallocated_tasks: im::HashMap<String, [u8; 0]>,
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
        let mut tasks = im::HashMap::new();
        for (id, task) in all_tasks {
            tasks.insert(id, task);
        }
        let all_unallocated_tasks: Vec<(String, [u8; 0])> =
            reader.get_all_rows_from_cf(IndexifyObjectsColumns::UnallocatedTasks)?;
        let mut unallocated_tasks = im::HashMap::new();
        for (id, task) in all_unallocated_tasks {
            unallocated_tasks.insert(id, task);
        }
        Ok(Self {
            namespaces,
            compute_graphs,
            executors,
            allocated_tasks,
            tasks,
            unallocated_tasks,
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
            }
            RequestPayload::DeleteComputeGraphRequest(req) => {
                self.compute_graphs
                    .remove(&format!("{}|{}", req.namespace, req.name));
            }
            RequestPayload::FinalizeTask(req) => {
                self.allocated_tasks
                    .entry(req.executor_id.get().to_string())
                    .or_default()
                    .remove(&req.task_id.to_string());
            }
            RequestPayload::TaskAllocationProcessorUpdate(req) => {
                for allocation in &req.allocations {
                    self.allocated_tasks
                        .entry(allocation.executor.get().to_string())
                        .or_default()
                        .insert(allocation.task.id.to_string());
                }
                for unplaced_task in &req.unplaced_task_keys {
                    self.unallocated_tasks
                        .insert(unplaced_task.to_string(), [0; 0]);
                }
            }
            RequestPayload::TaskCreatorUpdate(req) => {
                for task in &req.task_requests {
                    self.tasks.insert(task.id.to_string(), task.clone());
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub fn get_in_memory_state(&self) -> Self {
        InMemoryState {
            namespaces: self.namespaces.clone(),
            compute_graphs: self.compute_graphs.clone(),
            executors: self.executors.clone(),
            allocated_tasks: self.allocated_tasks.clone(),
            tasks: self.tasks.clone(),
            unallocated_tasks: self.unallocated_tasks.clone(),
        }
    }
}
