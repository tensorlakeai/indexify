use std::{sync::Arc, vec};

use anyhow::{anyhow, Result};
use data_model::{ComputeGraphVersion, ExecutorId, Node, Task};
use rand::seq::SliceRandom;
use state_store::{
    requests::{TaskPlacement, TaskPlacementDiagnostic},
    IndexifyState,
};
use tracing::{error, info, span};

pub struct FilteredExecutors {
    pub executors: Vec<ExecutorId>,
    pub diagnostic_msgs: Vec<String>,
}

pub struct TaskPlacementResult {
    pub task_placements: Vec<TaskPlacement>,
    pub unplaced_task_keys: Vec<String>,
    pub placement_diagnostics: Vec<TaskPlacementDiagnostic>,
}

struct ScheduleTaskResult {
    pub task_placements: Vec<TaskPlacement>,
    pub unplaced_task_keys: Vec<String>,
    pub diagnostic_msgs: Vec<String>,
}

pub struct TaskAllocationProcessor {
    indexify_state: Arc<IndexifyState>,
}

impl TaskAllocationProcessor {
    pub fn new(indexify_state: Arc<IndexifyState>) -> Self {
        Self { indexify_state }
    }
}
impl TaskAllocationProcessor {
    pub fn schedule_unplaced_tasks(&self) -> Result<TaskPlacementResult> {
        let tasks = self.indexify_state.reader().unallocated_tasks()?;
        self.schedule_tasks(tasks)
    }

    pub fn schedule_tasks(&self, tasks: Vec<Task>) -> Result<TaskPlacementResult> {
        let mut task_placements = Vec::new();
        let mut unplaced_task_keys: Vec<String> = Vec::new();
        let mut placement_diagnostics = Vec::new();
        for task in tasks {
            let span = span!(
                tracing::Level::INFO,
                "allocate_task",
                task_id = task.id.to_string(),
                namespace = task.namespace,
                compute_graph = task.compute_graph_name,
                compute_fn = task.compute_fn_name,
            );
            let _enter = span.enter();

            info!("allocate task {:?}", task.id);
            match self.allocate_task(task.clone()) {
                Ok(schedule_task_results) => {
                    task_placements.extend(schedule_task_results.task_placements);
                    unplaced_task_keys.extend(schedule_task_results.unplaced_task_keys);
                    placement_diagnostics.extend(schedule_task_results.diagnostic_msgs.iter().map(
                        |msg| TaskPlacementDiagnostic {
                            task: task.clone(),
                            message: msg.clone(),
                        },
                    ));
                }
                Err(err) => {
                    error!("failed to allocate task, skipping: {:?}", err);
                }
            }
        }
        Ok(TaskPlacementResult {
            task_placements,
            unplaced_task_keys,
            placement_diagnostics,
        })
    }

    fn allocate_task(&self, task: Task) -> Result<ScheduleTaskResult> {
        let mut task_placements = Vec::new();
        let mut unplaced_task_keys = Vec::new();
        let mut diagnostic_msgs = Vec::new();
        let compute_graph_version = self
            .indexify_state
            .reader()
            .get_compute_graph_version(
                &task.namespace,
                &task.compute_graph_name,
                &task.graph_version,
            )?
            .ok_or(anyhow!("compute graph not found"))?;
        let compute_fn = compute_graph_version
            .nodes
            .get(&task.compute_fn_name)
            .ok_or(anyhow!("compute fn not found"))?;
        let filtered_executors = self.filter_executors(&compute_graph_version, &compute_fn)?;
        if !filtered_executors.diagnostic_msgs.is_empty() {
            diagnostic_msgs.extend(filtered_executors.diagnostic_msgs);
        }
        let executor_id = filtered_executors.executors.choose(&mut rand::thread_rng());
        if let Some(executor_id) = executor_id {
            info!("assigning task {:?} to executor {:?}", task.id, executor_id);
            task_placements.push(TaskPlacement {
                task,
                executor: executor_id.clone(),
            });
        } else {
            unplaced_task_keys.push(task.key());
        }
        Ok(ScheduleTaskResult {
            task_placements,
            unplaced_task_keys,
            diagnostic_msgs,
        })
    }

    fn filter_executors(
        &self,
        compute_graph: &ComputeGraphVersion,
        node: &Node,
    ) -> Result<FilteredExecutors> {
        let executors = self.indexify_state.reader().get_all_executors()?;
        let mut filtered_executors = vec![];

        let mut diagnostic_msgs = vec![];

        for executor in &executors {
            match executor.function_allowlist {
                Some(ref allowlist) => {
                    for func_uri in allowlist {
                        if func_matches(func_uri, compute_graph, node) {
                            filtered_executors.push(executor.id.clone());
                            break;
                        }
                    }
                }
                None => {
                    filtered_executors.push(executor.id.clone());
                }
            }
        }
        if !filtered_executors.is_empty() {
            diagnostic_msgs.clear();
        }
        Ok(FilteredExecutors {
            executors: filtered_executors,
            diagnostic_msgs,
        })
    }
}

fn func_matches(
    func_uri: &data_model::FunctionURI,
    compute_graph: &ComputeGraphVersion,
    node: &Node,
) -> bool {
    func_uri.compute_fn_name.eq(node.name()) &&
        func_uri
            .compute_graph_name
            .eq(&compute_graph.compute_graph_name) &&
        func_uri.version == compute_graph.version &&
        func_uri.namespace.eq(&compute_graph.namespace)
}
