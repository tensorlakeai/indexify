use std::sync::Arc;

use anyhow::{anyhow, Result};
use data_model::{ExecutorId, GraphInvocationCtx, Node, ReduceTask, RuntimeInformation, Task};
use rand::seq::SliceRandom;
use state_store::{requests::TaskPlacement, state_machine::IndexifyObjectsColumns, IndexifyState};
use tracing::{error, info, span};

pub mod task_creator;

#[derive(Debug)]
pub struct TaskCreationResult {
    pub namespace: String,
    pub compute_graph: String,
    pub tasks: Vec<Task>,
    pub new_reduction_tasks: Vec<ReduceTask>,
    pub processed_reduction_tasks: Vec<String>,
    pub invocation_finished: bool,
    pub invocation_id: String,
}

impl TaskCreationResult {
    pub fn no_tasks(namespace: &str, compute_graph: &str, invocation_id: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            compute_graph: compute_graph.to_string(),
            tasks: vec![],
            new_reduction_tasks: vec![],
            processed_reduction_tasks: vec![],
            invocation_finished: false,
            invocation_id: invocation_id.to_string(),
        }
    }
}

pub struct FilteredExecutors {
    pub executors: Vec<ExecutorId>,
    pub diagnostic_msgs: Vec<String>,
}

pub struct TaskPlacementResult {
    pub task_placements: Vec<TaskPlacement>,
    pub diagnostic_msgs: Vec<String>,
}

struct ScheduleTaskResult {
    pub task_placements: Vec<TaskPlacement>,
    pub diagnostic_msgs: Vec<String>,
}

pub struct TaskScheduler {
    indexify_state: Arc<IndexifyState>,
}

impl TaskScheduler {
    pub fn new(indexify_state: Arc<IndexifyState>) -> Self {
        Self { indexify_state }
    }

    pub fn schedule_unplaced_tasks(&self) -> Result<TaskPlacementResult> {
        let tasks = self.indexify_state.reader().unallocated_tasks()?;
        self.schedule_tasks(tasks)
    }

    fn schedule_tasks(&self, tasks: Vec<Task>) -> Result<TaskPlacementResult> {
        let mut task_placements = Vec::new();
        let mut diagnostic_msgs = Vec::new();
        for task in tasks {
            let span = span!(
                tracing::Level::INFO,
                "scheduling_task",
                task_id = task.id.to_string(),
                namespace = task.namespace,
                compute_graph = task.compute_graph_name,
                compute_fn = task.compute_fn_name,
            );
            let _enter = span.enter();

            info!("scheduling task {:?}", task.id);
            match self.schedule_task(task) {
                Ok(ScheduleTaskResult {
                    task_placements: schedule_task_placements,
                    diagnostic_msgs: schedule_diagnostic_msgs,
                }) => {
                    task_placements.extend(schedule_task_placements);
                    diagnostic_msgs.extend(schedule_diagnostic_msgs);
                }
                Err(err) => {
                    error!("failed to schedule task, skipping: {:?}", err);
                }
            }
        }
        Ok(TaskPlacementResult {
            task_placements,
            diagnostic_msgs,
        })
    }

    fn schedule_task(&self, task: Task) -> Result<ScheduleTaskResult> {
        let mut task_placements = Vec::new();
        let mut diagnostic_msgs = Vec::new();
        let cg = self
            .indexify_state
            .reader()
            .get_compute_graph(&task.namespace, &task.compute_graph_name)?
            .ok_or(anyhow!("compute graph not found"))?;
        let compute_fn = cg
            .nodes
            .get(&task.compute_fn_name)
            .ok_or(anyhow!("compute fn not found"))?;
        let filtered_executors = self.filter_executors(&compute_fn, &cg.runtime_information)?;
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
        }
        return Ok(ScheduleTaskResult {
            task_placements,
            diagnostic_msgs,
        });
    }

    fn filter_executors(
        &self,
        node: &Node,
        graph_runtime: &RuntimeInformation,
    ) -> Result<FilteredExecutors> {
        let executors = self.indexify_state.reader().get_all_executors()?;
        let mut filtered_executors = Vec::new();

        let mut diagnostic_msgs = vec![];

        for executor in &executors {
            if let Some(minor_version) = executor.labels.get("python_minor_version") {
                if let Ok(executor_python_minor_version) =
                    serde_json::from_value::<u8>(minor_version.clone())
                {
                    if executor_python_minor_version != graph_runtime.minor_version {
                        info!(
                            "skipping executor {} because python version does not match",
                            executor.id
                        );
                        diagnostic_msgs.push(format!(
                            "executor {} python version: {} does not match function python version: {}",
                            executor.id, executor_python_minor_version, graph_runtime.minor_version
                        ));
                        continue;
                    }
                } else {
                    error!("failed to parse python_minor_version label");
                    continue;
                }
            }

            if executor.image_name != node.image_name() {
                diagnostic_msgs.push(format!(
                    "executor {}, image name: {} does not match function image name {}",
                    executor.id,
                    executor.image_name,
                    node.image_name()
                ));
                continue;
            }

            if node.matches_executor(executor, &mut diagnostic_msgs) {
                filtered_executors.push(executor.id.clone());
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
