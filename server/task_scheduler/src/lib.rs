use std::sync::Arc;

use anyhow::{anyhow, Result};
use data_model::{ExecutorId, Node, ReduceTask, RuntimeInformation, Task, TaskOutcome};
use rand::seq::SliceRandom;
use state_store::{requests::TaskPlacement, IndexifyState, UnAllocatedTasks};
use tracing::{error, info};

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

pub struct FilteredExecutors {
    pub executors: Vec<ExecutorId>,
    pub diagnostic_msgs: Vec<String>,
}

pub struct TaskPlacementResult {
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
        println!("---- Running schedule_unplace_tasks");
        let tasks = self.indexify_state.reader().unallocated_tasks()?;
        self.schedule_tasks(tasks)
    }

    fn schedule_tasks(&self, tasks: Vec<Task>) -> Result<TaskPlacementResult> {
        let mut task_allocations = Vec::new();
        let mut diagnostic_msgs = Vec::new();

        let unallocated_tasks = &mut self.indexify_state.metrics_data.lock()
            .map_err(|e| {
                anyhow!("Could not read metrics {}", e)
            })?.unallocated_tasks;
        unallocated_tasks.clear();

        if tasks.len() == 1 {
            println!("That single task: {:?}", tasks);
        }

        println!("task len {:?}", tasks.len());

        for task in tasks {
            let cg = self
                .indexify_state
                .clone()
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
                task_allocations.push(TaskPlacement {
                    task,
                    executor: executor_id.clone(),
                });
            } else {
                println!("unassigned task {:?}", task);
                let key = UnAllocatedTasks {
                    image_name: compute_fn.image_name().to_string(),
                    image_version: compute_fn.image_version().to_string(),
                    task_outcome: format!("{:?}", task.outcome)
                };
                *unallocated_tasks.entry(key).or_insert(0) += 1;
            }
        }
        Ok(TaskPlacementResult {
            task_placements: task_allocations,
            diagnostic_msgs,
        })
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
