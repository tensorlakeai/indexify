use std::sync::Arc;

use anyhow::{anyhow, Result};
use data_model::{ExecutorId, Node, ReduceTask, RuntimeInformation, Task};
use rand::seq::SliceRandom;
use state_store::{requests::TaskPlacement, IndexifyState};
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

pub struct TaskScheduler {
    indexify_state: Arc<IndexifyState>,
}

impl TaskScheduler {
    pub fn new(indexify_state: Arc<IndexifyState>) -> Self {
        Self { indexify_state }
    }

    pub fn schedule_unplaced_tasks(&self) -> Result<Vec<TaskPlacement>> {
        let tasks = self.indexify_state.reader().unallocated_tasks()?;
        self.schedule_tasks(tasks)
    }

    pub fn schedule_tasks(&self, tasks: Vec<Task>) -> Result<Vec<TaskPlacement>> {
        let mut task_allocations = Vec::new();
        for task in tasks {
            let cg = self
                .indexify_state
                .reader()
                .get_compute_graph(&task.namespace, &task.compute_graph_name)?
                .ok_or(anyhow!("Compute graph not found"))?;
            let compute_fn = cg
                .nodes
                .get(&task.compute_fn_name)
                .ok_or(anyhow!("Compute fn not found"))?;
            let executor_ids = self.filter_executors(&compute_fn, &cg.runtime_information)?;
            let executor_id = executor_ids.choose(&mut rand::thread_rng());
            if let Some(executor_id) = executor_id {
                info!("Assigning task {:?} to executor {:?}", task.id, executor_id);
                task_allocations.push(TaskPlacement {
                    task,
                    executor: executor_id.clone(),
                });
            }
        }
        Ok(task_allocations)
    }

    fn filter_executors(
        &self,
        node: &Node,
        runtime_information: &RuntimeInformation,
    ) -> Result<Vec<ExecutorId>> {
        let executors = self.indexify_state.reader().get_all_executors()?;
        let mut filtered_executors = Vec::new();

        for executor in &executors {
            let raw_minor_version = executor.labels.get("python_minor_version");
            if let Some(minor_version) = raw_minor_version {
                let minor_version = serde_json::from_value::<u8>(minor_version.clone());
                if let Ok(minor_version) = minor_version {
                    if minor_version != runtime_information.minor_version {
                        info!(
                            "skipping executor {} because python version does not match",
                            executor.id
                        );
                        continue;
                    }
                    info!("executor {} has python_minor_version label", executor.id);
                } else {
                    error!("failed to parse python_minor_version label");
                    continue;
                }
            }

            if executor.image_name != node.image_name() {
                continue;
            }

            if node.matches_executor(executor) {
                filtered_executors.push(executor.id.clone());
            }
        }
        Ok(filtered_executors)
    }
}
