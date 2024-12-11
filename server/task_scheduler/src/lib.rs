use std::sync::Arc;

use anyhow::{anyhow, Result};
use data_model::{ExecutorId, Node, ReduceTask, RuntimeInformation, Task};
use rand::seq::SliceRandom;
use state_store::{requests::TaskPlacement, IndexifyState};
use tracing::{error, info, span};

pub mod task_creator;

#[derive(Debug)]
pub struct TaskCreationResult {
    pub namespace: String,
    pub compute_graph: String,
    pub tasks: Vec<Task>,
    pub new_reduction_tasks: Vec<ReduceTask>,
    pub processed_reduction_tasks: Vec<String>,
    pub invocation_id: String,
}

impl TaskCreationResult {
    pub fn no_tasks(namespace: &str, compute_graph: &str, invocation_id: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            compute_graph: compute_graph.to_string(),
            invocation_id: invocation_id.to_string(),
            tasks: vec![],
            new_reduction_tasks: vec![],
            processed_reduction_tasks: vec![],
        }
    }
}

pub struct FilteredExecutors {
    pub executors: Vec<ExecutorId>,
}

pub struct TaskPlacementResult {
    pub task_placements: Vec<TaskPlacement>,
}

struct ScheduleTaskResult {
    pub task_placements: Vec<TaskPlacement>,
}

pub struct TaskScheduler {
    indexify_state: Arc<IndexifyState>,
}

impl TaskScheduler {
    pub fn new(indexify_state: Arc<IndexifyState>) -> Self {
        Self { indexify_state }
    }

    pub async fn schedule_unplaced_tasks(&self) -> Result<TaskPlacementResult> {
        let tasks = self.indexify_state.reader().unallocated_tasks()?;
        self.schedule_tasks(tasks).await
    }

    async fn schedule_tasks(&self, tasks: Vec<Task>) -> Result<TaskPlacementResult> {
        let mut task_placements = Vec::new();
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
            match self.schedule_task(task).await {
                Ok(ScheduleTaskResult {
                    task_placements: schedule_task_placements,
                }) => {
                    task_placements.extend(schedule_task_placements);
                }
                Err(err) => {
                    error!("failed to schedule task, skipping: {:?}", err);
                }
            }
        }
        Ok(TaskPlacementResult { task_placements })
    }

    async fn schedule_task(&self, task: Task) -> Result<ScheduleTaskResult> {
        let mut task_placements = Vec::new();
        let cg = self
            .indexify_state
            .reader()
            .get_compute_graph(&task.namespace, &task.compute_graph_name)?
            .ok_or(anyhow!("compute graph not found"))?;
        let compute_fn = cg
            .nodes
            .get(&task.compute_fn_name)
            .ok_or(anyhow!("compute fn not found"))?;
        let filtered_executors = self
            .filter_executors(&compute_fn, &cg.runtime_information)
            .await?;
        let executor_id = filtered_executors.executors.choose(&mut rand::thread_rng());
        if let Some(executor_id) = executor_id {
            info!("assigning task {:?} to executor {:?}", task.id, executor_id);
            task_placements.push(TaskPlacement {
                task,
                executor: executor_id.clone(),
            });
        }
        return Ok(ScheduleTaskResult { task_placements });
    }

    async fn filter_executors(
        &self,
        node: &Node,
        graph_runtime: &RuntimeInformation,
    ) -> Result<FilteredExecutors> {
        let state_cache = self.indexify_state.get_state_cache().await;
        let filtered_executors = node.filter_executors(
            &state_cache.executors_idx,
            &state_cache.executors_images_idx,
            &state_cache.executors_labels_idx,
            graph_runtime,
        );
        Ok(FilteredExecutors {
            executors: filtered_executors,
        })
    }
}
