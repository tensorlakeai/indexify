use std::sync::Arc;

use anyhow::{anyhow, Result};
use data_model::{ExecutorId, Node, Task, TaskId};
use rand::seq::SliceRandom;
use state_store::IndexifyState;

pub struct TaskPlacement {
    pub task_id: TaskId,
    pub executor: Option<ExecutorId>,
}

pub struct TaskPlacementRequest {
    pub task_id: TaskId,
    pub executor_id: ExecutorId,
}

pub struct TaskScheduler {
    indexify_state: Arc<IndexifyState>,
}

impl TaskScheduler {
    pub fn new(indexify_state: Arc<IndexifyState>) -> Self {
        Self { indexify_state }
    }

    pub fn schedule_unplaced_tasks(self) -> Result<Vec<TaskPlacement>> {
        let tasks = self.indexify_state.reader().unallocated_tasks()?;
        self.schedule_tasks(tasks)
    }

    pub fn reschedule_tasks(&self, executor_id: &str) -> Result<Vec<TaskPlacement>> {
        let tasks = self
            .indexify_state
            .reader()
            .get_tasks_by_executor(&ExecutorId::new(executor_id.to_string()), 100)?;
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
            let executor_ids = self.filter_executors(&compute_fn)?;
            let executor_id = executor_ids.choose(&mut rand::thread_rng());
            task_allocations.push(TaskPlacement {
                task_id: task.id,
                executor: executor_id.cloned(),
            });
        }
        Ok(task_allocations)
    }

    fn filter_executors(&self, node: &Node) -> Result<Vec<ExecutorId>> {
        let executors = self.indexify_state.reader().get_all_executors()?;
        let mut filtered_executors = Vec::new();

        for executor in &executors {
            if node.matches_executor(executor) {
                filtered_executors.push(executor.id.clone());
            }
        }
        Ok(filtered_executors)
    }
}
