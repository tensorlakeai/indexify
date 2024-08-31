use anyhow::Result;
use indexify_internal_api::{ComputeFunction, ExecutorId, ExecutorMetadata, TaskId};
use rand::seq::SliceRandom;

pub struct TaskPlacement {
    pub task_id: TaskId,
    pub executor: Option<ExecutorId>,
}

pub struct TaskPlacementRequest {
    pub task_id: TaskId,
    pub compute_fn: ComputeFunction,
}

pub struct TaskPlacements {
    pub placements: Vec<TaskPlacement>,
}

pub struct TaskScheduler {
    pub executors: Vec<ExecutorMetadata>,
}

impl TaskScheduler {
    pub fn new(executors: Vec<ExecutorMetadata>) -> Self {
        Self { executors }
    }

    pub fn schedule(&self, requests: Vec<TaskPlacementRequest>) -> Result<TaskPlacements> {
        let mut placements = Vec::new();
        for request in requests {
            let executor_ids = self.filter_executors(&request)?;
            let executor_id = executor_ids.choose(&mut rand::thread_rng());
            placements.push(TaskPlacement {
                task_id: request.task_id,
                executor: executor_id.cloned(),
            });
        }
        Ok(TaskPlacements { placements })
    }

    fn filter_executors(&self, request: &TaskPlacementRequest) -> Result<Vec<ExecutorId>> {
        let mut filtered_executors = Vec::new();

        for executor in &self.executors {
            if request.compute_fn.matches_executor(executor) {
                filtered_executors.push(executor.id.clone());
            }
        }
        Ok(filtered_executors)
    }
}
