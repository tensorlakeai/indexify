use anyhow::Result;
use indexify_internal_api::{ComputeFunction, ExecutorId, ExecutorMetadata, Task, TaskId};

pub struct TaskPlacement {
    pub task: Task,
    pub executor: ExecutorId,
}

pub struct TaskPlacementRequest {
    pub content_id: String,
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

    pub fn schedule(&self) -> Result<TaskPlacements> {
        // TODO: Implement task scheduling logic
        Ok(TaskPlacements {
            placements: Vec::new(),
        })
    }

    pub fn filter_executors(
        &self,
        request: &TaskPlacementRequest,
    ) -> Result<Vec<ExecutorMetadata>> {
        let mut filtered_executors = Vec::new();

        for executor in &self.executors {}
        Ok(filtered_executors)
    }
}
