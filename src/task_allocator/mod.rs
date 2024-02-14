use std::collections::HashSet;

use anyhow::Result;

use self::planner::plan::TaskAllocationPlan;
use crate::state::{store::TaskId, SharedState};

pub mod planner;

#[allow(dead_code)] // until scheduler integration
pub struct TaskAllocator {
    shared_state: SharedState,
    planner: Box<dyn planner::AllocationPlanner + Send + Sync>,
}

#[allow(dead_code)] // until scheduler integration
impl TaskAllocator {
    pub fn new(shared_state: SharedState) -> Self {
        Self {
            shared_state: shared_state.clone(),
            planner: Box::new(planner::load_aware_distributor::LoadAwareDistributor::new(
                shared_state.clone(),
            )),
        }
    }

    pub async fn allocate_tasks(&self, task_ids: HashSet<TaskId>) -> Result<TaskAllocationPlan> {
        self.planner.plan_allocations(task_ids).await
    }

    /// Reschedule all tasks that match an extractor, even if they are already
    /// assigned to a different executor.
    pub async fn reallocate_all_tasks_matching_extractor(
        &self,
        extractor_name: &str,
    ) -> Result<TaskAllocationPlan> {
        let task_ids = self
            .shared_state
            .unfinished_tasks_by_extractor(extractor_name)
            .await?;
        self.planner.plan_allocations(task_ids).await
    }
}
