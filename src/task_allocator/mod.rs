use std::collections::{HashMap, HashSet};

use crate::state::{
    store::{ExecutorId, TaskId},
    SharedState,
};

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

    async fn allocate_tasks(&self, task_ids: HashSet<TaskId>) -> Result<(), anyhow::Error> {
        let plan = self.planner.plan_allocations(task_ids).await?;
        self.commit_task_assignments(plan.into()).await?;
        Ok(())
    }

    /// Reschedule all tasks that match an extractor, even if they are already
    /// assigned to a different executor.
    async fn reallocate_all_tasks_matching_extractor(
        &self,
        extractor_name: String,
    ) -> Result<(), anyhow::Error> {
        let task_ids = self
            .shared_state
            .unfinished_tasks_by_extractor(extractor_name.as_str())
            .await?;
        let plan = self.planner.plan_allocations(task_ids).await?;
        self.commit_task_assignments(plan.into()).await?;
        Ok(())
    }

    async fn commit_task_assignments(
        &self,
        task_to_executor_assignments: HashMap<TaskId, ExecutorId>,
    ) -> Result<(), anyhow::Error> {
        self.shared_state
            .commit_task_assignments(task_to_executor_assignments)
            .await
    }
}
