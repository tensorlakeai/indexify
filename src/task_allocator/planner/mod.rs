pub mod plan;
pub mod round_robin;
use std::collections::HashSet;

use mockall::*;
use plan::TaskAllocationPlan;

use crate::state::store::TaskId;

pub type AllocationPlannerResult = Result<TaskAllocationPlan, anyhow::Error>;

#[automock]
#[async_trait::async_trait]
pub trait AllocationPlanner {
    async fn plan_allocations(&self, task_ids: HashSet<TaskId>) -> AllocationPlannerResult;
}
