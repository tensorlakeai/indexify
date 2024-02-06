pub mod load_aware_distributor;
pub mod plan;
use std::collections::HashSet;

use plan::TaskAllocationPlan;

use crate::state::store::TaskId;

pub type AllocationPlannerResult = Result<TaskAllocationPlan, anyhow::Error>;

#[async_trait::async_trait]
pub trait AllocationPlanner {
    async fn plan_allocations(&self, tasks: HashSet<TaskId>) -> AllocationPlannerResult;
}
