use std::collections::{HashMap, HashSet};

use crate::state::store::{ExecutorId, TaskId};

#[derive(Debug, Clone)]
pub struct TaskAllocationPlan(pub HashMap<TaskId, ExecutorId>);

impl From<TaskAllocationPlan> for HashMap<TaskId, ExecutorId> {
    fn from(plan: TaskAllocationPlan) -> Self {
        plan.0
    }
}

impl TaskAllocationPlan {
    pub fn into_tasks_by_executor(self) -> HashMap<ExecutorId, HashSet<TaskId>> {
        let mut tasks_by_executor: HashMap<ExecutorId, HashSet<TaskId>> = HashMap::new();
        for (task_id, executor_id) in self.0 {
            tasks_by_executor
                .entry(executor_id)
                .or_insert_with(HashSet::new)
                .insert(task_id);
        }
        tasks_by_executor
    }
}
