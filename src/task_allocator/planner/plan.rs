use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
};

use crate::state::store::{ExecutorId, TaskId};

#[derive(Debug, Clone)]
pub struct TaskAllocationPlan(pub HashMap<ExecutorId, HashSet<TaskId>>);

impl Deref for TaskAllocationPlan {
    type Target = HashMap<ExecutorId, HashSet<TaskId>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TaskAllocationPlan {
    pub fn into_task_executor_allocations(&self) -> HashMap<TaskId, ExecutorId> {
        let mut task_to_executor_assignments = HashMap::new();
        for (executor_id, task_ids) in self.0.iter() {
            for task_id in task_ids {
                task_to_executor_assignments.insert(task_id.clone(), executor_id.clone());
            }
        }
        task_to_executor_assignments
    }
}
