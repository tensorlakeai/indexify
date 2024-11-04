use std::collections::HashMap;
use data_model::TaskOutcome;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Hash, PartialEq, Eq)]
pub struct FnMetricsId {
    pub namespace: String,
    pub compute_graph: String,
    pub compute_fn: String,
    pub image_name: String,
    pub image_version: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct StateStoreMetrics {
    pub tasks_completed: u64,
    pub tasks_completed_with_errors: u64,
    pub assigned_tasks: HashMap<FnMetricsId, u64>,
    pub unassigned_tasks: HashMap<FnMetricsId, u64>,
}

impl StateStoreMetrics {
    pub fn new() -> Self {
        Self {
            tasks_completed: 0,
            tasks_completed_with_errors: 0,
            assigned_tasks: HashMap::new(),
            unassigned_tasks: HashMap::new(),
        }
    }
    
    pub fn update_task_completion(&mut self, outcome: TaskOutcome, fn_id: FnMetricsId) {
        match outcome {
            TaskOutcome::Success => self.tasks_completed += 1,
            TaskOutcome::Failure => self.tasks_completed_with_errors += 1,
            _ => (),
        }
        let count = self.assigned_tasks.entry(fn_id).or_insert(0);
        *count -= 1;
    }

    pub fn task_assigned(&mut self, id: FnMetricsId) {
        let count_assigned = self.assigned_tasks.entry(id).or_insert(0);
        *count_assigned += 1;
    }
}
