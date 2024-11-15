use std::{
    collections::HashMap,
    fmt::Display,
    sync::{Arc, RwLock},
};

use data_model::{Task, TaskOutcome};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Hash, PartialEq, Eq)]
pub struct FnMetricsId {
    pub namespace: String,
    pub compute_graph: String,
    pub compute_fn: String,
}

impl FnMetricsId {
    pub fn from_task(task: &Task) -> Self {
        Self {
            namespace: task.namespace.clone(),
            compute_graph: task.compute_graph_name.clone(),
            compute_fn: task.compute_fn_name.clone(),
        }
    }
}

impl Display for FnMetricsId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{}",
            self.namespace, self.compute_graph, self.compute_fn
        )
    }
}

/*
 * StateStoreMetrics is a struct that holds metrics for the state store.
 * It keeps track of the number of tasks completed, the number of tasks
 * completed with errors, the number of assigned tasks, and the number of
 * unassigned tasks. Currently metrics are not persisted across restarts
 * TODO: When the server starts up, we should scan the database for assigned
 * and unassigned tasks. But for now, it's fine to just emit metrics which
 * reflect the current state of the system since starting the server.
 */
#[derive(Clone, Debug, Default)]
pub struct StateStoreMetrics {
    pub tasks_completed: Arc<RwLock<u64>>,
    pub tasks_completed_with_errors: Arc<RwLock<u64>>,
    pub assigned_tasks: Arc<RwLock<HashMap<FnMetricsId, u64>>>,
    pub unassigned_tasks: Arc<RwLock<HashMap<FnMetricsId, u64>>>,
    pub executors_online: Arc<RwLock<u64>>,
    pub tasks_by_executor: Arc<RwLock<HashMap<String, u64>>>,
}

impl StateStoreMetrics {
    pub fn new() -> Self {
        Self {
            tasks_completed: Arc::new(RwLock::new(0)),
            tasks_completed_with_errors: Arc::new(RwLock::new(0)),
            assigned_tasks: Arc::new(RwLock::new(HashMap::new())),
            unassigned_tasks: Arc::new(RwLock::new(HashMap::new())),
            executors_online: Arc::new(RwLock::new(0)),
            tasks_by_executor: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn update_task_completion(&self, outcome: TaskOutcome, task: Task, executor_id: &str) {
        self.tasks_by_executor
            .write()
            .unwrap()
            .entry(executor_id.to_string())
            .and_modify(|e| *e -= 1)
            .or_insert(0);
        match outcome {
            TaskOutcome::Success => {
                let mut tasks_completed = self.tasks_completed.write().unwrap();
                *tasks_completed += 1;
            }
            TaskOutcome::Failure => {
                let mut tasks_completed_with_errors =
                    self.tasks_completed_with_errors.write().unwrap();
                *tasks_completed_with_errors += 1;
            }
            _ => (),
        }
        let id = FnMetricsId::from_task(&task);
        let mut count = self.assigned_tasks.write().unwrap();
        if *count.entry(id.clone()).or_insert(0) > 0 {
            *count.entry(id).or_insert(0) -= 1;
        }
    }

    pub fn task_unassigned(&self, tasks: Vec<Task>) {
        for task in tasks {
            let id = FnMetricsId::from_task(&task);
            let mut count = self.unassigned_tasks.write().unwrap();
            *count.entry(id).or_insert(0) += 1;
        }
    }

    pub fn task_assigned(&self, tasks: Vec<Task>, executor_id: &str) {
        self.tasks_by_executor
            .write()
            .unwrap()
            .entry(executor_id.to_string())
            .and_modify(|e| *e += tasks.len() as u64)
            .or_insert(tasks.len() as u64);
        for task in tasks {
            let id = FnMetricsId::from_task(&task);
            let mut count_assigned = self.assigned_tasks.write().unwrap();
            *count_assigned.entry(id.clone()).or_insert(0) += 1;
            let mut count_unassigned = self.unassigned_tasks.write().unwrap();
            if *count_unassigned.entry(id.clone()).or_insert(0) > 0 {
                *count_unassigned.entry(id).or_insert(0) -= 1;
            }
        }
    }

    pub fn add_executor(&self) {
        let mut executors_online = self.executors_online.write().unwrap();
        *executors_online += 1;
    }

    pub fn remove_executor(&self, executor_id: &str) {
        let mut executors_online = self.executors_online.write().unwrap();
        self.tasks_by_executor.write().unwrap().remove(executor_id);
        if *executors_online > 0 {
            *executors_online -= 1;
        }
    }
}
