use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::internal_api::{Task, TaskResult};

pub struct TaskStore {
    pending: Arc<RwLock<HashMap<String, Task>>>,
    finished: Arc<RwLock<HashMap<String, TaskResult>>>,
}

impl TaskStore {
    pub fn new() -> Self {
        Self {
            pending: Arc::new(RwLock::new(HashMap::new())),
            finished: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn clear_completed_work(&self) {
        self.finished.write().unwrap().clear();
    }

    pub fn add(&self, tasks: Vec<Task>) {
        let mut pending = self.pending.write().unwrap();
        for task in tasks {
            pending.insert(task.id.clone(), task);
        }
    }

    pub fn update(&self, task_results: Vec<TaskResult>) {
        let mut pending = self.pending.write().unwrap();
        let mut finished = self.finished.write().unwrap();
        for task_result in task_results {
            pending.remove(&task_result.task_id);
            finished.insert(task_result.task_id.clone(), task_result);
        }
    }

    pub fn pending_tasks(&self) -> Vec<Task> {
        let pending = self.pending.read().unwrap();
        pending.values().cloned().collect()
    }

    pub fn finished_tasks(&self) -> Vec<TaskResult> {
        let finished = self.finished.read().unwrap();
        finished.values().cloned().collect()
    }
}
