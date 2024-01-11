use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use tokio::sync::watch;
use tracing::info;

use crate::internal_api::{Task, TaskResult};

#[derive(Debug)]
pub struct TaskStore {
    pending: Arc<RwLock<HashMap<String, Task>>>,
    finished: Arc<RwLock<HashMap<String, TaskResult>>>,
    tx: watch::Sender<()>,
    rx: watch::Receiver<()>,
}

impl TaskStore {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(());
        Self {
            pending: Arc::new(RwLock::new(HashMap::new())),
            finished: Arc::new(RwLock::new(HashMap::new())),
            tx,
            rx,
        }
    }

    pub fn clear_completed_work(&self) {
        self.finished.write().unwrap().clear();
    }

    pub fn add(&self, tasks: Vec<Task>) {
        info!("Adding {} tasks to task store", tasks.len());
        let mut pending = self.pending.write().unwrap();
        for task in tasks {
            pending.insert(task.id.clone(), task);
        }
        self.tx.send(()).unwrap();
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

    pub fn has_finished(&self, task_id: &str) -> bool {
        let finished = self.finished.read().unwrap();
        finished.contains_key(task_id)
    }

    pub fn get_watcher(&self) -> watch::Receiver<()> {
        self.rx.clone()
    }
}
