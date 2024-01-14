use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use tokio::sync::watch;
use tracing::info;

use crate::internal_api::{Task, TaskResult};

#[derive(Debug)]
pub struct TaskStore {
    tasks: Arc<RwLock<HashMap<String, Task>>>,
    pending: Arc<RwLock<HashSet<String>>>,
    finished: Arc<RwLock<HashMap<String, TaskResult>>>,
    tx: watch::Sender<()>,
    rx: watch::Receiver<()>,
}

impl TaskStore {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(());
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            pending: Arc::new(RwLock::new(HashSet::new())),
            finished: Arc::new(RwLock::new(HashMap::new())),
            tx,
            rx,
        }
    }

    pub fn clear_completed_task(&self, task_id: &str) {
        self.finished.write().unwrap().remove(task_id);
        self.tasks.write().unwrap().remove(task_id);
    }

    pub fn add(&self, tasks: Vec<Task>) {
        info!("Adding {} tasks to task store", tasks.len());
        let mut pending = self.pending.write().unwrap();
        let mut tasks_store = self.tasks.write().unwrap();
        for task in tasks {
            pending.insert(task.id.clone());
            tasks_store.insert(task.id.clone(), task);
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
        let tasks = self.tasks.read().unwrap();
        pending
            .iter()
            .filter_map(|task_id| tasks.get(task_id).cloned())
            .collect()
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

    pub fn get_task(&self, task_id: &str) -> Option<Task> {
        let tasks = self.tasks.read().unwrap();
        tasks.get(task_id).cloned()
    }
}
