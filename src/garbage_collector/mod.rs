use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use indexify_internal_api::GarbageCollectionTask;
use rand::seq::IteratorRandom;
use tokio::sync::{broadcast, mpsc::Receiver, RwLock};
use tracing::error;

#[derive(Debug, Clone)]
pub enum TaskStatus {
    Unassigned,
    Assigned(String), //  contains the ingestion server id
}

#[derive(Debug, Clone)]
pub struct GCTaskInfo {
    task: GarbageCollectionTask,
    status: TaskStatus,
}

impl GCTaskInfo {
    fn new(task: GarbageCollectionTask) -> Self {
        Self {
            task,
            status: TaskStatus::Unassigned,
        }
    }
}

pub struct GarbageCollector {
    pub ingestion_servers: RwLock<HashSet<String>>,
    pub assigned_gc_tasks: RwLock<HashMap<String, String>>, // gc task id -> ingestion server id
    pub gc_tasks: RwLock<HashMap<String, GCTaskInfo>>,      //  gc task id -> task info
    pub task_deletion_allocation_events_sender:
        broadcast::Sender<(String, indexify_internal_api::GarbageCollectionTask)>,
    pub task_deletion_allocation_events_receiver:
        broadcast::Receiver<(String, indexify_internal_api::GarbageCollectionTask)>,
}

impl GarbageCollector {
    pub fn new() -> Arc<Self> {
        // let (tx, rx) = watch::channel((
        //     "".to_string(),
        //     indexify_internal_api::GarbageCollectionTask::default(),
        // ));
        let (tx, rx) = broadcast::channel(8);
        Arc::new(Self {
            ingestion_servers: RwLock::new(HashSet::new()),
            assigned_gc_tasks: RwLock::new(HashMap::new()),
            gc_tasks: RwLock::new(HashMap::new()),
            task_deletion_allocation_events_sender: tx,
            task_deletion_allocation_events_receiver: rx,
        })
    }

    async fn assign_task_to_server(&self, task: GarbageCollectionTask, server_id: String) {
        let mut tasks_guard = self.gc_tasks.write().await;
        let task_info = tasks_guard
            .entry(task.id.clone())
            .or_insert_with(|| GCTaskInfo::new(task.clone()));
        task_info.status = TaskStatus::Assigned(server_id.clone());

        if let Err(e) = self
            .task_deletion_allocation_events_sender
            .send((server_id, task))
        {
            error!("Unable to send task allocation event: {}", e);
        }
    }

    async fn choose_server(&self) -> Option<String> {
        let servers = self.ingestion_servers.read().await;
        let mut rng = rand::thread_rng();
        servers.iter().choose(&mut rng).cloned()
    }

    async fn watch_deletion_events(
        &self,
        mut rx: Receiver<indexify_internal_api::GarbageCollectionTask>,
    ) {
        while let Some(task) = rx.recv().await {
            let server = self.choose_server().await;

            if let Some(server) = server {
                self.assign_task_to_server(task.clone(), server).await;
            } else {
                error!("No server available to assign task to");
            }
        }
    }

    pub fn start_watching_deletion_events(
        self: Arc<Self>,
        rx: Receiver<indexify_internal_api::GarbageCollectionTask>,
    ) {
        let gc_clone = self.clone();
        tokio::spawn(async move {
            gc_clone.watch_deletion_events(rx).await;
        });
    }

    pub async fn register_ingestion_server(&self, server_id: String) {
        self.ingestion_servers.write().await.insert(server_id);
    }

    pub async fn remove_ingestion_server(&self, server_id: &str) {
        self.ingestion_servers.write().await.remove(server_id);

        //  Get the tasks that need to be re-assigned
        let mut tasks_to_reassign = Vec::new();
        let mut tasks_to_remove = Vec::new();
        for (task_id, assigned_server_id) in self.assigned_gc_tasks.write().await.iter() {
            if assigned_server_id != server_id {
                continue;
            }
            let mut gc_tasks_guard = self.gc_tasks.write().await;
            if let Some(task) = gc_tasks_guard.get_mut(task_id) {
                task.status = TaskStatus::Unassigned;
                tasks_to_reassign.push(task.clone());
                tasks_to_remove.push(task_id.clone());
            }
        }

        //  Remove the re-assigned tasks from the original list
        let mut assigned_gc_tasks_guard = self.assigned_gc_tasks.write().await;
        for task_id in tasks_to_remove {
            assigned_gc_tasks_guard.remove(&task_id);
        }

        //  Re-assign the tasks
        for task_to_reassign in tasks_to_reassign {
            //  reassign the task here
            let server = self.choose_server().await;
            if let Some(server_id) = server {
                self.assign_task_to_server(task_to_reassign.task, server_id)
                    .await;
            } else {
                error!("No server available to assign task to");
            }
        }
    }

    pub fn subscribe_to_events(
        &self,
    ) -> broadcast::Receiver<(String, indexify_internal_api::GarbageCollectionTask)> {
        self.task_deletion_allocation_events_sender.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use indexify_internal_api::GarbageCollectionTask;
    use tokio::sync::mpsc;

    use crate::garbage_collector::{GarbageCollector, TaskStatus};

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_new_task_deletion_event_allocation() {
        let gc = GarbageCollector::new();
        let gc_clone = Arc::clone(&gc);
        let server_id = "server1".to_string();
        gc.register_ingestion_server(server_id.clone()).await;

        // Simulate receiving a new deletion event
        let (tx, rx) = mpsc::channel(1);
        let task = GarbageCollectionTask::default();
        tx.send(task.clone()).await.unwrap();
        gc_clone.start_watching_deletion_events(rx);

        // Allow some time for the task to be processed
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify the task has been assigned
        let tasks_guard = gc.gc_tasks.read().await;
        let task_info = tasks_guard.get(&task.id).unwrap();
        matches!(task_info.status, TaskStatus::Assigned(ref id) if id == &server_id);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_ingestion_server_removal_and_task_reassignment() {
        let gc = GarbageCollector::new();
        gc.register_ingestion_server("server1".to_string()).await;
        gc.register_ingestion_server("server2".to_string()).await;

        // Assign a task to server1
        let task = GarbageCollectionTask::default();
        gc.assign_task_to_server(task.clone(), "server1".to_string())
            .await;

        // Remove server1 and expect reassignment
        gc.remove_ingestion_server("server1").await;

        // Allow some time for the reassignment to be processed
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify the task has been reassigned to server2
        let tasks_guard = gc.gc_tasks.read().await;
        let task_info = tasks_guard.get(&task.id).unwrap();
        matches!(task_info.status, TaskStatus::Assigned(ref id) if id == "server2");
    }
}
