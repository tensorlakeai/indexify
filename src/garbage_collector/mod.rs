use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use indexify_internal_api::{ContentMetadata, GarbageCollectionTask};
use rand::seq::IteratorRandom;
use tokio::sync::{broadcast, RwLock};
use tracing::error;

// pub fn start_watching_deletion_events(
//     garbage_collector: Arc<GarbageCollector>,
//     rx: Receiver<indexify_internal_api::GarbageCollectionTask>,
// ) {
//     tokio::spawn(async move {
//         garbage_collector.watch_deletion_events(rx).await;
//     });
// }

#[derive(Debug, Clone, PartialEq)]
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
        let (tx, rx) = broadcast::channel(8);
        Arc::new(Self {
            ingestion_servers: RwLock::new(HashSet::new()),
            assigned_gc_tasks: RwLock::new(HashMap::new()),
            gc_tasks: RwLock::new(HashMap::new()),
            task_deletion_allocation_events_sender: tx,
            task_deletion_allocation_events_receiver: rx,
        })
    }

    fn send_task(
        &self,
        server_id: String,
        task: GarbageCollectionTask,
    ) -> Result<usize, anyhow::Error> {
        self.task_deletion_allocation_events_sender
            .send((server_id, task))
            .map_err(|e| {
                error!("Unable to send task allocation event: {}", e);
                anyhow::anyhow!("Unable to send task allocation event: {}", e)
            })
    }

    async fn assign_task_to_server(&self, task: GarbageCollectionTask, server_id: String) {
        {
            let mut tasks_guard = self.gc_tasks.write().await;
            let mut tasks_assignment_guard = self.assigned_gc_tasks.write().await;
            //  check the initial condition
            if let Some(task) = tasks_guard.get_mut(&task.id) {
                if task.status != TaskStatus::Unassigned {
                    return;
                }
                task.status = TaskStatus::Assigned(server_id.to_string());
                tasks_assignment_guard.insert(task.task.id.clone(), server_id.clone());
            }
        }

        if let Err(_) = self.send_task(server_id, task.clone()) {
            //  remove task assignment
            let mut tasks_guard = self.gc_tasks.write().await;
            if let Some(task) = tasks_guard.get_mut(&task.id) {
                task.status = TaskStatus::Unassigned;
            }
            let mut assigned_tasks_guard = self.assigned_gc_tasks.write().await;
            assigned_tasks_guard.remove(&task.id);
        }
    }

    async fn choose_server(&self) -> Option<String> {
        let servers = self.ingestion_servers.read().await;
        let mut rng = rand::thread_rng();
        servers.iter().choose(&mut rng).cloned()
    }

    // async fn watch_deletion_events(
    //     &self,
    //     mut rx: Receiver<indexify_internal_api::GarbageCollectionTask>,
    // ) {
    //     while let Some(task) = rx.recv().await {
    //         //  add the task
    //         {
    //             println!("Adding a new task {:?}", task);
    //             let mut tasks_guard = self.gc_tasks.write().await;
    //             tasks_guard
    //                 .entry(task.id.clone())
    //                 .or_insert_with(|| GCTaskInfo::new(task.clone()));
    //         }
    //         let server = self.choose_server().await;

    //         if let Some(server) = server {
    //             println!("Assigning task to server {}", server);
    //             self.assign_task_to_server(task.clone(), server).await;
    //         } else {
    //             error!("No server available to assign task to");
    //         }
    //     }
    // }

    pub async fn mark_gc_task_completed(&self, task_id: &str) {
        let mut tasks_guard = self.gc_tasks.write().await;
        let mut assigned_tasks_guard = self.assigned_gc_tasks.write().await;
        if let Some(_) = tasks_guard.get_mut(task_id) {
            tasks_guard.remove(task_id);
            assigned_tasks_guard.remove(task_id);
        }
    }

    pub async fn register_ingestion_server(&self, server_id: &str) {
        let result = self
            .ingestion_servers
            .write()
            .await
            .insert(server_id.to_string());

        if result {
            //  newly added server
            let mut tasks_guard = self.gc_tasks.write().await;
            for (_, task_info) in tasks_guard.iter_mut() {
                if task_info.status == TaskStatus::Unassigned {
                    let server = self.choose_server().await;
                    if let Some(server_id) = server {
                        task_info.status = TaskStatus::Assigned(server_id.clone());
                        if let Err(_) = self.send_task(server_id, task_info.task.clone()) {
                            //  remove task assignment
                            task_info.status = TaskStatus::Unassigned;
                            let mut assigned_tasks_guard = self.assigned_gc_tasks.write().await;
                            assigned_tasks_guard.remove(&task_info.task.id);
                        }
                    }
                }
            }
        }
    }

    pub async fn remove_ingestion_server(&self, server_id: &str) {
        self.ingestion_servers.write().await.remove(server_id);

        let mut tasks_to_reassign = Vec::new();
        let mut tasks_to_remove = Vec::new();

        //  Get the tasks that need to be re-assigned
        let mut gc_tasks_guard = self.gc_tasks.write().await;
        let mut assigned_gc_tasks_guard = self.assigned_gc_tasks.write().await;
        for (task_id, assigned_server_id) in assigned_gc_tasks_guard.iter() {
            if assigned_server_id != server_id {
                continue;
            }
            if let Some(task) = gc_tasks_guard.get_mut(task_id) {
                task.status = TaskStatus::Unassigned;
                tasks_to_remove.push(task_id.clone());
                tasks_to_reassign.push(task.clone());
            }
        }

        //  Remove the tasks
        for task_id in tasks_to_remove {
            assigned_gc_tasks_guard.remove(&task_id);
        }

        //  Re-assign the tasks
        for mut task_to_reassign in tasks_to_reassign {
            //  reassign the task here
            let server = self.choose_server().await;
            if let Some(server_id) = server {
                task_to_reassign.status = TaskStatus::Assigned(server_id.clone());
                assigned_gc_tasks_guard.insert(task_to_reassign.task.id.clone(), server_id.clone());
                if let Err(_) = self.send_task(server_id, task_to_reassign.task.clone()) {
                    //  remove task assignment
                    task_to_reassign.status = TaskStatus::Unassigned;
                    assigned_gc_tasks_guard.remove(&task_to_reassign.task.id);
                }
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

    pub async fn create_gc_tasks(
        &self,
        content_metadata: Vec<ContentMetadata>,
        outputs: HashMap<String, HashSet<String>>,
        policy_ids: HashMap<String, String>,
    ) -> Result<Vec<GarbageCollectionTask>, anyhow::Error> {
        let mut created_gc_tasks = Vec::new();
        let namespace = content_metadata[0].namespace.clone();
        for content in content_metadata {
            let output_tables = outputs.get(&content.id).cloned().unwrap_or_default();
            let policy_id = policy_ids.get(&content.id).cloned().unwrap_or_default();
            let mut gc_task = indexify_internal_api::GarbageCollectionTask::new(
                &namespace,
                content,
                output_tables,
                &policy_id,
            );
            tracing::info!("created gc task {:?}", gc_task);

            //  add the task
            {
                let mut tasks_guard = self.gc_tasks.write().await;
                tasks_guard
                    .entry(gc_task.id.clone())
                    .or_insert_with(|| GCTaskInfo::new(gc_task.clone()));
            }
            let server = self.choose_server().await;

            //  set the server
            if let Some(server) = server {
                gc_task.assigned_to = Some(server.clone());
                self.assign_task_to_server(gc_task.clone(), server).await;
            } else {
                error!("No server available to assign task to");
            }
            created_gc_tasks.push(gc_task.clone());
        }
        Ok(created_gc_tasks)
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
        gc.register_ingestion_server(&server_id).await;

        // Simulate receiving a new deletion event
        // let (tx, rx) = mpsc::channel(1);
        let task = GarbageCollectionTask::default();
        // tx.send(task.clone()).await.unwrap();
        // super::start_watching_deletion_events(gc_clone, rx);

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
        let gc_clone = Arc::clone(&gc);
        gc.register_ingestion_server("server1").await;

        // Assign a task to server1
        // let (tx, rx) = mpsc::channel(1);
        let task = GarbageCollectionTask::default();
        // tx.send(task.clone()).await.unwrap();
        // super::start_watching_deletion_events(gc_clone, rx);

        // Allow some time for the reassignment to be processed
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Remove server1 and expect reassignment
        gc.remove_ingestion_server("server1").await;
        gc.register_ingestion_server("server2").await;

        // Verify the task has been reassigned to server2
        let tasks_guard = gc.gc_tasks.read().await;
        let task_info = tasks_guard.get(&task.id).unwrap();
        matches!(task_info.status, TaskStatus::Assigned(ref id) if id == "server2");
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_assign_unassigned_gc_tasks_to_new_server() {
        let server_id = "123";
        let gc = GarbageCollector::new();
        let gc_clone = Arc::clone(&gc);

        //  Simulate receiving deletion events
        let (tx, rx) = mpsc::channel(1);
        // super::start_watching_deletion_events(gc_clone, rx);
        let task = GarbageCollectionTask::default();
        tx.send(task.clone()).await.unwrap();
        tx.send(task.clone()).await.unwrap();

        // Allow some time for the task to be processed
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        //  all tasks should be unassigned
        {
            let stored_tasks = gc.gc_tasks.read().await;
            for task in stored_tasks.values() {
                matches!(task.status, TaskStatus::Unassigned);
            }
        }

        //  when a new server is registered, all tasks should be assigned to it
        gc.register_ingestion_server(server_id).await;
        let stored_tasks = gc.gc_tasks.read().await;
        for task in stored_tasks.values() {
            assert_eq!(task.status, TaskStatus::Assigned(server_id.to_string()));
        }
    }
}
