use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use indexify_internal_api::{
    ContentMetadata,
    ContentMetadataId,
    ExtractionGraph,
    GarbageCollectionTask,
    ServerTaskType,
};
use rand::seq::IteratorRandom;
use tokio::sync::RwLock;

pub struct GarbageCollector {
    pub ingestion_servers: RwLock<HashSet<String>>,
    pub gc_tasks: RwLock<HashMap<String, GarbageCollectionTask>>, //  gc task id -> gc task
}

impl GarbageCollector {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            ingestion_servers: RwLock::new(HashSet::new()),
            gc_tasks: RwLock::new(HashMap::new()),
        })
    }

    async fn choose_server(&self) -> Option<String> {
        let servers = self.ingestion_servers.read().await;
        let mut rng = rand::thread_rng();
        servers.iter().choose(&mut rng).cloned()
    }

    pub async fn mark_gc_task_completed(&self, task_id: &str) {
        let mut tasks_guard = self.gc_tasks.write().await;
        if tasks_guard.get_mut(task_id).is_some() {
            tasks_guard.remove(task_id);
        }
    }

    pub async fn register_ingestion_server(&self, server_id: &str) {
        let result = self
            .ingestion_servers
            .write()
            .await
            .insert(server_id.to_string());

        if result {
            //  get all unassigned tasks and try to assign them
            tracing::info!("registering new ingestion server {}", server_id);
            let mut tasks_guard = self.gc_tasks.write().await;
            for (_, task) in tasks_guard.iter_mut() {
                if task.assigned_to.is_none() {
                    task.assigned_to = Some(server_id.to_string());
                }
            }
        }
    }

    pub async fn remove_ingestion_server(&self, server_id: &str) {
        self.ingestion_servers.write().await.remove(server_id);

        //  get all tasks that were assigned to this server and try to re-assign them
        let mut tasks_guard = self.gc_tasks.write().await;
        for (_, task) in tasks_guard.iter_mut() {
            if task.assigned_to == Some(server_id.to_string()) {
                task.assigned_to = None;
                task.assigned_to = self.choose_server().await;
            }
        }
    }

    pub async fn create_delete_index_task(&self, graph: &ExtractionGraph) -> GarbageCollectionTask {
        let output_tables: HashSet<_> = graph
            .extraction_policies
            .iter()
            .map(|p| p.output_table_mapping.values())
            .flatten()
            .cloned()
            .collect();
        let mut gc_task = GarbageCollectionTask::new(
            &graph.namespace,
            ContentMetadata::default(),
            output_tables,
            ServerTaskType::DropIndexes,
        );
        gc_task.assigned_to = self.choose_server().await;
        let mut tasks_guard = self.gc_tasks.write().await;
        tasks_guard.insert(gc_task.id.clone(), gc_task.clone());
        gc_task
    }

    pub async fn create_gc_tasks(
        &self,
        content_metadata: Vec<ContentMetadata>,
        outputs: HashMap<ContentMetadataId, HashSet<String>>,
        task_type: ServerTaskType,
    ) -> Result<Vec<GarbageCollectionTask>, anyhow::Error> {
        let mut created_gc_tasks = Vec::new();
        let namespace = content_metadata[0].namespace.clone();
        for content in content_metadata {
            let output_tables = outputs.get(&content.id).cloned().unwrap_or_default();
            if task_type == ServerTaskType::UpdateLabels && output_tables.is_empty() {
                continue;
            }
            let mut gc_task = indexify_internal_api::GarbageCollectionTask::new(
                &namespace,
                content,
                output_tables,
                task_type,
            );

            //  add and assign the task
            let server = self.choose_server().await;
            gc_task.assigned_to = server;
            let mut tasks_guard = self.gc_tasks.write().await;
            tasks_guard.insert(gc_task.id.clone(), gc_task.clone());
            created_gc_tasks.push(gc_task.clone());
            tracing::info!("created gc task {:?}", gc_task);
        }
        Ok(created_gc_tasks)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use indexify_internal_api::{ContentMetadata, ContentMetadataId, ServerTaskType};

    use crate::garbage_collector::GarbageCollector;

    fn create_data_for_task(
        num: u64,
    ) -> (
        Vec<ContentMetadata>,
        HashMap<ContentMetadataId, HashSet<String>>,
        HashMap<String, String>,
    ) {
        let mut content_metadata = Vec::new();
        let mut outputs = HashMap::new();
        let mut policy_ids = HashMap::new();

        for i in 0..num {
            // let content_id = format!("content_id_{}", i);
            let content_id = ContentMetadataId {
                id: format!("content_id_{}", i),
                ..Default::default()
            };
            let content = ContentMetadata {
                id: content_id.clone(),
                ..Default::default()
            };
            content_metadata.push(content);

            let output_tables = HashSet::from([format!("table_{}", i)]);
            outputs.insert(content_id.clone(), output_tables);

            let policy_id = format!("policy_id_{}", i);
            policy_ids.insert(content_id.to_string().clone(), policy_id);
        }

        (content_metadata, outputs, policy_ids)
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_new_task_deletion_event_allocation() -> Result<(), anyhow::Error> {
        let gc = GarbageCollector::new();
        let server_id = "server1".to_string();
        gc.register_ingestion_server(&server_id).await;

        //  Create a task
        let (content_metadata, outputs, _) = create_data_for_task(1);
        let tasks = gc
            .create_gc_tasks(content_metadata, outputs, ServerTaskType::Delete)
            .await?;

        //  verify task has been stored and assigned
        let tasks_guard = gc.gc_tasks.read().await;
        assert_eq!(tasks.len(), 1);
        for task in tasks {
            let retrieved_task = tasks_guard.get(&task.id).unwrap();
            assert_eq!(retrieved_task, &task);
            assert_eq!(retrieved_task.assigned_to, Some(server_id.clone()));
        }
        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_ingestion_server_removal_and_task_reassignment() -> Result<(), anyhow::Error> {
        let gc = GarbageCollector::new();
        gc.register_ingestion_server("server1").await;

        // Assign a task to server1
        let (content_metadata, outputs, _) = create_data_for_task(1);
        let tasks = gc
            .create_gc_tasks(content_metadata, outputs, ServerTaskType::Delete)
            .await?;

        //  task should be assigned to server 1
        {
            let tasks_guard = gc.gc_tasks.read().await;
            assert_eq!(
                tasks_guard
                    .get(&tasks.first().unwrap().id)
                    .unwrap()
                    .assigned_to,
                Some("server1".to_string())
            );
        }

        // Remove server1
        gc.remove_ingestion_server("server1").await;
        {
            let tasks_guard = gc.gc_tasks.read().await;
            assert_eq!(
                tasks_guard
                    .get(&tasks.first().unwrap().id)
                    .unwrap()
                    .assigned_to,
                None
            );
        }

        //  Register server 2 and check that the tasks have been assigned to this
        gc.register_ingestion_server("server2").await;
        {
            let tasks_guard = gc.gc_tasks.read().await;
            assert_eq!(
                tasks_guard
                    .get(&tasks.first().unwrap().id)
                    .unwrap()
                    .assigned_to,
                Some("server2".to_string())
            );
        }
        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_assign_unassigned_gc_tasks_to_new_server() -> Result<(), anyhow::Error> {
        let server_id = "123";
        let gc = GarbageCollector::new();

        //  Create a couple of tasks
        let (content_metadata, outputs, _) = create_data_for_task(2);
        let _ = gc
            .create_gc_tasks(content_metadata, outputs, ServerTaskType::Delete)
            .await?;

        //  all tasks should be unassigned since there are no ingestion servers
        {
            let stored_tasks = gc.gc_tasks.read().await;
            for task in stored_tasks.values() {
                assert!(task.assigned_to.is_none());
            }
        }

        //  add a new server and all tasks should be assigned to it
        gc.register_ingestion_server(server_id).await;
        {
            let stored_tasks = gc.gc_tasks.read().await;
            for task in stored_tasks.values() {
                assert_eq!(task.assigned_to, Some(server_id.to_string()));
            }
        }

        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_mark_task_completed() -> Result<(), anyhow::Error> {
        let server_id = "123";
        let gc = GarbageCollector::new();

        //  Create a couple of tasks
        gc.register_ingestion_server(server_id).await;
        let (content_metadata, outputs, _) = create_data_for_task(3);
        let tasks = gc
            .create_gc_tasks(content_metadata, outputs, ServerTaskType::Delete)
            .await?;

        //  Mark all tasks as complete and check that they are removed
        for task in tasks {
            gc.mark_gc_task_completed(&task.id).await;
        }
        {
            let stored_tasks = gc.gc_tasks.read().await;
            assert!(stored_tasks.is_empty());
        }

        Ok(())
    }
}
