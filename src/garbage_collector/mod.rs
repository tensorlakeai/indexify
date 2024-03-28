use std::collections::{HashMap, HashSet};

use indexify_internal_api::GarbageCollectionTask;
use tokio::sync::{mpsc::Receiver, watch, RwLock};

use anyhow::Result;

pub struct GarbageCollector {
    pub ingestion_servers: RwLock<HashSet<String>>,
    pub assigned_gc_tasks: HashMap<String, String>,
    pub gc_tasks: HashMap<String, indexify_internal_api::GarbageCollectionTask>,
    pub task_deletion_allocation_events_sender:
        watch::Sender<(String, indexify_internal_api::GarbageCollectionTask)>,
    pub task_deletion_allocation_events_receiver:
        watch::Receiver<(String, indexify_internal_api::GarbageCollectionTask)>,
}

impl GarbageCollector {
    pub fn new() -> Self {
        let (tx, rx) = watch::channel((
            "".to_string(),
            indexify_internal_api::GarbageCollectionTask {
                id: "".to_string(),
                output_index_table_mapping: HashSet::new(),
                outcome: indexify_internal_api::TaskOutcome::Unknown,
            },
        ));
        Self {
            ingestion_servers: RwLock::new(HashSet::new()),
            assigned_gc_tasks: HashMap::new(),
            gc_tasks: HashMap::new(),
            task_deletion_allocation_events_sender: tx,
            task_deletion_allocation_events_receiver: rx,
        }
    }

    pub fn watch_deletion_events(
        &self,
        mut rx: Receiver<indexify_internal_api::GarbageCollectionTask>,
    ) {
        tokio::spawn(async move {
            while let Some(task) = rx.recv().await {
                // Handle received task...
                println!("Received garbage collection task: {:?}", task);
                //  Assign task to an ingestion server and store it
            }
        });
    }

    pub async fn register_ingestion_server(
        &self,
        node_id: String,
    ) -> Result<watch::Receiver<(String, GarbageCollectionTask)>> {
        self.ingestion_servers.write().await.insert(node_id);
        Ok(self.task_deletion_allocation_events_receiver.clone())
    }
}
