use std::collections::{HashMap, HashSet};

use tokio::sync::{mpsc::Receiver, RwLock};

pub struct GarbageCollector {
    pub ingestion_servers: RwLock<HashSet<String>>,
    pub assigned_gc_tasks: RwLock<HashMap<String, String>>,
    pub gc_tasks: RwLock<HashMap<String, indexify_internal_api::GarbageCollectionTask>>,
}

impl GarbageCollector {
    pub fn new() -> Self {
        Self {
            ingestion_servers: RwLock::new(HashSet::new()),
            assigned_gc_tasks: RwLock::new(HashMap::new()),
            gc_tasks: RwLock::new(HashMap::new()),
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
}
