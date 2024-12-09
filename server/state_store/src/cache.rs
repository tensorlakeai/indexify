use anyhow::Result;
use data_model::{ExecutorId, ExecutorMetadata};
use im::{HashMap, HashSet};

use crate::{
    requests::{RequestPayload, StateMachineUpdateRequest},
    scanner::StateReader,
};

#[derive(Debug, Clone)]
pub struct CEInMemoryState {
    pub executors_idx: HashMap<ExecutorId, ExecutorMetadata>,

    // Reverse index by images available on the executor
    // key: image_name|image_version
    // value: set of executor ids
    pub executors_images_idx: HashMap<String, HashSet<ExecutorId>>,

    // Reverse Index by labels for executors
    pub executors_labels_idx: HashMap<String, HashSet<ExecutorId>>,
}

impl CEInMemoryState {
    pub fn new(state_reader: StateReader) -> Result<CEInMemoryState> {
        let executors = state_reader.get_all_executors()?;

        let mut executors_idx = HashMap::new();
        let mut executors_labels_idx = HashMap::new();
        let mut executors_images_idx = HashMap::new();
        for executor in executors.iter() {
            executors_idx.insert(executor.id.clone(), executor.clone());

            for (k, v) in executor.labels.iter() {
                let key = format!("{}={}", k, v);
                executors_labels_idx
                    .entry(key)
                    .or_insert_with(HashSet::new)
                    .insert(executor.id.clone());
            }

            for image in executor.images.iter() {
                let key = format!("{}:{}", image.name, image.version);
                executors_images_idx
                    .entry(key)
                    .or_insert_with(HashSet::new)
                    .insert(executor.id.clone());
            }
        }

        Ok(CEInMemoryState {
            executors_idx,
            executors_labels_idx,
            executors_images_idx,
        })
    }

    pub fn update(&mut self, request: &StateMachineUpdateRequest) -> Result<()> {
        match &request.payload {
            RequestPayload::RegisterExecutor(register_executor_request) => {
                let executor = register_executor_request.executor.clone();
                self.executors_idx
                    .insert(executor.id.clone(), executor.clone());

                for image in executor.images.iter() {
                    let key = format!("{}:{}", image.name, image.version);
                    self.executors_images_idx
                        .entry(key)
                        .or_insert_with(HashSet::new)
                        .insert(executor.id.clone());
                }

                for (k, v) in executor.labels.iter() {
                    let value = match v {
                        serde_json::Value::String(s) => s.clone(),
                        _ => serde_json::to_string(v).unwrap(),
                    };
                    let key = format!("{}={}", k, value);
                    self.executors_labels_idx
                        .entry(key)
                        .or_insert_with(HashSet::new)
                        .insert(executor.id.clone());
                }
            }
            RequestPayload::DeregisterExecutor(deregister_executor_request) => {
                let executor_metadata = self
                    .executors_idx
                    .remove(&deregister_executor_request.executor_id);
                if let Some(executor_metadata) = executor_metadata {
                    for image in executor_metadata.images.iter() {
                        let key = format!("{}:{}", image.name, image.version);
                        self.executors_images_idx.entry(key).and_modify(|v| {
                            v.remove(&executor_metadata.id);
                        });
                    }
                    for (k, v) in executor_metadata.labels.iter() {
                        let key = format!("{}={}", k, v);
                        self.executors_labels_idx.entry(key).and_modify(|v| {
                            v.remove(&executor_metadata.id);
                        });
                    }
                }
            }
            RequestPayload::SchedulerUpdate(_scheduler_update_request) => {
                // TODO update executor resources
            }
            RequestPayload::FinalizeTask(_finish_task_request) => {
                // TODO update executor resources
            }
            _ => {}
        }
        Ok(())
    }
}
