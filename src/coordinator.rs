use anyhow::Result;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{error, info};

use crate::{
    attribute_index::AttributeIndexManager,
    extractors::ExtractedEmbeddings,
    internal_api::{self, CreateWork, ExecutorInfo},
    persistence::{
        ExtractedAttributes, ExtractionEventPayload, ExtractorBinding, ExtractorConfig, Repository,
        Work,
    },
    vector_index::VectorIndexManager,
};

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[derive(Debug)]
pub struct Coordinator {
    // Executor ID -> Last Seen Timestamp
    executor_health_checks: Arc<RwLock<HashMap<String, u64>>>,

    executors: Arc<RwLock<HashMap<String, ExecutorInfo>>>,

    // Extractor Name -> [Executor ID]
    extractors_table: Arc<RwLock<HashMap<String, Vec<String>>>>,

    repository: Arc<Repository>,

    vector_index_manager: Arc<VectorIndexManager>,

    attribute_index_manager: Arc<AttributeIndexManager>,

    tx: Sender<CreateWork>,
}

impl Coordinator {
    pub fn new(
        repository: Arc<Repository>,
        vector_index_manager: Arc<VectorIndexManager>,
        attribute_index_manager: Arc<AttributeIndexManager>,
    ) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(32);

        let coordinator = Arc::new(Self {
            executor_health_checks: Arc::new(RwLock::new(HashMap::new())),
            executors: Arc::new(RwLock::new(HashMap::new())),
            extractors_table: Arc::new(RwLock::new(HashMap::new())),
            repository,
            vector_index_manager,
            attribute_index_manager,
            tx,
        });
        let coordinator_clone = coordinator.clone();
        tokio::spawn(async move {
            coordinator_clone.loop_for_work(rx).await.unwrap();
        });
        coordinator
    }

    pub async fn get_executors(&self) -> Result<Vec<ExecutorInfo>> {
        let executors = self.executors.read().unwrap();
        Ok(executors.values().cloned().collect())
    }

    #[tracing::instrument(skip(self))]
    pub async fn record_executor(&self, worker: ExecutorInfo) -> Result<(), anyhow::Error> {
        // First see if the executor is already in the table
        let is_new_executor = self
            .executor_health_checks
            .read()
            .unwrap()
            .get(&worker.id)
            .is_none();
        self.executor_health_checks
            .write()
            .unwrap()
            .insert(worker.id.clone(), worker.last_seen);
        if is_new_executor {
            info!("recording new executor: {}", &worker.id);
            self.executors
                .write()
                .unwrap()
                .insert(worker.id.clone(), worker.clone());
            let mut extractors_table = self.extractors_table.write().unwrap();
            let executors = extractors_table
                .entry(worker.extractor.name.clone())
                .or_default();
            executors.push(worker.id.clone());
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn process_extraction_events(&self) -> Result<(), anyhow::Error> {
        let events = self.repository.unprocessed_extraction_events().await?;
        for event in &events {
            info!("processing extraction event: {}", event.id);
            match &event.payload {
                ExtractionEventPayload::ExtractorBindingAdded { repository, id } => {
                    let binding = self.repository.binding_by_id(repository, id).await?;
                    self.generate_work_for_extractor_bindings(repository, &binding)
                        .await?;
                }
                ExtractionEventPayload::CreateContent { content_id } => {
                    if let Err(err) = self
                        .create_work(&event.repository_id, Some(content_id))
                        .await
                    {
                        error!("unable to create work: {}", &err.to_string());
                        return Err(err);
                    }
                }
            };

            self.repository
                .mark_extraction_event_as_processed(&event.id)
                .await?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn generate_work_for_extractor_bindings(
        &self,
        repository: &str,
        extractor_binding: &ExtractorBinding,
    ) -> Result<(), anyhow::Error> {
        let content_list = self
            .repository
            .content_with_unapplied_extractor(repository, extractor_binding, None)
            .await?;
        for content in content_list {
            self.create_work(repository, Some(&content.id)).await?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn distribute_work(&self) -> Result<(), anyhow::Error> {
        let unallocated_work = self.repository.unallocated_work().await?;

        // work_id -> executor_id
        let mut work_assignment = HashMap::new();
        for work in unallocated_work {
            let extractor_table = self.extractors_table.read().unwrap();
            let executors = extractor_table.get(&work.extractor).ok_or(anyhow::anyhow!(
                "no executors for extractor: {}",
                work.extractor
            ))?;
            let rand_index = rand::random::<usize>() % executors.len();
            if !executors.is_empty() {
                let executor_id = executors[rand_index].clone();
                work_assignment.insert(work.id.clone(), executor_id);
            }
        }
        info!("finishing work assignment: {:}", work_assignment.len());
        self.repository.assign_work(work_assignment).await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn create_work(
        &self,
        repository_id: &str,
        content_id: Option<&str>,
    ) -> Result<(), anyhow::Error> {
        let extractor_bindings = self
            .repository
            .repository_by_name(repository_id)
            .await?
            .extractor_bindings;
        for extractor_binding in &extractor_bindings {
            let content_list = self
                .repository
                .content_with_unapplied_extractor(repository_id, extractor_binding, content_id)
                .await?;
            for content in content_list {
                info!(
                    "Creating work for repository: {}, content: {}, extractor: {}, index: {}",
                    &repository_id,
                    &content.id,
                    &extractor_binding.extractor_name,
                    &extractor_binding.index_name
                );
                let work = Work::new(
                    &content.id,
                    repository_id,
                    &extractor_binding.index_name,
                    &extractor_binding.extractor_name,
                    &extractor_binding.input_params,
                    None,
                );
                self.repository.insert_work(&work).await?;
                self.repository
                    .mark_content_as_processed(&work.content_id, &extractor_binding.id)
                    .await?;
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn record_extractor(&self, extractor: ExtractorConfig) -> Result<(), anyhow::Error> {
        self.repository.record_extractors(vec![extractor]).await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_work_for_worker(
        &self,
        worker_id: &str,
    ) -> Result<Vec<internal_api::Work>, anyhow::Error> {
        let work_list = self.repository.work_for_worker(worker_id).await?;
        let mut result = Vec::new();
        for work in work_list {
            let content_payload = self
                .repository
                .content_from_repo(&work.content_id, &work.repository_id)
                .await?;
            let internal_api_work = internal_api::create_work(work, content_payload)?;
            result.push(internal_api_work);
        }

        Ok(result)
    }

    #[tracing::instrument(skip(self, rx))]
    async fn loop_for_work(&self, mut rx: Receiver<CreateWork>) -> Result<(), anyhow::Error> {
        info!("starting work distribution loop");
        loop {
            if (rx.recv().await).is_none() {
                info!("no work to process");
                return Ok(());
            }
            if let Err(err) = self.process_and_distribute_work().await {
                error!("unable to process and distribute work: {}", err.to_string());
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn process_and_distribute_work(&self) -> Result<(), anyhow::Error> {
        info!("received work request, processing extraction events");
        self.process_extraction_events().await?;

        info!("doing distribution of work");
        self.distribute_work().await?;
        Ok(())
    }

    pub async fn get_executor(&self, extractor_name: &str) -> Result<ExecutorInfo, anyhow::Error> {
        let extractors_table = self.extractors_table.read().unwrap();
        let executors = extractors_table.get(extractor_name).ok_or(anyhow::anyhow!(
            "no executors for extractor: {}",
            extractor_name
        ))?;
        let rand_index = rand::random::<usize>() % executors.len();
        let executor_id = executors[rand_index].clone();
        let executors = self.executors.read().unwrap();
        let executor = executors
            .get(&executor_id)
            .ok_or(anyhow::anyhow!("no executor found for id: {}", executor_id))?;
        Ok(executor.clone())
    }

    pub async fn publish_work(&self, work: CreateWork) -> Result<(), anyhow::Error> {
        self.tx.send(work).await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn write_extracted_data(
        &self,
        work_status_list: Vec<internal_api::WorkStatus>,
    ) -> Result<()> {
        for work_status in work_status_list {
            let work = self
                .repository
                .update_work_state(&work_status.work_id, &work_status.status.into())
                .await?;
            if let Some(data) = work_status.data {
                match data.extracted_data {
                    internal_api::ExtractedData::Embeddings { embedding } => {
                        let embeddings = ExtractedEmbeddings {
                            content_id: work.content_id.clone(),
                            text: data.source.clone(),
                            embeddings: embedding.clone(),
                        };
                        self.vector_index_manager
                            .add_embedding(&work.repository_id, &work.index_name, vec![embeddings])
                            .await?;
                    }
                    internal_api::ExtractedData::Attributes { attributes } => {
                        let extracted_attributes = ExtractedAttributes::new(
                            &data.content_id,
                            attributes.clone(),
                            &work.extractor,
                        );
                        self.attribute_index_manager
                            .add_index(&work.repository_id, &work.index_name, extracted_attributes)
                            .await?;
                    }
                };
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        blob_storage::BlobStorageBuilder,
        persistence::ExtractorBinding,
        test_util::{
            self,
            db_utils::{DEFAULT_TEST_EXTRACTOR, DEFAULT_TEST_REPOSITORY},
        },
    };
    use std::collections::HashMap;

    use crate::{
        data_repository_manager::DataRepositoryManager,
        persistence::{ContentPayload, DataRepository},
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_create_work() -> Result<(), anyhow::Error> {
        let db = test_util::db_utils::create_db().await.unwrap();
        let (vector_index_manager, extractor_executor, coordinator) =
            test_util::db_utils::create_index_manager(db.clone()).await;
        let blob_storage =
            BlobStorageBuilder::new_disk_storage("/tmp/indexify_test".to_string()).unwrap();
        let repository_manager =
            DataRepositoryManager::new_with_db(db.clone(), vector_index_manager, blob_storage);

        // Create a repository
        repository_manager
            .create(&DataRepository {
                name: DEFAULT_TEST_REPOSITORY.into(),
                data_connectors: vec![],
                metadata: HashMap::new(),
                extractor_bindings: vec![ExtractorBinding::new(
                    DEFAULT_TEST_REPOSITORY,
                    DEFAULT_TEST_EXTRACTOR.into(),
                    DEFAULT_TEST_EXTRACTOR.into(),
                    vec![],
                    serde_json::json!({}),
                )],
            })
            .await?;

        repository_manager
            .add_texts(
                DEFAULT_TEST_REPOSITORY,
                vec![
                    ContentPayload::from_text(
                        DEFAULT_TEST_REPOSITORY,
                        "hello",
                        HashMap::from([("topic".to_string(), json!("pipe"))]),
                    ),
                    ContentPayload::from_text(
                        DEFAULT_TEST_REPOSITORY,
                        "world",
                        HashMap::from([("topic".to_string(), json!("baz"))]),
                    ),
                ],
            )
            .await?;

        // Insert a new worker and then create work
        coordinator.process_and_distribute_work().await.unwrap();

        let work_list = coordinator
            .get_work_for_worker(&extractor_executor.get_executor_info().id)
            .await?;

        // Check amount of work queued for the worker
        assert_eq!(work_list.len(), 2);
        Ok(())
    }
}
