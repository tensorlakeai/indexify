use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    str::FromStr,
    sync::{Arc, RwLock},
};

use anyhow::{anyhow, Result};
use jsonschema::JSONSchema;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tonic::IntoRequest;
use tracing::{error, info};

use crate::{
    attribute_index::AttributeIndexManager,
    extractor::ExtractedEmbeddings,
    indexify_coordinator,
    internal_api::{self, CreateWork, ExecutorInfo, ExecutorMetadata},
    persistence::{
        self,
        ExtractedAttributes,
        ExtractionEventPayload,
        ExtractorBinding,
        Repository,
        Work,
    },
    state::SharedState,
    vector_index::VectorIndexManager,
    vectordbs::IndexDistance,
};

pub struct Coordinator {
    // Extractor Name -> [Executor ID]
    extractors_table: Arc<RwLock<HashMap<String, Vec<String>>>>,

    repository: Arc<Repository>,

    vector_index_manager: Arc<VectorIndexManager>,

    attribute_index_manager: Arc<AttributeIndexManager>,

    pub shared_state: SharedState,
}

impl Coordinator {
    pub fn new(
        repository: Arc<Repository>,
        vector_index_manager: Arc<VectorIndexManager>,
        attribute_index_manager: Arc<AttributeIndexManager>,
        shared_state: SharedState,
    ) -> Arc<Self> {
        let coordinator = Arc::new(Self {
            extractors_table: Arc::new(RwLock::new(HashMap::new())),
            repository,
            vector_index_manager,
            attribute_index_manager,
            shared_state,
        });
        coordinator
    }

    pub async fn get_executors(&self) -> Result<Vec<ExecutorMetadata>> {
        self.shared_state.get_executors().await
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
                    &extractor_binding.extractor,
                    extractor_binding.name,
                );
                let work = Work::new(
                    &content.id,
                    repository_id,
                    &extractor_binding.extractor,
                    &extractor_binding.name,
                    &extractor_binding.input_params,
                    None,
                );
                self.repository.insert_work(&work).await?;
                self.repository
                    .mark_content_as_processed(&work.content_id, &extractor_binding.name)
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn list_content(
        &self,
        repository: &str,
    ) -> Result<Vec<internal_api::ContentMetadata>> {
        self.shared_state.list_content(repository).await
    }

    pub async fn list_bindings(&self, repository: &str) -> Result<Vec<internal_api::ExtractorBinding>> {
        self.shared_state.list_bindings(repository).await
    }

    pub async fn create_repository(&self, repository: &str) -> Result<()> {
        let _ = self.shared_state.create_repository(repository).await?;
        Ok(())
    }

    pub async fn list_repositories(&self) -> Result<Vec<internal_api::Repository>> {
        self.shared_state.list_repositories().await
    }

    pub async fn get_repository(&self, repository: &str) -> Result<internal_api::Repository> {
        self.shared_state.get_repository(repository).await
    }

    pub async fn list_extractors(&self) -> Result<Vec<internal_api::ExtractorDescription>> {
        self.shared_state.list_extractors().await
    }

    pub async fn create_binding(
        &self,
        binding: internal_api::ExtractorBinding,
    ) -> Result<Vec<String>> {
        let mut indexes = Vec::new();
        let extractor = self
            .shared_state
            .extractor_with_name(&binding.extractor)
            .await?;
        let input_params_schema = JSONSchema::compile(&extractor.input_params).map_err(|e| {
            anyhow!(
                "unable to compile json schema for input params: {:?}, error: {:?}",
                &extractor.input_params,
                e
            )
        })?;
        let extractor_params_schema = binding.input_params.clone();
        let validation_result = input_params_schema.validate(&extractor_params_schema);
        if let Err(errors) = validation_result {
            let errors = errors
                .into_iter()
                .map(|e| e.to_string())
                .collect::<Vec<String>>();
            return Err(anyhow!(
                "unable to validate input params for extractor binding: {}, errors: {}",
                &binding.name,
                errors.join(",")
            ));
        }
        for (name, output_schema) in &extractor.schema.output {
            match output_schema {
                internal_api::OutputSchema::Embedding { dim, distance } => {
                    let index_name = format!("{}-{}", binding.name, name);
                    indexes.push(index_name);
                }
                internal_api::OutputSchema::Feature(schema) => {
                    let index_name = format!("{}-{}", binding.name, name);
                    indexes.push(index_name);
                }
            }
        }
        let _ = self.shared_state.create_binding(binding).await?;
        Ok(indexes)
    }

    #[tracing::instrument(skip(self))]
    pub async fn record_extractor(
        &self,
        extractor: internal_api::ExtractorDescription,
    ) -> Result<(), anyhow::Error> {
        let mut outputs = HashMap::new();
        for (name, output_schema) in extractor.schema.output {
            match output_schema {
                internal_api::OutputSchema::Embedding { dim, distance } => {
                    let distance_metric = IndexDistance::from_str(&distance)?;
                    outputs.insert(
                        name,
                        persistence::ExtractorOutputSchema::Embedding(
                            persistence::EmbeddingSchema {
                                dim,
                                distance: distance_metric,
                            },
                        ),
                    );
                }
                internal_api::OutputSchema::Feature(schema) => {
                    outputs.insert(
                        name,
                        persistence::ExtractorOutputSchema::Attributes(
                            persistence::MetadataSchema { schema },
                        ),
                    );
                }
            }
        }
        let extractor = persistence::Extractor {
            name: extractor.name,
            description: extractor.description,
            input_params: extractor.input_params,
            schemas: persistence::ExtractorSchema { outputs },
        };
        self.repository.record_extractors(vec![extractor]).await?;
        Ok(())
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

    pub async fn create_content_metadata(
        &self,
        req: indexify_coordinator::CreateContentRequest,
    ) -> Result<String> {
        let mut s = DefaultHasher::new();
        req.repository.hash(&mut s);
        req.content.clone().unwrap().file_name.hash(&mut s);
        let id = format!("{:x}", s.finish());
        let metadata = HashMap::new();
        if let Some(content) = req.content {
            let content_metadata = internal_api::ContentMetadata {
                id: id.clone(),
                content_type: content.mime.clone(),
                created_at: content.created_at,
                storage_url: content.storage_url.clone(),
                parent_id: "".to_string(),
                repository: req.repository.clone(),
                name: content.file_name.clone(),
                metadata,
            };
            let _ = self
                .shared_state
                .create_content(&id, content_metadata)
                .await?;
        }
        Ok(id)
    }

    pub async fn get_executor(
        &self,
        extractor_name: &str,
    ) -> Result<ExecutorMetadata, anyhow::Error> {
        let executors = self
            .shared_state
            .get_executors_for_extractor(extractor_name)
            .await?;
        let rand_index = rand::random::<usize>() % executors.len();
        let executor = executors.get(rand_index).ok_or(anyhow::anyhow!(
            "no executor found at index: {}",
            rand_index
        ))?;
        Ok(executor.to_owned())
    }

    #[tracing::instrument(skip(self))]
    pub async fn write_extracted_data(
        &self,
        task_statuses: Vec<internal_api::TaskStatus>,
    ) -> Result<()> {
        for work_status in task_statuses {
            let work = self
                .repository
                .update_work_state(&work_status.task_id, &work_status.status.into())
                .await?;
            for extracted_content in work_status.extracted_content {
                if let Some(feature) = extracted_content.feature.clone() {
                    let index_name = format!("{}-{}", work.extractor_binding, feature.name);
                    if let Some(text) = extracted_content.source_as_text() {
                        if let Some(embedding) = feature.embedding() {
                            let embeddings = ExtractedEmbeddings {
                                content_id: work.content_id.clone(),
                                text: text.clone(),
                                embeddings: embedding.clone(),
                            };
                            self.vector_index_manager
                                .add_embedding(&work.repository_id, &index_name, vec![embeddings])
                                .await?;
                        }
                    }
                    if let Some(metadata) = feature.metadata() {
                        let extracted_attributes = ExtractedAttributes::new(
                            &work.content_id,
                            metadata.clone(),
                            &work.extractor,
                        );
                        self.attribute_index_manager
                            .add_index(&work.repository_id, &index_name, extracted_attributes)
                            .await?;
                    }
                }
            }
        }

        Ok(())
    }
}

//#[cfg(test)]
//mod tests {
//    use std::collections::HashMap;
//
//    use serde_json::json;
//
//    use crate::{
//        blob_storage::BlobStorageBuilder,
//        data_repository_manager::DataRepositoryManager,
//        persistence::{ContentPayload, DataRepository, ExtractorBinding},
//        test_util::{
//            self,
//            db_utils::{DEFAULT_TEST_EXTRACTOR, DEFAULT_TEST_REPOSITORY},
//        },
//    };
//
//    #[tokio::test]
//    #[tracing_test::traced_test]
//    async fn test_create_work() -> Result<(), anyhow::Error> {
//        let db = test_util::db_utils::create_db().await.unwrap();
//        let (vector_index_manager, extractor_executor, coordinator) =
//            test_util::db_utils::create_index_manager(db.clone()).await;
//        let blob_storage =
//
// BlobStorageBuilder::new_disk_storage("/tmp/indexify_test".to_string()).
// unwrap();        let repository_manager =
//            DataRepositoryManager::new_with_db(db.clone(),
// vector_index_manager, blob_storage);
//
//        // Create a repository
//        repository_manager
//            .create(&DataRepository {
//                name: DEFAULT_TEST_REPOSITORY.into(),
//                data_connectors: vec![],
//                metadata: HashMap::new(),
//                extractor_bindings: vec![ExtractorBinding::new(
//                    "test_extractor_binding",
//                    DEFAULT_TEST_REPOSITORY,
//                    DEFAULT_TEST_EXTRACTOR.into(),
//                    vec![],
//                    serde_json::json!({}),
//                )],
//            })
//            .await?;
//
//        repository_manager
//            .add_texts(
//                DEFAULT_TEST_REPOSITORY,
//                vec![
//                    ContentPayload::from_text(
//                        DEFAULT_TEST_REPOSITORY,
//                        "hello",
//                        HashMap::from([("topic".to_string(), json!("pipe"))]),
//                    ),
//                    ContentPayload::from_text(
//                        DEFAULT_TEST_REPOSITORY,
//                        "world",
//                        HashMap::from([("topic".to_string(), json!("baz"))]),
//                    ),
//                ],
//            )
//            .await?;
//
//        // Insert a new worker and then create work
//        coordinator.process_and_distribute_work().await.unwrap();
//
//        let task_list = coordinator
//            .shared_state
//            .tasks_for_executor(&extractor_executor.get_executor_info().id)
//            .await?;
//
//        // Check amount of work queued for the worker
//        assert_eq!(task_list.len(), 2);
//        Ok(())
//    }
//}
