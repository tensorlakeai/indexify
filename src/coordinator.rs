use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    sync::{Arc, RwLock},
};

use anyhow::{anyhow, Ok, Result};
use jsonschema::JSONSchema;
use tracing::info;

use crate::{
    indexify_coordinator,
    internal_api::{
        self,
        ContentMetadata,
        ExecutorMetadata,
        ExtractionEventPayload,
        ExtractorBinding,
        ExtractorDescription,
        Task,
    },
    persistence::Repository,
    state::SharedState,
};

pub struct Coordinator {
    repository: Arc<Repository>,

    shared_state: SharedState,
}

impl Coordinator {
    pub fn new(repository: Arc<Repository>, shared_state: SharedState) -> Arc<Self> {
        let coordinator = Arc::new(Self {
            repository,
            shared_state,
        });
        coordinator
    }

    pub async fn get_executors(&self) -> Result<Vec<ExecutorMetadata>> {
        self.shared_state.get_executors().await
    }

    #[tracing::instrument(skip(self))]
    pub async fn process_extraction_events(&self) -> Result<(), anyhow::Error> {
        let events = self.shared_state.unprocessed_extraction_events().await?;
        for event in &events {
            info!("processing extraction event: {}", event.id);
            let mut tasks = Vec::new();
            match event.payload.clone() {
                ExtractionEventPayload::ExtractorBindingAdded {
                    repository,
                    binding,
                } => {
                    let content_list = self
                        .shared_state
                        .content_matching_binding(&repository, &binding)
                        .await?;
                    let tasks_for_binding = self.create_task(&binding, content_list).await?;
                    tasks.extend(tasks_for_binding);
                }
                ExtractionEventPayload::CreateContent { content } => {
                    let bindings = self
                        .shared_state
                        .filter_extractor_binding_for_content(&content)
                        .await?;
                    for binding in bindings {
                        let task_for_binding =
                            self.create_task(&binding, vec![content.clone()]).await?;
                        tasks.extend(task_for_binding);
                    }
                }
            };
            self.shared_state.create_tasks(tasks).await?;
            self.shared_state
                .mark_extraction_event_processed(&event.id)
                .await?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn distribute_work(&self) -> Result<HashMap<String, String>, anyhow::Error> {
        let unallocated_tasks = self.shared_state.unassigned_tasks().await?;

        // work_id -> executor_id
        let mut task_assignments = HashMap::new();
        for task in unallocated_tasks {
            let executors = self
                .shared_state
                .get_executors_for_extractor(&task.extractor)
                .await?;
            let rand_index = rand::random::<usize>() % executors.len();
            if !executors.is_empty() {
                let executor_meta = executors[rand_index].clone();
                task_assignments.insert(task.id.clone(), executor_meta.id.clone());
            }
        }
        info!("finishing work assignment: {:}", task_assignments.len());
        Ok(task_assignments)
    }

    pub async fn create_task(
        &self,
        extractor_binding: &ExtractorBinding,
        content_list: Vec<ContentMetadata>,
    ) -> Result<Vec<Task>> {
        let mut tasks = Vec::new();
        for content in content_list {
            let task = Task {
                id: "".to_string(),
                extractor: extractor_binding.extractor.clone(),
                extractor_binding: extractor_binding.name.clone(),
                output_index_mapping: todo!(),
                repository: extractor_binding.repository.clone(),
                content_metadata: content.clone(),
                input_params: extractor_binding.input_params.clone(),
            };
            tasks.push(task);
        }
        Ok(tasks)
    }

    pub async fn list_content(
        &self,
        repository: &str,
    ) -> Result<Vec<internal_api::ContentMetadata>> {
        self.shared_state.list_content(repository).await
    }

    pub async fn list_bindings(
        &self,
        repository: &str,
    ) -> Result<Vec<internal_api::ExtractorBinding>> {
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

    pub async fn heartbeat(&self, executor_id: &str) -> Result<Vec<Task>> {
        self.shared_state.heartbeat(executor_id).await?;
        let tasks = self.shared_state.tasks_for_executor(executor_id).await?;
        Ok(tasks)
    }

    pub async fn list_indexes(&self, repository: &str) -> Result<Vec<String>> {
        // self.shared_state.list_indexes(repository).await
        Ok(vec![])
    }

    pub async fn register_executor(
        &self,
        addr: &str,
        executor_id: &str,
        extractor: ExtractorDescription,
    ) -> Result<()> {
        self.shared_state
            .register_executor(addr, executor_id, extractor)
            .await
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
                internal_api::OutputSchema::Embedding(embedding_schema) => {
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
        let content = req.content.ok_or(anyhow!("no content provided"))?;
        let mut s = DefaultHasher::new();
        content.repository.hash(&mut s);
        content.file_name.hash(&mut s);
        let id = format!("{:x}", s.finish());
        let mut metadata = HashMap::new();
        for (name, value) in content.labels.iter() {
            let v: serde_json::Value =
                serde_json::from_str(value).map_err(|e| anyhow!("unable to parse label: {}", e))?;
            metadata.insert(name.clone(), v);
        }
        let content_metadata = internal_api::ContentMetadata {
            id: id.clone(),
            content_type: content.mime.clone(),
            created_at: content.created_at,
            storage_url: content.storage_url.clone(),
            parent_id: "".to_string(),
            repository: content.repository.clone(),
            name: content.file_name.clone(),
            metadata,
        };
        let _ = self
            .shared_state
            .create_content(&id, content_metadata)
            .await?;
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
