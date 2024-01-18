use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    sync::Arc,
};

use anyhow::{anyhow, Ok, Result};
use jsonschema::JSONSchema;
use tokio::sync::watch::Receiver;
use tracing::info;

use crate::{
    indexify_coordinator,
    internal_api::{
        self,
        ContentMetadata,
        ExtractionEvent,
        ExtractionEventPayload,
        ExtractorBinding,
        ExtractorDescription,
        Index,
        Task,
    },
    state::{store::StateChange, SharedState},
    utils::timestamp_secs,
};

pub struct Coordinator {
    shared_state: SharedState,
}

impl Coordinator {
    pub fn new(shared_state: SharedState) -> Arc<Self> {
        Arc::new(Self { shared_state })
    }

    #[tracing::instrument(skip(self))]
    pub async fn process_extraction_events(&self) -> Result<(), anyhow::Error> {
        let events = self.shared_state.unprocessed_extraction_events().await?;
        info!("processing {} extraction events", events.len());
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
            info!("created {} tasks", tasks.len());
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
            if !executors.is_empty() {
                let rand_index = rand::random::<usize>() % executors.len();
                let executor_meta = executors[rand_index].clone();
                task_assignments.insert(task.id.clone(), executor_meta.id.clone());
            }
        }
        info!("finishing work assignment: {:?}", task_assignments);
        Ok(task_assignments)
    }

    pub async fn create_task(
        &self,
        extractor_binding: &ExtractorBinding,
        content_list: Vec<ContentMetadata>,
    ) -> Result<Vec<Task>> {
        let extractor = self
            .shared_state
            .extractor_with_name(&extractor_binding.extractor)
            .await?;
        let mut output_mapping: HashMap<String, String> = HashMap::new();
        for name in extractor.outputs.keys() {
            let index_name = extractor_binding
                .output_index_name_mapping
                .get(name)
                .unwrap();
            let table_name = extractor_binding
                .index_name_table_mapping
                .get(index_name)
                .unwrap();
            output_mapping.insert(name.clone(), table_name.clone());
        }
        let mut tasks = Vec::new();
        for content in content_list {
            let mut hasher = DefaultHasher::new();
            extractor_binding.name.hash(&mut hasher);
            extractor_binding.repository.hash(&mut hasher);
            content.id.hash(&mut hasher);
            let id = format!("{:x}", hasher.finish());
            let task = Task {
                id,
                extractor: extractor_binding.extractor.clone(),
                extractor_binding: extractor_binding.name.clone(),
                output_index_table_mapping: output_mapping.clone(),
                repository: extractor_binding.repository.clone(),
                content_metadata: content.clone(),
                input_params: extractor_binding.input_params.clone(),
                outcome: internal_api::TaskOutcome::Unknown,
            };
            info!("created task: {:?}", task);
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

    pub async fn update_task(
        &self,
        task_id: &str,
        executor_id: &str,
        outcome: internal_api::TaskOutcome,
        content_list: Vec<indexify_coordinator::ContentMetadata>,
    ) -> Result<()> {
        info!(
            "updating task: {}, executor_id: {}, outcome: {:?}",
            task_id, executor_id, outcome
        );
        let mut task = self.shared_state.task_with_id(task_id).await?;
        let (content_meta_list, extraction_events) =
            content_request_to_content_metadata(content_list)?;
        task.outcome = outcome;
        self.shared_state
            .update_task(
                task,
                Some(executor_id.to_string()),
                content_meta_list,
                extraction_events,
            )
            .await?;
        Ok(())
    }

    pub async fn create_repository(&self, repository: &str) -> Result<()> {
        self.shared_state.create_repository(repository).await?;
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

    pub async fn list_indexes(&self, repository: &str) -> Result<Vec<Index>> {
        self.shared_state.list_indexes(repository).await
    }

    pub async fn get_index(&self, repository: &str, name: &str) -> Result<Index> {
        let mut s = DefaultHasher::new();
        repository.hash(&mut s);
        name.hash(&mut s);
        let id = format!("{:x}", s.finish());
        self.shared_state.get_index(&id).await
    }

    pub async fn create_index(&self, repository: &str, index: Index) -> Result<()> {
        let id = index.id();
        self.shared_state.create_index(repository, index, id).await
    }

    pub async fn get_extractor_coordinates(&self, extractor_name: &str) -> Result<Vec<String>> {
        let executors = self
            .shared_state
            .get_executors_for_extractor(extractor_name)
            .await?;
        let addresses = executors
            .iter()
            .map(|e| e.addr.clone())
            .collect::<Vec<String>>();
        Ok(addresses)
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

    pub async fn get_content_metadata(
        &self,
        content_ids: Vec<String>,
    ) -> Result<Vec<ContentMetadata>> {
        self.shared_state
            .get_content_metadata_batch(content_ids)
            .await
    }

    pub async fn get_extractor(&self, extractor_name: &str) -> Result<ExtractorDescription> {
        self.shared_state.extractor_with_name(extractor_name).await
    }

    pub async fn create_binding(
        &self,
        binding: internal_api::ExtractorBinding,
        extractor: ExtractorDescription,
    ) -> Result<()> {
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
        let extraction_event = ExtractionEvent {
            id: nanoid::nanoid!(),
            repository: binding.repository.clone(),
            payload: ExtractionEventPayload::ExtractorBindingAdded {
                repository: binding.repository.clone(),
                binding: binding.clone(),
            },
            created_at: timestamp_secs(),
            processed_at: None,
        };
        self.shared_state
            .create_binding(binding, extraction_event)
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn process_and_distribute_work(&self) -> Result<(), anyhow::Error> {
        self.process_extraction_events().await?;

        let task_assignments = self.distribute_work().await?;
        self.shared_state
            .commit_task_assignments(task_assignments)
            .await
    }

    pub fn get_state_watcher(&self) -> Receiver<StateChange> {
        self.shared_state.get_state_change_watcher()
    }

    pub async fn create_content_metadata(
        &self,
        content_list: Vec<indexify_coordinator::ContentMetadata>,
    ) -> Result<()> {
        let (content_meta_list, extraction_events) =
            content_request_to_content_metadata(content_list)?;
        self.shared_state
            .create_content_batch(content_meta_list, extraction_events)
            .await?;
        Ok(())
    }

    pub fn get_leader_change_watcher(&self) -> Receiver<bool> {
        self.shared_state.leader_change_rx.clone()
    }
}

fn content_request_to_content_metadata(
    content_list: Vec<indexify_coordinator::ContentMetadata>,
) -> Result<(Vec<ContentMetadata>, Vec<ExtractionEvent>)> {
    let mut content_meta_list = Vec::new();
    let mut extraction_events = Vec::new();
    for content in content_list {
        let repository = content.repository.clone();
        let c: internal_api::ContentMetadata = content.try_into()?;
        content_meta_list.push(c.clone());
        let extraction_event = ExtractionEvent {
            id: nanoid::nanoid!(),
            repository,
            payload: ExtractionEventPayload::CreateContent { content: c },
            created_at: timestamp_secs(),
            processed_at: None,
        };
        extraction_events.push(extraction_event);
    }
    Ok((content_meta_list, extraction_events))
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::{
        indexify_coordinator::ContentMetadata,
        internal_api::ExtractorBinding,
        server_config::ServerConfig,
        state::App,
        test_util::db_utils::{mock_extractor, DEFAULT_TEST_EXTRACTOR, DEFAULT_TEST_REPOSITORY},
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_create_extraction_events() -> Result<(), anyhow::Error> {
        let config = Arc::new(ServerConfig::default());
        let shared_state = App::new(config).await.unwrap();
        shared_state.initialize_raft().await.unwrap();
        let coordinator = crate::coordinator::Coordinator::new(shared_state.clone());

        // Add a repository
        coordinator
            .create_repository(DEFAULT_TEST_REPOSITORY)
            .await?;

        // Add content and ensure that we are createing a extraction event
        coordinator
            .create_content_metadata(vec![ContentMetadata {
                id: "test".to_string(),
                repository: DEFAULT_TEST_REPOSITORY.to_string(),
                parent_id: "".to_string(),
                file_name: "test".to_string(),
                mime: "text/plain".to_string(),
                created_at: 0,
                storage_url: "test".to_string(),
                labels: HashMap::new(),
                source: "ingestion".to_string(),
            }])
            .await?;

        let events = shared_state.unprocessed_extraction_events().await?;
        assert_eq!(events.len(), 1);

        // Run scheduler without any bindings to make sure that the event is processed
        // and we don't have any tasks
        coordinator.process_and_distribute_work().await?;
        let events = shared_state.unprocessed_extraction_events().await?;
        assert_eq!(events.len(), 0);
        let tasks = shared_state.unassigned_tasks().await?;
        assert_eq!(tasks.len(), 0);

        // Add extractors and extractor bindings and ensure that we are creating tasks
        coordinator
            .register_executor("localhost:8956", "test_executor_id", mock_extractor())
            .await?;
        coordinator
            .create_binding(
                ExtractorBinding {
                    id: "test-binding-id".to_string(),
                    name: "test".to_string(),
                    extractor: DEFAULT_TEST_EXTRACTOR.to_string(),
                    repository: DEFAULT_TEST_REPOSITORY.to_string(),
                    input_params: serde_json::json!({}),
                    filters: HashMap::new(),
                    output_index_name_mapping: HashMap::from([(
                        "test_output".to_string(),
                        "test.test_output".to_string(),
                    )]),
                    index_name_table_mapping: HashMap::from([(
                        "test.test_output".to_string(),
                        "test_repository.test.test_output".to_string(),
                    )]),
                    content_source: "ingestion".to_string(),
                },
                mock_extractor(),
            )
            .await?;
        assert_eq!(1, shared_state.unprocessed_extraction_events().await?.len());
        coordinator.process_and_distribute_work().await?;
        assert_eq!(0, shared_state.unprocessed_extraction_events().await?.len());
        assert_eq!(
            1,
            shared_state
                .tasks_for_executor("test_executor_id")
                .await?
                .len()
        );
        assert_eq!(0, shared_state.unassigned_tasks().await?.len());

        // Add a content with a different source and ensure we don't create a task
        coordinator
            .create_content_metadata(vec![ContentMetadata {
                id: "test2".to_string(),
                repository: DEFAULT_TEST_REPOSITORY.to_string(),
                parent_id: "test".to_string(),
                file_name: "test2".to_string(),
                mime: "text/plain".to_string(),
                created_at: 0,
                storage_url: "test2".to_string(),
                labels: HashMap::new(),
                source: "some_extractor_produced_this".to_string(),
            }])
            .await?;
        coordinator.process_and_distribute_work().await?;
        assert_eq!(0, shared_state.unprocessed_extraction_events().await?.len());
        assert_eq!(
            1,
            shared_state
                .tasks_for_executor("test_executor_id")
                .await?
                .len()
        );
        assert_eq!(0, shared_state.unassigned_tasks().await?.len());
        Ok(())
    }
}
