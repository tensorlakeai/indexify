#[cfg(test)]
pub mod db_utils {
    use std::collections::HashMap;

    use filter::LabelsFilter;
    use indexify_internal_api as internal_api;
    use indexify_proto::indexify_coordinator::CreateContentStatus;
    use internal_api::{
        ContentMetadataId,
        ContentSource,
        ExtractionGraph,
        ExtractionPolicy,
        Task,
        TaskOutcome,
    };
    use serde_json::json;
    use tokio::time::{sleep, Duration};

    use crate::coordinator::Coordinator;
    pub const DEFAULT_TEST_NAMESPACE: &str = "test_namespace";

    pub const DEFAULT_TEST_EXTRACTOR: &str = "MockExtractor";

    pub fn create_metadata<I, K, V>(val: I) -> HashMap<String, serde_json::Value>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        val.into_iter()
            .map(|(k, v)| (k.as_ref().to_string(), json!(v.as_ref())))
            .collect()
    }

    pub fn test_mock_content_metadata(
        id: &str,
        root_content_id: &str,
        graph_name: &str,
    ) -> internal_api::ContentMetadata {
        internal_api::ContentMetadata {
            id: ContentMetadataId::new(id),
            root_content_id: if root_content_id.is_empty() {
                None
            } else {
                Some(root_content_id.to_string())
            },
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            extraction_graph_names: vec![graph_name.to_string()],
            labels: HashMap::new(),
            hash: id.to_string(),
            ..Default::default()
        }
    }

    pub fn create_test_extraction_graph(
        graph_name: &str,
        extraction_policy_names: Vec<&str>,
    ) -> ExtractionGraph {
        let id = ExtractionGraph::create_id(graph_name, DEFAULT_TEST_NAMESPACE);

        let mut extraction_policies = Vec::new();
        for policy_name in extraction_policy_names {
            let id = ExtractionPolicy::create_id(graph_name, policy_name, DEFAULT_TEST_NAMESPACE);
            let ep = ExtractionPolicy {
                id,
                graph_name: graph_name.to_string(),
                namespace: DEFAULT_TEST_NAMESPACE.to_string(),
                name: policy_name.to_string(),
                extractor: DEFAULT_TEST_EXTRACTOR.to_string(),
                input_params: json!({}),
                filter: LabelsFilter::default(),
                output_table_mapping: HashMap::from([(
                    "test_output".to_string(),
                    "test_table".to_string(),
                )]),
                content_source: internal_api::ContentSource::Ingestion,
            };
            extraction_policies.push(ep);
        }
        ExtractionGraph {
            id,
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            name: graph_name.to_string(),
            description: Some("test_description".to_string()),
            extraction_policies,
        }
    }

    pub enum Parent {
        Root,
        Child(usize),
    }

    pub fn create_test_extraction_graph_with_children(
        graph_name: &str,
        extraction_policy_names: Vec<&str>,
        parents: &[Parent],
    ) -> ExtractionGraph {
        let id = ExtractionGraph::create_id(graph_name, DEFAULT_TEST_NAMESPACE);

        let mut extraction_policies = Vec::new();
        for (index, policy_name) in extraction_policy_names.iter().enumerate() {
            let id = ExtractionPolicy::create_id(graph_name, policy_name, DEFAULT_TEST_NAMESPACE);
            let parent = &parents[index];
            let ep = ExtractionPolicy {
                id,
                graph_name: graph_name.to_string(),
                namespace: DEFAULT_TEST_NAMESPACE.to_string(),
                name: policy_name.to_string(),
                extractor: DEFAULT_TEST_EXTRACTOR.to_string(),
                input_params: json!({}),
                filter: LabelsFilter::default(),
                output_table_mapping: HashMap::from([(
                    "test_output".to_string(),
                    "test_table".to_string(),
                )]),
                content_source: match parent {
                    Parent::Root => internal_api::ContentSource::Ingestion,
                    Parent::Child(parent_index) => {
                        internal_api::ContentSource::ExtractionPolicyName(
                            extraction_policy_names[*parent_index].to_string(),
                        )
                    }
                },
            };
            extraction_policies.push(ep);
        }
        ExtractionGraph {
            id,
            namespace: DEFAULT_TEST_NAMESPACE.to_string(),
            description: Some("test_description".to_string()),
            name: graph_name.to_string(),
            extraction_policies,
        }
    }
    pub fn mock_extractor() -> internal_api::ExtractorDescription {
        let mut outputs = HashMap::new();
        outputs.insert(
            "test_output".to_string(),
            internal_api::OutputSchema::Embedding(internal_api::EmbeddingSchema {
                dim: 384,
                distance: "cosine".to_string(),
            }),
        );
        internal_api::ExtractorDescription {
            name: DEFAULT_TEST_EXTRACTOR.to_string(),
            description: "test_description".to_string(),
            input_params: json!({}),
            outputs,
            input_mime_types: vec!["*/*".to_string()],
        }
    }

    pub fn mock_extractors() -> Vec<internal_api::ExtractorDescription> {
        vec![mock_extractor()]
    }

    pub async fn complete_task(
        coordinator: &Coordinator,
        task: &Task,
        executor_id: &str,
    ) -> Result<(), anyhow::Error> {
        let mut task_clone = task.clone();
        task_clone.outcome = internal_api::TaskOutcome::Success;
        coordinator
            .shared_state
            .update_task(task_clone, Some(executor_id.to_string()))
            .await
    }

    pub fn next_child(child_id: &mut i32) -> String {
        let child_id_str = format!("{}", child_id);
        *child_id += 1;
        child_id_str
    }

    pub async fn create_content_for_task(
        coordinator: &Coordinator,
        task: &Task,
        id: &str,
    ) -> Result<internal_api::ContentMetadata, anyhow::Error> {
        let policy = coordinator
            .get_extraction_policy(
                &task.namespace,
                &task.extraction_graph_name,
                &task.extraction_policy_name,
            )
            .await?;
        let mut content =
            test_mock_content_metadata(id, task.content_metadata.get_root_id(), &policy.graph_name);
        content.parent_id = Some(task.content_metadata.id.clone());
        content.source = ContentSource::ExtractionPolicyName(policy.name);
        Ok(content)
    }

    pub async fn perform_task(
        coordinator: &Coordinator,
        task: &Task,
        id: &str,
        executor_id: &str,
    ) -> Result<(), anyhow::Error> {
        println!(
            "creating content for task parent: {:?} id: {:?}",
            task.content_metadata.id.id, id
        );
        let content = create_content_for_task(coordinator, task, id).await?;
        let create_res = coordinator
            .create_content_metadata(vec![content.clone()])
            .await?;
        assert_eq!(create_res.len(), 1);
        assert_eq!(*create_res.first().unwrap(), CreateContentStatus::Created);
        complete_task(coordinator, task, executor_id).await
    }

    // run all tasks creating child contents until no new tasks are generated
    pub async fn perform_all_tasks(
        coordinator: &Coordinator,
        executor_id: &str,
        child_id: &mut i32,
    ) -> Result<(), anyhow::Error> {
        loop {
            coordinator.run_scheduler().await?;
            let tasks = coordinator.shared_state.list_all_unfinished_tasks().await?;
            if tasks.is_empty() {
                break;
            }
            for task in tasks {
                perform_task(coordinator, &task, &next_child(child_id), executor_id).await?;
            }
        }
        Ok(())
    }

    pub async fn wait_changes_processed(coordinator: &Coordinator) -> Result<(), anyhow::Error> {
        while !coordinator
            .shared_state
            .unprocessed_state_change_events()
            .await?
            .is_empty()
        {
            sleep(Duration::from_millis(1)).await;
        }
        Ok(())
    }

    async fn gc_tasks_pending(coordinator: &Coordinator) -> Result<bool, anyhow::Error> {
        Ok(coordinator
            .shared_state
            .list_all_gc_tasks()
            .await?
            .iter()
            .any(|task| task.outcome == TaskOutcome::Unknown))
    }

    pub async fn wait_gc_tasks_completed(coordinator: &Coordinator) -> Result<(), anyhow::Error> {
        while gc_tasks_pending(coordinator).await? {
            sleep(Duration::from_millis(1)).await;
        }
        Ok(())
    }
}
