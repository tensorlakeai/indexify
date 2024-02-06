use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap, HashSet},
};

use tracing::error;

use super::{plan::TaskAllocationPlan, AllocationPlanner, AllocationPlannerResult};
use crate::state::{
    store::{ExtractorName, TaskId},
    SharedState,
};

/// Represents the load of an executor
#[derive(Debug, Clone)]
struct ExecutorLoad {
    executor_id: String,
    load: usize,
}

impl Ord for ExecutorLoad {
    fn cmp(&self, other: &Self) -> Ordering {
        self.load.cmp(&other.load)
    }
}

impl PartialOrd for ExecutorLoad {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for ExecutorLoad {}

impl PartialEq for ExecutorLoad {
    fn eq(&self, other: &Self) -> bool {
        self.load == other.load && self.executor_id == other.executor_id
    }
}

pub struct LoadAwareDistributor {
    shared_state: SharedState,
}

impl LoadAwareDistributor {
    pub fn new(shared_state: SharedState) -> Self {
        Self { shared_state }
    }

    async fn group_tasks_by_extractor<'a>(
        &self,
        task_ids: &'a HashSet<TaskId>,
    ) -> HashMap<ExtractorName, HashSet<TaskId>> {
        let sm = self.shared_state.indexify_state.read().await;

        // get the unfinished tasks by extractor table
        let mut result = HashMap::new();
        for (extractor, extractor_task_ids) in sm.unfinished_tasks_by_extractor.iter() {
            let filtered_task_ids: HashSet<String> = extractor_task_ids
                .intersection(task_ids)
                .cloned()
                .collect::<HashSet<_>>();
            if !filtered_task_ids.is_empty() {
                result.insert(extractor.clone(), filtered_task_ids);
            }
        }
        result
    }
}

#[async_trait::async_trait]
impl AllocationPlanner for LoadAwareDistributor {
    async fn plan_allocations(&self, task_ids: HashSet<TaskId>) -> AllocationPlannerResult {
        // if no tasks, return
        if task_ids.is_empty() {
            return Ok(TaskAllocationPlan(HashMap::new()));
        }
        let extractor_executors_map = self.shared_state.get_extractor_executors_map().await;
        // if no executors, return
        if extractor_executors_map.is_empty() {
            return Ok(TaskAllocationPlan(HashMap::new()));
        }

        let executor_load = self.shared_state.get_executor_load().await;
        let tasks_by_extractor = self.group_tasks_by_extractor(&task_ids).await;

        let mut plan = TaskAllocationPlan(HashMap::with_capacity(extractor_executors_map.len()));
        let mut executors_load_heap: HashMap<String, BinaryHeap<Reverse<ExecutorLoad>>> =
            HashMap::new();

        let sm = self.shared_state.indexify_state.read().await;
        for executor in executor_load.keys() {
            // get the extractors that this executor is bound to
            let extractor_name = sm.executors.get(executor).map(|e| e.extractor.name.clone());
            if let None = extractor_name {
                error!("Error getting extractor name for executor: {}", executor);
                continue;
            }
            let extractor_name = extractor_name.unwrap();
            // add the executor to the min-heap for the load of the extractor. create if it
            // doesn't exist.
            let load = executor_load.get(executor).cloned().unwrap_or_default();
            let heap = executors_load_heap
                .entry(extractor_name)
                .or_insert_with(BinaryHeap::new);
            heap.push(Reverse(ExecutorLoad {
                executor_id: executor.clone(),
                load,
            }));
        }
        // dropping here to free the lock before calculating allocations
        drop(sm);

        for (extractor_name, task_ids) in tasks_by_extractor.iter() {
            if let Some(heap) = executors_load_heap.get_mut(extractor_name) {
                for task_id in task_ids.iter() {
                    if let Some(executor_load) = heap.pop() {
                        plan.0
                            .insert(task_id.clone(), executor_load.0.executor_id.clone());
                        let mut load = executor_load.0;
                        load.load += 1;
                        heap.push(Reverse(load));
                    } else {
                        tracing::warn!("No matching executor found for task: {}", task_id);
                    }
                }
            } else {
                tracing::warn!(
                    "No matching executor found for extractor: {}",
                    extractor_name
                );
            }
        }

        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use indexify_internal_api as internal_api;
    use indexify_proto::indexify_coordinator;
    use tokio::time::Instant;

    use super::*;
    use crate::{
        server_config::ServerConfig,
        state::App,
        test_util::db_utils::{mock_extractor, DEFAULT_TEST_EXTRACTOR, DEFAULT_TEST_REPOSITORY},
    };

    #[tokio::test]
    async fn test_min_heap_ordering() {
        // create two loads and add them both to a min-heap
        let load1 = ExecutorLoad {
            executor_id: "executor1".to_string(),
            load: 1,
        };
        let load2 = ExecutorLoad {
            executor_id: "executor2".to_string(),
            load: 2,
        };
        let load3 = ExecutorLoad {
            executor_id: "executor3".to_string(),
            load: 13,
        };
        let load4 = ExecutorLoad {
            executor_id: "executor4".to_string(),
            load: 4,
        };
        let mut heap = BinaryHeap::new();
        heap.push(Reverse(load1));
        heap.push(Reverse(load2));
        heap.push(Reverse(load3));
        heap.push(Reverse(load4));
        // pop the first load and add 10 to it, then push it back
        let mut load1 = heap.pop().unwrap().0;
        load1.load += 10;
        heap.push(Reverse(load1));
        // pop the second load and add 5 to it, then push it back
        let mut load2 = heap.pop().unwrap().0;
        load2.load += 4;
        heap.push(Reverse(load2));
        // pop the loads and verify that the load with the lowest value is popped first
        assert_eq!(
            heap.pop().unwrap().0,
            ExecutorLoad {
                executor_id: "executor4".to_string(),
                load: 4,
            }
        );
        assert_eq!(
            heap.pop().unwrap().0,
            ExecutorLoad {
                executor_id: "executor2".to_string(),
                load: 6,
            }
        );
        assert_eq!(
            heap.pop().unwrap().0,
            ExecutorLoad {
                executor_id: "executor1".to_string(),
                load: 11,
            }
        );
        assert_eq!(
            heap.pop().unwrap().0,
            ExecutorLoad {
                executor_id: "executor3".to_string(),
                load: 13,
            }
        );
    }

    #[tokio::test]
    async fn test_plan_allocations_empty() {
        let config = Arc::new(ServerConfig::default());
        let _ = std::fs::remove_dir_all(config.sled.clone().path.unwrap()).unwrap();
        let shared_state = App::new(config).await.unwrap();
        shared_state.initialize_raft().await.unwrap();
        let _coordinator = crate::coordinator::Coordinator::new(shared_state.clone());
        let sm = shared_state.indexify_state.read().await;

        // get tasks from the state
        let tasks: HashSet<TaskId> = sm.tasks.values().map(|t| t.id.clone()).collect();

        // it's a blank slate, so allocation should result in no tasks being allocated
        let distributor = LoadAwareDistributor::new(shared_state.clone());

        let result = distributor.plan_allocations(tasks).await;
        assert!(result.is_ok());
        // should be empty
        assert_eq!(result.unwrap().0.len(), 0);
    }

    #[tokio::test]
    // #[tracing_test::traced_test]
    async fn test_allocate_task() -> Result<(), anyhow::Error> {
        let config = Arc::new(ServerConfig::default());
        let _ = std::fs::remove_dir_all(config.sled.clone().path.unwrap()).unwrap();
        let shared_state = App::new(config).await.unwrap();
        shared_state.initialize_raft().await.unwrap();
        let coordinator = crate::coordinator::Coordinator::new(shared_state.clone());

        // Add a repository
        coordinator
            .create_repository(DEFAULT_TEST_REPOSITORY)
            .await?;

        // Add content and ensure that we are creating a extraction event
        coordinator
            .create_content_metadata(vec![indexify_coordinator::ContentMetadata {
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

        let events = shared_state.unprocessed_state_change_events().await?;
        assert_eq!(events.len(), 1);

        // Run scheduler without any bindings to make sure that the event is processed
        // and we don't have any tasks
        coordinator.process_and_distribute_work().await?;
        let events = shared_state.unprocessed_state_change_events().await?;
        assert_eq!(events.len(), 0);
        let tasks = shared_state.unassigned_tasks().await?;
        assert_eq!(tasks.len(), 0);

        // Add extractors and extractor bindings and ensure that we are creating tasks
        coordinator
            .register_executor("localhost:8956", "test_executor_id", mock_extractor())
            .await?;
        coordinator
            .create_binding(
                internal_api::ExtractorBinding {
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
        assert_eq!(
            2,
            shared_state.unprocessed_state_change_events().await?.len()
        );
        coordinator.process_extraction_events().await?;

        // Get the unallocated tasks
        let tasks = shared_state.unassigned_tasks().await?;

        let distributor = LoadAwareDistributor::new(shared_state.clone());
        let result = distributor
            .plan_allocations(tasks.clone().into_iter().map(|t| t.id).collect())
            .await?;

        // Verify that the tasks are allocated
        assert_eq!(result.0.len(), 1);
        assert_eq!(
            result.0.get(tasks[0].id.as_str()).unwrap(),
            "test_executor_id"
        );

        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_round_robin_distribution() -> Result<(), anyhow::Error> {
        let config = Arc::new(ServerConfig::default());
        let _ = std::fs::remove_dir_all(config.sled.clone().path.unwrap()).unwrap();
        let shared_state = App::new(config).await.unwrap();
        shared_state.initialize_raft().await.unwrap();
        let coordinator = crate::coordinator::Coordinator::new(shared_state.clone());

        // Add a repository
        coordinator
            .create_repository(DEFAULT_TEST_REPOSITORY)
            .await?;

        // Add 50 text/plain content
        for i in 1..=50 {
            coordinator
                .create_content_metadata(vec![indexify_coordinator::ContentMetadata {
                    id: format!("test{}", i),
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
        }

        // Add 50 application/json content
        for i in 51..=100 {
            coordinator
                .create_content_metadata(vec![indexify_coordinator::ContentMetadata {
                    id: format!("test{}", i),
                    repository: DEFAULT_TEST_REPOSITORY.to_string(),
                    parent_id: "".to_string(),
                    file_name: "test".to_string(),
                    mime: "application/json".to_string(),
                    created_at: 0,
                    storage_url: "test".to_string(),
                    labels: HashMap::new(),
                    source: "ingestion".to_string(),
                }])
                .await?;
        }

        let events = shared_state.unprocessed_state_change_events().await?;
        assert_eq!(events.len(), 100);

        // Run scheduler without any bindings to make sure that the event is processed
        // and we don't have any tasks
        coordinator.process_and_distribute_work().await?;
        let events = shared_state.unprocessed_state_change_events().await?;
        assert_eq!(events.len(), 0);
        let tasks = shared_state.unassigned_tasks().await?;
        assert_eq!(tasks.len(), 0);

        let text_extractor = {
            let mut extractor = mock_extractor();
            extractor.name = "MockTextExtractor".to_string();
            extractor.input_mime_types = vec!["text/plain".to_string()];
            extractor
        };
        let json_extractor = {
            let mut extractor = mock_extractor();
            extractor.name = "MockJsonExtractor".to_string();
            extractor.input_mime_types = vec!["application/json".to_string()];
            extractor
        };

        // register 5 text extractors and 5 json extractors. increment the port by 1 for
        // each
        for i in 1..=5 {
            coordinator
                .register_executor(
                    format!("localhost:{}", 8955 + i).as_str(),
                    format!("text_executor{}", i).as_str(),
                    text_extractor.clone(),
                )
                .await?;
            coordinator
                .register_executor(
                    format!("localhost:{}", 8965 + i).as_str(),
                    format!("json_executor{}", i).as_str(),
                    json_extractor.clone(),
                )
                .await?;
        }

        // create bindings for text and json extractors
        coordinator
            .create_binding(
                internal_api::ExtractorBinding {
                    id: "text-binding-id".to_string(),
                    name: "text".to_string(),
                    extractor: "MockTextExtractor".to_string(),
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
                text_extractor,
            )
            .await?;
        coordinator
            .create_binding(
                internal_api::ExtractorBinding {
                    id: "json-binding-id".to_string(),
                    name: "json".to_string(),
                    extractor: "MockJsonExtractor".to_string(),
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
                json_extractor,
            )
            .await?;
        coordinator.process_extraction_events().await?;

        // Get the unallocated tasks
        let tasks = shared_state.unassigned_tasks().await?;

        // There should be 100 unallocated tasks
        assert_eq!(tasks.len(), 100);

        let distributor = LoadAwareDistributor::new(shared_state.clone());
        let result = distributor
            .plan_allocations(tasks.clone().into_iter().map(|t| t.id).collect())
            .await?;

        // Verify that the tasks are allocated
        assert_eq!(result.clone().0.len(), 100);
        let mapped_result = result.into_tasks_by_executor();

        // every executor should have 20 tasks
        for i in 1..=5 {
            let executor_id = format!("text_executor{}", i);
            assert_eq!(
                mapped_result.get(&executor_id).unwrap().len(),
                10,
                "unbalanced for executor: {}",
                executor_id
            );
        }
        for i in 1..=5 {
            let executor_id = format!("json_executor{}", i);
            assert_eq!(
                mapped_result.get(&executor_id).unwrap().len(),
                10,
                "unbalanced for executor: {}",
                executor_id
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_balance_imbalanced_executors() -> Result<(), anyhow::Error> {
        let config = Arc::new(ServerConfig::default());
        let _ = std::fs::remove_dir_all(config.sled.clone().path.unwrap()).unwrap();
        let shared_state = App::new(config).await.unwrap();
        shared_state.initialize_raft().await.unwrap();
        let coordinator = crate::coordinator::Coordinator::new(shared_state.clone());

        // Add a repository
        coordinator
            .create_repository(DEFAULT_TEST_REPOSITORY)
            .await?;

        // Add 100 text/plain content
        for i in 1..=100 {
            coordinator
                .create_content_metadata(vec![indexify_coordinator::ContentMetadata {
                    id: format!("test{}", i),
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
        }

        // Add 100 application/json content
        for i in 101..=200 {
            coordinator
                .create_content_metadata(vec![indexify_coordinator::ContentMetadata {
                    id: format!("test{}", i),
                    repository: DEFAULT_TEST_REPOSITORY.to_string(),
                    parent_id: "".to_string(),
                    file_name: "test".to_string(),
                    mime: "application/json".to_string(),
                    created_at: 0,
                    storage_url: "test".to_string(),
                    labels: HashMap::new(),
                    source: "ingestion".to_string(),
                }])
                .await?;
        }

        let events = shared_state.unprocessed_state_change_events().await?;
        assert_eq!(events.len(), 200);

        // Run scheduler without any bindings to make sure that the event is processed
        // and we don't have any tasks
        coordinator.process_and_distribute_work().await?;
        let events = shared_state.unprocessed_state_change_events().await?;
        assert_eq!(events.len(), 0);
        let tasks = shared_state.unassigned_tasks().await?;
        assert_eq!(tasks.len(), 0);

        let text_extractor = {
            let mut extractor = mock_extractor();
            extractor.name = "MockTextExtractor".to_string();
            extractor.input_mime_types = vec!["text/plain".to_string()];
            extractor
        };
        let json_extractor = {
            let mut extractor = mock_extractor();
            extractor.name = "MockJsonExtractor".to_string();
            extractor.input_mime_types = vec!["application/json".to_string()];
            extractor
        };

        // register 5 text extractors and 5 json extractors. increment the port by 1 for
        // each
        for i in 1..=5 {
            coordinator
                .register_executor(
                    format!("localhost:{}", 8955 + i).as_str(),
                    format!("text_executor{}", i).as_str(),
                    text_extractor.clone(),
                )
                .await?;
            coordinator
                .register_executor(
                    format!("localhost:{}", 8965 + i).as_str(),
                    format!("json_executor{}", i).as_str(),
                    json_extractor.clone(),
                )
                .await?;
        }

        // create bindings for text and json extractors
        coordinator
            .create_binding(
                internal_api::ExtractorBinding {
                    id: "text-binding-id".to_string(),
                    name: "text".to_string(),
                    extractor: "MockTextExtractor".to_string(),
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
                text_extractor,
            )
            .await?;
        coordinator
            .create_binding(
                internal_api::ExtractorBinding {
                    id: "json-binding-id".to_string(),
                    name: "json".to_string(),
                    extractor: "MockJsonExtractor".to_string(),
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
                json_extractor,
            )
            .await?;
        coordinator.process_extraction_events().await?;

        // Get the unallocated tasks
        let tasks = shared_state.unassigned_tasks().await?;

        // There should be 200 unallocated tasks
        assert_eq!(tasks.len(), 200);

        // arbitrarily increase the load on the first text executor and json executor
        let mut sm = shared_state.indexify_state.write().await;
        sm.executor_load.insert("text_executor1".to_string(), 20);
        sm.executor_load.insert("json_executor1".to_string(), 20);
        drop(sm);

        let distributor = LoadAwareDistributor::new(shared_state.clone());
        let result = distributor
            .plan_allocations(tasks.clone().into_iter().map(|t| t.id).collect())
            .await?;

        // Verify that the tasks are allocated
        assert_eq!(result.clone().0.len(), 200);
        let mapped_result = result.into_tasks_by_executor();

        // every executor should have 24 tasks
        // except for the first text executor, which will only receive 4 tasks
        for i in 1..=5 {
            let executor_id = format!("text_executor{}", i);
            assert_eq!(
                mapped_result.get(&executor_id).unwrap().len(),
                if i == 1 { 4 } else { 24 },
                "unbalanced for executor: {}",
                executor_id
            );
        }
        // every executor should have 24 tasks
        // except for the first json executor, which will only receive 4 tasks
        for i in 1..=5 {
            let executor_id = format!("json_executor{}", i);
            assert_eq!(
                mapped_result.get(&executor_id).unwrap().len(),
                if i == 1 { 4 } else { 24 },
                "unbalanced for executor: {}",
                executor_id
            );
        }

        Ok(())
    }

    /// Test setup can take a long time, so keep the number of tasks low.
    /// Previously it distributed 500,000 tasks in 2.7 seconds, but
    /// setup took almost 7 minutes.
    #[tokio::test]
    async fn test_benchmark() -> Result<(), anyhow::Error> {
        // let total_tasks = 500_000;
        let total_tasks = 1000;

        // total_tasks should be divisible by 200
        assert_eq!(total_tasks % 200, 0);
        let text_tasks = total_tasks / 2;
        let json_tasks = total_tasks / 2;
        let config = Arc::new(ServerConfig::default());
        let _ = std::fs::remove_dir_all(config.sled.clone().path.unwrap()).unwrap();
        let shared_state = App::new(config).await.unwrap();
        shared_state.initialize_raft().await.unwrap();
        let coordinator = crate::coordinator::Coordinator::new(shared_state.clone());

        // Add a repository
        coordinator
            .create_repository(DEFAULT_TEST_REPOSITORY)
            .await?;

        let mut text_content = Vec::new();
        let mut json_content = Vec::new();

        // add text and json content
        for i in 1..=text_tasks {
            text_content.push(indexify_coordinator::ContentMetadata {
                id: format!("test{}", i),
                repository: DEFAULT_TEST_REPOSITORY.to_string(),
                parent_id: "".to_string(),
                file_name: "test".to_string(),
                mime: "text/plain".to_string(),
                created_at: 0,
                storage_url: "test".to_string(),
                labels: HashMap::new(),
                source: "ingestion".to_string(),
            });
        }

        for i in (text_tasks + 1)..=(text_tasks + json_tasks) {
            json_content.push(indexify_coordinator::ContentMetadata {
                id: format!("test{}", i),
                repository: DEFAULT_TEST_REPOSITORY.to_string(),
                parent_id: "".to_string(),
                file_name: "test".to_string(),
                mime: "application/json".to_string(),
                created_at: 0,
                storage_url: "test".to_string(),
                labels: HashMap::new(),
                source: "ingestion".to_string(),
            });
        }

        // commit the content
        let chunk_size = text_tasks / 100;
        let mut chunks = text_content.chunks(chunk_size);
        futures::future::join_all(
            chunks
                .by_ref()
                .map(|chunk| coordinator.create_content_metadata(chunk.to_vec())),
        )
        .await;

        let chunk_size = json_tasks / 100;
        let mut chunks = json_content.chunks(chunk_size);
        futures::future::join_all(
            chunks
                .by_ref()
                .map(|chunk| coordinator.create_content_metadata(chunk.to_vec())),
        )
        .await;

        let events = shared_state.unprocessed_state_change_events().await?;
        assert_eq!(events.len(), total_tasks);

        // Run scheduler without any bindings to make sure that the event is processed
        // and we don't have any tasks
        coordinator.process_and_distribute_work().await?;
        let events = shared_state.unprocessed_state_change_events().await?;
        assert_eq!(events.len(), 0);
        let tasks = shared_state.unassigned_tasks().await?;
        assert_eq!(tasks.len(), 0);

        let text_extractor = {
            let mut extractor = mock_extractor();
            extractor.name = "MockTextExtractor".to_string();
            extractor.input_mime_types = vec!["text/plain".to_string()];
            extractor
        };
        let json_extractor = {
            let mut extractor = mock_extractor();
            extractor.name = "MockJsonExtractor".to_string();
            extractor.input_mime_types = vec!["application/json".to_string()];
            extractor
        };

        let text_executors = {
            let mut executors = Vec::new();
            for i in 1..=(total_tasks / 25) {
                let addr = format!("localhost:{}", 8955 + i);
                let name = format!("text_executor{}", i);
                executors.push((addr, name));
            }
            executors
        };
        futures::future::join_all((1..=(total_tasks / 25)).map(|i| {
            coordinator.register_executor(
                text_executors[i - 1].0.as_str(),
                text_executors[i - 1].1.as_str(),
                text_extractor.clone(),
            )
        }))
        .await;
        let json_executors = {
            let mut executors = Vec::new();
            for i in 1..=(total_tasks / 25) {
                let addr = format!("localhost:{}", 8965 + i);
                let name = format!("json_executor{}", i);
                executors.push((addr, name));
            }
            executors
        };
        futures::future::join_all((1..=(total_tasks / 25)).map(|i| {
            coordinator.register_executor(
                json_executors[i - 1].0.as_str(),
                json_executors[i - 1].1.as_str(),
                json_extractor.clone(),
            )
        }))
        .await;

        // create bindings for text and json extractors
        coordinator
            .create_binding(
                internal_api::ExtractorBinding {
                    id: "text-binding-id".to_string(),
                    name: "text".to_string(),
                    extractor: "MockTextExtractor".to_string(),
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
                text_extractor,
            )
            .await?;
        coordinator
            .create_binding(
                internal_api::ExtractorBinding {
                    id: "json-binding-id".to_string(),
                    name: "json".to_string(),
                    extractor: "MockJsonExtractor".to_string(),
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
                json_extractor,
            )
            .await?;
        coordinator.process_extraction_events().await?;

        // Get the unallocated tasks
        let tasks = shared_state.unassigned_tasks().await?;

        assert_eq!(tasks.len(), total_tasks);

        let distributor = LoadAwareDistributor::new(shared_state.clone());
        // start the timer
        let task_ids = tasks.clone().into_iter().map(|t| t.id).collect();
        let start = Instant::now();
        let result = distributor.plan_allocations(task_ids).await?;
        // stop the timer
        let duration = start.elapsed();

        // Verify that the tasks are allocated
        assert_eq!(result.clone().0.len(), total_tasks);

        println!(
            "Time elapsed in round_robin_distribution() is: {:?}",
            duration
        );

        Ok(())
    }
}
