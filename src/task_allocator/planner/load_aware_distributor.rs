use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap, HashSet},
};

use indexify_internal_api::ExecutorMetadata;
use tracing::error;

use super::{plan::TaskAllocationPlan, AllocationPlanner, AllocationPlannerResult};
use crate::state::{
    store::{ExecutorId, ExtractorName, StateMachineColumns, TaskId},
    SharedState,
};

type MinHeap<T> = BinaryHeap<Reverse<T>>;

/// Represents the load of an executor, used to prioritize executors for task
/// allocation.
///
/// Example usage:
/// ```no_run
/// let load = ExecutorLoad {
///   executor_id: "executor1".to_string(),
///   running_task_count: 5,
/// };
/// let mut heap = BinaryHeap::new();
/// heap.push(Reverse(load));
/// ```
#[derive(Debug, Clone)]
struct ExecutorLoad {
    // The unique identifier of the executor.
    executor_id: ExecutorId,
    // Current count of tasks being processed by the executor.
    running_task_count: usize,
}

impl Ord for ExecutorLoad {
    /// Compares two `ExecutorLoad` instances to establish their ordering based
    /// on load, with a lower load being ranked higher.
    ///
    /// Note on Binary Heap initialization: By default, Rust's binary heap is a
    /// max-heap, meaning that elements with a greater value according to
    /// the `Ord` trait are given higher priority. However, for load
    /// balancing, we want executors with fewer tasks (i.e., a lower load)
    /// to have higher priority. To achieve this, use the `Reverse`
    /// wrapper when inserting `ExecutorLoad` instances into the heap. This
    /// inverts the comparison logic defined here, turning the heap into a
    /// min-heap. As a result, executors with the smallest
    /// `running_task_count` (or however the executor load ranking is defined)
    /// are prioritized for receiving new tasks.
    ///
    /// Keep this method aligned with the load balancing strategy. If
    /// additional factors should be considered in the future, incorporate them
    /// here, keeping in mind the inverted logic due to the `Reverse`
    /// wrapper.
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare the running task count to establish the ordering. `.cmp` is
        // equivalent to <=>, and it returns a corresponding Ordering.
        self.running_task_count.cmp(&other.running_task_count)
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
        self.running_task_count == other.running_task_count && self.executor_id == other.executor_id
    }
}

/// See comment for `plan_allocations` method for more details.
pub struct LoadAwareDistributor {
    shared_state: SharedState,
}

impl LoadAwareDistributor {
    pub fn new(shared_state: SharedState) -> Self {
        Self { shared_state }
    }

    /// Groups task IDs by their associated extractors.
    ///
    /// This function examines all unfinished tasks, filtering them by the
    /// provided task IDs, and then groups them by their extractor name.
    /// Only extractors with at least one matching task ID are included in
    /// the result.
    ///
    /// # Parameters
    /// - `task_ids`: A set of `TaskId` representing the task IDs to be grouped.
    ///
    /// # Returns
    /// A `HashMap` where each key is an `ExtractorName` associated with a
    /// `HashSet` of `TaskId` that represents the grouped task IDs for that
    /// extractor.
    async fn group_tasks_by_extractor<'a>(
        &self,
        task_ids: &'a HashSet<TaskId>,
    ) -> HashMap<ExtractorName, HashSet<TaskId>> {
        // let sm = self.shared_state.indexify_state.read().await;

        // Initialize the result HashMap to collect the filtered task IDs by extractor.
        let mut result = HashMap::new();
        for (extractor, extractor_task_ids) in self
            .shared_state
            .get_unfinished_tasks_by_extractor()
            .await
            .iter()
        {
            let filtered_task_ids: HashSet<TaskId> =
                extractor_task_ids.intersection(task_ids).cloned().collect();
            if !filtered_task_ids.is_empty() {
                // Only insert if there are actually task IDs to avoid empty entries.
                result.insert(extractor.clone(), filtered_task_ids);
            }
        }
        result
    }

    /// This method creates a mapping from extractor names to min-heaps
    /// (priority queues) of executors, sorted by their current load.
    ///
    /// The load of an executor is determined by the number of tasks it is
    /// currently running, allowing for efficient selection of the least loaded
    /// executor for task allocation. "Pop"-ing from the heap will yield the
    /// executor with the least load, and "push"-ing an updated
    /// load back into the heap will maintain the min-heap property.
    ///
    /// # Returns
    /// Returns a `HashMap` where each key is a `String` representing the
    /// extractor name, and each value is a `BinaryHeap<Reverse<ExecutorLoad>>`
    /// representing the priority queue of executors by their load for that
    /// extractor.
    ///
    /// # Errors
    /// Logs an error if an executor referenced in the running task count is not
    /// found in the executors table, indicating a potential inconsistency
    /// in the application's state management.
    async fn initialize_executor_load_min_heaps_by_extractor(
        &self,
    ) -> HashMap<ExtractorName, MinHeap<ExecutorLoad>> {
        let mut executors_load_min_heap: HashMap<ExtractorName, MinHeap<ExecutorLoad>> =
            HashMap::new();
        // Retrieve the current running task count for each executor from the shared
        // state.
        let executor_running_task_count = self.shared_state.get_executor_running_task_count().await;

        // Populate the executors' load heap for each extractor based on the current
        // running tasks.
        for executor_id in executor_running_task_count.keys() {
            let executor = self
                .shared_state
                .state_machine
                .get_from_cf::<ExecutorMetadata, _>(StateMachineColumns::Executors, executor_id)
                .map(|opt| {
                    if opt.is_none() {
                        error!("Executor with id {} not found", executor_id);
                    }
                    opt
                })
                .unwrap_or(None);
            match executor {
                Some(executor) => {
                    let extractor_names = executor
                        .extractors
                        .into_iter()
                        .map(|e| e.name)
                        .collect::<Vec<String>>();

                    let running_task_count = executor_running_task_count
                        .get(executor_id)
                        .cloned()
                        .unwrap_or_default() as usize;

                    // Update or create the heap for the extractor and add the executor's load.
                    for extractor_name in extractor_names {
                        executors_load_min_heap
                            .entry(extractor_name)
                            .or_default()
                            // use `Reverse` here to make it a min-heap
                            .push(Reverse(ExecutorLoad {
                                executor_id: executor_id.clone(),
                                running_task_count,
                            }));
                    }
                }
                None => {
                    // Inconsistency: an executor is in the running task count but not in
                    // the executors table.
                    error!(
                        "Executor '{}' not found in executors table - this shouldn't be possible.",
                        executor_id
                    );
                }
            }
        }
        executors_load_min_heap
    }
}

#[async_trait::async_trait]
impl AllocationPlanner for LoadAwareDistributor {
    /// Plans task allocations across available executors based on current load
    /// and task extractor requirements.
    ///
    /// This method asynchronously calculates an allocation plan for a set of
    /// tasks, aiming to distribute the tasks evenly across executors based
    /// on their current load and the specific extractors the tasks require.
    ///
    /// Calculation of executor priority is handled by the implementation of
    /// `Ord` for `ExecutorLoad`. A min-heap is used to keep track of the
    /// running task count for each executor, updated as tasks are allocated.
    ///
    /// # Parameters
    /// - `task_ids`: A `HashSet` of `TaskId` representing the tasks to be
    ///   allocated. Each `TaskId` is unique and corresponds to a specific task
    ///   that requires execution.
    ///
    /// # Returns
    /// Returns an `AllocationPlannerResult`, which is a result type that wraps
    /// a `TaskAllocationPlan`. The `TaskAllocationPlan` itself is a
    /// `HashMap` of `TaskId` to `ExecutorId`. If no tasks are provided (i.e.,
    /// the `HashSet` is empty), the method returns an empty
    /// `TaskAllocationPlan`.
    async fn plan_allocations(&self, task_ids: HashSet<TaskId>) -> AllocationPlannerResult {
        // Early return if there are no tasks to allocate
        if task_ids.is_empty() {
            return Ok(TaskAllocationPlan(HashMap::new()));
        }

        // Group tasks by their required extractor. This allows targeting a subset of
        // executors rather than iterating over all of them.
        let tasks_by_extractor = self.group_tasks_by_extractor(&task_ids).await;

        // Initialize a mapping from extractor names to priority queues (min-heaps) of
        // executors based on their load.
        let mut executor_load_min_heaps_by_extractor: HashMap<
            ExtractorName,
            MinHeap<ExecutorLoad>,
        > = self.initialize_executor_load_min_heaps_by_extractor().await;

        // Prepare the allocation plan structure to record task assignments.
        let mut plan = TaskAllocationPlan(HashMap::new());

        for (extractor_name, task_ids) in tasks_by_extractor.iter() {
            // Attempt to retrieve the min-heap of executor loads for the current extractor.
            // If no heap is found (an invariant violation), log an error and skip to the
            // next extractor.
            let heap = match executor_load_min_heaps_by_extractor.get_mut(extractor_name) {
                Some(heap) => heap,
                None => {
                    // Logging at error level because this situation indicates a logic error
                    // that should be investigated.
                    error!("No matching executor found for extractor '{}'. This shouldn't be possible.", extractor_name);
                    continue;
                }
            };
            // Iterate over each task ID assigned to the current extractor.
            for task_id in task_ids.iter() {
                // Attempt to pop the executor with the least load from the heap.
                match heap.pop() {
                    Some(executor_load) => {
                        // If an executor is found, assign the task to it and increment its load.
                        // Then, push the updated load back into the heap to maintain the min-heap
                        // property.
                        plan.0
                            .insert(task_id.clone(), executor_load.0.executor_id.clone());
                        let mut load = executor_load.0;
                        load.running_task_count += 1;
                        heap.push(Reverse(load));
                    }
                    None => {
                        // If no executor is available for this task, log an error.
                        // This case might require attention to ensure tasks are not left unhandled.
                        error!("No matching executor found for task: {}", task_id);
                    }
                }
            }
        }

        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::Arc,
        time::{Instant, SystemTime},
    };

    use indexify_internal_api as internal_api;
    use internal_api::{ContentMetadata, ContentMetadataId};
    use serde_json::json;

    use super::*;
    use crate::{
        server_config::ServerConfig,
        state::App,
        test_util::db_utils::{mock_extractor, mock_extractors},
    };

    fn create_task(
        id: &str,
        extractor: &str,
        extractor_graph_name: &str,
        policy: &str,
        content: ContentMetadata,
    ) -> internal_api::Task {
        internal_api::Task {
            id: id.to_string(),
            extractor: extractor.to_string(),
            extraction_graph_name: extractor_graph_name.to_string(),
            extraction_policy_name: policy.to_string(),
            output_index_table_mapping: HashMap::new(),
            namespace: "default".to_string(),
            content_metadata: content,
            input_params: json!(null),
            outcome: internal_api::TaskOutcome::Unknown,
            index_tables: vec![],
            creation_time: SystemTime::now(),
        }
    }

    #[tokio::test]
    async fn test_min_heap_ordering() {
        // create two loads and add them both to a min-heap
        let load1 = ExecutorLoad {
            executor_id: "executor1".to_string(),
            running_task_count: 1,
        };
        let load2 = ExecutorLoad {
            executor_id: "executor2".to_string(),
            running_task_count: 2,
        };
        let load3 = ExecutorLoad {
            executor_id: "executor3".to_string(),
            running_task_count: 13,
        };
        let load4 = ExecutorLoad {
            executor_id: "executor4".to_string(),
            running_task_count: 4,
        };
        let mut heap = BinaryHeap::new();
        heap.push(Reverse(load1));
        heap.push(Reverse(load2));
        heap.push(Reverse(load3));
        heap.push(Reverse(load4));
        // pop the first load and add 10 to it, then push it back
        let mut load1 = heap.pop().unwrap().0;
        load1.running_task_count += 10;
        heap.push(Reverse(load1));
        // pop the second load and add 5 to it, then push it back
        let mut load2 = heap.pop().unwrap().0;
        load2.running_task_count += 4;
        heap.push(Reverse(load2));
        // pop the loads and verify that the load with the lowest value is popped first
        assert_eq!(
            heap.pop().unwrap().0,
            ExecutorLoad {
                executor_id: "executor4".to_string(),
                running_task_count: 4,
            }
        );
        assert_eq!(
            heap.pop().unwrap().0,
            ExecutorLoad {
                executor_id: "executor2".to_string(),
                running_task_count: 6,
            }
        );
        assert_eq!(
            heap.pop().unwrap().0,
            ExecutorLoad {
                executor_id: "executor1".to_string(),
                running_task_count: 11,
            }
        );
        assert_eq!(
            heap.pop().unwrap().0,
            ExecutorLoad {
                executor_id: "executor3".to_string(),
                running_task_count: 13,
            }
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_plan_allocations_empty() -> Result<(), anyhow::Error> {
        let config = Arc::new(ServerConfig::default());
        std::fs::remove_dir_all(config.state_store.clone().path.unwrap()).unwrap();
        let garbage_collector = crate::garbage_collector::GarbageCollector::new();
        let shared_state = App::new(
            config.clone(),
            None,
            Arc::clone(&garbage_collector),
            &config.coordinator_addr,
            Arc::new(crate::metrics::init_provider()),
        )
        .await
        .unwrap();
        shared_state.initialize_raft().await.unwrap();

        let task_ids: HashSet<TaskId> = shared_state
            .state_machine
            .get_all_rows_from_cf::<internal_api::Task>(StateMachineColumns::Tasks)
            .await?
            .into_iter()
            .map(|(_, task)| task.id.clone())
            .collect();

        // it's a blank slate, so allocation should result in no tasks being allocated
        let distributor = LoadAwareDistributor::new(shared_state.clone());

        let result = distributor.plan_allocations(task_ids).await;
        assert!(result.is_ok());
        // should be empty
        assert_eq!(result.unwrap().0.len(), 0);
        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_allocate_task() -> Result<(), anyhow::Error> {
        let config = Arc::new(ServerConfig::default());
        std::fs::remove_dir_all(config.state_store.clone().path.unwrap()).unwrap();
        let garbage_collector = crate::garbage_collector::GarbageCollector::new();
        let shared_state = App::new(
            config.clone(),
            None,
            Arc::clone(&garbage_collector),
            &config.coordinator_addr,
            Arc::new(crate::metrics::init_provider()),
        )
        .await
        .unwrap();
        shared_state.initialize_raft().await.unwrap();

        // Add extractors and extractor bindings and ensure that we are creating tasks
        shared_state
            .register_executor("localhost:8956", "test_executor_id", mock_extractors())
            .await?;

        let content = ContentMetadata {
            id: ContentMetadataId::new("content_id"),
            ..Default::default()
        };
        shared_state
            .create_content_batch(vec![content.clone()])
            .await?;

        let task = create_task(
            "test-task",
            &mock_extractor().name,
            "mock-extraction-graph",
            "test-binding",
            content,
        );
        let state_changes = shared_state.unprocessed_state_change_events().await?;
        shared_state
            .create_tasks(vec![task.clone()], state_changes.last().unwrap().id)
            .await?;

        let distributor = LoadAwareDistributor::new(shared_state.clone());
        let result = distributor
            .plan_allocations(HashSet::from([task.clone().id.clone()]))
            .await?;

        // Verify that the tasks are allocated
        assert_eq!(result.0.len(), 1);
        assert_eq!(result.0.get(&task.id).unwrap(), "test_executor_id");

        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_round_robin_distribution() -> Result<(), anyhow::Error> {
        let config = Arc::new(ServerConfig::default());
        std::fs::remove_dir_all(config.state_store.clone().path.unwrap()).unwrap();
        let garbage_collector = crate::garbage_collector::GarbageCollector::new();
        let shared_state = App::new(
            config.clone(),
            None,
            Arc::clone(&garbage_collector),
            &config.coordinator_addr,
            Arc::new(crate::metrics::init_provider()),
        )
        .await
        .unwrap();
        shared_state.initialize_raft().await.unwrap();

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
            shared_state
                .register_executor(
                    format!("localhost:{}", 8955 + i).as_str(),
                    format!("text_executor{}", i).as_str(),
                    vec![text_extractor.clone()],
                )
                .await?;
            shared_state
                .register_executor(
                    format!("localhost:{}", 8965 + i).as_str(),
                    format!("json_executor{}", i).as_str(),
                    vec![json_extractor.clone()],
                )
                .await?;
        }

        let mut tasks = Vec::new();
        let mut content = Vec::new();
        // Crate the tasks
        for i in 1..=50 {
            let content1 = ContentMetadata {
                id: ContentMetadataId::new(&format!("content_id_{}", i)),
                ..Default::default()
            };
            let task1 = create_task(
                &format!("test-text-task-{}", i),
                "MockTextExtractor",
                "MockTextExtractionGraph",
                "text-binding",
                content1.clone(),
            );

            let content2 = ContentMetadata {
                id: ContentMetadataId::new(&format!("content_id_{}", i + 50)),
                ..Default::default()
            };
            let task2 = create_task(
                &format!("test-json-task-{}", i),
                "MockJsonExtractor",
                "MockJsonExtractionGraph",
                "json-binding",
                content2.clone(),
            );
            tasks.push(task1);
            tasks.push(task2);
            content.push(content1);
            content.push(content2);
        }
        shared_state.create_content_batch(content).await?;

        let state_changes = shared_state.unprocessed_state_change_events().await?;
        shared_state
            .create_tasks(tasks.clone(), state_changes.first().unwrap().id)
            .await?;

        let distributor = LoadAwareDistributor::new(shared_state.clone());
        let result = distributor
            .plan_allocations(tasks.clone().into_iter().map(|t| t.id).collect())
            .await?;
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

    //  NOTE: This test has been temporarily commented out because there is no good
    // way to call a mut method  on the App state because that returns data
    // wrapped in an Arc. However, here we are calling
    // insert_executor_running_task_count  which takes a mutable reference to
    // self. It's not worth changing the design for this since this is a function
    // required only for tests  Need to find a way to increment the executor
    // running task count without having to expose it on the App interface

    // #[tokio::test]
    // async fn test_balance_imbalanced_executors() -> Result<(), anyhow::Error> {
    //     let config = Arc::new(ServerConfig::default());
    //     std::fs::remove_dir_all(config.state_store.clone().path.unwrap()).
    // unwrap();     let shared_state = App::new(config, None).await.unwrap();
    //     shared_state.initialize_raft().await.unwrap();

    //     let text_extractor = {
    //         let mut extractor = mock_extractor();
    //         extractor.name = "MockTextExtractor".to_string();
    //         extractor.input_mime_types = vec!["text/plain".to_string()];
    //         extractor
    //     };
    //     let json_extractor = {
    //         let mut extractor = mock_extractor();
    //         extractor.name = "MockJsonExtractor".to_string();
    //         extractor.input_mime_types = vec!["application/json".to_string()];
    //         extractor
    //     };

    //     // register 5 text extractors and 5 json extractors. increment the port
    // by 1 for     // each
    //     let mut state_change_ids: Vec<String> = Vec::new();
    //     for i in 1..=5 {
    //         let state_change_id = shared_state
    //             .register_executor(
    //                 format!("localhost:{}", 8955 + i).as_str(),
    //                 format!("text_executor{}", i).as_str(),
    //                 text_extractor.clone(),
    //             )
    //             .await?;
    //         state_change_ids.push(state_change_id);
    //         let state_change_id = shared_state
    //             .register_executor(
    //                 format!("localhost:{}", 8965 + i).as_str(),
    //                 format!("json_executor{}", i).as_str(),
    //                 json_extractor.clone(),
    //             )
    //             .await?;
    //         state_change_ids.push(state_change_id);
    //     }

    //     let mut tasks = Vec::new();
    //     // Create the tasks
    //     for i in 1..=100 {
    //         let task1 = create_task(
    //             &format!("test-text-task-{}", i),
    //             "MockTextExtractor",
    //             "text-binding",
    //         );
    //         let task2 = create_task(
    //             &format!("test-json-task-{}", i),
    //             "MockJsonExtractor",
    //             "json-binding",
    //         );
    //         tasks.push(task1);
    //         tasks.push(task2);
    //     }
    //     shared_state
    //         .create_tasks(tasks.clone(), state_change_ids.first().unwrap())
    //         .await?;

    //     // arbitrarily increase the load on the first text executor and json
    // executor     shared_state
    //         .insert_executor_running_task_count("text_executor1", 20)
    //         .await;
    //     shared_state
    //         .insert_executor_running_task_count("json_executor1", 20)
    //         .await;

    //     let distributor = LoadAwareDistributor::new(shared_state.clone());
    //     let result = distributor
    //         .plan_allocations(tasks.clone().into_iter().map(|t| t.id).collect())
    //         .await?;

    //     // Verify that the tasks are allocated
    //     assert_eq!(result.clone().0.len(), 200);
    //     let mapped_result = result.into_tasks_by_executor();

    //     // every executor should have 24 tasks
    //     // except for the first text executor, which will only receive 4 tasks
    //     for i in 1..=5 {
    //         let executor_id = format!("text_executor{}", i);
    //         assert_eq!(
    //             mapped_result.get(&executor_id).unwrap().len(),
    //             if i == 1 { 4 } else { 24 },
    //             "unbalanced for executor: {}",
    //             executor_id
    //         );
    //     }
    //     // every executor should have 24 tasks
    //     // except for the first json executor, which will only receive 4 tasks
    //     for i in 1..=5 {
    //         let executor_id = format!("json_executor{}", i);
    //         assert_eq!(
    //             mapped_result.get(&executor_id).unwrap().len(),
    //             if i == 1 { 4 } else { 24 },
    //             "unbalanced for executor: {}",
    //             executor_id
    //         );
    //     }

    //     Ok(())
    // }

    /// Test setup can take a long time, so keep the number of tasks low.
    /// Previously it distributed 500,000 tasks in 2.7 seconds, but
    /// setup took almost 7 minutes.
    #[tokio::test]
    async fn test_benchmark() -> Result<(), anyhow::Error> {
        // let total_tasks = 500_000;
        let total_tasks = 1000;

        // total_tasks should be divisible by 200
        assert_eq!(total_tasks % 200, 0);
        let config = Arc::new(ServerConfig::default());
        std::fs::remove_dir_all(config.state_store.clone().path.unwrap()).unwrap();
        let garbage_collector = crate::garbage_collector::GarbageCollector::new();
        let shared_state = App::new(
            config.clone(),
            None,
            Arc::clone(&garbage_collector),
            &config.coordinator_addr,
            Arc::new(crate::metrics::init_provider()),
        )
        .await
        .unwrap();
        shared_state.initialize_raft().await.unwrap();

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
            shared_state.register_executor(
                text_executors[i - 1].0.as_str(),
                text_executors[i - 1].1.as_str(),
                vec![text_extractor.clone()],
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
            shared_state.register_executor(
                json_executors[i - 1].0.as_str(),
                json_executors[i - 1].1.as_str(),
                vec![json_extractor.clone()],
            )
        }))
        .await;

        let mut tasks = Vec::new();
        let mut content = Vec::new();
        // Crate the tasks
        for i in 1..=500 {
            let content1 = ContentMetadata {
                id: ContentMetadataId::new(&format!("content_id_{}", i)),
                ..Default::default()
            };
            let task1 = create_task(
                &format!("test-text-task-{}", i),
                "MockTextExtractor",
                "MockTextExtractionGraph",
                "text-binding",
                content1.clone(),
            );

            let content2 = ContentMetadata {
                id: ContentMetadataId::new(&format!("content_id_{}", i + 500)),
                ..Default::default()
            };
            let task2 = create_task(
                &format!("test-json-task-{}", i),
                "MockJsonExtractor",
                "MockJsonExtractionGraph",
                "json-binding",
                content2.clone(),
            );
            tasks.push(task1);
            tasks.push(task2);
            content.push(content1);
            content.push(content2);
        }
        shared_state.create_content_batch(content).await?;

        let state_changes = shared_state.unprocessed_state_change_events().await?;
        shared_state
            .create_tasks(tasks.clone(), state_changes.first().unwrap().id)
            .await?;

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
