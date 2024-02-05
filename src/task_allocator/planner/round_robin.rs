use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
};

use indexify_internal_api::Task;

use super::{plan::TaskAllocationPlan, AllocationPlanner, AllocationPlannerResult};
use crate::state::{
    store::{ExtractorExecutorsMap, TaskId},
    SharedState,
};

/// Represents the load of an executor. This is used to keep track of the
/// load of each executor and to prioritize executors with lower loads.
struct ExecutorLoad {
    executor_id: String,
    load: usize,
}

// Implement ordering for ExecutorLoad to make it work with a min-heap.
// We want to prioritize executors with lower loads, so we reverse the
// comparison.
impl Ord for ExecutorLoad {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .load
            .cmp(&self.load)
            .then_with(|| self.executor_id.cmp(&other.executor_id))
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

pub struct RoundRobinDistributor {
    shared_state: SharedState,
}

impl RoundRobinDistributor {
    pub fn new(shared_state: SharedState) -> Self {
        Self { shared_state }
    }

    async fn plan_allocations_inner(
        &self,
        tasks: Vec<Task>,
        extractor_executors_map: ExtractorExecutorsMap,
    ) -> AllocationPlannerResult {
        // if no tasks, return
        if tasks.is_empty() {
            return Ok(TaskAllocationPlan(HashMap::new()));
        }
        // if no executors, return
        if extractor_executors_map.is_empty() {
            return Ok(TaskAllocationPlan(HashMap::new()));
        }

        let tasks_len = tasks.len();
        let mut plan = TaskAllocationPlan(HashMap::with_capacity(extractor_executors_map.len()));
        let mut executors_load_heap: HashMap<String, BinaryHeap<ExecutorLoad>> =
            HashMap::with_capacity(extractor_executors_map.len());

        let avg_tasks_per_executor = tasks_len / extractor_executors_map.len() / 100;
        let increment_size = if avg_tasks_per_executor > 0 {
            avg_tasks_per_executor
        } else {
            1
        };

        // Initialize a min-heap for each extractor
        for (extractor, executors) in extractor_executors_map.iter() {
            let mut heap = BinaryHeap::new();
            for executor_id in executors {
                heap.push(ExecutorLoad {
                    executor_id: executor_id.clone(),
                    load: 0,
                });
            }
            executors_load_heap.insert(extractor.clone(), heap);
        }

        for task in tasks {
            if let Some(heap) = executors_load_heap.get_mut(&task.extractor) {
                if let Some(mut executor_load) = heap.pop() {
                    let executor_id = executor_load.executor_id.clone();
                    // Update the load for the selected executor
                    executor_load.load += 1;
                    heap.push(executor_load);

                    // Update the plan. If the executor is not in the plan, create a new entry
                    let allocations = plan.0.entry(executor_id).or_insert_with(|| {
                        let mut set = HashSet::with_capacity(increment_size);
                        set.reserve(increment_size);
                        set
                    });
                    allocations.insert(task.id.clone());
                    // if the number of tasks allocated to the executor is a multiple of the
                    // increment size, reserve more space
                    if allocations.len() % increment_size == 0 {
                        allocations.reserve(increment_size);
                    }
                }
            } else {
                tracing::warn!("No matching executor found for task: {}", task.id);
            }
        }

        Ok(plan)
    }
}

#[async_trait::async_trait]
impl AllocationPlanner for RoundRobinDistributor {
    async fn plan_allocations(&self, task_ids: HashSet<TaskId>) -> AllocationPlannerResult {
        let extractor_executors_map = self.shared_state.get_extractor_executors_map().await;
        let tasks = self.shared_state.get_tasks_by_ids(task_ids).await?;
        self.plan_allocations_inner(tasks, extractor_executors_map)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use indexify_internal_api as internal_api;
    use tokio::time::Instant;

    use super::*;
    use crate::{server_config::ServerConfig, state::App};

    async fn create_test_shared_state() -> SharedState {
        App::new(Arc::new(ServerConfig::default())).await.unwrap()
    }

    #[tokio::test]
    async fn test_plan_allocations_empty() {
        let shared_state = create_test_shared_state().await;
        let distributor = RoundRobinDistributor::new(shared_state);
        let tasks = vec![];
        let extractor_executors_map = HashMap::new();
        let result = distributor
            .plan_allocations_inner(tasks, extractor_executors_map)
            .await;
        assert!(result.is_ok());
        // should be empty
        assert_eq!(result.unwrap().0.len(), 0);
    }

    fn test_fixtures() -> (internal_api::Task, internal_api::ContentMetadata) {
        let content_metadata = internal_api::ContentMetadata {
            id: "test".to_string(),
            parent_id: "test".to_string(),
            repository: "test".to_string(),
            name: "test".to_string(),
            content_type: "test".to_string(),
            labels: HashMap::new(),
            storage_url: "test".to_string(),
            created_at: 123,
            source: "test".to_string(),
        };
        let task = internal_api::Task {
            id: "test".to_string(),
            extractor: "test".to_string(),
            extractor_binding: "test".to_string(),
            output_index_table_mapping: HashMap::new(),
            repository: "test".to_string(),
            content_metadata: content_metadata.clone(),
            input_params: serde_json::Value::Null,
            outcome: internal_api::TaskOutcome::Unknown,
        };
        (task, content_metadata)
    }

    #[tokio::test]
    async fn test_plan_allocations_tasks_but_no_extractors() {
        let shared_state = create_test_shared_state().await;
        let distributor = RoundRobinDistributor::new(shared_state);
        let (task_fixture, _) = test_fixtures();
        let tasks = vec![Task {
            id: "test".to_string(),
            extractor: "test".to_string(),
            ..task_fixture.clone()
        }];

        let extractor_executors_map = HashMap::new();
        let result = distributor
            .plan_allocations_inner(tasks, extractor_executors_map)
            .await;
        assert!(result.is_ok());
        // should be empty
        assert_eq!(result.unwrap().0.len(), 0);
    }

    #[tokio::test]
    async fn test_plan_allocations_tasks_but_no_executors() {
        let shared_state = create_test_shared_state().await;
        let distributor = RoundRobinDistributor::new(shared_state);
        let (task_fixture, _) = test_fixtures();
        let tasks = vec![Task {
            id: "test".to_string(),
            extractor: "test".to_string(),
            ..task_fixture.clone()
        }];

        let mut extractor_executors_map = HashMap::new();
        extractor_executors_map.insert("test".to_string(), HashSet::new());
        let result = distributor
            .plan_allocations_inner(tasks, extractor_executors_map)
            .await;
        assert!(result.is_ok());
        // should be empty
        assert_eq!(result.unwrap().0.len(), 0);
    }

    #[tokio::test]
    async fn test_round_robin_distribution() {
        let shared_state = create_test_shared_state().await;
        let distributor = RoundRobinDistributor::new(shared_state);
        let (task_fixture, _) = test_fixtures();
        // Assume each task has a unique ID and the same extractor type
        let tasks = (1..=100)
            .map(|i| Task {
                id: format!("task{}", i),
                extractor: "extractor1".to_string(),
                ..task_fixture.clone()
            })
            .collect::<Vec<_>>();

        let mut extractor_executors_map = HashMap::new();
        let executors = (1..=5)
            .map(|i| format!("executor{}", i))
            .collect::<HashSet<_>>();
        extractor_executors_map.insert("extractor1".to_string(), executors);

        let result = distributor
            .plan_allocations_inner(tasks, extractor_executors_map)
            .await
            .unwrap()
            .0;

        // Verify round-robin distribution
        // every executor should have 20 tasks
        for executor in 1..=5 {
            let executor_id = format!("executor{}", executor);
            assert_eq!(result.get(&executor_id).unwrap().len(), 20);
        }
    }

    #[tokio::test]
    async fn test_round_robin_distribution_match_extractors() {
        let shared_state = create_test_shared_state().await;
        let distributor = RoundRobinDistributor::new(shared_state);
        let (task_fixture, _) = test_fixtures();
        // Assume each task has a unique ID and the same extractor type
        let mut tasks = (1..=100)
            .map(|i| Task {
                id: format!("task{}", i),
                extractor: "extractor1".to_string(),
                ..task_fixture.clone()
            })
            .collect::<Vec<_>>();
        tasks.extend(
            (101..=200)
                .map(|i| Task {
                    id: format!("task{}", i),
                    extractor: "extractor2".to_string(),
                    ..task_fixture.clone()
                })
                .collect::<Vec<_>>(),
        );

        let read_tasks = Arc::new(tasks.clone());

        let mut extractor_executors_map = HashMap::new();
        let executors_1 = (1..=5)
            .map(|i| format!("executor{}", i))
            .collect::<HashSet<_>>();
        let executors_2 = (6..=10)
            .map(|i| format!("executor{}", i))
            .collect::<HashSet<_>>();
        extractor_executors_map.insert("extractor1".to_string(), executors_1);
        extractor_executors_map.insert("extractor2".to_string(), executors_2);
        let extractor_executors_map_clone = Arc::new(extractor_executors_map.clone());

        let mut reverse_lookup = HashMap::new();
        for (extractor, executors) in extractor_executors_map_clone.iter() {
            for executor in executors {
                reverse_lookup.insert(executor, extractor);
            }
        }
        let reverse_lookup_clone = Arc::new(reverse_lookup.clone());

        let result = distributor
            .plan_allocations_inner(tasks, extractor_executors_map)
            .await
            .unwrap()
            .0;

        // Verify round-robin distribution
        // every executor should have 20 tasks
        for executor in 1..=10 {
            let executor_id = format!("executor{}", executor);
            assert_eq!(
                result.get(&executor_id).unwrap().len(),
                20,
                "unbalanced for executor: {}",
                executor_id
            );
        }

        // Verify that task extractor matches the executor extractor it was assigned to
        for (executor_id, task_ids) in result.iter() {
            let extractor = (&reverse_lookup_clone)
                .get(executor_id)
                .map(|s| s.to_string())
                .unwrap();
            for task_id in task_ids {
                let task = read_tasks.iter().find(|t| t.id == *task_id).unwrap();
                assert_eq!(task.extractor, *extractor);
            }
        }
    }

    #[tokio::test]
    async fn benchmark_plan_allocations_large_scale() {
        let num_tasks = 100000;
        let num_extrators = 100;
        let executors_per_extractor = 400;

        // Setup for the benchmark
        let shared_state = create_test_shared_state().await;
        let distributor = RoundRobinDistributor::new(shared_state);

        // Generate a large number of tasks and executors
        let (tasks, extractor_executors_map) =
            generate_tasks_and_extractors(num_tasks, num_extrators, executors_per_extractor);

        // Start timing
        let start = Instant::now();

        // Execute the allocation planning
        let _ = distributor
            .plan_allocations_inner(tasks, extractor_executors_map)
            .await
            .unwrap();

        // End timing
        let duration = start.elapsed();

        // In tests, time taken was under 10 seconds for 1,000,000 tasks across 40,000
        // executors. Putting at 20 here so that it's not too strict.
        println!(
            "Time taken for {} tasks across {} extractors with {} executors per extractor: {:?}",
            num_tasks, num_extrators, executors_per_extractor, duration
        );
        assert!(
            duration < Duration::from_secs(20),
            "Time taken for plan_allocations_large_scale took too long: {:?}",
            duration
        );
    }

    fn generate_tasks_and_extractors(
        num_tasks: usize,
        num_extractors: usize,
        executors_per_extractor: usize,
    ) -> (Vec<internal_api::Task>, HashMap<String, HashSet<String>>) {
        let mut tasks = Vec::with_capacity(num_tasks);
        let (task_fixture, _) = test_fixtures();
        let mut extractor_executors_map = HashMap::with_capacity(num_extractors);

        let executors_for_each_extractor: Vec<HashSet<String>> = (0..num_extractors)
            .map(|i| {
                (0..executors_per_extractor)
                    .map(|e| format!("executor{}_{}", i + 1, e))
                    .collect::<HashSet<_>>()
            })
            .collect();

        for (i, executors) in executors_for_each_extractor.iter().enumerate() {
            extractor_executors_map.insert(format!("extractor{}", i + 1), executors.clone());
        }

        for i in 0..num_tasks {
            let extractor_id = format!("extractor{}", i % num_extractors + 1);
            tasks.push(internal_api::Task {
                id: format!("task{}", i),
                extractor: extractor_id,
                ..task_fixture.clone()
            });
        }

        (tasks, extractor_executors_map)
    }
}
