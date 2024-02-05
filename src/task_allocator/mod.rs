use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use mockall::*;

use crate::state::{
    store::{ExecutorId, TaskId},
    SharedState,
};

pub mod planner;

pub struct TaskAllocator {
    shared_state: SharedState,
    /// so a mock can be used for testing
    planner: Box<dyn planner::AllocationPlanner + Send + Sync>,
    committer: Box<dyn CanCommitToRaft + Send + Sync>,
}

impl TaskAllocator {
    pub fn new(shared_state: SharedState) -> Self {
        Self {
            shared_state: shared_state.clone(),
            planner: Box::new(planner::round_robin::RoundRobinDistributor::new(
                shared_state.clone(),
            )),
            committer: Box::new(RaftCommitter { shared_state }),
        }
    }

    pub fn new_with_custom_raft_committer(
        shared_state: SharedState,
        planner: impl planner::AllocationPlanner + Send + Sync + 'static,
        raft_committer: impl CanCommitToRaft + Send + Sync + 'static,
    ) -> Self {
        Self {
            planner: Box::new(planner),
            shared_state: shared_state.clone(),
            committer: Box::new(raft_committer),
        }
    }
}

/// Separated out so it can be mocked
#[automock]
#[async_trait]
pub trait IsTaskAllocator {
    async fn allocate_tasks(&self, task_ids: HashSet<TaskId>) -> Result<(), anyhow::Error>;
    async fn reallocate_all_tasks_matching_executor(
        &self,
        executor_id: ExecutorId,
    ) -> Result<(), anyhow::Error>;
    async fn reallocate_all_tasks_matching_content_types(
        &self,
        content_types: HashSet<String>,
    ) -> Result<(), anyhow::Error>;
}

#[async_trait::async_trait]
impl IsTaskAllocator for TaskAllocator {
    async fn allocate_tasks(&self, task_ids: HashSet<TaskId>) -> Result<(), anyhow::Error> {
        let plan = self.planner.plan_allocations(task_ids).await?;
        self.committer
            .commit_task_assignments(plan.into_task_executor_allocations())
            .await?;
        Ok(())
    }

    /// Reschedule all tasks that match the executor's input mime types, even if
    /// they are already assigned to a different executor.
    async fn reallocate_all_tasks_matching_executor(
        &self,
        executor_id: ExecutorId,
    ) -> Result<(), anyhow::Error> {
        let task_ids = self
            .shared_state
            .get_task_ids_for_executor(executor_id.as_str())
            .await?;
        let plan = self.planner.plan_allocations(task_ids).await?;
        self.committer
            .commit_task_assignments(plan.into_task_executor_allocations())
            .await?;
        Ok(())
    }

    /// Reschedule all tasks that match the content types, even if
    /// they are already assigned to a different executor.
    /// This is useful for example when a new binding is added to an executor
    async fn reallocate_all_tasks_matching_content_types(
        &self,
        content_types: HashSet<String>,
    ) -> Result<(), anyhow::Error> {
        let task_ids = self
            .shared_state
            .get_unfinished_task_ids_by_content_type(content_types)
            .await?;
        let plan = self.planner.plan_allocations(task_ids).await?;
        self.committer
            .commit_task_assignments(plan.into_task_executor_allocations())
            .await?;
        Ok(())
    }
}

struct RaftCommitter {
    shared_state: SharedState,
}

/// Separated out so it can be mocked, and the entire state doesn't need to be
/// modeled in testing
#[automock]
#[async_trait]
pub trait CanCommitToRaft {
    async fn commit_task_assignments(
        &self,
        task_to_executor_assignments: HashMap<TaskId, ExecutorId>,
    ) -> Result<(), anyhow::Error>;
}

#[async_trait::async_trait]
impl CanCommitToRaft for RaftCommitter {
    async fn commit_task_assignments(
        &self,
        task_to_executor_assignments: HashMap<TaskId, ExecutorId>,
    ) -> Result<(), anyhow::Error> {
        self.shared_state
            .commit_task_assignments(task_to_executor_assignments)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::{iter::FromIterator, sync::Arc};

    use mockall::predicate::*;
    use tests::planner::{plan::TaskAllocationPlan, MockAllocationPlanner};

    use super::*;
    use crate::{server_config::ServerConfig, state::App};

    #[tokio::test]
    async fn test_allocate_tasks() {
        let shared_state = App::new(Arc::new(ServerConfig::default())).await.unwrap();
        let mut raft_committer = MockCanCommitToRaft::new();

        let task_ids = HashSet::from_iter(vec![
            "task1".to_string(),
            "task2".to_string(),
            "task3".to_string(),
        ]);
        let cloned_task_ids = task_ids.clone();
        let mut planner = MockAllocationPlanner::new();
        let task_allocation_plan = TaskAllocationPlan({
            let mut map = HashMap::new();
            map.insert(
                "executor1".to_string(),
                HashSet::from_iter(cloned_task_ids.clone()),
            );
            map
        });
        let task_allocation_plan_clone = task_allocation_plan.clone();
        let task_executor_allocations = task_allocation_plan.into_task_executor_allocations();
        planner
            .expect_plan_allocations()
            .with(eq(task_ids.clone()))
            .times(1)
            .returning(move |_| Ok(task_allocation_plan_clone.clone()));

        // commits to raft with the task allocation plan result
        raft_committer
            .expect_commit_task_assignments()
            .with(eq(task_executor_allocations.clone()))
            .times(1)
            .returning(|_| Ok(()));

        let task_allocator = TaskAllocator::new_with_custom_raft_committer(
            shared_state.clone(),
            planner,
            raft_committer,
        );

        task_allocator
            .allocate_tasks(task_ids)
            .await
            .expect("allocate_tasks failed");
    }

    fn test_fixtures() -> (
        internal_api::ExecutorMetadata,
        internal_api::ExtractorDescription,
    ) {
        let extractor_description_fixture = internal_api::ExtractorDescription {
            input_mime_types: vec!["application/json".to_string(), "text/csv".to_string()],
            name: "test".to_string(),
            description: "test".to_string(),
            input_params: serde_json::Value::Null,
            outputs: HashMap::new(),
        };
        let executor_metadata_fixture = internal_api::ExecutorMetadata {
            extractor: extractor_description_fixture.clone(),
            last_seen: 123,
            addr: "test".to_string(),
            id: "test".to_string(),
        };
        (executor_metadata_fixture, extractor_description_fixture)
    }

    use indexify_internal_api as internal_api;
    #[tokio::test]
    async fn test_reallocate_all_tasks_matching_executor() {
        let shared_state = App::new(Arc::new(ServerConfig::default())).await.unwrap();
        let mut raft_committer = MockCanCommitToRaft::new();
        let mut sm = shared_state.indexify_state.write().await;
        let matching_input_mime_types =
            vec!["application/json".to_string(), "text/csv".to_string()];
        let not_matching_input_mime_types =
            vec!["application/xml".to_string(), "text/html".to_string()];
        // populate the executors table
        let (executor_metadata_fixture, extractor_description_fixture) = test_fixtures();

        sm.executors.insert(
            "executor1".to_string(),
            internal_api::ExecutorMetadata {
                extractor: internal_api::ExtractorDescription {
                    input_mime_types: matching_input_mime_types.clone(),
                    ..extractor_description_fixture.clone()
                },
                ..executor_metadata_fixture.clone()
            },
        );
        sm.executors.insert(
            "executor2".to_string(),
            internal_api::ExecutorMetadata {
                extractor: internal_api::ExtractorDescription {
                    input_mime_types: not_matching_input_mime_types.clone(),
                    ..extractor_description_fixture.clone()
                },
                ..executor_metadata_fixture.clone()
            },
        );
        sm.executors.insert(
            "executor3".to_string(),
            internal_api::ExecutorMetadata {
                extractor: internal_api::ExtractorDescription {
                    input_mime_types: matching_input_mime_types.clone(),
                    ..extractor_description_fixture.clone()
                },
                ..executor_metadata_fixture.clone()
            },
        );

        // populate the unfinished_tasks_by_content_type table
        sm.unfinished_tasks_by_content_type.insert(
            "application/json".to_string(),
            HashSet::from_iter(vec!["task1".to_string(), "task2".to_string()]),
        );
        sm.unfinished_tasks_by_content_type.insert(
            "text/csv".to_string(),
            HashSet::from_iter(vec!["task3".to_string(), "task4".to_string()]),
        );
        // tasks that don't match
        sm.unfinished_tasks_by_content_type.insert(
            "application/xml".to_string(),
            HashSet::from_iter(vec!["task5".to_string(), "task6".to_string()]),
        );
        sm.unfinished_tasks_by_content_type.insert(
            "text/html".to_string(),
            HashSet::from_iter(vec!["task7".to_string(), "task8".to_string()]),
        );

        // release the lock
        drop(sm);

        let mut planner = MockAllocationPlanner::new();
        let task_allocation_plan = TaskAllocationPlan({
            let mut map = HashMap::new();
            map.insert(
                "executor1".to_string(),
                HashSet::from_iter(vec!["task1".to_string(), "task3".to_string()]),
            );
            map.insert(
                "executor3".to_string(),
                HashSet::from_iter(vec!["task2".to_string(), "task4".to_string()]),
            );
            map
        });
        let task_allocation_plan_clone = task_allocation_plan.clone();
        let task_executor_allocations = task_allocation_plan.into_task_executor_allocations();
        planner
            .expect_plan_allocations()
            .with(eq(HashSet::from_iter(vec![
                "task1".to_string(),
                "task2".to_string(),
                "task3".to_string(),
                "task4".to_string(),
            ])))
            .times(1)
            .returning(move |_| Ok(task_allocation_plan_clone.clone()));

        // commits to raft with the task allocation plan result
        raft_committer
            .expect_commit_task_assignments()
            .with(eq(task_executor_allocations.clone()))
            .times(1)
            .returning(|_| Ok(()));

        let task_allocator = TaskAllocator::new_with_custom_raft_committer(
            shared_state.clone(),
            planner,
            raft_committer,
        );

        task_allocator
            .reallocate_all_tasks_matching_executor("executor1".to_string())
            .await
            .expect("reallocate_all_tasks_matching_executor failed");
    }

    #[tokio::test]
    async fn test_reallocate_all_tasks_matching_content_types() {
        let shared_state = App::new(Arc::new(ServerConfig::default())).await.unwrap();
        let mut raft_committer = MockCanCommitToRaft::new();
        let mut sm = shared_state.indexify_state.write().await;
        let matching_input_mime_types =
            vec!["application/json".to_string(), "text/csv".to_string()];
        let not_matching_input_mime_types =
            vec!["application/xml".to_string(), "text/html".to_string()];
        let (executor_metadata_fixture, extractor_description_fixture) = test_fixtures();

        // populate the executors table
        sm.executors.insert(
            "executor1".to_string(),
            internal_api::ExecutorMetadata {
                extractor: internal_api::ExtractorDescription {
                    input_mime_types: matching_input_mime_types.clone(),
                    ..extractor_description_fixture.clone()
                },
                ..executor_metadata_fixture.clone()
            },
        );
        sm.executors.insert(
            "executor2".to_string(),
            internal_api::ExecutorMetadata {
                extractor: internal_api::ExtractorDescription {
                    input_mime_types: not_matching_input_mime_types.clone(),
                    ..extractor_description_fixture.clone()
                },
                ..executor_metadata_fixture.clone()
            },
        );
        sm.executors.insert(
            "executor3".to_string(),
            internal_api::ExecutorMetadata {
                extractor: internal_api::ExtractorDescription {
                    input_mime_types: matching_input_mime_types.clone(),
                    ..extractor_description_fixture.clone()
                },
                ..executor_metadata_fixture.clone()
            },
        );

        // populate the unfinished_tasks_by_content_type table
        sm.unfinished_tasks_by_content_type.insert(
            "application/json".to_string(),
            HashSet::from_iter(vec!["task1".to_string(), "task2".to_string()]),
        );
        sm.unfinished_tasks_by_content_type.insert(
            "text/csv".to_string(),
            HashSet::from_iter(vec!["task3".to_string(), "task4".to_string()]),
        );
        // tasks that don't match
        sm.unfinished_tasks_by_content_type.insert(
            "application/xml".to_string(),
            HashSet::from_iter(vec!["task5".to_string(), "task6".to_string()]),
        );
        sm.unfinished_tasks_by_content_type.insert(
            "text/html".to_string(),
            HashSet::from_iter(vec!["task7".to_string(), "task8".to_string()]),
        );

        // release the lock
        drop(sm);

        let mut planner = MockAllocationPlanner::new();
        let task_allocation_plan = TaskAllocationPlan({
            let mut map = HashMap::new();
            map.insert(
                "executor1".to_string(),
                HashSet::from_iter(vec!["task1".to_string(), "task3".to_string()]),
            );
            map.insert(
                "executor3".to_string(),
                HashSet::from_iter(vec!["task2".to_string(), "task4".to_string()]),
            );
            map
        });

        let task_allocation_plan_clone = task_allocation_plan.clone();
        let task_executor_allocations = task_allocation_plan.into_task_executor_allocations();
        planner
            .expect_plan_allocations()
            .with(eq(HashSet::from_iter(vec![
                "task1".to_string(),
                "task2".to_string(),
                "task3".to_string(),
                "task4".to_string(),
            ])))
            .times(1)
            .returning(move |_| Ok(task_allocation_plan_clone.clone()));

        // commits to raft with the task allocation plan result
        raft_committer
            .expect_commit_task_assignments()
            .with(eq(task_executor_allocations.clone()))
            .times(1)
            .returning(|_| Ok(()));

        let task_allocator = TaskAllocator::new_with_custom_raft_committer(
            shared_state.clone(),
            planner,
            raft_committer,
        );

        task_allocator
            .reallocate_all_tasks_matching_content_types(HashSet::from_iter(vec![
                "application/json".to_string(),
                "text/csv".to_string(),
            ]))
            .await
            .expect("reallocate_all_tasks_matching_content_types failed");
    }
}
