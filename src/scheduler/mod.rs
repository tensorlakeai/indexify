use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
};

use anyhow::{anyhow, Ok, Result};
use indexify_internal_api as internal_api;
use indexify_internal_api::StateChange;
use tracing::info;

use crate::{
    state::SharedState,
    task_allocator::{planner::plan::TaskAllocationPlan, TaskAllocator},
};

pub struct Scheduler {
    shared_state: SharedState,
    task_allocator: TaskAllocator,
}

impl Scheduler {
    pub fn new(shared_state: SharedState, task_allocator: TaskAllocator) -> Self {
        Scheduler {
            shared_state,
            task_allocator,
        }
    }

    pub async fn handle_change_event(&self, state_change: StateChange) -> Result<()> {
        println!(
            "handle_change_event triggered with state_change {:#?}",
            state_change
        );
        let mut state_change_processed = false;

        //  Check if garbage collection tasks need to be created
        if state_change.change_type == internal_api::ChangeType::DeleteContent {
            let content = self
                .shared_state
                .get_conent_metadata(&state_change.object_id)
                .await?;
            let gc_tasks = self.create_garbage_collection_tasks(content).await;
            self.shared_state
                .create_garbage_collection_tasks(gc_tasks, &state_change.id)
                .await?;
            state_change_processed = true;
        }

        // Create new tasks
        let tasks = self.create_new_tasks(state_change.clone()).await?;
        println!("\nTasks created: {:?}\n", tasks);
        // Commit them
        if !tasks.is_empty() {
            self.shared_state
                .create_tasks(tasks.clone(), &state_change.id)
                .await?;
            state_change_processed = true;
        }

        // Allocate tasks and commit task assignments
        let allocation_plan = self.allocate_tasks(tasks).await?;
        if !allocation_plan.0.is_empty() {
            self.shared_state
                .commit_task_assignments(allocation_plan.0, &state_change.id)
                .await?;
            state_change_processed = true;
        }

        // Redistribute tasks and commit task assignments
        let allocation_plan = self.redistribute_tasks(&state_change).await?;
        if !allocation_plan.0.is_empty() {
            self.shared_state
                .commit_task_assignments(allocation_plan.0, &state_change.id)
                .await?;
            state_change_processed = true;
        }

        // Mark the state change as processed
        if !state_change_processed {
            self.shared_state
                .mark_change_events_as_processed(vec![state_change])
                .await?;
        }

        Ok(())
    }

    pub async fn create_garbage_collection_tasks(
        &self,
        content_metadata: internal_api::ContentMetadata,
    ) -> Vec<internal_api::GarbageCollectionTask> {
        let mut hasher = DefaultHasher::new();
        content_metadata.id.hash(&mut hasher);
        content_metadata.name.hash(&mut hasher);
        content_metadata.namespace.hash(&mut hasher);
        //  TODO: Should the id here just be a nanoid rather than a hash of values?
        let id = format!("{:x}", hasher.finish());
        let task = internal_api::GarbageCollectionTask {
            id,
            namespace: content_metadata.namespace.clone(),
            content_metadata,
            outcome: internal_api::TaskOutcome::Unknown,
        };
        vec![task]
    }

    pub async fn create_new_tasks(
        &self,
        state_change: StateChange,
    ) -> Result<Vec<internal_api::Task>> {
        let tasks = match &state_change.change_type {
            internal_api::ChangeType::NewExtractionPolicy => {
                println!("Creating tasks for the new extraction policy");
                let content_list = self
                    .shared_state
                    .content_matching_policy(&state_change.object_id)
                    .await?;
                println!("The content list is {:?}", content_list);

                self.create_task(&state_change.object_id, content_list)
                    .await?
            }
            internal_api::ChangeType::NewContent => {
                let extraction_policies = self
                    .shared_state
                    .filter_extraction_policy_for_content(&state_change.object_id)
                    .await?;
                let content = self
                    .shared_state
                    .get_conent_metadata(&state_change.object_id)
                    .await?;
                let mut tasks = Vec::new();
                for extraction_policy in extraction_policies {
                    let tasks_for_policy = self
                        .create_task(&extraction_policy.id, vec![content.clone()])
                        .await?;
                    tasks.extend(tasks_for_policy)
                }
                tasks
            }
            _ => Vec::new(),
        };
        Ok(tasks)
    }

    pub async fn allocate_tasks(
        &self,
        tasks: Vec<internal_api::Task>,
    ) -> Result<TaskAllocationPlan> {
        let task_ids = tasks.iter().map(|task| task.id.clone()).collect();
        self.task_allocator
            .allocate_tasks(task_ids)
            .await
            .map_err(|e| anyhow!("allocate_tasks: {}", e))
    }

    pub async fn redistribute_tasks(
        &self,
        state_change: &StateChange,
    ) -> Result<TaskAllocationPlan> {
        if state_change.change_type == internal_api::ChangeType::ExecutorAdded {
            let executor = self
                .shared_state
                .get_executor_by_id(&state_change.object_id)
                .await
                .map_err(|e| anyhow!("redistribute_tasks: {}", e))?;
            return self
                .task_allocator
                .reallocate_all_tasks_matching_extractor(&executor.extractor.name)
                .await
                .map_err(|e| anyhow!("redistribute_tasks: {}", e));
        }
        Ok(TaskAllocationPlan(HashMap::new()))
    }

    pub async fn create_task(
        &self,
        extraction_policy_id: &str,
        content_list: Vec<internal_api::ContentMetadata>,
    ) -> Result<Vec<internal_api::Task>> {
        let extraction_policy = self
            .shared_state
            .get_extraction_policy(extraction_policy_id)
            .await?;
        let extractor = self
            .shared_state
            .extractor_with_name(&extraction_policy.extractor)
            .await?;
        let mut output_mapping: HashMap<String, String> = HashMap::new();
        for name in extractor.outputs.keys() {
            let index_name = extraction_policy
                .output_index_name_mapping
                .get(name)
                .unwrap();
            let table_name = extraction_policy
                .index_name_table_mapping
                .get(index_name)
                .unwrap();
            output_mapping.insert(name.clone(), table_name.clone());
        }
        let mut tasks = Vec::new();
        for content in content_list {
            let mut hasher = DefaultHasher::new();
            extraction_policy.name.hash(&mut hasher);
            extraction_policy.namespace.hash(&mut hasher);
            content.id.hash(&mut hasher);
            let id = format!("{:x}", hasher.finish());
            let task = internal_api::Task {
                id,
                extractor: extraction_policy.extractor.clone(),
                extraction_policy: extraction_policy.name.clone(),
                output_index_table_mapping: output_mapping.clone(),
                namespace: extraction_policy.namespace.clone(),
                content_metadata: content.clone(),
                input_params: extraction_policy.input_params.clone(),
                outcome: internal_api::TaskOutcome::Unknown,
            };
            info!("created task: {:?}", task);
            tasks.push(task);
        }
        Ok(tasks)
    }
}
