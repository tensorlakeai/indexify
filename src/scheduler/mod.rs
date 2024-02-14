use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
};

use anyhow::{Ok, Result};
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
        let mut state_change_processed = false;
        // Create new tasks
        let tasks = self.create_new_tasks(state_change.clone()).await?;
        // Commit them
        if tasks.len() > 0 {
            self.shared_state
                .create_tasks(tasks.clone(), &state_change.id)
                .await?;
            state_change_processed = true;
        }

        // Allocate tasks and commit task assignments
        let allocation_plan = self.allocate_tasks(tasks).await?;
        if allocation_plan.0.len() > 0 {
            self.shared_state
                .commit_task_assignments(allocation_plan.0, &state_change.id)
                .await?;
            state_change_processed = true;
        }

        // Redistribute tasks and commit task assignments
        let allocation_plan = self.redistribute_tasks(&state_change).await?;
        if allocation_plan.0.len() > 0 {
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

    pub async fn create_new_tasks(
        &self,
        state_change: StateChange,
    ) -> Result<Vec<internal_api::Task>> {
        let tasks = match &state_change.change_type {
            internal_api::ChangeType::NewBinding => {
                let content_list = self
                    .shared_state
                    .content_matching_binding(&state_change.object_id)
                    .await?;

                self.create_task(&state_change.object_id, content_list)
                    .await?
            }
            internal_api::ChangeType::NewContent => {
                let bindings = self
                    .shared_state
                    .filter_extractor_binding_for_content(&state_change.object_id)
                    .await?;
                let content = self
                    .shared_state
                    .get_conent_metadata(&state_change.object_id)
                    .await?;
                let mut tasks = Vec::new();
                for binding in bindings {
                    let tasks_for_binding =
                        self.create_task(&binding.id, vec![content.clone()]).await?;
                    tasks.extend(tasks_for_binding)
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
        self.task_allocator.allocate_tasks(task_ids).await
    }

    pub async fn redistribute_tasks(
        &self,
        state_change: &StateChange,
    ) -> Result<TaskAllocationPlan> {
        if state_change.change_type == internal_api::ChangeType::ExecutorRemoved ||
            state_change.change_type == internal_api::ChangeType::ExecutorAdded
        {
            let executor = self
                .shared_state
                .get_executor_by_id(&state_change.object_id)
                .await?;
            return self
                .task_allocator
                .reallocate_all_tasks_matching_extractor(&executor.extractor.name)
                .await;
        }
        Ok(TaskAllocationPlan(HashMap::new()))
    }

    pub async fn create_task(
        &self,
        extractor_binding_id: &str,
        content_list: Vec<internal_api::ContentMetadata>,
    ) -> Result<Vec<internal_api::Task>> {
        let extractor_binding = self
            .shared_state
            .get_extractor_binding(extractor_binding_id)
            .await?;
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
            extractor_binding.namespace.hash(&mut hasher);
            content.id.hash(&mut hasher);
            let id = format!("{:x}", hasher.finish());
            let task = internal_api::Task {
                id,
                extractor: extractor_binding.extractor.clone(),
                extractor_binding: extractor_binding.name.clone(),
                output_index_table_mapping: output_mapping.clone(),
                namespace: extractor_binding.namespace.clone(),
                content_metadata: content.clone(),
                input_params: extractor_binding.input_params.clone(),
                outcome: internal_api::TaskOutcome::Unknown,
            };
            info!("created task: {:?}", task);
            tasks.push(task);
        }
        Ok(tasks)
    }
}
