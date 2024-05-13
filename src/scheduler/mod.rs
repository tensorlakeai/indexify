use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
};

use anyhow::{anyhow, Ok, Result};
use indexify_internal_api as internal_api;
use indexify_internal_api::StateChange;
use internal_api::OutputSchema;
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

    async fn tables_for_policies(
        &self,
        policies: &[internal_api::ExtractionPolicy],
    ) -> Result<Vec<String>> {
        let mut tables = Vec::new();
        for policy in policies {
            let extractor = self.shared_state.extractor_with_name(&policy.extractor)?;
            for (name, schema) in extractor.outputs {
                if let OutputSchema::Embedding(_) = schema {
                    let table_name = policy.output_table_mapping.get(&name).unwrap();
                    tables.push(table_name.clone());
                }
            }
        }
        Ok(tables)
    }

    pub async fn create_new_tasks(
        &self,
        state_change: StateChange,
    ) -> Result<Vec<internal_api::Task>> {
        let tasks = match &state_change.change_type {
            internal_api::ChangeType::NewContent => {
                let extraction_policies = self
                    .shared_state
                    .match_extraction_policies_for_content(
                        &state_change.object_id.clone().try_into()?,
                    )
                    .await?;
                let content = self
                    .shared_state
                    .get_content_metadata_with_version(&state_change.object_id.clone().try_into()?)
                    .await?;
                let mut tasks: Vec<internal_api::Task> = Vec::new();
                let tables = self.tables_for_policies(&extraction_policies).await?;
                for extraction_policy in extraction_policies {
                    let task = self
                        .create_task(&extraction_policy.id, &content, &tables)
                        .await?;
                    tasks.push(task);
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

            // Get all extractor names own by the executor
            let extractor_names = executor
                .extractors
                .iter()
                .map(|extractor| extractor.name.clone())
                .collect::<Vec<String>>();

            // This HashMap is used to aggregate the task re-allocation
            // plan for each extractor in the executor.
            let mut task_allocation_plan = HashMap::new();

            for extractor_name in extractor_names {
                let plan = self
                    .task_allocator
                    .reallocate_all_tasks_matching_extractor(&extractor_name)
                    .await
                    .map_err(|e| anyhow!("redistribute_tasks: {}", e))?;

                // Transfer the task id and executor id from the plan to the
                // aggregated task allocation plan.
                for (task_id, executor_id) in plan.0 {
                    task_allocation_plan.insert(task_id, executor_id);
                }
            }

            return Ok(TaskAllocationPlan(task_allocation_plan));
        }
        Ok(TaskAllocationPlan(HashMap::new()))
    }

    pub async fn create_task(
        &self,
        extraction_policy_id: &str,
        content: &internal_api::ContentMetadata,
        index_tables: &[String],
    ) -> Result<internal_api::Task> {
        let extraction_policy = self
            .shared_state
            .get_extraction_policy(extraction_policy_id)?;
        let extractor = self
            .shared_state
            .extractor_with_name(&extraction_policy.extractor)?;

        let mut output_mapping: HashMap<String, String> = HashMap::new();
        for name in extractor.outputs.keys() {
            let table_name = extraction_policy.output_table_mapping.get(name).unwrap();
            output_mapping.insert(name.clone(), table_name.clone());
        }

        let mut hasher = DefaultHasher::new();
        extraction_policy.name.hash(&mut hasher);
        extraction_policy.namespace.hash(&mut hasher);
        content.id.hash(&mut hasher);
        let id = format!("{:x}", hasher.finish());
        let task = internal_api::Task {
            id,
            extractor: extraction_policy.extractor.clone(),
            extraction_graph_name: extraction_policy.graph_name.clone(),
            extraction_policy_id: extraction_policy.id.clone(),
            output_index_table_mapping: output_mapping.clone(),
            namespace: extraction_policy.namespace.clone(),
            content_metadata: content.clone(),
            input_params: extraction_policy.input_params.clone(),
            outcome: internal_api::TaskOutcome::Unknown,
            index_tables: index_tables.to_vec(),
        };
        info!("created task: {:?}", task);
        Ok(task)
    }
}
