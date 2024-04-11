use std::{
    collections::{hash_map::DefaultHasher, HashMap, HashSet},
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
        let mut state_change_processed = false;
        // Create new tasks
        let tuple_result = self.create_new_tasks(state_change.clone()).await?;
        let tasks = tuple_result.0;
        let content_extraction_policy_mappings = tuple_result.1;

        // Commit them
        if !tasks.is_empty() {
            self.shared_state
                .create_tasks(tasks.clone(), &state_change.id)
                .await?;
            self.shared_state
                .set_content_extraction_policy_mappings(content_extraction_policy_mappings)
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
            let extractor = self
                .shared_state
                .extractor_with_name(&policy.extractor)
                .await?;
            for name in extractor.outputs.keys() {
                let index_name = policy.output_index_name_mapping.get(name).unwrap();
                let table_name = policy.index_name_table_mapping.get(index_name).unwrap();
                tables.push(table_name.clone());
            }
        }
        Ok(tables)
    }

    pub async fn create_new_tasks(
        &self,
        state_change: StateChange,
    ) -> Result<(
        Vec<internal_api::Task>,
        Vec<internal_api::ContentExtractionPolicyMapping>,
    )> {
        let tasks_and_content_policy_mapping = match &state_change.change_type {
            internal_api::ChangeType::NewExtractionPolicy => {
                let content_list = self
                    .shared_state
                    .content_matching_policy(&state_change.object_id)
                    .await?;
                self.create_task_list(&state_change.object_id, content_list)
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
                let mut tasks: Vec<internal_api::Task> = Vec::new();
                let mut content_extraction_policy_mapppings: Vec<
                    internal_api::ContentExtractionPolicyMapping,
                > = Vec::new();
                let tables = self.tables_for_policies(&extraction_policies).await?;
                for extraction_policy in extraction_policies {
                    let tasks_and_content_extraction_policy_mappings = self
                        .create_task(&extraction_policy.id, &content, &tables)
                        .await?;
                    tasks.extend(tasks_and_content_extraction_policy_mappings.0);
                    content_extraction_policy_mapppings
                        .extend(tasks_and_content_extraction_policy_mappings.1);
                }
                (tasks, content_extraction_policy_mapppings)
            }
            _ => (Vec::new(), Vec::new()),
        };
        Ok(tasks_and_content_policy_mapping)
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

    pub async fn create_task_list(
        &self,
        extraction_policy_id: &str,
        contents: Vec<internal_api::ContentMetadata>,
    ) -> Result<(
        Vec<internal_api::Task>,
        Vec<internal_api::ContentExtractionPolicyMapping>,
    )> {
        let mut tasks = Vec::new();
        let mut content_extraction_policy_mappings = Vec::new();
        for content in &contents {
            // Get list of existing extraction policies for content.
            let mappings = self
                .shared_state
                .get_content_extraction_policy_mappings_for_content_id(&content.id)
                .await?;
            let mut policy_ids = if let Some(mappings) = mappings {
                mappings.extraction_policy_ids
            } else {
                HashSet::new()
            };

            // Add the new extraction policy to the list of policies for the content.
            policy_ids.insert(extraction_policy_id.to_string());

            // Collect all index tables associated with the content including the new
            // policy.
            let tables = if let Some(policies) = self
                .shared_state
                .get_extraction_policies_from_ids(policy_ids)
                .await?
            {
                self.tables_for_policies(&policies).await?
            } else {
                Vec::new()
            };

            let (new_tasks, new_policy_mapping) = self
                .create_task(extraction_policy_id, content, &tables)
                .await?;
            tasks.extend(new_tasks);
            content_extraction_policy_mappings.extend(new_policy_mapping);
        }
        Ok((tasks, content_extraction_policy_mappings))
    }

    pub async fn create_task(
        &self,
        extraction_policy_id: &str,
        content: &internal_api::ContentMetadata,
        index_tables: &[String],
    ) -> Result<(
        Vec<internal_api::Task>,
        Vec<internal_api::ContentExtractionPolicyMapping>,
    )> {
        let extraction_policy = self
            .shared_state
            .get_extraction_policy(extraction_policy_id)
            .await?;
        let extractor = self
            .shared_state
            .extractor_with_name(&extraction_policy.extractor)
            .await?;
        let mut output_mapping: HashMap<String, String> = HashMap::new();

        //  TODO: This will be done in create_extraction_policy in coordinator_service.
        // Just store the mapping
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
        let mut content_extraction_policy_mappings = Vec::new();
        let mut hasher = DefaultHasher::new();
        extraction_policy.name.hash(&mut hasher);
        extraction_policy.namespace.hash(&mut hasher);
        content.id.hash(&mut hasher);
        let id = format!("{:x}", hasher.finish());
        let task = internal_api::Task {
            id,
            extractor: extraction_policy.extractor.clone(),
            extraction_policy_id: extraction_policy.id.clone(),
            output_index_table_mapping: output_mapping.clone(),
            namespace: extraction_policy.namespace.clone(),
            content_metadata: content.clone(),
            input_params: extraction_policy.input_params.clone(),
            outcome: internal_api::TaskOutcome::Unknown,
            index_tables: index_tables.to_vec(),
        };
        info!("created task: {:?}", task);
        tasks.push(task);

        let content_extraction_policy_mapping = internal_api::ContentExtractionPolicyMapping {
            content_id: content.id.clone(),
            extraction_policy_ids: HashSet::from_iter(vec![extraction_policy.id.clone()]),
            time_of_policy_completion: HashMap::new(),
        };
        content_extraction_policy_mappings.push(content_extraction_policy_mapping);
        Ok((tasks, content_extraction_policy_mappings))
    }
}
