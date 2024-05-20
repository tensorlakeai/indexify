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
    utils::timestamp_secs,
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

    pub async fn create_new_tasks(&self, state_change: StateChange) -> Result<()> {
        let mut tasks: Vec<internal_api::Task> = Vec::new();
        if let Some(content) = self
            .shared_state
            .state_machine
            .get_latest_version_of_content(&state_change.object_id)?
        {
            let extraction_policies = self
                .shared_state
                .match_extraction_policies_for_content(&content)
                .await?;
            let tables = self.tables_for_policies(&extraction_policies).await?;
            for extraction_policy in extraction_policies {
                let task = self
                    .create_task(&extraction_policy.id, &content, &tables)
                    .await?;
                tasks.push(task);
            }
            if !tasks.is_empty() {
                self.shared_state
                    .create_tasks(tasks.clone(), state_change.id)
                    .await?;
                let allocation_plan = self.allocate_tasks(tasks).await?;
                if !allocation_plan.0.is_empty() {
                    self.shared_state
                        .commit_task_assignments(allocation_plan.0, state_change.id)
                        .await
                } else {
                    Ok(())
                }
            } else {
                let root_content_id = if let Some(root_id) = content.root_content_id {
                    self.shared_state
                        .state_machine
                        .get_latest_version_of_content(&&root_id)?
                        .map(|c| c.id)
                } else {
                    Some(content.id)
                };
                // Since we processed NewContent without creating a task, need to trigger
                // garbage collection for previous content if root content was updated.
                let new_state_changes = if let Some(root_content_id) = root_content_id {
                    if root_content_id.version > 1 {
                        vec![StateChange::new(
                            root_content_id.to_string(),
                            indexify_internal_api::ChangeType::TaskCompleted { root_content_id },
                            timestamp_secs(),
                        )]
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                };
                self.shared_state
                    .mark_change_events_as_processed(vec![state_change], new_state_changes)
                    .await
            }
        } else {
            self.shared_state
                .mark_change_events_as_processed(vec![state_change], Vec::new())
                .await
        }
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

    pub async fn redistribute_tasks(&self, state_change: &StateChange) -> Result<()> {
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

        if !task_allocation_plan.is_empty() {
            self.shared_state
                .commit_task_assignments(task_allocation_plan, state_change.id)
                .await
        } else {
            self.shared_state
                .mark_change_events_as_processed(vec![state_change.clone()], Vec::new())
                .await
        }
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
