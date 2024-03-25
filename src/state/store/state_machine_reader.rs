use std::{collections::HashSet, sync::Arc};

use indexify_internal_api;
use rocksdb::OptimisticTransactionDB;
use tracing::error;

use super::{
    serializer::{JsonEncode, JsonEncoder},
    StateMachineColumns,
    StateMachineError,
    TaskId,
};

#[derive(Clone)]
pub struct StateMachineReader {}

//  TODO: Initialize this with a DB reference, so DB doesn't need to be passed
// everywhere
impl StateMachineReader {
    pub async fn get_content_extraction_policy_mappings_for_content_id(
        &self,
        content_id: &str,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Option<indexify_internal_api::ContentExtractionPolicyMapping>, StateMachineError>
    {
        let mapping_bytes = match db
            .get_cf(
                StateMachineColumns::ExtractionPoliciesAppliedOnContent.cf(db),
                content_id.as_bytes(),
            )
            .map_err(|e| StateMachineError::TransactionError(e.to_string()))?
        {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        JsonEncoder::decode::<indexify_internal_api::ContentExtractionPolicyMapping>(&mapping_bytes)
            .map(Some)
    }

    pub async fn get_tasks_for_executor(
        &self,
        executor_id: &str,
        limit: Option<u64>,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Vec<indexify_internal_api::Task>, StateMachineError> {
        //  NOTE: Don't do deserialization within the transaction
        let txn = db.transaction();
        let task_ids_bytes = txn
            .get_cf(StateMachineColumns::TaskAssignments.cf(db), executor_id)
            .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;

        let task_ids: Vec<String> = task_ids_bytes
            .map(|task_id_bytes| {
                JsonEncoder::decode(&task_id_bytes)
                    .map_err(StateMachineError::from)
                    .unwrap_or_else(|e| {
                        error!("Failed to deserialize task id: {}", e);
                        Vec::new()
                    })
            })
            .unwrap_or_else(Vec::new);

        // FIXME Use MULTIGET
        let limit = limit.unwrap_or(task_ids.len() as u64) as usize;

        let tasks: Result<Vec<indexify_internal_api::Task>, StateMachineError> = task_ids
            .into_iter()
            .take(limit)
            .map(|task_id| {
                let task_bytes = txn
                    .get_cf(StateMachineColumns::Tasks.cf(db), task_id.as_bytes())
                    .map_err(|e| StateMachineError::TransactionError(e.to_string()))?
                    .ok_or_else(|| {
                        StateMachineError::DatabaseError(format!("Task {} not found", task_id))
                    })?;
                JsonEncoder::decode(&task_bytes).map_err(StateMachineError::from)
            })
            .collect();
        tasks
    }

    pub async fn get_indexes_from_ids(
        &self,
        task_ids: HashSet<TaskId>,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Vec<indexify_internal_api::Index>, StateMachineError> {
        let txn = db.transaction();
        let indexes: Result<Vec<indexify_internal_api::Index>, StateMachineError> = task_ids
            .into_iter()
            .map(|task_id| {
                let index_bytes = txn
                    .get_cf(StateMachineColumns::IndexTable.cf(db), task_id.as_bytes())
                    .map_err(|e| StateMachineError::TransactionError(e.to_string()))?
                    .ok_or_else(|| {
                        StateMachineError::DatabaseError(format!("Index {} not found", task_id))
                    })?;
                JsonEncoder::decode(&index_bytes).map_err(StateMachineError::from)
            })
            .collect();
        indexes
    }

    pub async fn get_executors_from_ids(
        &self,
        executor_ids: HashSet<String>,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Vec<indexify_internal_api::ExecutorMetadata>, StateMachineError> {
        let txn = db.transaction();
        let executors: Result<Vec<indexify_internal_api::ExecutorMetadata>, StateMachineError> =
            executor_ids
                .into_iter()
                .map(|executor_id| {
                    let executor_bytes = txn
                        .get_cf(
                            StateMachineColumns::Executors.cf(db),
                            executor_id.as_bytes(),
                        )
                        .map_err(|e| StateMachineError::TransactionError(e.to_string()))?
                        .ok_or_else(|| {
                            StateMachineError::DatabaseError(format!(
                                "Executor {} not found",
                                executor_id
                            ))
                        })?;
                    JsonEncoder::decode(&executor_bytes).map_err(StateMachineError::from)
                })
                .collect();
        executors
    }

    pub async fn get_content_from_ids(
        &self,
        content_ids: HashSet<String>,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Vec<indexify_internal_api::ContentMetadata>, StateMachineError> {
        let txn = db.transaction();
        let content: Result<Vec<indexify_internal_api::ContentMetadata>, StateMachineError> =
            content_ids
                .into_iter()
                .map(|content_id| {
                    let content_bytes = txn
                        .get_cf(
                            StateMachineColumns::ContentTable.cf(db),
                            content_id.as_bytes(),
                        )
                        .map_err(|e| StateMachineError::TransactionError(e.to_string()))?
                        .ok_or_else(|| {
                            StateMachineError::DatabaseError(format!(
                                "Content {} not found",
                                content_id
                            ))
                        })?;
                    serde_json::from_slice(&content_bytes).map_err(StateMachineError::from)
                })
                .collect();
        content
    }

    /// This method tries to retrieve all policies based on id's. If it cannot
    /// find any, it skips them If it encounters an error at any point
    /// during the transaction, it returns out immediately
    pub async fn get_extraction_policies_from_ids(
        &self,
        extraction_policy_ids: HashSet<String>,
        db: &Arc<OptimisticTransactionDB>,
    ) -> Result<Option<Vec<indexify_internal_api::ExtractionPolicy>>, StateMachineError> {
        let txn = db.transaction();

        let mut policies = Vec::new();
        for id in extraction_policy_ids.iter() {
            let bytes_opt = txn
                .get_cf(
                    StateMachineColumns::ExtractionPolicies.cf(db),
                    id.as_bytes(),
                )
                .map_err(|e| StateMachineError::TransactionError(e.to_string()))?;

            if let Some(bytes) = bytes_opt {
                let policy =
                    serde_json::from_slice::<indexify_internal_api::ExtractionPolicy>(&bytes)
                        .map_err(StateMachineError::SerializationError)?;
                policies.push(policy);
            }
            // If None, the policy is not found; we simply skip it.
        }

        if policies.is_empty() {
            Ok(None)
        } else {
            Ok(Some(policies))
        }
    }
}
