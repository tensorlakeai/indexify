use anyhow::anyhow;
use indexify_internal_api::{v1, *};
use rocksdb::OptimisticTransactionDB;

use crate::state::store::{
    serializer::{JsonEncode, JsonEncoder},
    StateMachineColumns,
};

pub fn convert_v1_task(
    task: v1::Task,
    db: &OptimisticTransactionDB,
) -> Result<Task, anyhow::Error> {
    let policy = db
        .get_cf(
            StateMachineColumns::ExtractionPolicies.cf(db),
            &task.extraction_policy_id,
        )?
        .ok_or_else(|| anyhow!("Extraction policy not found"))?;
    let policy: ExtractionPolicy =
        JsonEncoder::decode(&policy).map_err(|e| anyhow!("Failed to decode policy: {:?}", e))?;
    Ok(Task {
        id: task.id,
        extractor: task.extractor,
        extraction_policy_name: policy.name,
        extraction_graph_name: task.extraction_graph_name,
        output_index_table_mapping: task.output_index_table_mapping,
        namespace: task.namespace,
        content_metadata: task.content_metadata.into(),
        input_params: task.input_params,
        outcome: task.outcome,
        index_tables: task.index_tables,
        creation_time: std::time::UNIX_EPOCH,
    })
}
