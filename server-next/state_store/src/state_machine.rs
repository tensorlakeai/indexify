use std::{collections::HashSet, sync::Arc};

use anyhow::Result;
use data_model::{ComputeGraph, DataObject, GraphInvocationCtx, Namespace, Task, TaskAnalytics};
use indexify_utils::OptionInspectNone;
use rocksdb::{
    BoundColumnFamily,
    Direction,
    IteratorMode,
    OptimisticTransactionDB,
    ReadOptions,
    Transaction,
    TransactionDB,
};
use strum::AsRefStr;
use tracing::error;

use super::serializer::{JsonEncode, JsonEncoder};

pub type TaskId = String;
pub type ContentId = String;
pub type ExecutorId = String;
pub type ExecutorIdRef<'a> = &'a str;
pub type ExtractionEventId = String;
pub type ExtractionPolicyId = String;
pub type ExtractorName = String;
pub type ContentType = String;
pub type ExtractionGraphId = String;
pub type SchemaId = String;

#[derive(AsRefStr, strum::Display, strum::EnumIter)]
pub enum IndexifyObjectsColumns {
    Executors,     //  ExecutorId -> Executor Metadata
    Namespaces,    //  Namespaces
    ComputeGraphs, //  Ns_ComputeGraphName -> ComputeGraph

    Tasks,              //  Ns_CG_Fn_TaskId -> Task
    GraphInvocationCtx, //  Ns_CG_IngestedId -> GraphInvocationCtx

    IngestedData, //  Ns_Graph_Id -> DataObject
    FnOutputData, //  Ns_Graph_<Ingested_Id>_Fn_Id -> DataObject

    StateChanges, //  StateChangeId -> StateChange

    // Reverse Indexes
    UnprocessedStateChanges, //  StateChangeId -> Empty
    ExecutorTaskAssignments, //  ExecutorId -> TaskId
    UnallocatedTasks,        //  NS_TaskId -> Empty
}

impl IndexifyObjectsColumns {
    pub fn cf<'a>(&'a self, db: &'a OptimisticTransactionDB) -> Arc<BoundColumnFamily> {
        db.cf_handle(self.as_ref())
            .inspect_none(|| {
                tracing::error!("failed to get column family handle for {}", self.as_ref());
            })
            .unwrap()
    }

    pub fn cf_db<'a>(&'a self, db: &'a TransactionDB) -> Arc<BoundColumnFamily> {
        db.cf_handle(self.as_ref())
            .inspect_none(|| {
                tracing::error!("failed to get column family handle for {}", self.as_ref());
            })
            .unwrap()
    }
}

pub(crate) fn create_namespace(db: Arc<TransactionDB>, namespace: &Namespace) -> Result<()> {
    let serialized_namespace = JsonEncoder::encode(&namespace)?;
    db.put_cf(
        &IndexifyObjectsColumns::Namespaces.cf_db(&db),
        &namespace.name,
        serialized_namespace,
    )?;
    Ok(())
}

pub fn create_graph_input(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    namespace: &str,
    compute_graph_name: &str,
    data_object: DataObject,
) -> Result<()> {
    let compute_graph_key = format!("{}_{}", namespace, compute_graph_name);
    let _ = txn
        .get_cf(
            &IndexifyObjectsColumns::ComputeGraphs.cf_db(&db),
            &compute_graph_key,
        )?
        .ok_or(anyhow::anyhow!("Compute graph not found"))?;
    let ingestion_object_key = data_object.ingestion_object_key();
    let serialized_data_object = JsonEncoder::encode(&data_object)?;
    txn.put_cf(
        &IndexifyObjectsColumns::IngestedData.cf_db(&db),
        ingestion_object_key,
        &serialized_data_object,
    )?;
    Ok(())
}

pub fn create_compute_fn_output(
    db: &OptimisticTransactionDB,
    txn: &Transaction<OptimisticTransactionDB>,
    data_object: DataObject,
    ingested_data_id: &str,
) -> Result<()> {
    let serialized_data_object = JsonEncoder::encode(&data_object)?;
    txn.put_cf(
        &IndexifyObjectsColumns::FnOutputData.cf(db),
        data_object.fn_output_key(ingested_data_id),
        &serialized_data_object,
    )?;
    Ok(())
}

pub(crate) fn delete_input_data_object(
    db: Arc<TransactionDB>,
    namespace: &str,
    compute_graph: &str,
    invocation_id: &str,
) -> Result<()> {
    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(4_194_304);
    let prefix = format!("{}_{}_{}", namespace, compute_graph, invocation_id);
    let iterator_mode = IteratorMode::From(prefix.as_bytes(), Direction::Forward);
    let iter = db.iterator_cf_opt(
        &IndexifyObjectsColumns::IngestedData.cf_db(&db),
        read_options,
        iterator_mode,
    );
    for key in iter {
        let key = key?;
        db.delete_cf(&IndexifyObjectsColumns::IngestedData.cf_db(&db), &key.0)?;
    }

    // FIXME - Delete the data objects which are outputs of the compute functions of
    // the invocation
    Ok(())
}

pub(crate) fn create_compute_graph(
    db: Arc<TransactionDB>,
    compute_graph: ComputeGraph,
) -> Result<()> {
    let serialized_compute_graph = JsonEncoder::encode(&compute_graph)?;
    db.put_cf(
        &IndexifyObjectsColumns::ComputeGraphs.cf_db(&db),
        compute_graph.key(),
        &serialized_compute_graph,
    )?;
    Ok(())
}

pub fn delete_compute_graph(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    namespace: &str,
    name: &str,
) -> Result<()> {
    txn.delete_cf(
        &IndexifyObjectsColumns::ComputeGraphs.cf_db(&db),
        format!("{}_{}", namespace, name),
    )?;
    // WHY IS THIS NOT WORKING
    // db.delete_range_cf(StateMachineColumns::DataObjectsTable.cf(&db),
    // format!("{}_{}", namespace, name), format!("{}_{}", namespace, name))?;
    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(4_194_304);
    let prefix = format!("{}_{}_{}", namespace, name, "");
    let iterator_mode = IteratorMode::From(prefix.as_bytes(), Direction::Forward);
    let iter = db.iterator_cf_opt(
        &IndexifyObjectsColumns::IngestedData.cf_db(&db),
        read_options,
        iterator_mode,
    );
    for key in iter {
        let key = key?;
        txn.delete_cf(&IndexifyObjectsColumns::IngestedData.cf_db(&db), &key.0)?;
    }

    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(4_194_304);
    let prefix = format!("{}_{}_{}", namespace, name, "");
    let iterator_mode = IteratorMode::From(prefix.as_bytes(), Direction::Forward);
    let iter = db.iterator_cf_opt(
        &IndexifyObjectsColumns::FnOutputData.cf_db(&db),
        read_options,
        iterator_mode,
    );
    for key in iter {
        let key = key?;
        txn.delete_cf(&IndexifyObjectsColumns::FnOutputData.cf_db(&db), &key.0)?;
    }
    Ok(())
}

pub(crate) fn create_tasks(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    tasks: Vec<Task>,
) -> Result<()> {
    for task in tasks {
        let serialized_task = JsonEncoder::encode(&task)?;
        txn.put_cf(
            &IndexifyObjectsColumns::Tasks.cf_db(&db),
            task.key(),
            &serialized_task,
        )?;
        let key = format!(
            "{}_{}_{}",
            task.namespace, task.compute_graph_name, task.ingested_data_id
        );
        let graph_ctx = txn.get_cf(&IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db), &key)?;
        if graph_ctx.is_none() {
            error!("Graph context not found for task: {}", task.key());
        }
        let mut graph_ctx: GraphInvocationCtx = JsonEncoder::decode(&graph_ctx.unwrap())?;
        let analytics = graph_ctx
            .fn_task_analytics
            .entry(task.compute_fn_name.clone())
            .or_insert_with(|| TaskAnalytics::default());
        analytics.pending();
        let serialized_analytics = JsonEncoder::encode(&graph_ctx)?;

        txn.put_cf(
            &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
            key,
            serialized_analytics,
        )?;
    }

    Ok(())
}

pub fn update_task_assignment(
    db: &OptimisticTransactionDB,
    txn: &Transaction<OptimisticTransactionDB>,
    task_id: TaskId,
    executor_id: ExecutorId,
    should_add: bool,
) -> Result<()> {
    let task_assignments = db
        .get_cf(
            &IndexifyObjectsColumns::ExecutorTaskAssignments.cf(db),
            &executor_id,
        )?
        .unwrap_or_default();
    let mut task_assignments: HashSet<TaskId> = task_assignments
        .iter()
        .map(|task_id| task_id.to_string())
        .collect();
    if should_add {
        task_assignments.insert(task_id.clone());
    } else {
        task_assignments.remove(&task_id);
    }
    let serialized_task_assignments = JsonEncoder::encode(&task_assignments)?;
    txn.put_cf(
        &IndexifyObjectsColumns::ExecutorTaskAssignments.cf(db),
        &executor_id,
        &serialized_task_assignments,
    )?;
    Ok(())
}

pub fn mark_task_completed(
    _db: &OptimisticTransactionDB,
    _txn: &Transaction<OptimisticTransactionDB>,
    _task: Task,
) -> Result<()> {
    Ok(())
}
