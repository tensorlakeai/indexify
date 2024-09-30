use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};
use data_model::{
    ChangeType,
    ComputeGraph,
    ExecutorId,
    GraphInvocationCtx,
    GraphInvocationCtxBuilder,
    InvokeComputeGraphEvent,
    Namespace,
    NodeOutput,
    OutputPayload,
    StateChange,
    StateChangeBuilder,
    StateChangeId,
    SystemTask,
    Task,
    TaskAnalytics,
};
use indexify_utils::{get_epoch_time_in_ms, OptionInspectNone};
use rocksdb::{
    AsColumnFamilyRef,
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
use crate::requests::{
    CreateTasksRequest,
    DeleteInvocationRequest,
    DeregisterExecutorRequest,
    FinalizeTaskRequest,
    InvokeComputeGraphRequest,
    NamespaceRequest,
    ReductionTasks,
    RegisterExecutorRequest,
    RemoveSystemTaskRequest,
    RerunComputeGraphRequest,
    RerunInvocationRequest,
    UpdateSystemTaskRequest,
};

pub type ContentId = String;
pub type ExecutorIdRef<'a> = &'a str;
pub type ExtractionEventId = String;
pub type ExtractionPolicyId = String;
pub type ExtractorName = String;
pub type ContentType = String;
pub type ExtractionGraphId = String;
pub type SchemaId = String;

#[derive(AsRefStr, strum::Display, strum::EnumIter)]
pub enum IndexifyObjectsColumns {
    StateMachineMetadata, //  StateMachineMetadata
    Executors,            //  ExecutorId -> Executor Metadata
    Namespaces,           //  Namespaces
    ComputeGraphs,        //  Ns_ComputeGraphName -> ComputeGraph

    Tasks,              //  Ns_CG_<Invocation_Id>_Fn_TaskId -> Task
    GraphInvocationCtx, //  Ns_CG_IngestedId -> GraphInvocationCtx
    ReductionTasks,     //  Ns_CG_Fn_TaskId -> ReduceTask

    GraphInvocations, //  Ns_Graph_Id -> InvocationPayload
    FnOutputs,        //  Ns_Graph_<Ingested_Id>_Fn_Id -> NodeOutput
    TaskOutputs,      //  NS_TaskID -> NodeOutputID

    StateChanges, //  StateChangeId -> StateChange

    UnprocessedStateChanges, //  StateChangeId -> Empty
    TaskAllocations,         //  ExecutorId -> Task_Key
    UnallocatedTasks,        //  Task_Key -> Empty

    GcUrls, // List of URLs pending deletion

    SystemTasks, // Long running tasks involving multiple invocations

    Stats, // Stats
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

pub(crate) fn create_namespace(db: Arc<TransactionDB>, req: &NamespaceRequest) -> Result<()> {
    let ns = Namespace {
        name: req.name.clone(),
        created_at: get_epoch_time_in_ms(),
    };
    let serialized_namespace = JsonEncoder::encode(&ns)?;
    db.put_cf(
        &IndexifyObjectsColumns::Namespaces.cf_db(&db),
        &ns.name,
        serialized_namespace,
    )?;
    Ok(())
}

pub fn remove_system_task(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: RemoveSystemTaskRequest,
) -> Result<()> {
    let task_key = SystemTask::key_from(&req.namespace, &req.compute_graph_name);
    txn.delete_cf(&IndexifyObjectsColumns::SystemTasks.cf_db(&db), &task_key)?;
    Ok(())
}

pub fn update_system_task(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: UpdateSystemTaskRequest,
) -> Result<()> {
    let key = SystemTask::key_from(&req.namespace, &req.compute_graph_name);
    let task = txn
        .get_for_update_cf(&IndexifyObjectsColumns::SystemTasks.cf_db(&db), &key, true)?
        .ok_or(anyhow::anyhow!("Task not found"))?;
    let mut task = JsonEncoder::decode::<SystemTask>(&task)?;
    task.restart_key = Some(req.restart_key);
    let serialized_task = JsonEncoder::encode(&task)?;
    txn.put_cf(
        &IndexifyObjectsColumns::SystemTasks.cf_db(&db),
        &key,
        &serialized_task,
    )?;
    Ok(())
}

pub fn rerun_compute_graph(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: RerunComputeGraphRequest,
) -> Result<()> {
    let key = format!("{}|{}", req.namespace, req.compute_graph_name);
    let graph = txn
        .get_for_update_cf(
            &IndexifyObjectsColumns::ComputeGraphs.cf_db(&db),
            &key,
            false,
        )?
        .ok_or(anyhow::anyhow!("Compute graph not found"))?;
    let graph: ComputeGraph = JsonEncoder::decode(&graph).unwrap();
    let task_key = SystemTask::key_from(&req.namespace, &req.compute_graph_name);
    let existing_task = txn.get_for_update_cf(
        &IndexifyObjectsColumns::SystemTasks.cf_db(&db),
        &task_key,
        true,
    )?;
    if let Some(existing_task) = existing_task {
        let existing_task: SystemTask = JsonEncoder::decode(&existing_task)?;
        if existing_task.graph_version >= graph.version {
            return Err(anyhow::anyhow!("Task already exists"));
        }
    }
    let task = SystemTask::new(
        req.namespace.clone(),
        req.compute_graph_name.clone(),
        graph.version,
    );
    let serialized_task = JsonEncoder::encode(&task)?;
    txn.put_cf(
        &IndexifyObjectsColumns::SystemTasks.cf_db(&db),
        &task_key,
        &serialized_task,
    )?;

    Ok(())
}

pub fn rerun_invocation(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: RerunInvocationRequest,
) -> Result<Vec<StateChange>> {
    let graph_ctx_key =
        GraphInvocationCtx::key_from(&req.namespace, &req.compute_graph_name, &req.invocation_id);
    let graph_ctx = txn
        .get_for_update_cf(
            &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
            &graph_ctx_key,
            false,
        )?
        .ok_or(anyhow::anyhow!("Graph context not found"))?;
    let graph_ctx: GraphInvocationCtx = JsonEncoder::decode(&graph_ctx)?;
    if graph_ctx.graph_version >= req.graph_version {
        tracing::info!(
            "skipping rerun of invocation: {}, already latest version of invocation context",
            req.invocation_id
        );
        return Ok(Vec::new());
    }
    let output_key = format!(
        "{}|{}|{}|",
        req.namespace, req.compute_graph_name, req.invocation_id
    );
    let outputs = make_prefix_iterator(
        txn,
        &IndexifyObjectsColumns::FnOutputs.cf_db(&db),
        output_key.as_bytes(),
        &None,
    );
    for output in outputs {
        let (_, value) = output?;
        let value: NodeOutput = JsonEncoder::decode(&value)?;
        if value.graph_version >= req.graph_version {
            tracing::info!(
                "skipping rerun of invocation: {}, already latest version of outputs",
                req.invocation_id
            );
            return Ok(Vec::new());
        }
    }

    let compute_graph_key = format!("{}|{}", req.namespace, req.compute_graph_name);
    let graph = txn
        .get_for_update_cf(
            &IndexifyObjectsColumns::ComputeGraphs.cf_db(&db),
            &compute_graph_key,
            false,
        )?
        .ok_or(anyhow::anyhow!("Compute graph not found"))?;
    let graph = JsonEncoder::decode::<ComputeGraph>(&graph)?;
    if graph.version > req.graph_version {
        // Graph was updated after rerun task was created
        return Ok(Vec::new());
    }

    // Delete any previous outputs and any in progress context.
    // The tasks will abort when they fail to find the context.
    let outputs = make_prefix_iterator(
        txn,
        &IndexifyObjectsColumns::FnOutputs.cf_db(&db),
        output_key.as_bytes(),
        &None,
    );
    for output in outputs {
        let (key, _) = output?;
        txn.delete_cf(&IndexifyObjectsColumns::FnOutputs.cf_db(&db), key)?;
    }
    txn.delete_cf(
        &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
        graph_ctx_key,
    )?;

    // Create a new invocation context after all checks passed
    let graph_invocation_ctx = GraphInvocationCtxBuilder::default()
        .namespace(req.namespace.to_string())
        .compute_graph_name(req.compute_graph_name.to_string())
        .graph_version(graph.version)
        .invocation_id(req.invocation_id.clone())
        .fn_task_analytics(HashMap::new())
        .is_system_task(true)
        .build(graph)?;
    txn.put_cf(
        &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
        graph_invocation_ctx.key(),
        &JsonEncoder::encode(&graph_invocation_ctx)?,
    )?;

    // Increment number of outstanding tasks
    let cf = IndexifyObjectsColumns::Stats.cf_db(&db);
    let key = b"pending_system_tasks";
    let value = txn.get_for_update_cf(&cf, key, true)?;
    let mut pending_system_tasks = match value {
        Some(value) => {
            let bytes: [u8; 8] = value
                .as_slice()
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid length for usize conversion"))?;
            usize::from_be_bytes(bytes)
        }
        None => 0,
    };
    pending_system_tasks += 1;
    txn.put_cf(&cf, key, &pending_system_tasks.to_be_bytes())?;

    let state_change = StateChangeBuilder::default()
        .change_type(ChangeType::InvokeComputeGraph(InvokeComputeGraphEvent {
            namespace: req.namespace.clone(),
            invocation_id: req.invocation_id.clone(),
            compute_graph: req.compute_graph_name.clone(),
        }))
        .created_at(get_epoch_time_in_ms())
        .object_id(req.invocation_id.clone())
        .id(StateChangeId::new(0)) // updated with correct id by the caller
        .processed_at(None)
        .build()?;

    Ok(vec![state_change])
}

pub fn create_graph_input(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: &InvokeComputeGraphRequest,
) -> Result<()> {
    let compute_graph_key = format!("{}|{}", req.namespace, req.compute_graph_name);
    let cg = txn
        .get_for_update_cf(
            &IndexifyObjectsColumns::ComputeGraphs.cf_db(&db),
            &compute_graph_key,
            false,
        )?
        .ok_or(anyhow::anyhow!("Compute graph not found"))?;
    let cg: ComputeGraph = JsonEncoder::decode(&cg)?;
    let serialized_data_object = JsonEncoder::encode(&req.invocation_payload)?;
    txn.put_cf(
        &IndexifyObjectsColumns::GraphInvocations.cf_db(&db),
        req.invocation_payload.key(),
        &serialized_data_object,
    )?;

    let graph_invocation_ctx = GraphInvocationCtxBuilder::default()
        .namespace(req.namespace.to_string())
        .compute_graph_name(req.compute_graph_name.to_string())
        .graph_version(cg.version)
        .invocation_id(req.invocation_payload.id.clone())
        .fn_task_analytics(HashMap::new())
        .build(cg)?;
    txn.put_cf(
        &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
        graph_invocation_ctx.key(),
        &JsonEncoder::encode(&graph_invocation_ctx)?,
    )?;
    Ok(())
}

pub(crate) fn delete_input_data_object(
    db: Arc<TransactionDB>,
    req: &DeleteInvocationRequest,
) -> Result<()> {
    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(4_194_304);
    let prefix = format!(
        "{}|{}|{}",
        req.namespace, req.compute_graph, req.invocation_id
    );
    let iterator_mode = IteratorMode::From(prefix.as_bytes(), Direction::Forward);
    let iter = db.iterator_cf_opt(
        &IndexifyObjectsColumns::GraphInvocations.cf_db(&db),
        read_options,
        iterator_mode,
    );
    for key in iter {
        let key = key?;
        db.delete_cf(&IndexifyObjectsColumns::GraphInvocations.cf_db(&db), &key.0)?;
    }

    // FIXME - Delete the data objects which are outputs of the compute functions of
    // the invocation
    Ok(())
}

pub(crate) fn create_compute_graph(
    db: Arc<TransactionDB>,
    mut compute_graph: ComputeGraph,
) -> Result<()> {
    let existing_compute_graph = db.get_cf(
        &IndexifyObjectsColumns::ComputeGraphs.cf_db(&db),
        compute_graph.key(),
    )?;

    if let Some(existing_compute_graph) = existing_compute_graph {
        let existing_compute_graph: ComputeGraph = JsonEncoder::decode(&existing_compute_graph)?;
        if compute_graph.code.sha256_hash != existing_compute_graph.code.sha256_hash ||
            compute_graph.edges != existing_compute_graph.edges ||
            compute_graph.nodes != existing_compute_graph.nodes ||
            compute_graph.start_fn != existing_compute_graph.start_fn
        {
            compute_graph.version = existing_compute_graph.version.next();
        }
    };

    let serialized_compute_graph = JsonEncoder::encode(&compute_graph)?;
    db.put_cf(
        &IndexifyObjectsColumns::ComputeGraphs.cf_db(&db),
        compute_graph.key(),
        &serialized_compute_graph,
    )?;
    Ok(())
}

fn delete_cf_prefix(
    txn: &Transaction<TransactionDB>,
    cf: &impl AsColumnFamilyRef,
    prefix: &[u8],
) -> Result<()> {
    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(4_194_304);
    let iterator_mode = IteratorMode::From(prefix, Direction::Forward);
    let iter = txn.iterator_cf_opt(cf, read_options, iterator_mode);
    for key in iter {
        let (key, _) = key?;
        if !key.starts_with(prefix) {
            break;
        }
        txn.delete_cf(cf, &key)?;
    }
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
        format!("{}|{}", namespace, name),
    )?;
    let prefix = format!("{}|{}|", namespace, name);
    delete_cf_prefix(
        txn,
        &IndexifyObjectsColumns::GraphInvocations.cf_db(&db),
        prefix.as_bytes(),
    )?;

    delete_cf_prefix(
        txn,
        &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
        prefix.as_bytes(),
    )?;

    for iter in make_prefix_iterator(
        txn,
        &IndexifyObjectsColumns::FnOutputs.cf_db(&db),
        prefix.as_bytes(),
        &None,
    ) {
        let (key, value) = iter?;
        let value = JsonEncoder::decode::<NodeOutput>(&value)?;
        match &value.payload {
            OutputPayload::Router(_) => {}
            OutputPayload::Fn(payload) => {
                println!("delete_compute_graph: {:?}", value.clone());
                txn.put_cf(
                    &IndexifyObjectsColumns::GcUrls.cf_db(&db),
                    payload.path.as_bytes(),
                    &[],
                )?;
            }
        }
        txn.delete_cf(&IndexifyObjectsColumns::FnOutputs.cf_db(&db), &key)?;
    }

    Ok(())
}

pub fn remove_gc_urls(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    urls: Vec<String>,
) -> Result<()> {
    for url in urls {
        txn.delete_cf(&IndexifyObjectsColumns::GcUrls.cf_db(&db), &url)?;
    }
    Ok(())
}

pub fn make_prefix_iterator<'a>(
    txn: &'a Transaction<TransactionDB>,
    cf_handle: &impl AsColumnFamilyRef,
    prefix: &'a [u8],
    restart_key: &'a Option<Vec<u8>>,
) -> impl Iterator<Item = Result<(Box<[u8]>, Box<[u8]>)>> + 'a {
    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(4_194_304);
    let iter = txn.iterator_cf_opt(
        cf_handle,
        read_options,
        match restart_key {
            Some(restart_key) => IteratorMode::From(&restart_key, Direction::Forward),
            None => IteratorMode::From(prefix, Direction::Forward),
        },
    );
    iter.map(|item| item.map_err(|e| anyhow!(e.to_string())))
        .take_while(move |item| match item {
            Ok((key, _)) => key.starts_with(prefix),
            Err(_) => true,
        })
}

pub(crate) fn processed_reduction_tasks(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    task: &ReductionTasks,
) -> Result<()> {
    let cf = &IndexifyObjectsColumns::ReductionTasks.cf_db(&db);
    for task in &task.new_reduction_tasks {
        let serialized_task = JsonEncoder::encode(&task)?;
        txn.put_cf(cf, task.key(), &serialized_task)?;
    }
    for key in &task.processed_reduction_tasks {
        txn.delete_cf(cf, key)?;
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum InvocationCompletion {
    User,
    System,
}

// returns true if system task has finished
pub(crate) fn create_tasks(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: &CreateTasksRequest,
) -> Result<Option<InvocationCompletion>> {
    let ctx_key = format!(
        "{}|{}|{}",
        req.namespace, req.compute_graph, req.invocation_id
    );
    let graph_ctx = txn.get_for_update_cf(
        &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
        &ctx_key,
        false,
    )?;
    if graph_ctx.is_none() {
        error!("Graph context not found for graph: {}", req.compute_graph);
    }
    let mut graph_ctx: GraphInvocationCtx = JsonEncoder::decode(&graph_ctx.unwrap())?;
    for task in &req.tasks {
        let serialized_task = JsonEncoder::encode(&task)?;
        txn.put_cf(
            &IndexifyObjectsColumns::Tasks.cf_db(&db),
            task.key(),
            &serialized_task,
        )?;
        txn.put_cf(
            &IndexifyObjectsColumns::UnallocatedTasks.cf_db(&db),
            task.key(),
            &[],
        )?;

        let analytics = graph_ctx
            .fn_task_analytics
            .entry(task.compute_fn_name.clone())
            .or_insert_with(|| TaskAnalytics::default());
        analytics.pending();
    }
    graph_ctx.outstanding_tasks += req.tasks.len() as u64;
    // Subtract reference for completed state change event
    graph_ctx.outstanding_tasks -= 1;
    let serialized_analytics = JsonEncoder::encode(&graph_ctx)?;
    txn.put_cf(
        &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
        ctx_key,
        serialized_analytics,
    )?;
    if graph_ctx.outstanding_tasks == 0 {
        Ok(Some(mark_invocation_finished(
            db,
            txn,
            &req.namespace,
            &req.compute_graph,
            &req.invocation_id,
        )?))
    } else {
        Ok(None)
    }
}

pub fn allocate_tasks(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    task: &Task,
    executor_id: &ExecutorId,
) -> Result<()> {
    txn.put_cf(
        &IndexifyObjectsColumns::TaskAllocations.cf_db(&db),
        task.make_allocation_key(executor_id),
        &[],
    )?;
    txn.delete_cf(
        &IndexifyObjectsColumns::UnallocatedTasks.cf_db(&db),
        task.key(),
    )?;
    Ok(())
}

pub fn mark_task_completed(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: FinalizeTaskRequest,
) -> Result<()> {
    let task_key = format!(
        "{}|{}|{}|{}|{}",
        req.namespace, req.compute_graph, req.invocation_id, req.compute_fn, req.task_id
    );
    let task = txn
        .get_cf(&IndexifyObjectsColumns::Tasks.cf_db(&db), &task_key)?
        .ok_or(anyhow!("Task not found: {}", &req.task_id))?;
    let mut task = JsonEncoder::decode::<Task>(&task)?;
    let graph_ctx_key = format!(
        "{}|{}|{}",
        req.namespace, req.compute_graph, req.invocation_id
    );
    let graph_ctx = txn
        .get_for_update_cf(
            &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
            &graph_ctx_key,
            false,
        )?
        .ok_or(anyhow!(
            "Graph context not found for task: {}",
            &req.task_id
        ))?;
    let mut graph_ctx: GraphInvocationCtx = JsonEncoder::decode(&graph_ctx)?;
    for mut output in req.node_outputs {
        // Update with correct graph version
        output.graph_version = graph_ctx.graph_version;

        let serialized_output = JsonEncoder::encode(&output)?;
        // Create an output key
        let output_key = output.key(&req.invocation_id);
        txn.put_cf(
            &IndexifyObjectsColumns::FnOutputs.cf_db(&db),
            &output_key,
            serialized_output,
        )?;

        // Create a key to store the pointer to the node output to the task
        // NS_TASK_ID_<OutputID> -> Output Key
        let task_output_key = task.key_output(&output.id);
        let node_output_id = JsonEncoder::encode(&output_key)?;
        txn.put_cf(
            &IndexifyObjectsColumns::TaskOutputs.cf_db(&db),
            task_output_key,
            node_output_id,
        )?;
    }
    let analytics = graph_ctx
        .fn_task_analytics
        .entry(req.compute_fn.to_string())
        .or_insert_with(|| TaskAnalytics::default());
    match req.task_outcome {
        data_model::TaskOutcome::Success => analytics.success(),
        data_model::TaskOutcome::Failure => analytics.fail(),
        _ => {}
    }
    let serialized_analytics = JsonEncoder::encode(&graph_ctx)?;
    txn.put_cf(
        &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
        graph_ctx_key,
        serialized_analytics,
    )?;

    txn.delete_cf(
        &IndexifyObjectsColumns::TaskAllocations.cf_db(&db),
        &task.make_allocation_key(&req.executor_id),
    )?;

    task.diagnostics = req.diagnostics.clone();

    task.outcome = req.task_outcome.clone();
    let task_bytes = JsonEncoder::encode(&task)?;
    txn.put_cf(
        &IndexifyObjectsColumns::Tasks.cf_db(&db),
        task.key(),
        task_bytes,
    )?;
    Ok(())
}

pub(crate) fn save_state_changes(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    state_changes: &Vec<StateChange>,
) -> Result<()> {
    for state_change in state_changes {
        let serialized_state_change = JsonEncoder::encode(&state_change)?;
        txn.put_cf(
            &IndexifyObjectsColumns::StateChanges.cf_db(&db),
            &state_change.id.to_key(),
            serialized_state_change.clone(),
        )?;

        if state_change.processed_at.is_none() {
            txn.put_cf(
                &IndexifyObjectsColumns::UnprocessedStateChanges.cf_db(&db),
                &state_change.id.to_key(),
                serialized_state_change,
            )?;
        } else {
            txn.delete_cf(
                &IndexifyObjectsColumns::UnprocessedStateChanges.cf_db(&db),
                &state_change.id.to_key(),
            )?;
        }
    }
    Ok(())
}

pub(crate) fn mark_state_changes_processed(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    state_change_ids: &Vec<StateChangeId>,
) -> Result<()> {
    let mut state_changes = Vec::new();
    for state_change_id in state_change_ids {
        let state_change = txn.get_cf(
            &IndexifyObjectsColumns::StateChanges.cf_db(&db),
            state_change_id.to_key(),
        )?;
        if state_change.is_none() {
            error!("State change not found: {}", state_change_id);
            continue;
        }
        let state_change = state_change.unwrap();
        let mut state_change: StateChange = JsonEncoder::decode(&state_change)?;
        state_change.processed_at = Some(get_epoch_time_in_ms());
        state_changes.push(state_change);
    }
    save_state_changes(db, txn, &state_changes)?;
    Ok(())
}

// Returns true if the invocation was a system task
fn mark_invocation_finished(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    namespace: &str,
    compute_graph: &str,
    invocation_id: &str,
) -> Result<InvocationCompletion> {
    tracing::info!(
        "Marking invocation finished: {} {} {}",
        namespace,
        compute_graph,
        invocation_id
    );
    let key = GraphInvocationCtx::key_from(&namespace, &compute_graph, &invocation_id);
    let graph_ctx = txn
        .get_cf(&IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db), &key)?
        .ok_or(anyhow!(
            "Graph context not found for invocation: {}",
            &invocation_id
        ))?;
    let mut graph_ctx: GraphInvocationCtx = JsonEncoder::decode(&graph_ctx)?;
    graph_ctx.completed = true;
    let serialized_graph_ctx = JsonEncoder::encode(&graph_ctx)?;
    txn.put_cf(
        &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
        key,
        serialized_graph_ctx,
    )?;
    if graph_ctx.is_system_task {
        let cf = IndexifyObjectsColumns::Stats.cf_db(&db);
        let key = b"pending_system_tasks";
        let value = txn.get_cf(&cf, key)?;
        let mut pending_system_tasks = match value {
            Some(value) => {
                let bytes: [u8; 8] = value
                    .as_slice()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("Invalid length for usize conversion"))?;
                usize::from_be_bytes(bytes)
            }
            None => 0,
        };
        pending_system_tasks -= 1;
        txn.put_cf(&cf, key, &pending_system_tasks.to_be_bytes())?;
        Ok(InvocationCompletion::System)
    } else {
        Ok(InvocationCompletion::User)
    }
}

pub(crate) fn register_executor(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: &RegisterExecutorRequest,
) -> Result<()> {
    let serialized_executor_metadata = JsonEncoder::encode(&req.executor)?;
    txn.put_cf(
        &IndexifyObjectsColumns::Executors.cf_db(&db),
        req.executor.key(),
        serialized_executor_metadata,
    )?;
    Ok(())
}

pub(crate) fn deregister_executor(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: &DeregisterExecutorRequest,
) -> Result<()> {
    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(4_194_304);
    let prefix = format!("{}|", req.executor_id);
    let iterator_mode = IteratorMode::From(prefix.as_bytes(), Direction::Forward);
    let iter = txn.iterator_cf_opt(
        &IndexifyObjectsColumns::TaskAllocations.cf_db(&db),
        read_options,
        iterator_mode,
    );
    for key in iter {
        let (key, _) = key?;
        txn.delete_cf(&IndexifyObjectsColumns::TaskAllocations.cf_db(&db), &key)?;
        let task_key = Task::key_from_allocation_key(&key)?;
        txn.put_cf(
            &IndexifyObjectsColumns::UnallocatedTasks.cf_db(&db),
            &task_key,
            &[],
        )?;
    }
    txn.delete_cf(
        &IndexifyObjectsColumns::Executors.cf_db(&db),
        req.executor_id.to_string(),
    )?;
    Ok(())
}
