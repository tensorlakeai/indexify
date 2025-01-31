use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};
use data_model::{
    ChangeType,
    ComputeGraph,
    ComputeGraphError,
    ComputeGraphVersion,
    ExecutorId,
    GraphInvocationCtx,
    GraphInvocationCtxBuilder,
    InvocationPayload,
    InvokeComputeGraphEvent,
    Namespace,
    NodeOutput,
    OutputPayload,
    StateChange,
    StateChangeBuilder,
    StateChangeId,
    StateMachineMetadata,
    SystemTask,
    Task,
    TaskAnalytics,
    TaskOutputsIngestionStatus,
};
use indexify_utils::{get_epoch_time_in_ms, OptionInspectNone};
use metrics::StateStoreMetrics;
use rocksdb::{
    AsColumnFamilyRef,
    ColumnFamily,
    Direction,
    IteratorMode,
    ReadOptions,
    Transaction,
    TransactionDB,
};
use strum::AsRefStr;
use tracing::{debug, error, info, warn};

use super::serializer::{JsonEncode, JsonEncoder};
use crate::requests::{
    DeleteInvocationRequest,
    FinalizeTaskRequest,
    IngestTaskOutputsRequest,
    InvokeComputeGraphRequest,
    NamespaceRequest,
    ReductionTasks,
    RegisterExecutorRequest,
    RemoveSystemTaskRequest,
    ReplayComputeGraphRequest,
    ReplayInvocationsRequest,
    TaskAllocationUpdateRequest,
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
    /// Compute graph versions
    ///
    /// Keys:
    /// - `<Ns>_<ComputeGraphName>_<Version> -> ComputeGraphVersion`
    ComputeGraphVersions, //  Ns_ComputeGraphName_Version -> ComputeGraphVersion
    Tasks,                //  Ns_CG_<Invocation_Id>_Fn_TaskId -> Task
    GraphInvocationCtx,   //  Ns_CG_IngestedId -> GraphInvocationCtx
    ReductionTasks,       //  Ns_CG_Fn_TaskId -> ReduceTask

    GraphInvocations, //  Ns_Graph_Id -> InvocationPayload
    FnOutputs,        //  Ns_Graph_<Ingested_Id>_Fn_Id -> NodeOutput
    TaskOutputs,      //  NS_TaskID -> NodeOutputID

    UnprocessedStateChanges, //  StateChangeId -> StateChange
    TaskAllocations,         //  ExecutorId -> Task_Key
    UnallocatedTasks,        //  Task_Key -> Empty

    GcUrls, // List of URLs pending deletion

    SystemTasks, // Long running tasks involving multiple invocations

    Stats, // Stats
}

impl IndexifyObjectsColumns {
    pub fn cf_db<'a>(&'a self, db: &'a TransactionDB) -> &ColumnFamily {
        db.cf_handle(self.as_ref())
            .inspect_none(|| {
                error!("failed to get column family handle for {}", self.as_ref());
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
    info!("created namespace: {}", ns.name);
    Ok(())
}

pub fn remove_system_task(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: RemoveSystemTaskRequest,
) -> Result<()> {
    let task_key = SystemTask::key_from(&req.namespace, &req.compute_graph_name);
    txn.delete_cf(&IndexifyObjectsColumns::SystemTasks.cf_db(&db), &task_key)?;
    do_cf_update::<ComputeGraph>(
        txn,
        &task_key,
        &IndexifyObjectsColumns::ComputeGraphs.cf_db(&db),
        |o| {
            o.replaying = false;
        },
        true,
    )?;
    Ok(())
}

pub fn update_system_task(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: UpdateSystemTaskRequest,
) -> Result<()> {
    let key = SystemTask::key_from(&req.namespace, &req.compute_graph_name);
    do_cf_update::<SystemTask>(
        txn,
        &key,
        &IndexifyObjectsColumns::SystemTasks.cf_db(&db),
        |task| {
            task.waiting_for_running_invocations = req.waiting_for_running_invocations;
        },
        true,
    )
}

pub fn replay_compute_graph(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: ReplayComputeGraphRequest,
) -> Result<()> {
    let key = ComputeGraph::key_from(&req.namespace, &req.compute_graph_name);
    let graph = txn
        .get_for_update_cf(
            &IndexifyObjectsColumns::ComputeGraphs.cf_db(&db),
            &key,
            true,
        )?
        .ok_or(anyhow::anyhow!("Compute graph not found"))?;
    let graph: ComputeGraph = JsonEncoder::decode(&graph)?;
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

    // Mark the compute graph as replaying since a system task was created.
    do_cf_update::<ComputeGraph>(
        txn,
        &task_key,
        &IndexifyObjectsColumns::ComputeGraphs.cf_db(&db),
        |graph| {
            graph.replaying = true;
        },
        true, // reintrant lock
    )?;

    Ok(())
}

pub fn replay_invocations(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: ReplayInvocationsRequest,
) -> Result<Vec<StateChange>> {
    let compute_graph_key =
        ComputeGraph::key_from(req.namespace.as_str(), req.compute_graph_name.as_str());
    let graph = txn
        .get_for_update_cf(
            &IndexifyObjectsColumns::ComputeGraphs.cf_db(&db),
            &compute_graph_key,
            false,
        )?
        .ok_or(anyhow::anyhow!("Compute graph not found"))?;
    let graph = JsonEncoder::decode::<ComputeGraph>(&graph)?;
    if graph.version != req.graph_version {
        // Graph was updated after replay task was created, stopping replay.
        return Ok(Vec::new());
    }
    let system_task_key = SystemTask::key_from(&req.namespace, &req.compute_graph_name);

    let state_changes_res = req
        .invocation_ids
        .iter()
        .map(|invocation_id| {
            let graph_ctx_key = GraphInvocationCtx::key_from(
                &req.namespace,
                &req.compute_graph_name,
                &invocation_id,
            );

            let graph_ctx = txn
                .get_for_update_cf(
                    &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
                    &graph_ctx_key,
                    true,
                )?
                .ok_or(anyhow::anyhow!("Graph context not found"))?;
            let graph_ctx: GraphInvocationCtx = JsonEncoder::decode(&graph_ctx)?;
            if graph_ctx.graph_version == req.graph_version {
                info!(
                "skipping replay of invocation: {}, it has the same version in invocation context",
                invocation_id
            );
                return Ok(None);
            }
            let output_key = format!(
                "{}|{}|{}|",
                req.namespace, req.compute_graph_name, invocation_id
            );

            // Delete any previous outputs and any in progress context.
            // The tasks will abort when they fail to find the context.
            let outputs = make_prefix_iterator(
                &txn,
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
                .graph_version(graph.version.clone())
                .invocation_id(invocation_id.clone())
                .fn_task_analytics(HashMap::new())
                .is_system_task(true)
                .build(graph.clone())?;

            txn.put_cf(
                &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
                graph_invocation_ctx.key(),
                &JsonEncoder::encode(&graph_invocation_ctx)?,
            )?;

            let state_change = StateChangeBuilder::default()
                .change_type(ChangeType::InvokeComputeGraph(InvokeComputeGraphEvent {
                    namespace: req.namespace.clone(),
                    invocation_id: invocation_id.clone(),
                    compute_graph: req.compute_graph_name.clone(),
                }))
                .created_at(get_epoch_time_in_ms())
                .object_id(invocation_id.clone())
                .id(StateChangeId::new(0)) // updated with correct id by the caller
                .processed_at(None)
                .namespace(Some(req.namespace.clone()))
                .compute_graph(Some(req.compute_graph_name.clone()))
                .invocation(Some(invocation_id.clone()))
                .build()?;

            Ok(Some(state_change))
        })
        .collect::<Result<Vec<Option<StateChange>>>>()?;

    let state_changes = state_changes_res
        .into_iter()
        .flatten()
        .collect::<Vec<StateChange>>();

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
    pending_system_tasks += state_changes.len();
    txn.put_cf(&cf, key, &pending_system_tasks.to_be_bytes())?;

    do_cf_update::<SystemTask>(
        txn,
        &system_task_key,
        &IndexifyObjectsColumns::SystemTasks.cf_db(&db),
        |task| {
            // Increment the number of running invocations on the system task to allow
            // determining when the system task has finished.
            task.num_running_invocations += state_changes.len();
            // Persist the restart key needing to be used for the next replay.
            task.restart_key = req.restart_key.clone();
        },
        true,
    )?;
    Ok(state_changes)
}

pub fn create_invocation(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: &InvokeComputeGraphRequest,
) -> Result<()> {
    info!(
        "create invocation: namespace: {}, compute_graph: {}",
        req.namespace, req.compute_graph_name
    );
    let compute_graph_key = format!("{}|{}", req.namespace, req.compute_graph_name);
    let cg = txn
        .get_for_update_cf(
            &IndexifyObjectsColumns::ComputeGraphs.cf_db(&db),
            &compute_graph_key,
            true,
        )?
        .ok_or(anyhow::anyhow!("Compute graph not found"))?;
    let cg: ComputeGraph = JsonEncoder::decode(&cg)?;
    if cg.tombstoned {
        return Err(anyhow::anyhow!("Compute graph is tomb-stoned"));
    }
    let serialized_data_object = JsonEncoder::encode(&req.invocation_payload)?;
    txn.put_cf(
        &IndexifyObjectsColumns::GraphInvocations.cf_db(&db),
        req.invocation_payload.key(),
        &serialized_data_object,
    )?;

    let graph_invocation_ctx = GraphInvocationCtxBuilder::default()
        .namespace(req.namespace.to_string())
        .compute_graph_name(req.compute_graph_name.to_string())
        .graph_version(cg.version.clone())
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

pub(crate) fn delete_invocation(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: &DeleteInvocationRequest,
) -> Result<()> {
    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(4_194_304);

    // Delete the invocation payload
    let prefix = format!(
        "{}|{}|{}",
        req.namespace, req.compute_graph, req.invocation_id
    );
    delete_cf_prefix(
        txn,
        &IndexifyObjectsColumns::GraphInvocations.cf_db(&db),
        prefix.as_bytes(),
    )?;

    // Delete Tasks
    delete_cf_prefix(
        txn,
        IndexifyObjectsColumns::Tasks.cf_db(&db),
        prefix.as_bytes(),
    )?;
    // Delete Allocated Tasks
    delete_cf_prefix(
        txn,
        IndexifyObjectsColumns::TaskAllocations.cf_db(&db),
        prefix.as_bytes(),
    )?;

    // Delete Unallocated Tasks
    delete_cf_prefix(
        txn,
        IndexifyObjectsColumns::UnallocatedTasks.cf_db(&db),
        prefix.as_bytes(),
    )?;

    // Delete Task Outputs
    // FIXME: This requires putting the invocation id in the task output key

    // Delete Graph Invocation Context
    delete_cf_prefix(
        txn,
        IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
        prefix.as_bytes(),
    )?;

    // mark all fn output urls for gc.
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
                txn.put_cf(
                    &IndexifyObjectsColumns::GcUrls.cf_db(&db),
                    payload.path.as_bytes(),
                    [],
                )?;
            }
        }
        txn.delete_cf(&IndexifyObjectsColumns::FnOutputs.cf_db(&db), &key)?;
    }
    Ok(())
}

fn update_task_versions_for_cg(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    compute_graph: &ComputeGraph,
) -> Result<()> {
    let tasks_prefix = Task::keys_for_compute_graph(&compute_graph.namespace, &compute_graph.name);
    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(10_194_304);
    let iter = db.iterator_cf_opt(
        &IndexifyObjectsColumns::Tasks.cf_db(&db),
        read_options,
        IteratorMode::From(&tasks_prefix.as_bytes(), Direction::Forward),
    );

    let mut tasks_to_update = HashMap::new();
    for kv in iter {
        let (key, val) = kv?;
        let mut task: Task = JsonEncoder::decode(&val)?;
        if task.graph_version != compute_graph.version && !task.outcome.is_terminal() {
            info!(
                "updating task: {} from version: {} to version: {}",
                task.id, task.graph_version.0, compute_graph.version.0
            );
            task.graph_version = compute_graph.version.clone();
        }
        tasks_to_update.insert(key, task);
    }
    info!(
        "upgrading tasks to latest version: {}",
        tasks_to_update.len()
    );
    for (task_id, task) in tasks_to_update {
        let serialized_task = JsonEncoder::encode(&task)?;
        txn.put_cf(
            &IndexifyObjectsColumns::Tasks.cf_db(&db),
            &task_id,
            &serialized_task,
        )?;
    }
    Ok(())
}

fn update_graph_invocations_for_cg(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    compute_graph: &ComputeGraph,
) -> Result<()> {
    let cg_prefix =
        GraphInvocationCtx::key_prefix_for_cg(&compute_graph.namespace, &compute_graph.name);
    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(10_194_304);
    let iter = db.iterator_cf_opt(
        &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
        read_options,
        IteratorMode::From(&cg_prefix.as_bytes(), Direction::Forward),
    );

    let mut graph_invocation_ctx_to_update = HashMap::new();
    for kv in iter {
        let (key, val) = kv?;
        let mut graph_invocation_ctx: GraphInvocationCtx = JsonEncoder::decode(&val)?;
        if graph_invocation_ctx.graph_version != compute_graph.version &&
            !graph_invocation_ctx.completed
        {
            info!(
                "updating graph_invocation_ctx for invocation id: {} from version: {} to version: {}",
                graph_invocation_ctx.invocation_id, graph_invocation_ctx.graph_version.0, compute_graph.version.0
            );
            graph_invocation_ctx.graph_version = compute_graph.version.clone();
        }
        graph_invocation_ctx_to_update.insert(key, graph_invocation_ctx);
    }
    info!(
        "upgrading graph invocation ctxs: {}",
        graph_invocation_ctx_to_update.len()
    );
    for (invocation_id, graph_invocation_ctx) in graph_invocation_ctx_to_update {
        let serialized_task = JsonEncoder::encode(&graph_invocation_ctx)?;
        txn.put_cf(
            &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
            &invocation_id,
            &serialized_task,
        )?;
    }
    Ok(())
}

pub(crate) fn create_or_update_compute_graph(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    compute_graph: ComputeGraph,
    upgrade_existing_tasks_to_current_version: bool,
) -> Result<()> {
    info!(
        "creating compute graph: ns: {} name: {}, upgrade invocations: {}",
        compute_graph.namespace, compute_graph.name, upgrade_existing_tasks_to_current_version
    );
    let existing_compute_graph = txn
        .get_for_update_cf(
            &IndexifyObjectsColumns::ComputeGraphs.cf_db(&db),
            compute_graph.key(),
            true,
        )?
        .map(|v| JsonEncoder::decode::<ComputeGraph>(&v));

    let new_compute_graph_version = match existing_compute_graph {
        Some(Ok(mut existing_compute_graph)) => {
            if existing_compute_graph.version == compute_graph.version {
                return Err(anyhow!(ComputeGraphError::VersionExists));
            }
            existing_compute_graph.update(compute_graph.clone());
            Ok::<ComputeGraphVersion, anyhow::Error>(existing_compute_graph.into_version())
        }
        Some(Err(e)) => {
            return Err(anyhow!("failed to decode existing compute graph: {}", e));
        }
        None => Ok(compute_graph.into_version()),
    }?;
    info!(
        "new compute graph version: {}",
        &new_compute_graph_version.version.0
    );
    let serialized_compute_graph_version = JsonEncoder::encode(&new_compute_graph_version)?;
    txn.put_cf(
        &IndexifyObjectsColumns::ComputeGraphVersions.cf_db(&db),
        new_compute_graph_version.key(),
        &serialized_compute_graph_version,
    )?;

    let serialized_compute_graph = JsonEncoder::encode(&compute_graph)?;
    txn.put_cf(
        &IndexifyObjectsColumns::ComputeGraphs.cf_db(&db),
        compute_graph.key(),
        &serialized_compute_graph,
    )?;
    if upgrade_existing_tasks_to_current_version {
        update_task_versions_for_cg(db.clone(), txn, &compute_graph)?;
        update_graph_invocations_for_cg(db.clone(), txn, &compute_graph)?;
    }
    info!(
        "finished creating compute graph namespace: {} name: {}, version: {}",
        compute_graph.namespace, compute_graph.name, compute_graph.version.0
    );
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

pub fn tombstone_compute_graph(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    namespace: &str,
    name: &str,
) -> Result<()> {
    info!(
        "tombstoning compute graph: namespace: {}, name: {}",
        namespace, name
    );
    let mut existing_compute_graph = txn
        .get_for_update_cf(
            &IndexifyObjectsColumns::ComputeGraphs.cf_db(&db),
            ComputeGraph::key_from(namespace, name),
            true,
        )?
        .map(|v| JsonEncoder::decode::<ComputeGraph>(&v))
        .ok_or(anyhow!(
            "compute graph not found namespace: {},  {}",
            namespace,
            name
        ))?
        .map_err(|e| anyhow!("failed to decode existing compute graph: {}", e))?;

    existing_compute_graph.tombstoned = true;
    Ok(())
}

pub fn delete_compute_graph(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    namespace: &str,
    name: &str,
) -> Result<()> {
    info!(
        "deleting compute graph: namespace: {}, name: {}",
        namespace, name
    );
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
        &IndexifyObjectsColumns::Tasks.cf_db(&db),
        prefix.as_bytes(),
        &None,
    ) {
        let (key, value) = iter?;
        let value = JsonEncoder::decode::<Task>(&value)?;

        // mark all diagnostics urls for gc.
        match &value.diagnostics {
            Some(diagnostics) => {
                [diagnostics.stdout.clone(), diagnostics.stderr.clone()]
                    .iter()
                    .flatten()
                    .try_for_each(|data| -> Result<()> {
                        txn.put_cf(
                            &IndexifyObjectsColumns::GcUrls.cf_db(&db),
                            data.path.as_bytes(),
                            [],
                        )?;

                        Ok(())
                    })?;
            }
            None => {}
        }
        txn.delete_cf(&IndexifyObjectsColumns::Tasks.cf_db(&db), &key)?;

        delete_cf_prefix(
            txn,
            &IndexifyObjectsColumns::TaskOutputs.cf_db(&db),
            format!("{}|{}", namespace, value.id).as_bytes(),
        )?;
    }

    for iter in make_prefix_iterator(
        txn,
        &IndexifyObjectsColumns::ComputeGraphVersions.cf_db(&db),
        prefix.as_bytes(),
        &None,
    ) {
        let (key, value) = iter?;
        let value = JsonEncoder::decode::<ComputeGraphVersion>(&value)?;

        // mark all code urls for gc.
        txn.put_cf(
            &IndexifyObjectsColumns::GcUrls.cf_db(&db),
            value.code.path.as_bytes(),
            [],
        )?;
        txn.delete_cf(
            &IndexifyObjectsColumns::ComputeGraphVersions.cf_db(&db),
            &key,
        )?;
    }

    delete_cf_prefix(
        txn,
        &IndexifyObjectsColumns::UnallocatedTasks.cf_db(&db),
        prefix.as_bytes(),
    )?;

    // mark all fn output urls for gc.
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
                txn.put_cf(
                    &IndexifyObjectsColumns::GcUrls.cf_db(&db),
                    payload.path.as_bytes(),
                    [],
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

// returns whether the invocation was completed or not and whether it was a user
// or system task invocation.
pub(crate) fn create_tasks(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    tasks: &[Task],
    sm_metrics: Arc<StateStoreMetrics>,
    namespace: &str,
    compute_graph: &str,
    invocation_id: &str,
) -> Result<Option<InvocationCompletion>> {
    let ctx_key = format!("{}|{}|{}", namespace, compute_graph, invocation_id);
    let graph_ctx = txn.get_for_update_cf(
        &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
        &ctx_key,
        true,
    )?;
    if graph_ctx.is_none() {
        error!(
            "Graph context not found for graph {} and invocation {}",
            &compute_graph, &invocation_id
        );
        return Ok(None);
    }
    let graph_ctx = &graph_ctx.ok_or(anyhow!(
        "Graph context not found for graph {} and invocation {}",
        &compute_graph,
        &invocation_id
    ))?;
    let mut graph_ctx: GraphInvocationCtx = JsonEncoder::decode(&graph_ctx)?;
    if graph_ctx.completed {
        return Ok(None);
    }
    for task in tasks {
        let serialized_task = JsonEncoder::encode(&task)?;
        info!(
            "creating task: ns: {}, compute graph: {}, invocation id: {},  task: {}, outcome: {:?}",
            task.namespace,
            task.compute_graph_name,
            task.invocation_id,
            task.key(),
            task.outcome
        );
        txn.put_cf(
            &IndexifyObjectsColumns::Tasks.cf_db(&db),
            task.key(),
            &serialized_task,
        )?;
        let analytics = graph_ctx
            .fn_task_analytics
            .entry(task.compute_fn_name.clone())
            .or_insert_with(|| TaskAnalytics::default());
        analytics.pending();
    }
    graph_ctx.outstanding_tasks += tasks.len() as u64;
    // Subtract reference for completed state change event
    graph_ctx.outstanding_tasks -= 1;
    let serialized_graphctx = JsonEncoder::encode(&graph_ctx)?;
    txn.put_cf(
        &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
        ctx_key,
        serialized_graphctx,
    )?;
    info!(
        "invocation ctx for invocation : {}, {:?}",
        invocation_id, graph_ctx
    );
    sm_metrics.task_unassigned(tasks);
    if graph_ctx.outstanding_tasks == 0 {
        Ok(Some(mark_invocation_finished(
            db,
            txn,
            &namespace,
            &compute_graph,
            &invocation_id,
        )?))
    } else {
        Ok(None)
    }
}

pub fn handle_task_allocation_update(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    sm_metrics: Arc<StateStoreMetrics>,
    request: &TaskAllocationUpdateRequest,
) -> Result<()> {
    for task_placement in &request.allocations {
        info!("task allocation: ns: {}, compute_graph: {}, invocation id: {}, task id: {}, executor: {}",
            task_placement.task.namespace,
            task_placement.task.compute_graph_name,
            task_placement.task.invocation_id,
            task_placement.task.id,
            task_placement.executor.get()
        );
        txn.put_cf(
            &IndexifyObjectsColumns::TaskAllocations.cf_db(&db),
            task_placement
                .task
                .make_allocation_key(&task_placement.executor),
            &[],
        )?;
        info!("unallocated task: addition: ns: {}, compute_graph: {}, invocation id: {}, task key: {}",
            task_placement.task.namespace,
            task_placement.task.compute_graph_name,
            task_placement.task.invocation_id,
            task_placement.task.key(),
        );
        txn.delete_cf(
            &IndexifyObjectsColumns::UnallocatedTasks.cf_db(&db),
            task_placement.task.key(),
        )?;

        sm_metrics.task_assigned(
            &vec![task_placement.task.clone()],
            task_placement.executor.get(),
        );
    }
    for unplaced_task_key in &request.unplaced_task_keys {
        info!("unallocated task: removing task key: {}", unplaced_task_key);
        txn.put_cf(
            &IndexifyObjectsColumns::UnallocatedTasks.cf_db(&db),
            unplaced_task_key.as_bytes(),
            &[],
        )?;
    }
    Ok(())
}

// returns true if task the task finishing state should be emitted.
pub fn ingest_task_outputs(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: IngestTaskOutputsRequest,
) -> Result<bool> {
    // Check if the graph exists before proceeding since
    // the graph might have been deleted before the task completes
    let graph_key = ComputeGraph::key_from(&req.namespace, &req.compute_graph);
    let graph = txn
        .get_cf(
            &IndexifyObjectsColumns::ComputeGraphs.cf_db(&db),
            &graph_key,
        )
        .map_err(|e| anyhow!("failed to get compute graph: {}", e))?;
    if graph.is_none() {
        info!("Compute graph not found: {}", &req.compute_graph);
        return Ok(false);
    }

    // Check if the invocation was deleted before the task completes
    let invocation_id =
        InvocationPayload::key_from(&req.namespace, &req.compute_graph, &req.invocation_id);
    let invocation = txn
        .get_cf(
            &IndexifyObjectsColumns::GraphInvocations.cf_db(&db),
            &invocation_id,
        )
        .map_err(|e| anyhow!("failed to get invocation: {}", e))?;
    if invocation.is_none() {
        info!("Invocation not found: {} ", &req.invocation_id);
        return Ok(false);
    }
    let task_key = format!(
        "{}|{}|{}|{}|{}",
        req.namespace, req.compute_graph, req.invocation_id, req.compute_fn, req.task_id
    );
    let task = txn.get_for_update_cf(&IndexifyObjectsColumns::Tasks.cf_db(&db), &task_key, true)?;
    if task.is_none() {
        info!("Task not found: {}", &task_key);
        return Ok(false);
    }
    let mut task = JsonEncoder::decode::<Task>(&task.unwrap())?;

    // idempotency check guaranteeing that we emit a finalizing state change only
    // once.
    if task.output_status == TaskOutputsIngestionStatus::Ingested {
        warn!(
            task_key = task.key(),
            "Task outputs already uploaded, skipping setting outputs",
        );
        return Ok(false);
    }

    for output in req.node_outputs {
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

    task.output_status = TaskOutputsIngestionStatus::Ingested;
    let task_bytes = JsonEncoder::encode(&task)?;
    txn.put_cf(
        &IndexifyObjectsColumns::Tasks.cf_db(&db),
        task.key(),
        task_bytes,
    )?;

    Ok(true)
}

/// Returns true if the task was marked as finalized.
pub fn mark_task_finalized(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: FinalizeTaskRequest,
    sm_metrics: Arc<StateStoreMetrics>,
) -> Result<bool> {
    info!(
        "task finalization begin: ns: {}, compute graph: {}, invocation_id: {}, task: {}, outcome: {:?}",
        req.namespace, req.compute_graph, req.invocation_id, req.task_id, req.task_outcome
    );
    // Check if the graph exists before proceeding since
    // the graph might have been deleted before the task completes
    let graph_key = ComputeGraph::key_from(&req.namespace, &req.compute_graph);
    let graph = txn
        .get_cf(
            &IndexifyObjectsColumns::ComputeGraphs.cf_db(&db),
            &graph_key,
        )
        .map_err(|e| anyhow!("failed to get compute graph: {}", e))?;
    if graph.is_none() {
        error!(
            "task finalization end: task: {}, Compute graph not found: {}",
            &req.task_id, &req.compute_graph
        );
        return Ok(false);
    }

    // Check if the invocation was deleted before the task completes
    let invocation_id =
        InvocationPayload::key_from(&req.namespace, &req.compute_graph, &req.invocation_id);
    let invocation = txn
        .get_cf(
            &IndexifyObjectsColumns::GraphInvocations.cf_db(&db),
            &invocation_id,
        )
        .map_err(|e| anyhow!("failed to get invocation: {}", e))?;
    if invocation.is_none() {
        error!(
            "task finalization end: Invocation not found: {}",
            &req.invocation_id
        );
        return Ok(false);
    }
    let task_key = format!(
        "{}|{}|{}|{}|{}",
        req.namespace, req.compute_graph, req.invocation_id, req.compute_fn, req.task_id
    );
    let task = txn.get_for_update_cf(&IndexifyObjectsColumns::Tasks.cf_db(&db), &task_key, true)?;
    if task.is_none() {
        error!("task finalization end: Task not found: {}", &task_key);
        return Ok(false);
    }
    let mut task = JsonEncoder::decode::<Task>(&task.unwrap())?;

    let graph_ctx_key = format!(
        "{}|{}|{}",
        req.namespace, req.compute_graph, req.invocation_id
    );
    let graph_ctx = txn
        .get_for_update_cf(
            &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
            &graph_ctx_key,
            true,
        )
        .map_err(|e| anyhow!("failed to get graph context: {}", e))?;
    if graph_ctx.is_none() {
        error!(
            "task finalization end: Graph context not found, ns: {} compute graph: {} invocation id: {} task: {}",
            &req.namespace, &req.compute_graph, &req.invocation_id, &req.task_id
        );
        return Ok(false);
    }
    let mut graph_ctx: GraphInvocationCtx = JsonEncoder::decode(&graph_ctx.ok_or(anyhow!(
        "unable to deserialize graph context for task: {}",
        &req.task_id
    ))?)?;

    let analytics = graph_ctx
        .fn_task_analytics
        .entry(req.compute_fn.to_string())
        .or_insert_with(|| TaskAnalytics::default());
    match req.task_outcome {
        data_model::TaskOutcome::Success => analytics.success(),
        data_model::TaskOutcome::Failure => analytics.fail(),
        _ => {}
    }
    info!(
        "task finalization graph ctx updated: task: {}, graph ctx: {:?}",
        task.key(),
        graph_ctx
    );
    let graph_ctx = JsonEncoder::encode(&graph_ctx)?;
    txn.put_cf(
        &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
        graph_ctx_key,
        graph_ctx,
    )?;

    // Delete the task allocation since task is finished.
    info!("task finalization deleting task allocation: ns: {}, compute_graph: {}, invocation id: {}, task key: {}, allocation key: {}",
        req.namespace,
        req.compute_graph,
        req.invocation_id,
        task.key(),
        task.make_allocation_key(&req.executor_id)
    );
    txn.delete_cf(
        &IndexifyObjectsColumns::TaskAllocations.cf_db(&db),
        task.make_allocation_key(&req.executor_id),
    )?;

    task.diagnostics = req.diagnostics.clone();

    task.outcome = req.task_outcome.clone();
    let task_bytes = JsonEncoder::encode(&task)?;
    info!(
        "marking task as finalized: {}, outcome: {:?}",
        task.key(),
        task.outcome
    );
    txn.put_cf(
        &IndexifyObjectsColumns::Tasks.cf_db(&db),
        task.key(),
        task_bytes,
    )?;

    sm_metrics.update_task_completion(req.task_outcome, task.clone(), req.executor_id.get());
    Ok(true)
}

pub(crate) fn save_state_changes(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    state_changes: &[StateChange],
) -> Result<()> {
    for state_change in state_changes {
        let key = &state_change.key();
        let serialized_state_change = JsonEncoder::encode(&state_change)?;
        txn.put_cf(
            &IndexifyObjectsColumns::UnprocessedStateChanges.cf_db(&db),
            key,
            serialized_state_change,
        )?;
    }
    Ok(())
}

pub(crate) fn mark_state_changes_processed(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    processed_state_changes: &[StateChange],
) -> Result<()> {
    for state_change in processed_state_changes {
        debug!(
            "marking state change as processed: {}",
            state_change.change_type
        );
        let key = &state_change.key();
        txn.delete_cf(
            &IndexifyObjectsColumns::UnprocessedStateChanges.cf_db(&db),
            key,
        )?;
    }
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
    info!(
        "marking invocation finished: {} {} {}",
        namespace, compute_graph, invocation_id
    );
    let key = GraphInvocationCtx::key_from(&namespace, &compute_graph, &invocation_id);
    let graph_ctx = txn
        .get_for_update_cf(
            &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
            &key,
            true,
        )?
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
        pending_system_tasks = pending_system_tasks.saturating_sub(1);
        txn.put_cf(&cf, key, &pending_system_tasks.to_be_bytes())?;

        // Decrement the number of running invocations on the system task to allow
        // determining when the system task has finished.
        let key = SystemTask::key_from(namespace, compute_graph);
        do_cf_update::<SystemTask>(
            txn,
            &key,
            &IndexifyObjectsColumns::SystemTasks.cf_db(&db),
            |task| {
                task.num_running_invocations -= 1;
            },
            true,
        )?;

        Ok(InvocationCompletion::System)
    } else {
        Ok(InvocationCompletion::User)
    }
}

pub(crate) fn register_executor(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: &RegisterExecutorRequest,
    sm_metrics: Arc<StateStoreMetrics>,
) -> Result<()> {
    let serialized_executor_metadata = JsonEncoder::encode(&req.executor)?;
    txn.put_cf(
        &IndexifyObjectsColumns::Executors.cf_db(&db),
        req.executor.key(),
        serialized_executor_metadata,
    )?;
    sm_metrics.add_executor();
    Ok(())
}

pub(crate) fn deregister_executor(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    executor_id: &ExecutorId,
    sm_metrics: Arc<StateStoreMetrics>,
) -> Result<()> {
    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(4_194_304);
    let prefix = format!("{}|", executor_id);
    let iterator_mode = IteratorMode::From(prefix.as_bytes(), Direction::Forward);
    let iter = txn.iterator_cf_opt(
        &IndexifyObjectsColumns::TaskAllocations.cf_db(&db),
        read_options,
        iterator_mode,
    );
    for key in iter {
        let (key, _) = key?;
        info!(
            "deregister executor: executor id: {}, removing task allocation: {}",
            executor_id,
            String::from_utf8(key.to_vec())?
        );
        txn.delete_cf(&IndexifyObjectsColumns::TaskAllocations.cf_db(&db), &key)?;
        let task_key = Task::key_from_allocation_key(&key)?;
        info!(
            "deregister executor id: {}, adding to unallocated tasks: {}",
            executor_id,
            String::from_utf8(task_key.clone())?
        );
        txn.put_cf(
            &IndexifyObjectsColumns::UnallocatedTasks.cf_db(&db),
            &task_key,
            &[],
        )?;
    }
    info!(
        "deregister executor: executor id: {}, removing executor metadata",
        executor_id
    );
    txn.delete_cf(
        &IndexifyObjectsColumns::Executors.cf_db(&db),
        executor_id.to_string(),
    )?;
    sm_metrics.remove_executor(executor_id.get());
    Ok(())
}

/// Helper function to update a column family entry in a transaction
/// by fetching the entry, deserializing it, applying the update function,
/// serializing it and putting it back in the column family.
///
/// This can be done with an exclusive lock or not.
fn do_cf_update<T>(
    txn: &Transaction<TransactionDB>,
    key: &str,
    cf: &impl AsColumnFamilyRef,
    update_fn: impl FnOnce(&mut T),
    exclusive: bool,
) -> Result<()>
where
    T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug,
{
    let task = txn
        .get_for_update_cf(cf, &key, exclusive)?
        .ok_or(anyhow::anyhow!("Task not found"))?;
    let mut task = JsonEncoder::decode::<T>(&task)?;
    update_fn(&mut task);
    let serialized_task = JsonEncoder::encode(&task)?;
    txn.put_cf(cf, &key, &serialized_task)?;
    Ok(())
}

pub fn write_sm_meta(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    meta: &StateMachineMetadata,
) -> Result<()> {
    let serialized_meta = JsonEncoder::encode(&meta)?;
    txn.put_cf(
        &IndexifyObjectsColumns::StateMachineMetadata.cf_db(&db),
        b"sm_meta",
        &serialized_meta,
    )?;
    Ok(())
}

pub fn read_sm_meta(db: &TransactionDB) -> Result<StateMachineMetadata> {
    let meta = db.get_cf(
        &IndexifyObjectsColumns::StateMachineMetadata.cf_db(&db),
        b"sm_meta",
    )?;
    match meta {
        Some(meta) => Ok(JsonEncoder::decode(&meta)?),
        None => Ok(StateMachineMetadata {
            db_version: 0,
            last_change_idx: 0,
        }),
    }
}
