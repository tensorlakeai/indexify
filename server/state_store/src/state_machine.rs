use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};
use data_model::{
    Allocation,
    ComputeGraph,
    ComputeGraphError,
    ComputeGraphVersion,
    GraphInvocationCtx,
    InvocationPayload,
    Namespace,
    NodeOutput,
    OutputPayload,
    StateChange,
    Task,
    TaskOutputsIngestionStatus,
};
use indexify_utils::{get_elapsed_time, get_epoch_time_in_ms, OptionInspectNone, TimeUnit};
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
use tracing::{error, info, trace, warn};

use super::serializer::{JsonEncode, JsonEncoder};
use crate::requests::{
    DeleteInvocationRequest,
    IngestTaskOutputsRequest,
    InvokeComputeGraphRequest,
    NamespaceRequest,
    ReductionTasks,
    SchedulerUpdateRequest,
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
    Namespaces,           //  Namespaces
    ComputeGraphs,        //  Ns_ComputeGraphName -> ComputeGraph
    /// Compute graph versions
    ///
    /// Keys:
    /// - `<Ns>_<ComputeGraphName>_<Version> -> ComputeGraphVersion`
    ComputeGraphVersions, //  Ns_ComputeGraphName_Version -> ComputeGraphVersion
    Tasks,                //  Ns_CG_<Invocation_Id>_Fn_TaskId -> Task
    GraphInvocationCtx,   //  Ns_CG_IngestedId -> GraphInvocationCtx
    GraphInvocationCtxSecondaryIndex, // NS_CG_InvocationId_CreatedAt -> empty
    ReductionTasks,       //  Ns_CG_Fn_TaskId -> ReduceTask

    GraphInvocations, //  Ns_Graph_Id -> InvocationPayload
    FnOutputs,        //  Ns_Graph_<Ingested_Id>_Fn_Id -> NodeOutput
    TaskOutputs,      //  NS_TaskID -> NodeOutputID

    UnprocessedStateChanges, //  StateChangeId -> StateChange
    Allocations,             // Allocation ID -> Allocation

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
    info!(namespace = ns.name, "created namespace: {}", ns.name);
    Ok(())
}

pub fn create_invocation(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: &InvokeComputeGraphRequest,
) -> Result<()> {
    let compute_graph_key = ComputeGraph::key_from(&req.namespace, &req.compute_graph_name);
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
    txn.put_cf(
        &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
        req.ctx.key(),
        &JsonEncoder::encode(&req.ctx)?,
    )?;
    txn.put_cf(
        &IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex.cf_db(&db),
        req.ctx.secondary_index_key(),
        &[],
    )?;

    info!(
        namespace = req.namespace,
        graph = req.compute_graph_name,
        invocation_id = req.invocation_payload.id,
        "created invocation: namespace: {}, compute_graph: {}",
        req.namespace,
        req.compute_graph_name
    );

    Ok(())
}

pub(crate) fn delete_invocation(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: &DeleteInvocationRequest,
) -> Result<()> {
    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(4_194_304);

    info!(
        namespace = req.namespace,
        graph = req.compute_graph,
        invocation_id = req.invocation_id,
        "Deleting invocation",
    );

    // Check if the invocation was deleted before the task completes
    let invocation_ctx_key =
        GraphInvocationCtx::key_from(&req.namespace, &req.compute_graph, &req.invocation_id);
    let invocation_ctx = txn
        .get_cf(
            &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
            &invocation_ctx_key,
        )
        .map_err(|e| anyhow!("failed to get invocation: {:?}", e))?;
    let invocation_ctx = match invocation_ctx {
        Some(v) => JsonEncoder::decode::<GraphInvocationCtx>(&v)?,
        None => {
            info!(
                namespace = &req.namespace,
                compute_graph = &req.compute_graph,
                invocation_id = &req.invocation_id,
                invocation_ctx_key = &invocation_ctx_key,
                "Invocation to delete not found: {}",
                &req.invocation_id
            );
            return Ok(());
        }
    };

    // Delete the invocation payload
    {
        let invocation_key =
            InvocationPayload::key_from(&req.namespace, &req.compute_graph, &req.invocation_id);

        txn.delete_cf(
            &IndexifyObjectsColumns::GraphInvocations.cf_db(&db),
            &invocation_key,
        )?;
    }

    let mut tasks_deleted = Vec::new();
    let task_prefix =
        Task::key_prefix_for_invocation(&req.namespace, &req.compute_graph, &req.invocation_id);
    // delete all tasks for this invocation
    for iter in make_prefix_iterator(
        txn,
        IndexifyObjectsColumns::Tasks.cf_db(&db),
        task_prefix.as_bytes(),
        &None,
    ) {
        let (key, value) = iter?;
        let value = Box::new(JsonEncoder::decode::<Task>(&value)?);
        tasks_deleted.push(value);
        txn.delete_cf(IndexifyObjectsColumns::Tasks.cf_db(&db), &key)?;
    }

    // delete all task outputs for this invocation
    // delete all tasks for this invocation
    for task in tasks_deleted {
        info!(
            namespace = req.namespace,
            graph = req.compute_graph,
            invocation_id = req.invocation_id,
            task_id = &task.id.get(),
            "deleting task",
        );
        match task.diagnostics {
            Some(diagnostic) => {
                [diagnostic.stdout.clone(), diagnostic.stderr.clone()]
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
        let task_output_prefix = Task::key_output_prefix_from(&req.namespace, &task.id.get());
        delete_cf_prefix(
            txn,
            &IndexifyObjectsColumns::TaskOutputs.cf_db(&db),
            task_output_prefix.as_bytes(),
        )?;
    }

    let allocation_prefix = Allocation::key_prefix_from_invocation(
        &req.namespace,
        &req.compute_graph,
        &req.invocation_id,
    );
    // delete all allocations for this invocation
    for iter in make_prefix_iterator(
        txn,
        IndexifyObjectsColumns::Allocations.cf_db(&db),
        allocation_prefix.as_bytes(),
        &None,
    ) {
        let (key, value) = iter?;
        let value = JsonEncoder::decode::<Allocation>(&value)?;
        if value.invocation_id == req.invocation_id {
            info!(
                namespace = req.namespace,
                graph = req.compute_graph,
                invocation_id = req.invocation_id,
                allocation_id = value.id,
                "deleting allocation",
            );
            txn.delete_cf(IndexifyObjectsColumns::Allocations.cf_db(&db), &key)?;
        }
    }

    // Delete Graph Invocation Context
    delete_cf_prefix(
        txn,
        IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
        invocation_ctx_key.as_bytes(),
    )?;

    // Delete Graph Invocation Context Secondary Index
    txn.delete_cf(
        IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex.cf_db(&db),
        &invocation_ctx.secondary_index_key(),
    )?;

    let node_output_prefix =
        NodeOutput::key_prefix_from(&req.namespace, &req.compute_graph, &req.invocation_id);

    // mark all fn output urls for gc.
    for iter in make_prefix_iterator(
        txn,
        &IndexifyObjectsColumns::FnOutputs.cf_db(&db),
        node_output_prefix.as_bytes(),
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
    let tasks_prefix =
        Task::key_prefix_for_compute_graph(&compute_graph.namespace, &compute_graph.name);
    let mut read_options = ReadOptions::default();
    read_options.set_readahead_size(10_194_304);
    let iter = make_prefix_iterator(
        txn,
        &IndexifyObjectsColumns::Tasks.cf_db(&db),
        tasks_prefix.as_bytes(),
        &None,
    );

    let mut tasks_to_update = HashMap::new();
    for kv in iter {
        let (key, val) = kv?;
        let mut task: Task = JsonEncoder::decode(&val)?;
        if task.graph_version != compute_graph.version && !task.outcome.is_terminal() {
            info!(
                namespace = compute_graph.namespace,
                graph = compute_graph.name,
                graph_version = compute_graph.version.0,
                function = task.compute_fn_name,
                invocation_id = task.invocation_id,
                task_id = task.id.to_string(),
                "updating task: {} from version: {} to version: {}",
                task.id,
                task.graph_version.0,
                compute_graph.version.0
            );
            task.graph_version = compute_graph.version.clone();
            tasks_to_update.insert(key, task);
        }
    }
    info!(
        namespace = compute_graph.namespace,
        graph = compute_graph.name,
        graph_version = compute_graph.version.0,
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
    let cg_prefix = GraphInvocationCtx::key_prefix_for_compute_graph(
        &compute_graph.namespace,
        &compute_graph.name,
    );

    let iter = make_prefix_iterator(
        txn,
        &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
        cg_prefix.as_bytes(),
        &None,
    );

    let mut graph_invocation_ctx_to_update = HashMap::new();
    for kv in iter {
        let (key, val) = kv?;
        let mut graph_invocation_ctx: GraphInvocationCtx = JsonEncoder::decode(&val)?;
        if graph_invocation_ctx.graph_version != compute_graph.version &&
            !graph_invocation_ctx.completed
        {
            info!(
                namespace= graph_invocation_ctx.namespace,
                invocation_id =  graph_invocation_ctx.invocation_id,
                graph_version = graph_invocation_ctx.graph_version.0,
                graph = graph_invocation_ctx.compute_graph_name,
                "updating graph_invocation_ctx for invocation id: {} from version: {} to version: {}",
                graph_invocation_ctx.invocation_id, graph_invocation_ctx.graph_version.0, compute_graph.version.0
            );
            graph_invocation_ctx.graph_version = compute_graph.version.clone();
            graph_invocation_ctx_to_update.insert(key, graph_invocation_ctx);
        }
    }
    info!(
        namespace = compute_graph.namespace,
        graph = compute_graph.name,
        graph_version = compute_graph.version.0,
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
        namespace = compute_graph.namespace,
        graph = compute_graph.name,
        graph_version = compute_graph.version.0,
        "creating compute graph: ns: {} name: {}, upgrade invocations: {}",
        compute_graph.namespace,
        compute_graph.name,
        upgrade_existing_tasks_to_current_version
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
        namespace = compute_graph.namespace,
        graph = compute_graph.name,
        graph_version = compute_graph.version.0,
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
        namespace = compute_graph.namespace,
        graph = compute_graph.name,
        graph_version = compute_graph.version.0,
        "finished creating compute graph namespace: {} name: {}, version: {}",
        compute_graph.namespace,
        compute_graph.name,
        compute_graph.version.0
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
        namespace = namespace,
        graph = name,
        "tombstoning compute graph: namespace: {}, name: {}",
        namespace,
        name
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
        namespace = namespace,
        graph = name,
        "deleting compute graph: namespace: {}, name: {}",
        namespace,
        name
    );
    txn.delete_cf(
        &IndexifyObjectsColumns::ComputeGraphs.cf_db(&db),
        ComputeGraph::key_from(namespace, name).as_bytes(),
    )?;

    let graph_invocation_prefix = GraphInvocationCtx::key_prefix_for_compute_graph(namespace, name);

    for iter in make_prefix_iterator(
        txn,
        &IndexifyObjectsColumns::GraphInvocations.cf_db(&db),
        graph_invocation_prefix.as_bytes(),
        &None,
    ) {
        let (_key, value) = iter?;
        let value = JsonEncoder::decode::<InvocationPayload>(&value)?;
        let req = DeleteInvocationRequest {
            namespace: value.namespace,
            compute_graph: value.compute_graph_name,
            invocation_id: value.id,
        };
        delete_invocation(db.clone(), txn, &req)?;
    }

    for iter in make_prefix_iterator(
        txn,
        &IndexifyObjectsColumns::ComputeGraphVersions.cf_db(&db),
        ComputeGraphVersion::key_prefix_from(namespace, name).as_bytes(),
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

pub(crate) fn handle_scheduler_update(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    request: &SchedulerUpdateRequest,
) -> Result<()> {
    for alloc in &request.remove_allocations {
        info!(
            namespace = alloc.namespace,
            graph = alloc.compute_graph,
            invocation_id = alloc.invocation_id,
            function_name = alloc.compute_fn,
            task_id = alloc.id.to_string(),
            allocation_id = alloc.id,
            "delete_allocation",
        );
        txn.delete_cf(IndexifyObjectsColumns::Allocations.cf_db(&db), &alloc.key())?;
    }

    for alloc in &request.new_allocations {
        info!(
            namespace = alloc.namespace,
            graph = alloc.compute_graph,
            invocation_id = alloc.invocation_id,
            function_name = alloc.compute_fn,
            task_id = alloc.task_id.to_string(),
            allocation_id = alloc.id,
            function_executor_id = alloc.function_executor_id.get(),
            executor_id = alloc.executor_id.get(),
            "add_allocation",
        );
        let serialized_alloc = JsonEncoder::encode(&alloc)?;
        txn.put_cf(
            &IndexifyObjectsColumns::Allocations.cf_db(&db),
            &alloc.key(),
            serialized_alloc,
        )?;
    }

    for (_, task) in &request.updated_tasks {
        info!(
            namespace = task.namespace,
            graph = task.compute_graph_name,
            invocation_id = task.invocation_id,
            function_name = task.compute_fn_name,
            task_id = task.id.to_string(),
            status = task.status.to_string(),
            outcome = task.outcome.to_string(),
            duration_secs = get_elapsed_time(task.creation_time_ns, TimeUnit::Nanoseconds),
            "updated task",
        );

        let serialized_task = JsonEncoder::encode(&task)?;
        txn.put_cf(
            IndexifyObjectsColumns::Tasks.cf_db(&db),
            task.key(),
            serialized_task,
        )?;
    }

    processed_reduction_tasks(db.clone(), txn, &request.reduction_tasks)?;

    for invocation_ctx in &request.updated_invocations_states {
        if invocation_ctx.completed {
            info!(
                invocation_id = invocation_ctx.invocation_id.to_string(),
                namespace = invocation_ctx.namespace,
                compute_graph = invocation_ctx.compute_graph_name,
                outcome = invocation_ctx.outcome.to_string(),
                duration_secs =
                    get_elapsed_time(invocation_ctx.created_at.into(), TimeUnit::Milliseconds),
                "invocation completed"
            );
        }
        let serialized_graph_ctx = JsonEncoder::encode(&invocation_ctx)?;
        txn.put_cf(
            &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
            invocation_ctx.key(),
            &serialized_graph_ctx,
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
        info!(
            namespace = &req.namespace,
            compute_graph = &req.compute_graph,
            invocation_id = &req.invocation_id,
            compute_fn = &req.compute_fn,
            task_id = req.task.id.get(),
            "Compute graph not found: {}",
            &req.compute_graph
        );
        return Ok(false);
    }

    // Check if the invocation was deleted before the task completes
    let invocation_ctx_key =
        GraphInvocationCtx::key_from(&req.namespace, &req.compute_graph, &req.invocation_id);
    let invocation = txn
        .get_cf(
            &IndexifyObjectsColumns::GraphInvocationCtx.cf_db(&db),
            &invocation_ctx_key,
        )
        .map_err(|e| anyhow!("failed to get invocation: {}", e))?;
    let invocation = match invocation {
        Some(v) => JsonEncoder::decode::<GraphInvocationCtx>(&v)?,
        None => {
            info!(
                namespace = &req.namespace,
                compute_graph = &req.compute_graph,
                invocation_id = &req.invocation_id,
                compute_fn = &req.compute_fn,
                task_id = req.task.id.get(),
                "Invocation not found: {}",
                &req.invocation_id
            );
            return Ok(false);
        }
    };

    let existing_task = txn.get_for_update_cf(
        &IndexifyObjectsColumns::Tasks.cf_db(&db),
        req.task.key(),
        true,
    )?;
    if existing_task.is_none() {
        info!(
            namespace = &req.namespace,
            compute_graph = &req.compute_graph,
            invocation_id = &req.invocation_id,
            compute_fn = &req.compute_fn,
            task_id = req.task.id.get(),
            "Task not found: {}",
            req.task.key()
        );
        return Ok(false);
    }
    let existing_task = JsonEncoder::decode::<Task>(&existing_task.unwrap())?;

    txn.delete_cf(
        &IndexifyObjectsColumns::Allocations.cf_db(&db),
        Allocation::key_from(
            &req.namespace,
            &req.compute_graph,
            &req.invocation_id,
            &req.compute_fn,
            &req.task.id,
            &req.executor_id,
        ),
    )?;

    // Skip finalize task if it's invocation is already completed
    if invocation.completed {
        warn!(
            namespace = &req.namespace,
            compute_graph = &req.compute_graph,
            invocation_id = &req.invocation_id,
            compute_fn = &req.compute_fn,
            task_id = req.task.id.get(),
            "Invocation already completed, skipping setting outputs",
        );
        return Ok(false);
    }

    // idempotency check guaranteeing that we emit a finalizing state change only
    // once.
    if existing_task.output_status == TaskOutputsIngestionStatus::Ingested {
        warn!(
            namespace = &req.namespace,
            compute_graph = &req.compute_graph,
            invocation_id = &req.invocation_id,
            compute_fn = &req.compute_fn,
            task_id = req.task.id.get(),
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
        let task_output_key = &req.task.key_output(&output.id);
        let node_output_id = JsonEncoder::encode(&output_key)?;
        txn.put_cf(
            &IndexifyObjectsColumns::TaskOutputs.cf_db(&db),
            task_output_key,
            node_output_id,
        )?;
    }

    let existing_task = req.task;

    let task_bytes = JsonEncoder::encode(&existing_task)?;
    txn.put_cf(
        &IndexifyObjectsColumns::Tasks.cf_db(&db),
        existing_task.key(),
        task_bytes,
    )?;

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
        trace!(
            change_type = %state_change.change_type,
            "marking state change as processed"
        );
        let key = &state_change.key();
        txn.delete_cf(
            &IndexifyObjectsColumns::UnprocessedStateChanges.cf_db(&db),
            key,
        )?;
    }
    Ok(())
}

/// Helper function to update a column family entry in a transaction
/// by fetching the entry, deserializing it, applying the update function,
/// serializing it and putting it back in the column family.
///
/// This can be done with an exclusive lock or not.
fn _do_cf_update<T>(
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
