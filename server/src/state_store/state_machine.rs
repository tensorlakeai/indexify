use std::{
    collections::HashMap,
    sync::{
        atomic::{self, AtomicU64},
        Arc,
    },
};

use anyhow::{anyhow, Result};
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
use tracing::{debug, error, info, info_span, trace, warn};

use super::serializer::{JsonEncode, JsonEncoder};
use crate::{
    data_model::{
        self,
        Allocation,
        AllocationOutputIngestedEvent,
        ComputeGraph,
        ComputeGraphError,
        ComputeGraphVersion,
        GcUrl,
        GraphInvocationCtx,
        InvocationPayload,
        Namespace,
        NodeOutput,
        StateChange,
        StateChangeBuilder,
        StateChangeId,
        Task,
    },
    state_store::requests::{
        AllocationOutput,
        DeleteInvocationRequest,
        InvokeComputeGraphRequest,
        NamespaceRequest,
        ReductionTasks,
        SchedulerUpdateRequest,
    },
    utils::{get_elapsed_time, get_epoch_time_in_ms, OptionInspectNone, TimeUnit},
};

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

    UnprocessedStateChanges,     //  StateChangeId -> StateChange
    Allocations,                 // Allocation ID -> Allocation
    FunctionExecutorDiagnostics, // Function Executor ID -> FunctionExecutorDiagnostics

    GcUrls, // List of URLs pending deletion

    SystemTasks, // Long running tasks involving multiple invocations

    Stats, // Stats
}

impl IndexifyObjectsColumns {
    pub fn cf_db<'a>(&self, db: &'a TransactionDB) -> &'a ColumnFamily {
        db.cf_handle(self.as_ref())
            .inspect_none(|| {
                error!("failed to get column family handle for {}", self.as_ref());
            })
            .unwrap()
    }
}

pub(crate) fn upsert_namespace(db: Arc<TransactionDB>, req: &NamespaceRequest) -> Result<()> {
    let ns = Namespace {
        name: req.name.clone(),
        created_at: get_epoch_time_in_ms(),
        blob_storage_bucket: req.blob_storage_bucket.clone(),
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
    let span = info_span!(
        "create_invocation",
        namespace = req.namespace,
        graph = req.compute_graph_name,
        invocation_id = req.invocation_payload.id
    );
    let _guard = span.enter();

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
        [],
    )?;

    info!(
        "created invocation: namespace: {}, compute_graph: {}",
        req.namespace, req.compute_graph_name
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
    let span = info_span!(
        "delete_invocation",
        namespace = req.namespace,
        graph = req.compute_graph,
        invocation_id = req.invocation_id,
    );
    let _guard = span.enter();

    info!("Deleting invocation",);

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
                invocation_ctx_key = &invocation_ctx_key,
                "Invocation to delete not found: {}", &req.invocation_id
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
            task_id = &task.id.get(),
            "fn" = &task.compute_fn_name,
            "deleting task",
        );
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
                allocation_id = value.id,
                task_id = value.task_id.get(),
                "fn" = value.compute_fn,
                "deleting allocation",
            );

            txn.delete_cf(IndexifyObjectsColumns::Allocations.cf_db(&db), &key)?;
            if let Some(diagnostic) = value.diagnostics {
                [diagnostic.stdout.clone(), diagnostic.stderr.clone()]
                    .iter()
                    .flatten()
                    .try_for_each(|data| -> Result<()> {
                        let gc_url = GcUrl {
                            url: data.path.clone(),
                            namespace: req.namespace.clone(),
                        };
                        let serialized_gc_url = JsonEncoder::encode(&gc_url)?;
                        txn.put_cf(
                            &IndexifyObjectsColumns::GcUrls.cf_db(&db),
                            gc_url.key().as_bytes(),
                            &serialized_gc_url,
                        )?;
                        Ok(())
                    })?;
            }
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
        invocation_ctx.secondary_index_key(),
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
        for payload in value.payloads {
            let gc_url = GcUrl {
                url: payload.path.clone(),
                namespace: req.namespace.clone(),
            };
            let serialized_gc_url = JsonEncoder::encode(&gc_url)?;
            txn.put_cf(
                &IndexifyObjectsColumns::GcUrls.cf_db(&db),
                gc_url.key().as_bytes(),
                &serialized_gc_url,
            )?;
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
    let span = info_span!(
        "update_task_versions_for_cg",
        namespace = compute_graph.namespace,
        graph = compute_graph.name,
        graph_version = compute_graph.version.0,
    );
    let _guard = span.enter();

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

    let span = info_span!(
        "update_graph_invocations_for_cg",
        namespace = compute_graph.namespace,
        graph = compute_graph.name,
        graph_version = compute_graph.version.0,
    );
    let _guard = span.enter();

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
                invocation_id =  graph_invocation_ctx.invocation_id,
                "updating graph_invocation_ctx for invocation id: {} from version: {} to version: {}",
                graph_invocation_ctx.invocation_id, graph_invocation_ctx.graph_version.0, compute_graph.version.0
            );
            graph_invocation_ctx.graph_version = compute_graph.version.clone();
            graph_invocation_ctx_to_update.insert(key, graph_invocation_ctx);
        }
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
    let span = info_span!(
        "create_or_update_compute_graph",
        namespace = compute_graph.namespace,
        graph = compute_graph.name,
        graph_version = compute_graph.version.0,
    );
    let _guard = span.enter();

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

pub fn delete_compute_graph(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    namespace: &str,
    name: &str,
) -> Result<()> {
    let span = info_span!("delete_compute_graph", namespace = namespace, graph = name,);
    let _guard = span.enter();

    info!(
        "deleting compute graph: namespace: {}, name: {}",
        namespace, name
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
        let gc_url = GcUrl {
            url: value.code.path.clone(),
            namespace: namespace.to_string(),
        };
        let serialized_gc_url = JsonEncoder::encode(&gc_url)?;
        txn.put_cf(
            &IndexifyObjectsColumns::GcUrls.cf_db(&db),
            gc_url.key().as_bytes(),
            &serialized_gc_url,
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
    urls: Vec<GcUrl>,
) -> Result<()> {
    for url in urls {
        txn.delete_cf(
            &IndexifyObjectsColumns::GcUrls.cf_db(&db),
            url.key().as_bytes(),
        )?;
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
            Some(restart_key) => IteratorMode::From(restart_key, Direction::Forward),
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
    last_state_change_id: &AtomicU64,
) -> Result<Vec<StateChange>> {
    last_state_change_id.fetch_add(1, atomic::Ordering::Relaxed);

    for alloc in &request.new_allocations {
        info!(
            namespace = alloc.namespace,
            graph = alloc.compute_graph,
            invocation_id = alloc.invocation_id,
            "fn" = alloc.compute_fn,
            task_id = alloc.task_id.to_string(),
            allocation_id = alloc.id,
            fn_executor_id = alloc.target.function_executor_id.get(),
            executor_id = alloc.target.executor_id.get(),
            "add_allocation",
        );
        let serialized_alloc = JsonEncoder::encode(&alloc)?;
        txn.put_cf(
            &IndexifyObjectsColumns::Allocations.cf_db(&db),
            alloc.key(),
            serialized_alloc,
        )?;
    }

    for task in request.updated_tasks.values() {
        info!(
            namespace = task.namespace,
            graph = task.compute_graph_name,
            invocation_id = task.invocation_id,
            "fn" = task.compute_fn_name,
            task_id = task.id.to_string(),
            status = task.status.to_string(),
            outcome = task.outcome.to_string(),
            duration_sec = get_elapsed_time(task.creation_time_ns, TimeUnit::Nanoseconds),
            "updated task",
        );

        let serialized_task = JsonEncoder::encode(&task)?;
        txn.put_cf(
            IndexifyObjectsColumns::Tasks.cf_db(&db),
            task.key(),
            serialized_task,
        )?;
    }

    let mut state_changes = vec![];

    for (task_key, node_output) in &request.cached_task_outputs {
        let serialized_output = JsonEncoder::encode(&node_output)?;
        // Create an output key
        let output_key = node_output.key();
        txn.put_cf(
            &IndexifyObjectsColumns::FnOutputs.cf_db(&db),
            &output_key,
            serialized_output,
        )?;

        let task = txn.get_cf(&IndexifyObjectsColumns::Tasks.cf_db(&db), task_key)?;

        let Some(task) = task else {
            error!(task_key = task_key, "Task not found in tasks database");
            continue;
        };

        let task = JsonEncoder::decode::<Task>(&task)?;

        let last_change_id = last_state_change_id.fetch_add(1, atomic::Ordering::Relaxed);
        let event = StateChangeBuilder::default()
            .namespace(Some(task.namespace.clone()))
            .compute_graph(Some(task.compute_graph_name.clone()))
            .invocation(Some(task.invocation_id.clone()))
            .change_type(data_model::ChangeType::AllocationOutputsIngested(
                AllocationOutputIngestedEvent {
                    namespace: task.namespace.clone(),
                    compute_graph: task.compute_graph_name.clone(),
                    compute_fn: task.compute_fn_name.clone(),
                    invocation_id: task.invocation_id.clone(),
                    task_id: task.id.clone(),
                    node_output_key: output_key,
                    allocation_key: None,
                },
            ))
            .created_at(get_epoch_time_in_ms())
            .object_id(task.id.clone().to_string())
            .id(StateChangeId::new(last_change_id))
            .processed_at(None)
            .build()?;

        debug!(cache_event = ?event);
        state_changes.push(event);
    }

    processed_reduction_tasks(db.clone(), txn, &request.reduction_tasks)?;

    for invocation_ctx in &request.updated_invocations_states {
        if invocation_ctx.completed {
            info!(
                invocation_id = invocation_ctx.invocation_id.to_string(),
                namespace = invocation_ctx.namespace,
                graph = invocation_ctx.compute_graph_name,
                outcome = invocation_ctx.outcome.to_string(),
                duration_sec =
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

    Ok(state_changes)
}

// returns true if task the task finishing state should be emitted.
pub fn ingest_task_outputs(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    req: AllocationOutput,
) -> Result<bool> {
    let span = info_span!(
        "ingest_task_outputs",
        namespace = &req.namespace,
        graph = &req.compute_graph,
        invocation_id = &req.invocation_id,
        "fn" = &req.compute_fn,
        task_id = req.allocation.task_id.get(),
    );
    let _guard = span.enter();

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
            info!("Invocation not found: {}", &req.invocation_id);
            return Ok(false);
        }
    };
    // Skip finalize task if it's invocation is already completed
    if invocation.completed {
        warn!("Invocation already completed, skipping setting outputs",);
        return Ok(false);
    }

    let existing_allocation = txn.get_for_update_cf(
        &IndexifyObjectsColumns::Allocations.cf_db(&db),
        &req.allocation_key,
        true,
    )?;
    let Some(existing_allocation) = existing_allocation else {
        info!("Allocation not found",);
        return Ok(false);
    };
    let existing_allocation = JsonEncoder::decode::<Allocation>(&existing_allocation)?;
    // idempotency check guaranteeing that we emit a finalizing state change only
    // once.
    if existing_allocation.is_terminal() {
        warn!("allocation already terminal, skipping setting outputs",);
        return Ok(false);
    }

    let serialized_allocation = JsonEncoder::encode(&req.allocation)?;
    txn.put_cf(
        &IndexifyObjectsColumns::Allocations.cf_db(&db),
        &req.allocation_key,
        &serialized_allocation,
    )?;

    let serialized_output = JsonEncoder::encode(&req.node_output)?;

    if req.node_output.reducer_output {
        let mut acc_value = req.node_output.clone();
        acc_value.reducer_output = false;
        let reducer_acc_value = JsonEncoder::encode(&acc_value)?;
        let reducer_output_key = req.node_output.reducer_acc_value_key();
        txn.put_cf(
            &IndexifyObjectsColumns::FnOutputs.cf_db(&db),
            &reducer_output_key,
            reducer_acc_value,
        )?;
    }
    // Create an output key
    let output_key = req.node_output.key();
    txn.put_cf(
        &IndexifyObjectsColumns::FnOutputs.cf_db(&db),
        &output_key,
        serialized_output,
    )?;

    Ok(true)
}

pub fn upsert_function_executor_diagnostics(
    db: Arc<TransactionDB>,
    txn: &Transaction<TransactionDB>,
    fe_diagnostics: &data_model::FunctionExecutorDiagnostics,
) -> Result<()> {
    let serialized_fe_diagnostics = JsonEncoder::encode(fe_diagnostics)?;
    txn.put_cf(
        &IndexifyObjectsColumns::FunctionExecutorDiagnostics.cf_db(&db),
        fe_diagnostics.key(),
        serialized_fe_diagnostics,
    )?;
    Ok(())
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
        .get_for_update_cf(cf, key, exclusive)?
        .ok_or(anyhow::anyhow!("Task not found"))?;
    let mut task = JsonEncoder::decode::<T>(&task)?;
    update_fn(&mut task);
    let serialized_task = JsonEncoder::encode(&task)?;
    txn.put_cf(cf, key, &serialized_task)?;
    Ok(())
}
