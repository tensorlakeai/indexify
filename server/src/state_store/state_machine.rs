use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::{anyhow, Result};
use strum::AsRefStr;
use tracing::{debug, info, info_span, trace, warn};

use super::serializer::{JsonEncode, JsonEncoder};
use crate::{
    data_model::{
        Allocation,
        ComputeGraph,
        ComputeGraphVersion,
        GcUrl,
        GcUrlBuilder,
        GraphInvocationCtx,
        NamespaceBuilder,
        StateChange,
    },
    state_store::{
        driver::{rocksdb::RocksDBDriver, Reader, Transaction, Writer},
        requests::{
            AllocationOutput,
            DeleteInvocationRequest,
            InvokeComputeGraphRequest,
            NamespaceRequest,
            SchedulerUpdateRequest,
        },
    },
    utils::{get_elapsed_time, get_epoch_time_in_ms, TimeUnit},
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
    GraphInvocationCtx,   //  Ns_CG_IngestedId -> GraphInvocationCtx
    GraphInvocationCtxSecondaryIndex, // NS_CG_InvocationId_CreatedAt -> empty

    UnprocessedStateChanges, //  StateChangeId -> StateChange
    Allocations,             // Allocation ID -> Allocation

    GcUrls, // List of URLs pending deletion

    Stats, // Stats
}

pub(crate) fn upsert_namespace(db: Arc<RocksDBDriver>, req: &NamespaceRequest) -> Result<()> {
    let ns = NamespaceBuilder::default()
        .name(req.name.clone())
        .created_at(get_epoch_time_in_ms())
        .blob_storage_bucket(req.blob_storage_bucket.clone())
        .blob_storage_region(req.blob_storage_region.clone())
        .build()?;
    let serialized_namespace = JsonEncoder::encode(&ns)?;
    db.put(
        IndexifyObjectsColumns::Namespaces.as_ref(),
        &ns.name,
        serialized_namespace,
    )?;
    info!(namespace = ns.name, "created namespace: {}", ns.name);
    Ok(())
}

pub fn create_invocation(txn: &Transaction, req: &InvokeComputeGraphRequest) -> Result<()> {
    let span = info_span!(
        "create_invocation",
        namespace = req.namespace,
        graph = req.compute_graph_name,
        invocation_id = req.ctx.request_id
    );
    let _guard = span.enter();

    let compute_graph_key = ComputeGraph::key_from(&req.namespace, &req.compute_graph_name);
    let cg = txn
        .get(
            IndexifyObjectsColumns::ComputeGraphs.as_ref(),
            &compute_graph_key,
        )?
        .ok_or(anyhow::anyhow!("Compute graph not found"))?;
    let cg: ComputeGraph = JsonEncoder::decode(&cg)?;
    if cg.tombstoned {
        return Err(anyhow::anyhow!("Compute graph is tomb-stoned"));
    }
    txn.put(
        &IndexifyObjectsColumns::GraphInvocationCtx.as_ref(),
        req.ctx.key(),
        &JsonEncoder::encode(&req.ctx)?,
    )?;
    txn.put(
        IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex.as_ref(),
        req.ctx.secondary_index_key(),
        [],
    )?;

    info!(
        "created invocation: namespace: {}, compute_graph: {}",
        req.namespace, req.compute_graph_name
    );

    Ok(())
}

pub(crate) fn upsert_allocation(txn: &Transaction, allocation: &Allocation) -> Result<()> {
    let serialized_allocation = JsonEncoder::encode(&allocation)?;
    txn.put(
        &IndexifyObjectsColumns::Allocations.as_ref(),
        allocation.key().as_bytes(),
        &serialized_allocation,
    )?;
    Ok(())
}

pub(crate) fn delete_invocation(txn: &Transaction, req: &DeleteInvocationRequest) -> Result<()> {
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
        .get(
            IndexifyObjectsColumns::GraphInvocationCtx.as_ref(),
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
    let mut payload_urls: HashSet<String> = HashSet::new();
    for (_, function_run) in invocation_ctx.function_runs.iter() {
        for input_arg in function_run.input_args.iter() {
            payload_urls.insert(input_arg.data_payload.path.clone());
        }
        for output in function_run.output.iter() {
            payload_urls.insert(output.path.clone());
        }
    }
    println!("GC payload_urls: {:?}", payload_urls);
    for payload_url in payload_urls {
        let gc_url = GcUrlBuilder::default()
            .url(payload_url)
            .namespace(req.namespace.clone())
            .build()?;
        let serialized_gc_url = JsonEncoder::encode(&gc_url)?;
        txn.put(
            &IndexifyObjectsColumns::GcUrls.as_ref(),
            gc_url.key().as_bytes(),
            &serialized_gc_url,
        )?;
    }

    let allocation_prefix = Allocation::key_prefix_from_invocation(
        &req.namespace,
        &req.compute_graph,
        &req.invocation_id,
    );
    // delete all allocations for this invocation
    let cf = IndexifyObjectsColumns::Allocations.as_ref();
    for iter in txn.iter(cf, allocation_prefix.as_bytes(), Default::default()) {
        let (key, value) = iter?;
        let value = JsonEncoder::decode::<Allocation>(&value)?;
        if value.invocation_id == req.invocation_id {
            info!(
                allocation_id = %value.id,
                function_call_id = value.function_call_id.to_string(),
                "fn" = value.compute_fn,
                "deleting allocation",
            );
            txn.delete(IndexifyObjectsColumns::Allocations.as_ref(), &key)?;
        }
    }

    // Delete Graph Invocation Context
    delete_cf_prefix(
        txn,
        IndexifyObjectsColumns::GraphInvocationCtx.as_ref(),
        invocation_ctx_key.as_bytes(),
    )?;

    // Delete Graph Invocation Context Secondary Index
    txn.delete(
        IndexifyObjectsColumns::GraphInvocationCtxSecondaryIndex.as_ref(),
        invocation_ctx.secondary_index_key(),
    )?;

    Ok(())
}

fn update_graph_invocations_for_cg(txn: &Transaction, compute_graph: &ComputeGraph) -> Result<()> {
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

    let iter = txn.iter(
        IndexifyObjectsColumns::GraphInvocationCtx.as_ref(),
        cg_prefix.as_bytes(),
        Default::default(),
    );

    let mut graph_invocation_ctx_to_update = HashMap::new();
    for kv in iter {
        let (key, val) = kv?;
        let mut request_ctx: GraphInvocationCtx = JsonEncoder::decode(&val)?;
        if request_ctx.graph_version != compute_graph.version && request_ctx.outcome.is_none() {
            info!(
                invocation_id = request_ctx.request_id,
                "updating request_ctx for invocation id: {} from version: {} to version: {}",
                request_ctx.request_id,
                request_ctx.graph_version.0,
                compute_graph.version.0
            );
            request_ctx.graph_version = compute_graph.version.clone();
            for (_function_call_id, function_run) in request_ctx.function_runs.iter_mut() {
                if function_run.graph_version != compute_graph.version &&
                    function_run.outcome.is_none()
                {
                    function_run.graph_version = compute_graph.version.clone();
                }
            }
            graph_invocation_ctx_to_update.insert(key, request_ctx);
        }
    }
    info!(
        "upgrading request ctxs: {}",
        graph_invocation_ctx_to_update.len()
    );
    for (invocation_id, graph_invocation_ctx) in graph_invocation_ctx_to_update {
        let serialized_task = JsonEncoder::encode(&graph_invocation_ctx)?;
        txn.put(
            IndexifyObjectsColumns::GraphInvocationCtx.as_ref(),
            &invocation_id,
            &serialized_task,
        )?;
    }
    Ok(())
}

pub(crate) fn create_or_update_compute_graph(
    txn: &Transaction,
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
        .get(
            IndexifyObjectsColumns::ComputeGraphs.as_ref(),
            compute_graph.key(),
        )?
        .map(|v| JsonEncoder::decode::<ComputeGraph>(&v));

    let new_compute_graph_version = match existing_compute_graph {
        Some(Ok(mut existing_compute_graph)) => {
            existing_compute_graph.update(compute_graph.clone());
            existing_compute_graph.to_version()
        }
        Some(Err(e)) => {
            return Err(anyhow!("failed to decode existing compute graph: {}", e));
        }
        None => compute_graph.to_version(),
    }?;
    info!(
        "new compute graph version: {}",
        &new_compute_graph_version.version.0
    );
    let serialized_compute_graph_version = JsonEncoder::encode(&new_compute_graph_version)?;
    txn.put(
        IndexifyObjectsColumns::ComputeGraphVersions.as_ref(),
        new_compute_graph_version.key(),
        &serialized_compute_graph_version,
    )?;

    let serialized_compute_graph = JsonEncoder::encode(&compute_graph)?;
    txn.put(
        IndexifyObjectsColumns::ComputeGraphs.as_ref(),
        compute_graph.key(),
        &serialized_compute_graph,
    )?;

    if upgrade_existing_tasks_to_current_version {
        update_graph_invocations_for_cg(txn, &compute_graph)?;
    }

    info!(
        "finished creating compute graph namespace: {} name: {}, version: {}",
        compute_graph.namespace, compute_graph.name, compute_graph.version.0
    );
    Ok(())
}

fn delete_cf_prefix(txn: &Transaction, cf: &str, prefix: &[u8]) -> Result<()> {
    let iter = txn.iter(cf, prefix, Default::default());
    for key in iter {
        let (key, _) = key?;
        if !key.starts_with(prefix) {
            break;
        }
        txn.delete(cf, &key)?;
    }
    Ok(())
}

pub fn delete_compute_graph(txn: &Transaction, namespace: &str, name: &str) -> Result<()> {
    let span = info_span!("delete_compute_graph", namespace = namespace, graph = name,);
    let _guard = span.enter();

    info!(
        "deleting compute graph: namespace: {}, name: {}",
        namespace, name
    );
    txn.delete(
        IndexifyObjectsColumns::ComputeGraphs.as_ref(),
        ComputeGraph::key_from(namespace, name).as_bytes(),
    )?;

    let graph_invocation_prefix = GraphInvocationCtx::key_prefix_for_compute_graph(namespace, name);

    for iter in txn.iter(
        &IndexifyObjectsColumns::GraphInvocationCtx.as_ref(),
        graph_invocation_prefix.as_bytes(),
        Default::default(),
    ) {
        let (_key, value) = iter?;
        let value = JsonEncoder::decode::<GraphInvocationCtx>(&value)?;
        delete_invocation(
            txn,
            &DeleteInvocationRequest {
                namespace: value.namespace,
                compute_graph: value.compute_graph_name,
                invocation_id: value.request_id,
            },
        )?;
    }

    for iter in txn.iter(
        IndexifyObjectsColumns::ComputeGraphVersions.as_ref(),
        ComputeGraphVersion::key_prefix_from(namespace, name).as_bytes(),
        Default::default(),
    ) {
        let (key, value) = iter?;
        let value = JsonEncoder::decode::<ComputeGraphVersion>(&value)?;

        // mark all code urls for gc.
        let gc_url = GcUrlBuilder::default()
            .url(value.code.path.clone())
            .namespace(namespace.to_string())
            .build()?;
        let serialized_gc_url = JsonEncoder::encode(&gc_url)?;
        txn.put(
            IndexifyObjectsColumns::GcUrls.as_ref(),
            gc_url.key().as_bytes(),
            &serialized_gc_url,
        )?;
        txn.delete(IndexifyObjectsColumns::ComputeGraphVersions.as_ref(), &key)?;
    }

    Ok(())
}

pub fn remove_gc_urls(txn: &Transaction, urls: Vec<GcUrl>) -> Result<()> {
    for url in urls {
        txn.delete(
            IndexifyObjectsColumns::GcUrls.as_ref(),
            url.key().as_bytes(),
        )?;
    }
    Ok(())
}

pub(crate) fn handle_scheduler_update(
    txn: &Transaction,
    request: &SchedulerUpdateRequest,
) -> Result<()> {
    for alloc in &request.new_allocations {
        debug!(
            namespace = alloc.namespace,
            graph = alloc.compute_graph,
            invocation_id = alloc.invocation_id,
            "fn" = alloc.compute_fn,
            task_id = alloc.function_call_id.to_string(),
            allocation_id = %alloc.id,
            fn_executor_id = alloc.target.function_executor_id.get(),
            executor_id = alloc.target.executor_id.get(),
            "add_allocation",
        );
        let serialized_alloc = JsonEncoder::encode(&alloc)?;
        txn.put(
            IndexifyObjectsColumns::Allocations.as_ref(),
            alloc.key(),
            serialized_alloc,
        )?;
    }

    for (_, invocation_ctx) in &request.updated_invocations_states {
        if invocation_ctx.outcome.is_some() {
            info!(
                invocation_id = invocation_ctx.request_id.to_string(),
                namespace = invocation_ctx.namespace,
                graph = invocation_ctx.compute_graph_name,
                outcome = invocation_ctx
                    .outcome
                    .as_ref()
                    .map(|o| o.to_string())
                    .unwrap_or_default(),
                duration_sec =
                    get_elapsed_time(invocation_ctx.created_at.into(), TimeUnit::Milliseconds),
                "invocation completed"
            );
        }
        let serialized_graph_ctx = JsonEncoder::encode(&invocation_ctx)?;
        txn.put(
            IndexifyObjectsColumns::GraphInvocationCtx.as_ref(),
            invocation_ctx.key(),
            &serialized_graph_ctx,
        )?;
    }

    Ok(())
}

/// Check if an allocation output can be updated in the state store.
/// Returns true if the following conditions are met:
/// - The compute graph exists.
/// - The invocation exists and is not completed.
/// - The allocation exists and is not terminal.
/// - The allocation output is not already set.
///
/// If any of these conditions are not met, it returns false.
///
/// This is used to ensure that we do not update outputs for tasks that have
/// already completed or for which the compute graph or invocation has been
/// deleted.
pub fn can_allocation_output_be_updated(
    db: Arc<RocksDBDriver>,
    req: &AllocationOutput,
) -> Result<bool> {
    let span = info_span!(
        "can_allocation_output_be_updated",
        namespace = &req.allocation.namespace,
        graph = &req.allocation.compute_graph,
        invocation_id = &req.invocation_id,
        "fn" = &req.allocation.compute_fn,
        task_id = req.allocation.function_call_id.to_string(),
    );
    let _guard = span.enter();

    // Check if the graph exists before proceeding since
    // the graph might have been deleted before the task completes
    let graph_key =
        ComputeGraph::key_from(&req.allocation.namespace, &req.allocation.compute_graph);
    let graph = db
        .get(IndexifyObjectsColumns::ComputeGraphs.as_ref(), &graph_key)
        .map_err(|e| anyhow!("failed to get compute graph: {}", e))?;
    if graph.is_none() {
        info!("Compute graph not found: {}", &req.allocation.compute_graph);
        return Ok(false);
    }

    // Check if the invocation was deleted before the task completes
    let invocation_ctx_key = GraphInvocationCtx::key_from(
        &req.allocation.namespace,
        &req.allocation.compute_graph,
        &req.invocation_id,
    );
    let invocation = db
        .get(
            IndexifyObjectsColumns::GraphInvocationCtx.as_ref(),
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
    if invocation.outcome.is_some() {
        warn!("Invocation already completed, skipping setting outputs");
        return Ok(false);
    }

    let existing_allocation = db.get(
        IndexifyObjectsColumns::Allocations.as_ref(),
        &req.allocation.key(),
    )?;
    let Some(existing_allocation) = existing_allocation else {
        info!("Allocation not found",);
        return Ok(false);
    };
    let existing_allocation = JsonEncoder::decode::<Allocation>(&existing_allocation)?;
    // idempotency check guaranteeing that we emit a finalizing state change only
    // once.
    if existing_allocation.is_terminal() {
        warn!("allocation already terminal, skipping setting outputs");
        return Ok(false);
    }

    Ok(true)
}

pub(crate) fn save_state_changes(txn: &Transaction, state_changes: &[StateChange]) -> Result<()> {
    for state_change in state_changes {
        let key = &state_change.key();
        let serialized_state_change = JsonEncoder::encode(&state_change)?;
        txn.put(
            IndexifyObjectsColumns::UnprocessedStateChanges.as_ref(),
            key,
            serialized_state_change,
        )?;
    }
    Ok(())
}

pub(crate) fn mark_state_changes_processed(
    txn: &Transaction,
    processed_state_changes: &[StateChange],
) -> Result<()> {
    for state_change in processed_state_changes {
        trace!(
            change_type = %state_change.change_type,
            "marking state change as processed"
        );
        let key = &state_change.key();
        txn.delete(
            IndexifyObjectsColumns::UnprocessedStateChanges.as_ref(),
            key,
        )?;
    }
    Ok(())
}
