use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Result, anyhow};
use strum::AsRefStr;
use tracing::{debug, info, info_span, trace, warn};

use super::serializer::{JsonEncode, JsonEncoder};
use crate::{
    data_model::{
        Allocation,
        AllocationUsageEvent,
        AllocationUsageEventBuilder,
        AllocationUsageId,
        Application,
        ApplicationVersion,
        ChangeType,
        GcUrl,
        GcUrlBuilder,
        NamespaceBuilder,
        RequestCtx,
        StateChange,
    },
    state_store::{
        driver::{Transaction, Writer, rocksdb::RocksDBDriver},
        requests::{
            DeleteRequestRequest,
            InvokeApplicationRequest,
            NamespaceRequest,
            SchedulerUpdateRequest,
        },
    },
    utils::{TimeUnit, get_elapsed_time, get_epoch_time_in_ms},
};

#[derive(AsRefStr, strum::Display, strum::EnumIter)]
pub enum IndexifyObjectsColumns {
    StateMachineMetadata, //  StateMachineMetadata
    Namespaces,           //  Namespaces
    Applications,         //  Ns_ApplicationName -> Application
    /// Application versions
    ///
    /// Keys:
    /// - `<Ns>_<ApplicationName>_<Version> -> ApplicationVersion`
    ApplicationVersions, //  Ns_ApplicationName_Version -> ApplicationVersion
    RequestCtx,           //  Ns_CG_RequestId -> RequestCtx
    RequestCtxSecondaryIndex, // NS_CG_RequestId_CreatedAt -> empty

    UnprocessedStateChanges, //  StateChangeId -> StateChange
    Allocations,             // Allocation ID -> Allocation
    AllocationUsage,         // Allocation Usage ID -> Allocation Usage

    GcUrls, // List of URLs pending deletion

    Stats, // Stats

    // State changes for executors -> Upsert and Removal
    ExecutorStateChanges,

    // State changes for applications -> Request updates
    ApplicationStateChanges,
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

pub fn create_request(txn: &Transaction, req: &InvokeApplicationRequest) -> Result<()> {
    let span = info_span!(
        "create_request",
        namespace = req.namespace,
        app = req.application_name,
        request_id = req.ctx.request_id,
    );
    let _guard = span.enter();

    let application_key = Application::key_from(&req.namespace, &req.application_name);
    let cg = txn
        .get(
            IndexifyObjectsColumns::Applications.as_ref(),
            &application_key,
        )?
        .ok_or(anyhow::anyhow!("Application not found"))?;
    let app: Application = JsonEncoder::decode(&cg)?;
    if let Some(reason) = app.state.as_disabled() {
        return Err(anyhow::anyhow!("Application is not enabled: {reason}"));
    }
    if app.tombstoned {
        return Err(anyhow::anyhow!("Application is tomb-stoned"));
    }
    txn.put(
        IndexifyObjectsColumns::RequestCtx.as_ref(),
        req.ctx.key(),
        &JsonEncoder::encode(&req.ctx)?,
    )?;
    txn.put(
        IndexifyObjectsColumns::RequestCtxSecondaryIndex.as_ref(),
        req.ctx.secondary_index_key(),
        [],
    )?;

    info!(
        "created request: namespace: {}, app: {}",
        req.namespace, req.application_name
    );

    Ok(())
}

pub struct AllocationUpsertResult {
    pub usage_recorded: bool,
    pub create_state_change: bool,
}

pub(crate) fn upsert_allocation(
    txn: &Transaction,
    allocation: &Allocation,
    usage_event_sequence_id: Option<&AtomicU64>,
) -> Result<AllocationUpsertResult> {
    let span = info_span!(
        "upsert_allocation",
        namespace = &allocation.namespace,
        app = &allocation.application,
        request_id = &allocation.request_id,
        "fn" = &allocation.function,
        fn_call_id = allocation.function_call_id.to_string(),
    );
    let _guard = span.enter();

    let mut allocation_upsert_result = AllocationUpsertResult {
        usage_recorded: false,
        create_state_change: false,
    };
    let existing_allocation = txn.get(
        IndexifyObjectsColumns::Allocations.as_ref(),
        allocation.key(),
    )?;
    let Some(existing_allocation) = existing_allocation else {
        info!("Allocation not found",);
        return Ok(allocation_upsert_result);
    };
    let existing_allocation = JsonEncoder::decode::<Allocation>(&existing_allocation)?;
    // idempotency check guaranteeing that we emit a finalizing state change only
    // once.
    if existing_allocation.is_terminal() {
        warn!("allocation already terminal, skipping setting outputs");
        return Ok(allocation_upsert_result);
    }

    let serialized_allocation = JsonEncoder::encode(&allocation)?;
    txn.put(
        IndexifyObjectsColumns::Allocations.as_ref(),
        allocation.key().as_bytes(),
        &serialized_allocation,
    )?;
    allocation_upsert_result.create_state_change = true;
    let Some(usage_event_sequence_id) = usage_event_sequence_id else {
        return Ok(allocation_upsert_result);
    };
    let Some(execution_duration_ms) = allocation.execution_duration_ms else {
        return Ok(allocation_upsert_result);
    };
    let allocation_usage = AllocationUsageEventBuilder::default()
        .id(AllocationUsageId::new(
            usage_event_sequence_id.fetch_add(1, Ordering::Relaxed),
        ))
        .namespace(allocation.namespace.clone())
        .application(allocation.application.clone())
        .application_version(allocation.application_version.clone())
        .request_id(allocation.request_id.clone())
        .allocation_id(allocation.id.clone())
        .execution_duration_ms(execution_duration_ms)
        .function(allocation.function.clone())
        .build()?;

    let serialized_usage = JsonEncoder::encode(&allocation_usage)?;
    txn.put(
        IndexifyObjectsColumns::AllocationUsage.as_ref(),
        allocation_usage.key(),
        &serialized_usage,
    )?;

    allocation_upsert_result.create_state_change = true;
    allocation_upsert_result.usage_recorded = true;
    Ok(allocation_upsert_result)
}

pub(crate) fn delete_request(txn: &Transaction, req: &DeleteRequestRequest) -> Result<()> {
    let span = info_span!(
        "delete_request",
        namespace = req.namespace,
        app = req.application,
        request_id = req.request_id,
    );
    let _guard = span.enter();

    info!("Deleting request",);

    // Check if the request was deleted before the task completes
    let request_ctx_key = RequestCtx::key_from(&req.namespace, &req.application, &req.request_id);
    let request_ctx = txn
        .get(
            IndexifyObjectsColumns::RequestCtx.as_ref(),
            &request_ctx_key,
        )
        .map_err(|e| anyhow!("failed to get request: {e:?}"))?;
    let request_ctx = match request_ctx {
        Some(v) => JsonEncoder::decode::<RequestCtx>(&v)?,
        None => {
            info!(
                request_ctx_key = &request_ctx_key,
                request_id = &req.request_id,
                "Request to delete not found"
            );
            return Ok(());
        }
    };
    let mut payload_urls: HashSet<String> = HashSet::new();
    for (_, function_run) in request_ctx.function_runs.iter() {
        for input_arg in function_run.input_args.iter() {
            payload_urls.insert(input_arg.data_payload.path.clone());
        }
        for output in function_run.output.iter() {
            payload_urls.insert(output.path.clone());
        }
    }
    for payload_url in payload_urls {
        let gc_url = GcUrlBuilder::default()
            .url(payload_url)
            .namespace(req.namespace.clone())
            .build()?;
        let serialized_gc_url = JsonEncoder::encode(&gc_url)?;
        txn.put(
            IndexifyObjectsColumns::GcUrls.as_ref(),
            gc_url.key().as_bytes(),
            &serialized_gc_url,
        )?;
    }

    let allocation_prefix =
        Allocation::key_prefix_from_request(&req.namespace, &req.application, &req.request_id);
    // delete all allocations for this request
    let cf = IndexifyObjectsColumns::Allocations.as_ref();
    for iter in txn.iter(cf, allocation_prefix.as_bytes(), Default::default()) {
        let (key, value) = iter?;
        let value = JsonEncoder::decode::<Allocation>(&value)?;
        if value.request_id == req.request_id {
            info!(
                allocation_id = %value.id,
                fn_call_id = value.function_call_id.to_string(),
                "fn" = value.function,
                "deleting allocation",
            );
            txn.delete(IndexifyObjectsColumns::Allocations.as_ref(), &key)?;
        }
    }

    // Delete Request Context
    delete_cf_prefix(
        txn,
        IndexifyObjectsColumns::RequestCtx.as_ref(),
        request_ctx_key.as_bytes(),
    )?;

    // Delete Request Context Secondary Index
    txn.delete(
        IndexifyObjectsColumns::RequestCtxSecondaryIndex.as_ref(),
        request_ctx.secondary_index_key(),
    )?;

    Ok(())
}

fn update_requests_for_application(txn: &Transaction, application: &Application) -> Result<()> {
    let cg_prefix =
        RequestCtx::key_prefix_for_application(&application.namespace, &application.name);

    let span = info_span!(
        "update_requests_for_application",
        namespace = application.namespace,
        app = application.name,
        app_version = application.version,
    );
    let _guard = span.enter();

    let iter = txn.iter(
        IndexifyObjectsColumns::RequestCtx.as_ref(),
        cg_prefix.as_bytes(),
        Default::default(),
    );

    let mut request_ctx_to_update = HashMap::new();
    for kv in iter {
        let (key, val) = kv?;
        let mut request_ctx: RequestCtx = JsonEncoder::decode(&val)?;
        if request_ctx.application_version != application.version && request_ctx.outcome.is_none() {
            info!(
                request_id = request_ctx.request_id,
                app_version = request_ctx.application_version,
                "updating request_ctx for request id: {} from version: {} to version: {}",
                request_ctx.request_id,
                request_ctx.application_version,
                application.version
            );
            request_ctx.application_version = application.version.clone();
            for (_function_call_id, function_run) in request_ctx.function_runs.clone().iter_mut() {
                if function_run.version != application.version && function_run.outcome.is_none() {
                    function_run.version = application.version.clone();
                    request_ctx
                        .function_runs
                        .insert(function_run.id.clone(), function_run.clone());
                }
            }
            request_ctx_to_update.insert(key, request_ctx);
        }
    }
    info!("upgrading request ctxs: {}", request_ctx_to_update.len());
    for (request_id, request_ctx) in request_ctx_to_update {
        let serialized_task = JsonEncoder::encode(&request_ctx)?;
        txn.put(
            IndexifyObjectsColumns::RequestCtx.as_ref(),
            &request_id,
            &serialized_task,
        )?;
    }
    Ok(())
}

pub(crate) fn create_or_update_application(
    txn: &Transaction,
    application: Application,
    upgrade_existing_function_runs_to_current_version: bool,
) -> Result<()> {
    let span = info_span!(
        "create_or_update_application",
        namespace = application.namespace,
        app = application.name,
        app_version = application.version,
    );
    let _guard = span.enter();

    info!(
        "creating application: ns: {} name: {}, upgrade requests: {}",
        application.namespace, application.name, upgrade_existing_function_runs_to_current_version
    );
    let existing_application = txn
        .get(
            IndexifyObjectsColumns::Applications.as_ref(),
            application.key(),
        )?
        .map(|v| JsonEncoder::decode::<Application>(&v));

    let new_application_version = match existing_application {
        Some(Ok(mut existing_application)) => {
            existing_application.update(application.clone());
            existing_application.to_version()
        }
        Some(Err(e)) => {
            return Err(anyhow!("failed to decode existing application: {e}"));
        }
        None => application.to_version(),
    }?;
    info!(
        "new application version: {}",
        &new_application_version.version
    );
    let serialized_application_version = JsonEncoder::encode(&new_application_version)?;
    txn.put(
        IndexifyObjectsColumns::ApplicationVersions.as_ref(),
        new_application_version.key(),
        &serialized_application_version,
    )?;

    let serialized_application = JsonEncoder::encode(&application)?;
    txn.put(
        IndexifyObjectsColumns::Applications.as_ref(),
        application.key(),
        &serialized_application,
    )?;

    if upgrade_existing_function_runs_to_current_version {
        update_requests_for_application(txn, &application)?;
    }

    info!(
        "finished creating application namespace: {} name: {}, version: {}",
        application.namespace, application.name, application.version
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

pub fn delete_application(txn: &Transaction, namespace: &str, name: &str) -> Result<()> {
    let span = info_span!("delete_application", namespace = namespace, app = name);
    let _guard = span.enter();

    info!(
        "deleting application: namespace: {}, name: {}",
        namespace, name
    );
    txn.delete(
        IndexifyObjectsColumns::Applications.as_ref(),
        Application::key_from(namespace, name).as_bytes(),
    )?;

    let request_prefix = RequestCtx::key_prefix_for_application(namespace, name);

    for iter in txn.iter(
        &IndexifyObjectsColumns::RequestCtx.as_ref(),
        request_prefix.as_bytes(),
        Default::default(),
    ) {
        let (_key, value) = iter?;
        let value = JsonEncoder::decode::<RequestCtx>(&value)?;
        delete_request(
            txn,
            &DeleteRequestRequest {
                namespace: value.namespace,
                application: value.application_name,
                request_id: value.request_id,
            },
        )?;
    }

    for iter in txn.iter(
        IndexifyObjectsColumns::ApplicationVersions.as_ref(),
        ApplicationVersion::key_prefix_from(namespace, name).as_bytes(),
        Default::default(),
    ) {
        let (key, value) = iter?;
        let value = JsonEncoder::decode::<ApplicationVersion>(&value)?;

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
        txn.delete(IndexifyObjectsColumns::ApplicationVersions.as_ref(), &key)?;
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
            app = alloc.application,
            request_id = alloc.request_id,
            "fn" = alloc.function,
            fn_call_id = alloc.function_call_id.to_string(),
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

    for request_ctx in request.updated_request_states.values() {
        if request_ctx.outcome.is_some() {
            info!(
                request_id = request_ctx.request_id.to_string(),
                namespace = request_ctx.namespace,
                app = request_ctx.application_name,
                outcome = request_ctx
                    .outcome
                    .as_ref()
                    .map(|o| o.to_string())
                    .unwrap_or_default(),
                duration_sec =
                    get_elapsed_time(request_ctx.created_at.into(), TimeUnit::Milliseconds),
                "request completed"
            );
        }
        let serialized_graph_ctx = JsonEncoder::encode(&request_ctx)?;
        txn.put(
            IndexifyObjectsColumns::RequestCtx.as_ref(),
            request_ctx.key(),
            &serialized_graph_ctx,
        )?;
    }
    for alloc in &request.updated_allocations {
        upsert_allocation(txn, alloc, None)?;
    }

    Ok(())
}

pub(crate) fn save_state_changes(txn: &Transaction, state_changes: &[StateChange]) -> Result<()> {
    for state_change in state_changes {
        let key = &state_change.key();
        let serialized_state_change = JsonEncoder::encode(&state_change)?;
        let cf = match &state_change.change_type {
            ChangeType::ExecutorUpserted(_) | ChangeType::TombStoneExecutor(_) => {
                IndexifyObjectsColumns::ExecutorStateChanges.as_ref()
            }
            _ => IndexifyObjectsColumns::ApplicationStateChanges.as_ref(),
        };
        txn.put(cf, key, serialized_state_change)?;
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
        let cf = match &state_change.change_type {
            ChangeType::ExecutorUpserted(_) | ChangeType::TombStoneExecutor(_) => {
                IndexifyObjectsColumns::ExecutorStateChanges.as_ref()
            }
            _ => IndexifyObjectsColumns::ApplicationStateChanges.as_ref(),
        };
        txn.delete(cf, key)?;
    }
    Ok(())
}

pub(crate) fn remove_allocation_usage_events(
    txn: &Transaction,
    usage_events: &[AllocationUsageEvent],
) -> Result<()> {
    for usage in usage_events {
        trace!(
            allocation_id = %usage.allocation_id,
            namespace = %usage.namespace,
            application = %usage.application,
            request_id = %usage.request_id,
            "removing allocation usage event"
        );
        let key = &usage.key();
        txn.delete(IndexifyObjectsColumns::AllocationUsage.as_ref(), key)?;
    }

    Ok(())
}
