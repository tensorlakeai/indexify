use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Result, anyhow};
use rocksdb::ErrorKind;
use strum::AsRefStr;
use tracing::{debug, info, trace, warn};

use super::{
    request_events::{
        PersistedRequestStateChangeEvent,
        RequestStateChangeEvent,
        RequestStateChangeEventId,
    },
    serializer::{JsonEncode, JsonEncoder},
};
use crate::{
    data_model::{
        Allocation,
        AllocationUsageEvent,
        AllocationUsageEventBuilder,
        AllocationUsageId,
        Application,
        ApplicationVersion,
        ChangeType,
        GcUrlBuilder,
        NamespaceBuilder,
        RequestCtx,
        StateChange,
    },
    state_store::{
        driver::{self, Transaction, Writer, rocksdb::RocksDBDriver},
        requests::{
            DeleteRequestRequest,
            InvokeApplicationRequest,
            NamespaceRequest,
            SchedulerUpdateRequest,
        },
    },
    utils::{TimeUnit, get_elapsed_time, get_epoch_time_in_ms},
};

#[derive(AsRefStr, strum::Display, strum::EnumIter, PartialEq, Eq)]
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

    // CF to hold the events we need to send out for observability
    RequestStateChangeEvents,
}

pub(crate) async fn upsert_namespace(
    db: Arc<RocksDBDriver>,
    req: &NamespaceRequest,
    clock: u64,
) -> Result<()> {
    let mut ns = NamespaceBuilder::default()
        .name(req.name.clone())
        .created_at(get_epoch_time_in_ms())
        .blob_storage_bucket(req.blob_storage_bucket.clone())
        .blob_storage_region(req.blob_storage_region.clone())
        .build()?;
    ns.prepare_for_persistence(clock);
    let serialized_namespace = JsonEncoder::encode(&ns)?;
    db.put(
        IndexifyObjectsColumns::Namespaces.as_ref(),
        ns.name.as_bytes(),
        &serialized_namespace,
    )
    .await?;
    info!(namespace = ns.name, "created namespace: {}", ns.name);
    Ok(())
}

#[tracing::instrument(skip_all, fields(namespace = req.namespace, application_name = req.application_name, request_id = req.ctx.request_id))]
pub async fn create_request(txn: &Transaction, req: &InvokeApplicationRequest) -> Result<()> {
    let application_key = Application::key_from(&req.namespace, &req.application_name);
    let cg = txn
        .get(
            IndexifyObjectsColumns::Applications.as_ref(),
            application_key.as_bytes(),
        )
        .await?
        .ok_or(anyhow::anyhow!("Application not found"))?;
    let app: Application = JsonEncoder::decode(&cg)?;
    if let Some(reason) = app.state.as_disabled() {
        return Err(anyhow::anyhow!("Application is not enabled: {reason}"));
    }
    if app.tombstoned {
        return Err(anyhow::anyhow!("Application is tomb-stoned"));
    }

    // In the RocksDB driver, we are using get_for_update with exclusive=True when
    // we retrieve values from the database. If the key has been read or written by
    // another transaction, RocksDB can return a `Busy` error while the key is
    // locked by that other transaction.
    //
    // The operations in this transaction handle the `Busy` error code gracefully by
    // turning it into a `RequestAlreadyExistsError`. That will inform the
    // client that the request already exists, returning a 409 Conflict.
    let request_key = req.ctx.key();
    let existing_request = txn
        .get(
            IndexifyObjectsColumns::RequestCtx.as_ref(),
            request_key.as_bytes(),
        )
        .await
        .map_err(|err| {
            check_if_key_is_busy(
                err,
                &req.namespace,
                &req.application_name,
                &req.ctx.request_id,
            )
        })?;

    if existing_request.is_some() {
        return Err(driver::Error::RequestAlreadyExists {
            namespace: req.namespace.clone(),
            application: req.application_name.clone(),
            request_id: req.ctx.request_id.clone(),
        }
        .into());
    }

    // The request context has already been prepared with clocks by
    // StateMachineUpdateRequest::prepare_for_persistence before this function is
    // called.
    txn.put(
        IndexifyObjectsColumns::RequestCtx.as_ref(),
        request_key.as_bytes(),
        &JsonEncoder::encode(&req.ctx)?,
    )
    .await
    .map_err(|err| {
        check_if_key_is_busy(
            err,
            &req.namespace,
            &req.application_name,
            &req.ctx.request_id,
        )
    })?;

    txn.put(
        IndexifyObjectsColumns::RequestCtxSecondaryIndex.as_ref(),
        &req.ctx.secondary_index_key(),
        &[],
    )
    .await?;

    info!("new request created");

    Ok(())
}

fn check_if_key_is_busy(
    err: driver::Error,
    namespace: &str,
    application: &str,
    request_id: &str,
) -> driver::Error {
    match err {
        driver::Error::RocksDBFailure { ref source } => match source {
            driver::rocksdb::Error::GenericRocksDBFailure { source }
                if source.kind() == ErrorKind::Busy =>
            {
                driver::Error::RequestAlreadyExists {
                    namespace: namespace.to_string(),
                    application: application.to_string(),
                    request_id: request_id.to_string(),
                }
            }
            _ => err,
        },
        other => other,
    }
}

pub struct AllocationUpsertResult {
    pub usage_recorded: bool,
    pub create_state_change: bool,
}

#[tracing::instrument(skip(txn, allocation, usage_event_sequence_id), fields(namespace = %allocation.namespace, app = %allocation.application, request_id = %allocation.request_id, fn_name = %allocation.function, fn_call_id = %allocation.function_call_id))]
pub(crate) async fn upsert_allocation(
    txn: &Transaction,
    allocation: &Allocation,
    usage_event_sequence_id: Option<&AtomicU64>,
    clock: u64,
) -> Result<AllocationUpsertResult> {
    let mut allocation_upsert_result = AllocationUpsertResult {
        usage_recorded: false,
        create_state_change: false,
    };
    let existing_allocation = txn
        .get(
            IndexifyObjectsColumns::Allocations.as_ref(),
            allocation.key().as_bytes(),
        )
        .await?;
    let Some(existing_allocation) = existing_allocation else {
        info!("Allocation not found",);
        return Ok(allocation_upsert_result);
    };
    let existing_allocation = JsonEncoder::decode::<Allocation>(&existing_allocation)?;

    // Idempotency check: skip if allocation is already terminal
    if existing_allocation.is_terminal() {
        warn!("allocation already terminal, skipping setting outputs");
        return Ok(allocation_upsert_result);
    }

    // Version-based optimistic locking: only update if incoming version >= existing
    // This prevents stale updates from overwriting newer ones in race conditions
    // (e.g., FE termination vs executor-reported outcome)
    if allocation.vector_clock() < existing_allocation.vector_clock() {
        warn!(
            incoming_clock = allocation.vector_clock().value(),
            existing_clock = existing_allocation.vector_clock().value(),
            "allocation has stale vector clock, skipping update"
        );
        return Ok(allocation_upsert_result);
    }

    let serialized_allocation = JsonEncoder::encode(&allocation)?;
    txn.put(
        IndexifyObjectsColumns::Allocations.as_ref(),
        allocation.key().as_bytes(),
        &serialized_allocation,
    )
    .await?;
    allocation_upsert_result.create_state_change = true;
    let Some(usage_event_sequence_id) = usage_event_sequence_id else {
        return Ok(allocation_upsert_result);
    };
    let Some(execution_duration_ms) = allocation.execution_duration_ms else {
        return Ok(allocation_upsert_result);
    };
    let mut allocation_usage = AllocationUsageEventBuilder::default()
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
    allocation_usage.prepare_for_persistence(clock);

    let serialized_usage = JsonEncoder::encode(&allocation_usage)?;
    txn.put(
        IndexifyObjectsColumns::AllocationUsage.as_ref(),
        &allocation_usage.key(),
        &serialized_usage,
    )
    .await?;

    allocation_upsert_result.create_state_change = true;
    allocation_upsert_result.usage_recorded = true;
    Ok(allocation_upsert_result)
}

#[tracing::instrument(skip_all, fields(namespace = req.namespace, application_name = req.application, request_id = req.request_id))]
pub(crate) async fn delete_request(
    txn: &Transaction,
    req: &DeleteRequestRequest,
    clock: u64,
) -> Result<()> {
    info!("deleting request");

    // Check if the request was deleted before the task completes
    let request_ctx_key = RequestCtx::key_from(&req.namespace, &req.application, &req.request_id);
    let request_ctx = txn
        .get(
            IndexifyObjectsColumns::RequestCtx.as_ref(),
            request_ctx_key.as_bytes(),
        )
        .await
        .map_err(|e| anyhow!("failed to get request: {e:?}"))?;
    let request_ctx = match request_ctx {
        Some(v) => JsonEncoder::decode::<RequestCtx>(&v)?,
        None => {
            info!(
                request_ctx_key = &request_ctx_key,
                request_id = &req.request_id,
                "request to delete not found"
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
        let mut gc_url = GcUrlBuilder::default()
            .url(payload_url)
            .namespace(req.namespace.clone())
            .build()?;
        gc_url.prepare_for_persistence(clock);
        let serialized_gc_url = JsonEncoder::encode(&gc_url)?;
        txn.put(
            IndexifyObjectsColumns::GcUrls.as_ref(),
            gc_url.key().as_bytes(),
            &serialized_gc_url,
        )
        .await?;
    }

    let allocation_prefix =
        Allocation::key_prefix_from_request(&req.namespace, &req.application, &req.request_id);
    // delete all allocations for this request
    let cf = IndexifyObjectsColumns::Allocations.as_ref();
    let iter = txn.iter(cf, allocation_prefix.into_bytes()).await;

    for kv in iter {
        let (key, value) = kv?;
        let value = JsonEncoder::decode::<Allocation>(&value)?;
        if value.request_id == req.request_id {
            info!(
                allocation_id = %value.id,
                fn_call_id = %value.function_call_id,
                "fn" = %value.function,
                "deleting allocation",
            );
            txn.delete(IndexifyObjectsColumns::Allocations.as_ref(), &key)
                .await?;
        }
    }

    // Delete Request Context
    txn.delete(
        IndexifyObjectsColumns::RequestCtx.as_ref(),
        request_ctx_key.as_bytes(),
    )
    .await?;

    // Delete Request Context Secondary Index
    txn.delete(
        IndexifyObjectsColumns::RequestCtxSecondaryIndex.as_ref(),
        &request_ctx.secondary_index_key(),
    )
    .await?;

    Ok(())
}

#[tracing::instrument(skip_all, fields(namespace = application.namespace, app = application.name, app_version = application.version))]
async fn update_requests_for_application(
    txn: &Transaction,
    application: &Application,
) -> Result<()> {
    let cg_prefix =
        RequestCtx::key_prefix_for_application(&application.namespace, &application.name);

    let mut request_ctx_to_update = HashMap::new();

    let iter = txn
        .iter(
            IndexifyObjectsColumns::RequestCtx.as_ref(),
            cg_prefix.into_bytes(),
        )
        .await;

    for kv in iter {
        let (key, val) = kv?;
        let mut request_ctx: RequestCtx = JsonEncoder::decode(&val)?;
        if request_ctx.application_version != application.version && request_ctx.outcome.is_none() {
            info!(
                request_id = request_ctx.request_id,
                app_version = request_ctx.application_version,
                "updating request_ctx for request id: {} from version: {} to
    version: {}",
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
        )
        .await?;
    }
    Ok(())
}

#[tracing::instrument(skip(txn, application), fields(namespace = application.namespace, name = application.name, app_version = application.version))]
pub(crate) async fn create_or_update_application(
    txn: &Transaction,
    application: Application,
    upgrade_existing_function_runs_to_current_version: bool,
    clock: u64,
) -> Result<()> {
    let application_key = application.key();

    let existing_application = txn
        .get(
            IndexifyObjectsColumns::Applications.as_ref(),
            application_key.as_bytes(),
        )
        .await?
        .map(|v| JsonEncoder::decode::<Application>(&v));

    let mut new_application_version = match existing_application {
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
        version = &new_application_version.version,
        "new application version"
    );
    new_application_version.prepare_for_persistence(clock);
    let serialized_application_version = JsonEncoder::encode(&new_application_version)?;
    txn.put(
        IndexifyObjectsColumns::ApplicationVersions.as_ref(),
        new_application_version.key().as_bytes(),
        &serialized_application_version,
    )
    .await?;

    let mut application_for_persistence = application.clone();
    application_for_persistence.prepare_for_persistence(clock);
    let serialized_application = JsonEncoder::encode(&application_for_persistence)?;
    txn.put(
        IndexifyObjectsColumns::Applications.as_ref(),
        application_key.as_bytes(),
        &serialized_application,
    )
    .await?;

    if upgrade_existing_function_runs_to_current_version {
        update_requests_for_application(txn, &application).await?;
    }

    info!("finished creating/updating application");
    Ok(())
}

#[tracing::instrument(skip(txn), fields(namespace = namespace, name = name))]
pub async fn delete_application(
    txn: &Transaction,
    namespace: &str,
    name: &str,
    clock: u64,
) -> Result<()> {
    info!("deleting application");
    txn.delete(
        IndexifyObjectsColumns::Applications.as_ref(),
        Application::key_from(namespace, name).as_bytes(),
    )
    .await?;

    let request_prefix = RequestCtx::key_prefix_for_application(namespace, name).into_bytes();

    for iter in txn
        .iter(IndexifyObjectsColumns::RequestCtx.as_ref(), request_prefix)
        .await
    {
        let (_key, value) = iter?;
        let value = JsonEncoder::decode::<RequestCtx>(&value)?;
        delete_request(
            txn,
            &DeleteRequestRequest {
                namespace: value.namespace,
                application: value.application_name,
                request_id: value.request_id,
            },
            clock,
        )
        .await?;
    }

    let app_version_prefix = ApplicationVersion::key_prefix_from(namespace, name).into_bytes();
    for iter in txn
        .iter(
            IndexifyObjectsColumns::ApplicationVersions.as_ref(),
            app_version_prefix,
        )
        .await
    {
        let (key, value) = iter?;
        let value = JsonEncoder::decode::<ApplicationVersion>(&value)?;

        // mark all code urls for gc.
        let mut gc_url = GcUrlBuilder::default()
            .url(value.code.path.clone())
            .namespace(namespace.to_string())
            .build()?;
        gc_url.prepare_for_persistence(clock);
        let serialized_gc_url = JsonEncoder::encode(&gc_url)?;
        txn.put(
            IndexifyObjectsColumns::GcUrls.as_ref(),
            gc_url.key().as_bytes(),
            &serialized_gc_url,
        )
        .await?;
        txn.delete(IndexifyObjectsColumns::ApplicationVersions.as_ref(), &key)
            .await?;
    }

    Ok(())
}

pub struct SchedulerUpdateResult {
    pub usage_recorded: bool,
}

pub(crate) async fn handle_scheduler_update(
    txn: &Transaction,
    request: &SchedulerUpdateRequest,
    usage_event_id_seq: Option<&AtomicU64>,
    clock: u64,
) -> Result<SchedulerUpdateResult> {
    let mut result = SchedulerUpdateResult {
        usage_recorded: false,
    };

    for alloc in &request.new_allocations {
        debug!(
            namespace = %alloc.namespace,
            app = %alloc.application,
            request_id = %alloc.request_id,
            "fn" = %alloc.function,
            fn_call_id = %alloc.function_call_id,
            allocation_id = %alloc.id,
            fn_executor_id = %alloc.target.function_executor_id,
            executor_id = %alloc.target.executor_id,
            "add_allocation",
        );
        let serialized_alloc = JsonEncoder::encode(&alloc)?;
        txn.put(
            IndexifyObjectsColumns::Allocations.as_ref(),
            alloc.key().as_bytes(),
            &serialized_alloc,
        )
        .await?;
    }

    for request_ctx in request.updated_request_states.values() {
        if let Some(outcome) = &request_ctx.outcome {
            info!(
                request_id = request_ctx.request_id.to_string(),
                namespace = request_ctx.namespace,
                app = request_ctx.application_name,
                outcome = outcome.to_string(),
                duration_sec =
                    get_elapsed_time(request_ctx.created_at.into(), TimeUnit::Milliseconds),
                "request completed"
            );
        }
        // The request context has already been prepared with clocks by
        // StateMachineUpdateRequest::prepare_for_persistence before this function is
        // called.
        let serialized_graph_ctx = JsonEncoder::encode(request_ctx)?;
        txn.put(
            IndexifyObjectsColumns::RequestCtx.as_ref(),
            request_ctx.key().as_bytes(),
            &serialized_graph_ctx,
        )
        .await?;
    }
    for alloc in &request.updated_allocations {
        let upsert_result = upsert_allocation(txn, alloc, usage_event_id_seq, clock).await?;
        if upsert_result.usage_recorded {
            result.usage_recorded = true;
        }
    }

    Ok(result)
}

pub(crate) async fn save_state_changes(
    txn: &Transaction,
    state_changes: &[StateChange],
    clock: u64,
) -> Result<()> {
    for state_change in state_changes {
        let key = &state_change.key();
        let mut state_change_for_persistence = state_change.clone();
        state_change_for_persistence.prepare_for_persistence(clock);
        let serialized_state_change = JsonEncoder::encode(&state_change_for_persistence)?;
        let cf = match &state_change.change_type {
            ChangeType::ExecutorUpserted(_) | ChangeType::TombStoneExecutor(_) => {
                IndexifyObjectsColumns::ExecutorStateChanges.as_ref()
            }
            _ => IndexifyObjectsColumns::ApplicationStateChanges.as_ref(),
        };
        txn.put(cf, key, &serialized_state_change).await?;
    }
    Ok(())
}

pub(crate) async fn mark_state_changes_processed(
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
        txn.delete(cf, key).await?;
    }
    Ok(())
}

pub(crate) async fn remove_allocation_usage_events(
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
        txn.delete(IndexifyObjectsColumns::AllocationUsage.as_ref(), key)
            .await?;
    }

    Ok(())
}

pub(crate) async fn remove_request_state_change_events(
    txn: &Transaction,
    events: &[PersistedRequestStateChangeEvent],
) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }

    if let (Some(first), Some(last)) = (events.first(), events.last()) {
        // Get the start and end keys for the range delete
        let start_key = first.key();
        let end_key = last.key();

        trace!(
            event_count = events.len(),
            ?start_key,
            ?end_key,
            "removing request state change events in range"
        );

        txn.delete_range(
            IndexifyObjectsColumns::RequestStateChangeEvents.as_ref(),
            &start_key,
            &end_key,
        )
        .await?;
    }

    Ok(())
}

/// Persist request state change events to RocksDB
pub(crate) async fn persist_request_state_change_events(
    txn: &Transaction,
    events: Vec<RequestStateChangeEvent>,
    request_event_id_seq: &AtomicU64,
) -> Result<()> {
    for event in events {
        let event_id =
            RequestStateChangeEventId::new(request_event_id_seq.fetch_add(1, Ordering::Relaxed));
        let persisted_event = PersistedRequestStateChangeEvent::new(event_id, event);
        let key = persisted_event.key();

        let serialized = JsonEncoder::encode(&persisted_event)?;
        txn.put(
            IndexifyObjectsColumns::RequestStateChangeEvents.as_ref(),
            &key,
            &serialized,
        )
        .await?;
    }

    Ok(())
}
