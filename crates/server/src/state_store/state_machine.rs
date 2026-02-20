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
    request_events::PersistedRequestStateChangeEvent,
    serializer::{StateStoreEncode, StateStoreEncoder},
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
        ContainerPool,
        FunctionCall,
        FunctionRun,
        GcUrlBuilder,
        NamespaceBuilder,
        PersistedRequestCtx,
        RequestCtx,
        Sandbox,
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

    // Sandboxes - Namespace|Application|SandboxId -> Sandbox
    Sandboxes,

    // Legacy Container Pools CF — kept for V13 (re-encode) and V15 (split)
    // migrations. Not used at runtime; V15 moves data to FunctionPools +
    // SandboxPools and drops this CF.
    ContainerPools,

    // Function Pools - Namespace|PoolId -> ContainerPool
    FunctionPools,

    // Sandbox Pools - Namespace|PoolId -> ContainerPool
    SandboxPools,

    // FunctionRuns - Namespace|Application|RequestId|FunctionCallId -> FunctionRun
    FunctionRuns,

    // FunctionCalls - Namespace|Application|RequestId|FunctionCallId -> FunctionCall
    FunctionCalls,
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
    let serialized_namespace = StateStoreEncoder::encode(&ns)?;
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
    let app: Application = StateStoreEncoder::decode(&cg)?;
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

    // Write PersistedRequestCtx (without embedded function_runs/function_calls)
    let persisted: PersistedRequestCtx = (&req.ctx).into();
    txn.put(
        IndexifyObjectsColumns::RequestCtx.as_ref(),
        request_key.as_bytes(),
        &StateStoreEncoder::encode(&persisted)?,
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

    // Write each FunctionRun to its own CF
    for function_run in req.ctx.function_runs.values() {
        let fr_key = function_run.key();
        txn.put(
            IndexifyObjectsColumns::FunctionRuns.as_ref(),
            fr_key.as_bytes(),
            &StateStoreEncoder::encode(function_run)?,
        )
        .await?;
    }

    // Write each FunctionCall to its own CF
    for function_call in req.ctx.function_calls.values() {
        let fc_key = FunctionCall::key_for_request(
            &req.namespace,
            &req.application_name,
            &req.ctx.request_id,
            &function_call.function_call_id,
        );
        txn.put(
            IndexifyObjectsColumns::FunctionCalls.as_ref(),
            fc_key.as_bytes(),
            &StateStoreEncoder::encode(function_call)?,
        )
        .await?;
    }

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
    let existing_allocation = StateStoreEncoder::decode::<Allocation>(&existing_allocation)?;

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

    let serialized_allocation = StateStoreEncoder::encode(&allocation)?;
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

    let serialized_usage = StateStoreEncoder::encode(&allocation_usage)?;
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
    let persisted_request_ctx = match request_ctx {
        Some(v) => StateStoreEncoder::decode::<PersistedRequestCtx>(&v)?,
        None => {
            info!(
                request_ctx_key = &request_ctx_key,
                request_id = &req.request_id,
                "request to delete not found"
            );
            return Ok(());
        }
    };

    // Scan FunctionRuns CF for this request to collect GC URLs
    let fr_prefix =
        FunctionRun::key_prefix_for_request(&req.namespace, &req.application, &req.request_id);
    let mut payload_urls: HashSet<String> = HashSet::new();
    let fr_cf = IndexifyObjectsColumns::FunctionRuns.as_ref();
    let fr_iter = txn.iter(fr_cf, fr_prefix.clone().into_bytes()).await;
    for kv in fr_iter {
        let (key, value) = kv?;
        if !key.starts_with(fr_prefix.as_bytes()) {
            break;
        }
        let function_run = StateStoreEncoder::decode::<FunctionRun>(&value)?;
        for input_arg in function_run.input_args.iter() {
            payload_urls.insert(input_arg.data_payload.path.clone());
        }
        for output in function_run.output.iter() {
            payload_urls.insert(output.path.clone());
        }
        // Delete the FunctionRun entry
        txn.delete(fr_cf, &key).await?;
    }

    for payload_url in payload_urls {
        let mut gc_url = GcUrlBuilder::default()
            .url(payload_url)
            .namespace(req.namespace.clone())
            .build()?;
        gc_url.prepare_for_persistence(clock);
        let serialized_gc_url = StateStoreEncoder::encode(&gc_url)?;
        txn.put(
            IndexifyObjectsColumns::GcUrls.as_ref(),
            gc_url.key().as_bytes(),
            &serialized_gc_url,
        )
        .await?;
    }

    // Delete all FunctionCalls for this request
    let fc_prefix =
        FunctionCall::key_prefix_for_request(&req.namespace, &req.application, &req.request_id);
    let fc_cf = IndexifyObjectsColumns::FunctionCalls.as_ref();
    let fc_iter = txn.iter(fc_cf, fc_prefix.clone().into_bytes()).await;
    for kv in fc_iter {
        let (key, _) = kv?;
        if !key.starts_with(fc_prefix.as_bytes()) {
            break;
        }
        txn.delete(fc_cf, &key).await?;
    }

    let allocation_prefix =
        Allocation::key_prefix_from_request(&req.namespace, &req.application, &req.request_id);
    // delete all allocations for this request
    let cf = IndexifyObjectsColumns::Allocations.as_ref();
    let iter = txn.iter(cf, allocation_prefix.into_bytes()).await;

    for kv in iter {
        let (key, value) = kv?;
        let value = StateStoreEncoder::decode::<Allocation>(&value)?;
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
        &persisted_request_ctx.secondary_index_key(),
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
        let mut persisted: PersistedRequestCtx = StateStoreEncoder::decode(&val)?;
        if persisted.application_version != application.version && persisted.outcome.is_none() {
            info!(
                request_id = persisted.request_id,
                app_version = persisted.application_version,
                "updating request_ctx for request id: {} from version: {} to
    version: {}",
                persisted.request_id,
                persisted.application_version,
                application.version
            );
            persisted.application_version = application.version.clone();

            // Scan FunctionRuns CF for this request and update versions
            let fr_prefix = FunctionRun::key_prefix_for_request(
                &persisted.namespace,
                &application.name,
                &persisted.request_id,
            );
            let fr_cf = IndexifyObjectsColumns::FunctionRuns.as_ref();
            let fr_iter = txn.iter(fr_cf, fr_prefix.clone().into_bytes()).await;
            for fr_kv in fr_iter {
                let (fr_key, fr_val) = fr_kv?;
                if !fr_key.starts_with(fr_prefix.as_bytes()) {
                    break;
                }
                let mut function_run: FunctionRun = StateStoreEncoder::decode(&fr_val)?;
                if function_run.version != application.version && function_run.outcome.is_none() {
                    function_run.version = application.version.clone();
                    txn.put(fr_cf, &fr_key, &StateStoreEncoder::encode(&function_run)?)
                        .await?;
                }
            }

            request_ctx_to_update.insert(key, persisted);
        }
    }

    info!("upgrading request ctxs: {}", request_ctx_to_update.len());
    for (request_id, persisted) in request_ctx_to_update {
        let serialized_task = StateStoreEncoder::encode(&persisted)?;
        txn.put(
            IndexifyObjectsColumns::RequestCtx.as_ref(),
            &request_id,
            &serialized_task,
        )
        .await?;
    }
    Ok(())
}

#[tracing::instrument(skip(txn, application, container_pools), fields(namespace = application.namespace, name = application.name, app_version = application.version))]
pub(crate) async fn create_or_update_application(
    txn: &Transaction,
    application: Application,
    upgrade_existing_function_runs_to_current_version: bool,
    container_pools: &[ContainerPool],
    clock: u64,
) -> Result<()> {
    let application_key = application.key();

    let existing_application = txn
        .get(
            IndexifyObjectsColumns::Applications.as_ref(),
            application_key.as_bytes(),
        )
        .await?
        .map(|v| StateStoreEncoder::decode::<Application>(&v));

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
    let serialized_application_version = StateStoreEncoder::encode(&new_application_version)?;
    txn.put(
        IndexifyObjectsColumns::ApplicationVersions.as_ref(),
        new_application_version.key().as_bytes(),
        &serialized_application_version,
    )
    .await?;

    let mut application_for_persistence = application.clone();
    application_for_persistence.prepare_for_persistence(clock);
    let serialized_application = StateStoreEncoder::encode(&application_for_persistence)?;
    txn.put(
        IndexifyObjectsColumns::Applications.as_ref(),
        application_key.as_bytes(),
        &serialized_application,
    )
    .await?;

    if upgrade_existing_function_runs_to_current_version {
        update_requests_for_application(txn, &application).await?;
    }

    let pool_key_prefix = format!("{}|{}|", application.namespace, application.name);
    delete_function_pools_by_prefix(txn, &pool_key_prefix).await?;

    for pool in container_pools {
        let mut pool = pool.clone();
        pool.prepare_for_persistence(clock);
        upsert_container_pool(txn, &pool, clock).await?;
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
        let value = StateStoreEncoder::decode::<PersistedRequestCtx>(&value)?;
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
        let value = StateStoreEncoder::decode::<ApplicationVersion>(&value)?;

        // mark all code urls for gc.
        if let Some(ref code) = value.code {
            let mut gc_url = GcUrlBuilder::default()
                .url(code.path.clone())
                .namespace(namespace.to_string())
                .build()?;
            gc_url.prepare_for_persistence(clock);
            let serialized_gc_url = StateStoreEncoder::encode(&gc_url)?;
            txn.put(
                IndexifyObjectsColumns::GcUrls.as_ref(),
                gc_url.key().as_bytes(),
                &serialized_gc_url,
            )
            .await?;
        }
        txn.delete(IndexifyObjectsColumns::ApplicationVersions.as_ref(), &key)
            .await?;
    }

    let pool_key_prefix = format!("{}|{}|", namespace, name);
    delete_function_pools_by_prefix(txn, &pool_key_prefix).await?;

    Ok(())
}

#[tracing::instrument(skip(txn, sandbox), fields(namespace = sandbox.namespace, sandbox_id = %sandbox.id))]
pub(crate) async fn upsert_sandbox(txn: &Transaction, sandbox: &Sandbox, clock: u64) -> Result<()> {
    let mut sandbox = sandbox.clone();
    sandbox.prepare_for_persistence(clock);
    let key = sandbox.key();
    let serialized = StateStoreEncoder::encode(&sandbox)?;
    txn.put(
        IndexifyObjectsColumns::Sandboxes.as_ref(),
        key.as_bytes(),
        &serialized,
    )
    .await?;
    debug!(
        namespace = %sandbox.namespace,
        sandbox_id = %sandbox.id,
        status = %sandbox.status,
        "upserted sandbox"
    );
    Ok(())
}

#[tracing::instrument(skip(txn, pool), fields(namespace = pool.namespace, pool_id = %pool.id))]
pub(crate) async fn upsert_container_pool(
    txn: &Transaction,
    pool: &ContainerPool,
    clock: u64,
) -> Result<()> {
    let mut pool = pool.clone();
    pool.prepare_for_persistence(clock);
    let key = pool.key().key();
    let serialized = StateStoreEncoder::encode(&pool)?;
    let cf = if pool.is_function_pool() {
        IndexifyObjectsColumns::FunctionPools
    } else {
        IndexifyObjectsColumns::SandboxPools
    };
    txn.put(cf.as_ref(), key.as_bytes(), &serialized).await?;
    debug!(
        namespace = %pool.namespace,
        pool_id = %pool.id,
        "upserted container pool"
    );
    Ok(())
}

#[tracing::instrument(skip(txn), fields(namespace = namespace, pool_id = pool_id))]
pub(crate) async fn delete_container_pool(
    txn: &Transaction,
    namespace: &str,
    pool_id: &str,
) -> Result<()> {
    let key = format!("{}|{}", namespace, pool_id);
    // Try both CFs — the pool type isn't threaded through the delete event/request,
    // and a key only exists in one CF, so the extra delete is a harmless no-op.
    txn.delete(
        IndexifyObjectsColumns::FunctionPools.as_ref(),
        key.as_bytes(),
    )
    .await?;
    txn.delete(
        IndexifyObjectsColumns::SandboxPools.as_ref(),
        key.as_bytes(),
    )
    .await?;
    debug!(namespace = %namespace, pool_id = %pool_id, "deleted container pool");
    Ok(())
}

#[tracing::instrument(skip(txn))]
pub(crate) async fn delete_function_pools_by_prefix(
    txn: &Transaction,
    key_prefix: &str,
) -> Result<()> {
    let cf = IndexifyObjectsColumns::FunctionPools.as_ref();
    let iter = txn.iter(cf, key_prefix.as_bytes().to_vec()).await;

    for kv in iter {
        let (key, _) = kv?;
        if key.starts_with(key_prefix.as_bytes()) {
            debug!(key = ?String::from_utf8_lossy(&key), "deleted container pool by prefix");
            txn.delete(cf, &key).await?;
        } else {
            break;
        }
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
            fn_executor_id = %alloc.target.container_id,
            executor_id = %alloc.target.executor_id,
            "add_allocation",
        );
        let serialized_alloc = StateStoreEncoder::encode(&alloc)?;
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
        // Write PersistedRequestCtx (without embedded function_runs/function_calls)
        let persisted: PersistedRequestCtx = request_ctx.into();
        let serialized_graph_ctx = StateStoreEncoder::encode(&persisted)?;
        txn.put(
            IndexifyObjectsColumns::RequestCtx.as_ref(),
            request_ctx.key().as_bytes(),
            &serialized_graph_ctx,
        )
        .await?;
    }

    // Write updated FunctionRuns individually
    for (ctx_key, fr_ids) in &request.updated_function_runs {
        if let Some(request_ctx) = request.updated_request_states.get(ctx_key) {
            for fr_id in fr_ids {
                if let Some(function_run) = request_ctx.function_runs.get(fr_id) {
                    let fr_key = function_run.key();
                    txn.put(
                        IndexifyObjectsColumns::FunctionRuns.as_ref(),
                        fr_key.as_bytes(),
                        &StateStoreEncoder::encode(function_run)?,
                    )
                    .await?;
                }
            }
        }
    }

    // Write updated FunctionCalls individually
    for (ctx_key, fc_ids) in &request.updated_function_calls {
        if let Some(request_ctx) = request.updated_request_states.get(ctx_key) {
            for fc_id in fc_ids {
                if let Some(function_call) = request_ctx.function_calls.get(fc_id) {
                    let fc_key = FunctionCall::key_for_request(
                        &request_ctx.namespace,
                        &request_ctx.application_name,
                        &request_ctx.request_id,
                        &function_call.function_call_id,
                    );
                    txn.put(
                        IndexifyObjectsColumns::FunctionCalls.as_ref(),
                        fc_key.as_bytes(),
                        &StateStoreEncoder::encode(function_call)?,
                    )
                    .await?;
                }
            }
        }
    }
    for alloc in &request.updated_allocations {
        let upsert_result = upsert_allocation(txn, alloc, usage_event_id_seq, clock).await?;
        if upsert_result.usage_recorded {
            result.usage_recorded = true;
        }
    }

    for sandbox in request.updated_sandboxes.values() {
        upsert_sandbox(txn, sandbox, clock).await?;
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
        let serialized_state_change = StateStoreEncoder::encode(&state_change_for_persistence)?;
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
    for event in events {
        trace!(
            event_id = %event.id,
            namespace = %event.event.namespace(),
            application = %event.event.application_name(),
            request_id = %event.event.request_id(),
            "removing request state change event"
        );
        let key = event.key();
        txn.delete(
            IndexifyObjectsColumns::RequestStateChangeEvents.as_ref(),
            &key,
        )
        .await?;
    }

    Ok(())
}

/// Persist a single request state change event to RocksDB.
/// Used by the HTTP export worker to persist events before batching.
pub(crate) async fn persist_single_request_state_change_event(
    txn: &Transaction,
    event: &PersistedRequestStateChangeEvent,
) -> Result<()> {
    let key = event.key();
    let serialized = StateStoreEncoder::encode(event)?;

    txn.put(
        IndexifyObjectsColumns::RequestStateChangeEvents.as_ref(),
        &key,
        &serialized,
    )
    .await?;

    Ok(())
}
