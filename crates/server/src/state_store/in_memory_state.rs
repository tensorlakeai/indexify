use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::Arc,
    time::Instant,
};

use anyhow::Result;
use opentelemetry::KeyValue;
use rayon::prelude::*;
use tracing::{debug, error, info};

use crate::{
    data_model::{
        Allocation,
        AllocationId,
        Application,
        ApplicationVersion,
        ContainerId,
        ContainerResources,
        ContainerServerMetadata,
        ContainerState,
        DataPayload,
        ExecutorId,
        FunctionCallId,
        FunctionRun,
        FunctionRunFailureReason,
        FunctionRunOutcome,
        FunctionRunStatus,
        Namespace,
        NamespaceBuilder,
        NetworkPolicy,
        RequestCtx,
        RequestCtxKey,
        Sandbox,
        SandboxKey,
        SandboxStatus,
    },
    state_store::{
        ExecutorCatalog,
        executor_watches::ExecutorWatch,
        in_memory_metrics::InMemoryStoreMetrics,
        requests::RequestPayload,
        scanner::StateReader,
        state_machine::IndexifyObjectsColumns,
    },
    utils::{TimeUnit, get_elapsed_time, get_epoch_time_in_ms},
};

pub struct DesiredStateFunctionExecutor {
    pub function_executor: Box<ContainerServerMetadata>,
    pub resources: ContainerResources,
    pub secret_names: Vec<String>,
    pub initialization_timeout_ms: u32,
    pub code_payload: Option<DataPayload>,
    pub image: Option<String>,
    pub allocation_timeout_ms: u32,
    pub sandbox_timeout_secs: u64,
    pub entrypoint: Vec<String>,
    pub network_policy: Option<NetworkPolicy>,
}

pub struct FunctionCallOutcome {
    pub namespace: String,
    pub request_id: String,
    pub function_call_id: FunctionCallId,
    pub outcome: FunctionRunOutcome,
    pub failure_reason: Option<FunctionRunFailureReason>,
    pub return_value: Option<DataPayload>,
    pub request_error: Option<DataPayload>,
}

pub struct DesiredExecutorState {
    #[allow(clippy::vec_box)]
    pub function_executors: Vec<Box<DesiredStateFunctionExecutor>>,
    #[allow(clippy::box_collection)]
    pub function_run_allocations: std::collections::HashMap<ContainerId, Vec<Allocation>>,
    pub function_call_outcomes: Vec<FunctionCallOutcome>,
    pub clock: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FunctionRunKey(String);

impl From<&ExecutorWatch> for FunctionRunKey {
    fn from(executor_watch: &ExecutorWatch) -> Self {
        FunctionRunKey(format!(
            "{}|{}|{}|{}",
            executor_watch.namespace,
            executor_watch.application,
            executor_watch.request_id,
            executor_watch.function_call_id
        ))
    }
}

impl From<&FunctionRun> for FunctionRunKey {
    fn from(function_run: &FunctionRun) -> Self {
        FunctionRunKey(function_run.key())
    }
}

impl From<Box<FunctionRun>> for FunctionRunKey {
    fn from(function_run: Box<FunctionRun>) -> Self {
        FunctionRunKey(function_run.key())
    }
}

impl From<&Box<FunctionRun>> for FunctionRunKey {
    fn from(function_run: &Box<FunctionRun>) -> Self {
        FunctionRunKey(function_run.key())
    }
}

impl From<&Allocation> for FunctionRunKey {
    fn from(allocation: &Allocation) -> Self {
        FunctionRunKey(format!(
            "{}|{}|{}|{}",
            allocation.namespace,
            allocation.application,
            allocation.request_id,
            allocation.function_call_id
        ))
    }
}

impl From<&Box<Allocation>> for FunctionRunKey {
    fn from(allocation: &Box<Allocation>) -> Self {
        FunctionRunKey(format!(
            "{}|{}|{}|{}",
            allocation.namespace,
            allocation.application,
            allocation.request_id,
            allocation.function_call_id
        ))
    }
}

impl Display for FunctionRunKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct InMemoryState {
    // clock is the value of the state_id this in-memory state is at.
    pub clock: u64,

    pub namespaces: imbl::HashMap<String, Box<Namespace>>,

    // Namespace|CG Name -> ComputeGraph
    pub applications: imbl::HashMap<String, Box<Application>>,

    // Namespace|CG Name|Version -> ComputeGraph
    pub application_versions: imbl::OrdMap<String, Box<ApplicationVersion>>,

    // ExecutorId -> (FE ID -> Map of AllocationId -> Allocation)
    pub allocations_by_executor:
        imbl::HashMap<ExecutorId, HashMap<ContainerId, HashMap<AllocationId, Box<Allocation>>>>,

    // TaskKey -> Task
    pub unallocated_function_runs: imbl::OrdSet<FunctionRunKey>,

    // Function Run Key -> Function Run
    pub function_runs: imbl::OrdMap<FunctionRunKey, Box<FunctionRun>>,

    // Request Context
    pub request_ctx: imbl::OrdMap<RequestCtxKey, Box<RequestCtx>>,

    // Configured executor label sets
    pub executor_catalog: ExecutorCatalog,

    // Index: Executor Catalog Entry Name -> Set of Function Run Keys
    // Maps executor catalog entry names to function runs that match those catalog entry labels
    pub function_runs_by_catalog_entry: imbl::HashMap<String, imbl::OrdSet<FunctionRunKey>>,

    // Sandboxes - SandboxKey -> Sandbox
    pub sandboxes: imbl::OrdMap<SandboxKey, Box<Sandbox>>,

    // Pending sandboxes waiting for executor allocation
    pub pending_sandboxes: imbl::OrdSet<SandboxKey>,

    // Histogram metrics for task latency measurements for direct recording
    metrics: InMemoryStoreMetrics,
}

impl InMemoryState {
    pub async fn new(
        clock: u64,
        reader: StateReader,
        executor_catalog: ExecutorCatalog,
    ) -> Result<Self> {
        info!(clock, "initializing in-memory state from state store",);
        let start_time = Instant::now();

        let metrics = InMemoryStoreMetrics::new();

        // Execute all independent database queries in parallel.
        let (
            all_ns,
            all_apps,
            all_cg_versions,
            allocations_result,
            all_app_request_ctx,
            all_sandboxes,
        ) = tokio::join!(
            reader.get_all_namespaces(),
            reader.get_all_rows_from_cf::<Application>(IndexifyObjectsColumns::Applications),
            reader.get_all_rows_from_cf::<ApplicationVersion>(
                IndexifyObjectsColumns::ApplicationVersions
            ),
            reader.get_rows_from_cf_with_limits::<Allocation>(
                &[],
                None,
                IndexifyObjectsColumns::Allocations,
                None,
            ),
            reader.get_all_rows_from_cf::<RequestCtx>(IndexifyObjectsColumns::RequestCtx),
            reader.get_all_rows_from_cf::<Sandbox>(IndexifyObjectsColumns::Sandboxes),
        );

        // Unwrap all results
        let all_ns = all_ns?;
        let all_apps: Vec<(String, Application)> = all_apps?;
        let all_cg_versions: Vec<(String, ApplicationVersion)> = all_cg_versions?;
        let (allocations, _) = allocations_result?;
        let all_app_request_ctx: Vec<(String, RequestCtx)> = all_app_request_ctx?;
        let all_sandboxes: Vec<(String, Sandbox)> = all_sandboxes?;

        debug!(
            duration = get_elapsed_time(start_time.elapsed().as_millis(), TimeUnit::Milliseconds),
            "loaded in-memory initialization data from database",
        );

        let mut namespaces = imbl::HashMap::new();
        for ns in all_ns {
            namespaces.insert(ns.name.clone(), Box::new(ns));
        }

        let mut applications = imbl::HashMap::new();
        for (_, app) in all_apps {
            applications.insert(app.key(), Box::new(app));
        }

        let mut application_versions = imbl::OrdMap::new();
        for (id, cg) in all_cg_versions {
            application_versions.insert(id, Box::new(cg));
        }

        let allocations_grouped: Vec<_> = allocations
            .par_iter()
            .filter(|allocation| !allocation.is_terminal())
            .map(|allocation| {
                (
                    allocation.target.executor_id.clone(),
                    allocation.target.function_executor_id.clone(),
                    allocation.id.clone(),
                    allocation.clone(),
                )
            })
            .collect();

        let mut allocations_by_executor: imbl::HashMap<
            ExecutorId,
            HashMap<ContainerId, HashMap<AllocationId, Box<Allocation>>>,
        > = imbl::HashMap::new();
        for (executor_id, fe_id, alloc_id, allocation) in allocations_grouped {
            allocations_by_executor
                .entry(executor_id)
                .or_default()
                .entry(fe_id)
                .or_default()
                .insert(alloc_id, Box::new(allocation));
        }

        let request_ctx_filtered: Vec<_> = all_app_request_ctx
            .par_iter()
            .filter(|(_id, ctx)| ctx.outcome.is_none())
            .map(|(_id, ctx)| (ctx.key().into(), Box::new(ctx.clone())))
            .collect();

        let mut request_ctx = imbl::OrdMap::new();
        for (key, ctx) in request_ctx_filtered {
            request_ctx.insert(key, ctx);
        }

        let mut function_runs = imbl::OrdMap::new();
        let mut unallocated_function_runs = imbl::OrdSet::new();
        for ctx in request_ctx.values() {
            for function_run in ctx.function_runs.values() {
                if function_run.status == FunctionRunStatus::Pending {
                    unallocated_function_runs.insert(function_run.into());
                }
                function_runs.insert(function_run.into(), Box::new(function_run.clone()));
            }
        }

        let function_runs_by_catalog_entry: imbl::HashMap<String, imbl::OrdSet<FunctionRunKey>> =
            imbl::HashMap::new();

        let sandboxes_filtered: Vec<_> = all_sandboxes
            .par_iter()
            .filter(|(_key, sandbox)| sandbox.status != SandboxStatus::Terminated)
            .map(|(_key, sandbox)| {
                let sandbox_key = SandboxKey::from(sandbox);
                let is_pending = sandbox.status == SandboxStatus::Pending;
                (sandbox_key, Box::new(sandbox.clone()), is_pending)
            })
            .collect();

        let mut sandboxes = imbl::OrdMap::new();
        let mut pending_sandboxes = imbl::OrdSet::new();
        for (sandbox_key, sandbox, is_pending) in sandboxes_filtered {
            if is_pending {
                pending_sandboxes.insert(sandbox_key.clone());
            }
            sandboxes.insert(sandbox_key, sandbox);
        }

        let mut in_memory_state = Self {
            clock,
            namespaces,
            applications,
            application_versions,
            function_runs,
            unallocated_function_runs,
            request_ctx,
            allocations_by_executor,
            executor_catalog,
            function_runs_by_catalog_entry,
            sandboxes,
            pending_sandboxes,
            metrics,
        };

        // Populate the catalog index for all existing function runs in parallel.
        let function_runs_to_index: Vec<_> =
            in_memory_state.function_runs.values().cloned().collect();

        let catalog_entries = in_memory_state.executor_catalog.entries.clone();
        let application_versions = in_memory_state.application_versions.clone();

        // Parallel computation: for each function run, determine which catalog entries
        // match
        let catalog_matches: Vec<_> = function_runs_to_index
            .par_iter()
            .filter_map(|function_run| {
                // Get the application version to check placement constraints
                let app_version = application_versions
                    .get(&function_run.key_application_version(&function_run.application))?;

                let function = app_version.functions.get(&function_run.name)?;

                // Find all catalog entries that match this function run
                let matching_entries: Vec<String> = catalog_entries
                    .iter()
                    .filter(|catalog_entry| {
                        function
                            .placement_constraints
                            .matches(&catalog_entry.labels) &&
                            function
                                .resources
                                .can_be_handled_by_catalog_entry(catalog_entry)
                    })
                    .map(|entry| entry.name.clone())
                    .collect();

                if matching_entries.is_empty() {
                    None
                } else {
                    Some((FunctionRunKey::from(&**function_run), matching_entries))
                }
            })
            .collect();

        // Merge the results into the catalog index
        for (function_run_key, catalog_entry_names) in catalog_matches {
            for catalog_entry_name in catalog_entry_names {
                in_memory_state
                    .function_runs_by_catalog_entry
                    .entry(catalog_entry_name)
                    .or_default()
                    .insert(function_run_key.clone());
            }
        }

        info!(
            clock,
            duration = get_elapsed_time(start_time.elapsed().as_millis(), TimeUnit::Milliseconds),
            "completed in-memory state initialization from state store",
        );
        Ok(in_memory_state)
    }

    pub fn update_state(
        &mut self,
        new_clock: u64,
        state_machine_update_request: &RequestPayload,
        _ctx: &str,
    ) -> Result<HashSet<ExecutorId>> {
        // keep track of what clock we are at for this update state
        self.clock = new_clock;

        // Collect all executors that are being changed to notify them.
        let mut changed_executors = HashSet::new();

        match state_machine_update_request {
            RequestPayload::InvokeApplication(req) => {
                self.request_ctx
                    .insert(req.ctx.key().into(), Box::new(req.ctx.clone()));
                for function_run in req.ctx.function_runs.values() {
                    self.function_runs
                        .insert(function_run.into(), Box::new(function_run.clone()));
                    self.unallocated_function_runs.insert(function_run.into());
                    // Index the function run by catalog entry
                    self.index_function_run_by_catalog(function_run);
                }
            }
            RequestPayload::CreateNameSpace(req) => {
                // If the namespace already exists, get its created_at time
                let created_at = if let Some(existing_ns) = self.namespaces.get(&req.name) {
                    existing_ns.created_at
                } else {
                    get_epoch_time_in_ms()
                };
                self.namespaces.insert(
                    req.name.clone(),
                    Box::new(
                        NamespaceBuilder::default()
                            .name(req.name.clone())
                            .created_at(created_at)
                            .blob_storage_bucket(req.blob_storage_bucket.clone())
                            .blob_storage_region(req.blob_storage_region.clone())
                            .build()?,
                    ),
                );
            }
            RequestPayload::CreateOrUpdateApplication(req) => {
                self.applications
                    .insert(req.application.key(), Box::new(req.application.clone()));
                let req_version = req.application.to_version()?;
                let version = req_version.version.clone();

                self.application_versions
                    .insert(req_version.key(), Box::new(req_version));

                // FIXME - we should set this in the API and not here, so that these things are
                // not set in the state store
                if req.upgrade_requests_to_current_version {
                    // Update request ctxs and function runs
                    {
                        let mut request_ctx_to_update = vec![];
                        let request_ctx_key_prefix = RequestCtx::key_prefix_for_application(
                            &req.namespace,
                            &req.application.name,
                        );
                        self.request_ctx
                            .range::<std::ops::RangeFrom<RequestCtxKey>, RequestCtxKey>(
                                request_ctx_key_prefix.clone().into()..,
                            )
                            .take_while(|(k, _v)| k.0.starts_with(&request_ctx_key_prefix))
                            .for_each(|(_k, v)| {
                                let mut ctx = v.clone();
                                ctx.application_version = version.clone();
                                request_ctx_to_update.push(ctx);
                            });

                        for ctx in request_ctx_to_update.iter_mut() {
                            for (_function_call_id, function_run) in
                                ctx.function_runs.clone().iter_mut()
                            {
                                if function_run.version != version {
                                    function_run.version = version.clone();
                                    ctx.function_runs
                                        .insert(function_run.id.clone(), function_run.clone());
                                }
                                let _ = self
                                    .function_runs
                                    .entry(FunctionRunKey(function_run.key()))
                                    .and_modify(|existing_function_run| {
                                        **existing_function_run = function_run.clone();
                                    });
                            }
                        }

                        for ctx in request_ctx_to_update {
                            self.request_ctx.insert(ctx.key().into(), ctx);
                        }
                    }
                }
            }
            RequestPayload::DeleteRequestRequest((req, _)) => {
                self.delete_request(&req.namespace, &req.application, &req.request_id);
            }
            RequestPayload::DeleteApplicationRequest((req, _)) => {
                // Remove app
                let key = Application::key_from(&req.namespace, &req.name);
                self.applications.remove(&key);

                // Remove app versions
                {
                    let version_key_prefix =
                        ApplicationVersion::key_prefix_from(&req.namespace, &req.name);
                    let keys_to_remove = self
                        .application_versions
                        .range(version_key_prefix.clone()..)
                        .take_while(|(k, _v)| k.starts_with(&version_key_prefix))
                        .map(|(k, _v)| k.clone())
                        .collect::<Vec<String>>();
                    for k in keys_to_remove {
                        self.application_versions.remove(&k);
                    }
                }

                // Remove request contexts
                {
                    let request_key_prefix =
                        RequestCtx::key_prefix_for_application(&req.namespace, &req.name);
                    let requests_to_remove = self
                        .request_ctx
                        .range::<std::ops::RangeFrom<RequestCtxKey>, RequestCtxKey>(
                            request_key_prefix.clone().into()..,
                        )
                        .take_while(|(k, _v)| k.0.starts_with(&request_key_prefix))
                        .map(|(_k, v)| v.request_id.clone())
                        .collect::<Vec<String>>();
                    for k in requests_to_remove {
                        self.delete_request(&req.namespace, &req.name, &k);
                    }
                }
            }
            RequestPayload::SchedulerUpdate((req, _)) => {
                // Note: Allocations are removed from allocations_by_executor in two places:
                // 1. UpsertExecutor handler - when allocation output is ingested
                // 2. remove_function_executors handling below - when FE is terminated
                // So we don't need to remove updated_allocations here.

                // Track executors with updated allocations for notification
                for allocation in &req.updated_allocations {
                    changed_executors.insert(allocation.target.executor_id.clone());
                }

                {
                    let start_time = Instant::now();
                    for (ctx_key, function_call_ids) in &req.updated_function_runs {
                        for function_call_id in function_call_ids {
                            let Some(ctx) = req.updated_request_states.get(ctx_key) else {
                                error!(
                                    ctx_key = ctx_key,
                                    "request ctx not found for updated function runs"
                                );
                                continue;
                            };
                            let Some(function_run) = ctx.function_runs.get(function_call_id) else {
                                error!(
                                    ctx_key = ctx_key,
                                    fn_call_id = %function_call_id,
                                    "function run not found for updated function runs"
                                );
                                continue;
                            };
                            if function_run.status == FunctionRunStatus::Pending {
                                self.unallocated_function_runs.insert(function_run.into());
                            } else {
                                self.unallocated_function_runs.remove(&function_run.into());
                            }
                            self.function_runs
                                .insert(function_run.into(), Box::new(function_run.clone()));
                            // Index the function run by catalog entry
                            self.index_function_run_by_catalog(function_run);
                        }
                    }
                    // record the time instead of using a timer because we cannot
                    // borrow the metric as immutable and borrow self as mutable inside the loop.
                    self.metrics
                        .scheduler_update_index_function_run_by_catalog
                        .record(start_time.elapsed().as_secs_f64(), &[]);
                }

                {
                    let start_time = Instant::now();
                    for (key, request_ctx) in &req.updated_request_states {
                        // Remove function runs for request ctx if completed
                        if request_ctx.outcome.is_some() {
                            self.delete_request(
                                &request_ctx.namespace,
                                &request_ctx.application_name,
                                &request_ctx.request_id,
                            );
                        } else {
                            self.request_ctx
                                .insert(key.into(), Box::new(request_ctx.clone()));
                        }
                    }
                    // record the time instead of using a timer because we cannot
                    // borrow the metric as immutable and borrow self as mutable inside the loop.
                    self.metrics
                        .scheduler_update_delete_requests
                        .record(start_time.elapsed().as_secs_f64(), &[]);
                }
                {
                    let start_time = Instant::now();
                    for allocation in &req.new_allocations {
                        if let Some(function_run) = self.function_runs.get(&allocation.into()) {
                            self.unallocated_function_runs.remove(&function_run.into());

                            self.allocations_by_executor
                                .entry(allocation.target.executor_id.clone())
                                .or_default()
                                .entry(allocation.target.function_executor_id.clone())
                                .or_default()
                                .insert(allocation.id.clone(), Box::new(allocation.clone()));

                            // Record metrics
                            self.metrics.function_run_pending_latency.record(
                                get_elapsed_time(
                                    function_run.creation_time_ns,
                                    TimeUnit::Nanoseconds,
                                ),
                                &[],
                            );

                            // Executor has a new allocation
                            changed_executors.insert(allocation.target.executor_id.clone());
                        } else {
                            error!(
                                namespace = &allocation.namespace,
                                app = &allocation.application,
                                "fn" = &allocation.function,
                                executor_id = allocation.target.executor_id.get(),
                                allocation_id = %allocation.id,
                                request_id = &allocation.request_id,
                                fn_call_id = allocation.function_call_id.to_string(),
                                "function run not found for new allocation"
                            );
                        }
                    }
                    // record the time instead of using a timer because we cannot
                    // borrow the metric as immutable and borrow self as mutable inside the loop.
                    self.metrics
                        .scheduler_update_insert_new_allocations
                        .record(start_time.elapsed().as_secs_f64(), &[]);
                }
                {
                    let start_time = Instant::now();
                    for executor_id in &req.remove_executors {
                        self.allocations_by_executor.remove(executor_id);
                        changed_executors.insert(executor_id.clone());
                    }
                    // record the time instead of using a timer because we cannot
                    // borrow the metric as immutable and borrow self as mutable inside the loop.
                    self.metrics
                        .scheduler_update_remove_executors
                        .record(start_time.elapsed().as_secs_f64(), &[]);
                }

                for (sandbox_key, sandbox) in &req.updated_sandboxes {
                    match sandbox.status {
                        SandboxStatus::Pending => {
                            self.sandboxes
                                .insert(sandbox_key.clone(), Box::new(sandbox.clone()));
                            self.pending_sandboxes.insert(sandbox_key.clone());
                        }
                        SandboxStatus::Running => {
                            self.sandboxes
                                .insert(sandbox_key.clone(), Box::new(sandbox.clone()));
                            self.pending_sandboxes.remove(sandbox_key);
                        }
                        SandboxStatus::Terminated => {
                            self.sandboxes.remove(sandbox_key);
                            self.pending_sandboxes.remove(sandbox_key);
                        }
                    }
                }

                for fc_metadata in req.containers.values() {
                    if matches!(
                        fc_metadata.function_container.state,
                        ContainerState::Pending
                    ) {
                        changed_executors.insert(fc_metadata.executor_id.clone());
                    }
                }
            }
            RequestPayload::UpsertExecutor(req) => {
                for allocation_output in &req.allocation_outputs {
                    // Remove the allocation
                    {
                        let _ = self
                            .allocations_by_executor
                            .entry(allocation_output.executor_id.clone())
                            .and_modify(|fe_allocations| {
                                if let Some(allocations) = fe_allocations.get_mut(
                                    &allocation_output.allocation.target.function_executor_id,
                                ) && let Some(existing_allocation) =
                                    allocations.remove(&allocation_output.allocation.id)
                                {
                                    // Record metrics
                                    self.metrics.allocation_running_latency.record(
                                        get_elapsed_time(
                                            existing_allocation.created_at,
                                            TimeUnit::Milliseconds,
                                        ),
                                        &[KeyValue::new(
                                            "outcome",
                                            allocation_output.allocation.outcome.to_string(),
                                        )],
                                    );
                                }

                                // Remove the function if no allocations left
                                fe_allocations.retain(|_, f| !f.is_empty());
                            });

                        // Executor's allocation is removed
                        changed_executors.insert(allocation_output.executor_id.clone());
                    }

                    // Record metrics
                    self.metrics.allocation_completion_latency.record(
                        get_elapsed_time(
                            allocation_output.allocation.created_at,
                            TimeUnit::Milliseconds,
                        ),
                        &[KeyValue::new(
                            "outcome",
                            allocation_output.allocation.outcome.to_string(),
                        )],
                    );
                }
            }
            RequestPayload::CreateSandbox(req) => {
                let sandbox_key = SandboxKey::from(&req.sandbox);
                self.sandboxes
                    .insert(sandbox_key.clone(), Box::new(req.sandbox.clone()));
                if req.sandbox.status == SandboxStatus::Pending {
                    self.pending_sandboxes.insert(sandbox_key);
                }
            }
            _ => {}
        }

        Ok(changed_executors)
    }

    pub fn delete_function_runs(&mut self, function_runs: Vec<FunctionRun>) {
        for function_run in function_runs.iter() {
            self.function_runs.remove(&function_run.into());
            self.unallocated_function_runs.remove(&function_run.into());
            // Remove from catalog entry index
            self.unindex_function_run_from_catalog(function_run);
        }

        for (_executor, allocations_by_fe) in self.allocations_by_executor.iter_mut() {
            for (_fe_id, allocations) in allocations_by_fe.iter_mut() {
                allocations.retain(|_, allocation| {
                    !function_runs
                        .iter()
                        .any(|function_run| function_run.id == allocation.function_call_id)
                });
            }
        }
    }

    pub fn delete_request(&mut self, namespace: &str, application: &str, request_id: &str) {
        // Remove request ctx
        self.request_ctx
            .remove(&RequestCtx::key_from(namespace, application, request_id).into());

        // Remove tasks
        let key_prefix = FunctionRun::key_prefix_for_request(namespace, application, request_id);
        let mut function_runs_to_remove = Vec::new();
        self.function_runs
            .range(FunctionRunKey(key_prefix.clone())..)
            .take_while(|(k, _v)| k.0.starts_with(&key_prefix))
            .for_each(|(_k, v)| {
                function_runs_to_remove.push(*v.clone());
            });
        self.delete_function_runs(function_runs_to_remove);
    }

    pub fn unallocated_function_runs(&self) -> Vec<FunctionRun> {
        let unallocated_function_run_keys = self
            .unallocated_function_runs
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let mut function_runs = Vec::new();
        for function_run_key in unallocated_function_run_keys {
            if let Some(function_run) = self.function_runs.get(&function_run_key) {
                function_runs.push(*function_run.clone());
            } else {
                error!(
                    function_run_key = function_run_key.0,
                    "function run not found for unallocated function run"
                );
            }
        }
        function_runs
    }

    pub fn clone(&self) -> Arc<tokio::sync::RwLock<Self>> {
        Arc::new(tokio::sync::RwLock::new(InMemoryState {
            clock: self.clock,
            namespaces: self.namespaces.clone(),
            applications: self.applications.clone(),
            application_versions: self.application_versions.clone(),
            request_ctx: self.request_ctx.clone(),
            allocations_by_executor: self.allocations_by_executor.clone(),
            executor_catalog: self.executor_catalog.clone(),
            function_runs_by_catalog_entry: self.function_runs_by_catalog_entry.clone(),
            metrics: self.metrics.clone(),
            function_runs: self.function_runs.clone(),
            unallocated_function_runs: self.unallocated_function_runs.clone(),
            sandboxes: self.sandboxes.clone(),
            pending_sandboxes: self.pending_sandboxes.clone(),
        }))
    }

    #[allow(clippy::borrowed_box)]
    pub fn application_version<'a>(
        &'a self,
        namespace: &str,
        application_name: &str,
        version: &str,
    ) -> Option<&'a Box<ApplicationVersion>> {
        self.application_versions
            .get(&ApplicationVersion::key_from(
                namespace,
                application_name,
                version,
            ))
            .or_else(|| {
                error!(
                    namespace = namespace,
                    app = application_name,
                    app_version = version,
                    "application version not found",
                );
                None
            })
    }

    #[allow(clippy::borrowed_box)]
    pub fn get_existing_application_version<'a>(
        &'a self,
        function_run: &FunctionRun,
    ) -> Option<&'a Box<ApplicationVersion>> {
        self.application_versions
            .get(&function_run.key_application_version(&function_run.application))
            .or_else(|| {
                error!(
                    fn_call_id = function_run.id.to_string(),
                    request_id = function_run.request_id.to_string(),
                    namespace = function_run.namespace,
                    app = function_run.application,
                    "fn" = function_run.name,
                    app_version = function_run.version,
                    "application version not found",
                );
                None
            })
    }

    /// Add a function run to the catalog entry index if it matches any catalog
    /// entry labels and resources
    fn index_function_run_by_catalog(&mut self, function_run: &FunctionRun) {
        // Get the application version to check placement constraints
        let Some(app_version) = self.get_existing_application_version(function_run) else {
            debug!(
                fn_call_id = function_run.id.to_string(),
                "Skipping catalog indexing: application version not found"
            );
            return;
        };

        let Some(function) = app_version.functions.get(&function_run.name).cloned() else {
            debug!(
                fn_call_id = function_run.id.to_string(),
                fn_name = &function_run.name,
                "Skipping catalog indexing: function not found"
            );
            return;
        };

        // Clone catalog entries to avoid borrow issues
        let catalog_entries = self.executor_catalog.entries.clone();

        // Check each catalog entry to see if this function run matches
        for catalog_entry in &catalog_entries {
            // Check if the function's placement constraints match this catalog entry's
            // labels
            if !function
                .placement_constraints
                .matches(&catalog_entry.labels)
            {
                continue;
            }

            // Check if the catalog entry has sufficient resources for this function
            if !function
                .resources
                .can_be_handled_by_catalog_entry(catalog_entry)
            {
                continue;
            }

            // Both labels and resources match - add to index
            self.function_runs_by_catalog_entry
                .entry(catalog_entry.name.clone())
                .or_default()
                .insert(function_run.into());
        }
    }

    /// Remove a function run from the catalog entry index
    fn unindex_function_run_from_catalog(&mut self, function_run: &FunctionRun) {
        let function_run_key: FunctionRunKey = function_run.into();

        // Remove from all catalog entries
        for catalog_entry in &self.executor_catalog.entries {
            let _ = self
                .function_runs_by_catalog_entry
                .entry(catalog_entry.name.clone())
                .and_modify(|run_keys| {
                    run_keys.remove(&function_run_key);
                });
        }
    }

    /// Get all function runs that match a specific catalog entry by name
    #[cfg(test)]
    fn function_runs_for_catalog_entry(&self, catalog_entry_name: &str) -> Vec<FunctionRun> {
        let Some(run_keys) = self.function_runs_by_catalog_entry.get(catalog_entry_name) else {
            return Vec::new();
        };

        let mut results = Vec::new();
        for run_key in run_keys.iter() {
            if let Some(function_run) = self.function_runs.get(run_key) {
                results.push(*function_run.clone());
            }
        }
        results
    }

    /// Get all function runs grouped by catalog entry
    #[cfg(test)]
    fn all_function_runs_by_catalog_entry(&self) -> HashMap<String, Vec<FunctionRun>> {
        let mut result = HashMap::new();

        for (catalog_entry_name, run_keys) in self.function_runs_by_catalog_entry.iter() {
            let mut function_runs = Vec::new();
            for run_key in run_keys.iter() {
                if let Some(function_run) = self.function_runs.get(run_key) {
                    function_runs.push(*function_run.clone());
                }
            }
            if !function_runs.is_empty() {
                result.insert(catalog_entry_name.clone(), function_runs);
            }
        }

        result
    }

    /// Simulates a server restart by clearing executor-related in-memory state
    /// while preserving allocations. This creates the scenario where:
    /// - allocations_by_executor has allocations (loaded from DB)
    /// - executors and executor_states are empty (executors haven't
    ///   re-registered)
    #[cfg(test)]
    pub fn simulate_server_restart_clear_executor_state(&mut self) {
        //self.executors.clear();
        //self.executor_states.clear();
        //self.function_executors_by_fn_uri.clear();
        // Note: allocations_by_executor is intentionally NOT cleared
        // as allocations are persisted and loaded from DB on restart
    }
}

#[cfg(test)]
mod test_helpers {
    use super::*;
    /// Macro to easily bootstrap an InMemoryState for tests.
    ///
    /// Usage:
    /// ```
    /// let state = in_memory_state_bootstrap! { clock: 42, tasks: my_tasks };
    /// ```
    /// You can specify any subset of fields; the rest will be defaulted.
    #[macro_export]
    macro_rules! in_memory_state_bootstrap {
        ( $($field:ident : $value:expr),* $(,)? ) => {{
            let mut state = super::InMemoryState::default();
            $( state.$field = $value; )*
            state
        }};
    }

    impl Default for InMemoryState {
        fn default() -> Self {
            Self {
                clock: 0,
                namespaces: imbl::HashMap::new(),
                applications: imbl::HashMap::new(),
                application_versions: imbl::OrdMap::new(),
                //executors: imbl::HashMap::new(),
                //executor_states: imbl::HashMap::new(),
                //function_executors_by_fn_uri: imbl::HashMap::new(),
                allocations_by_executor: imbl::HashMap::new(),
                request_ctx: imbl::OrdMap::new(),
                executor_catalog: ExecutorCatalog::default(),
                function_runs_by_catalog_entry: imbl::HashMap::new(),
                metrics: InMemoryStoreMetrics::new(),
                function_runs: imbl::OrdMap::new(),
                unallocated_function_runs: imbl::OrdSet::new(),
                sandboxes: imbl::OrdMap::new(),
                pending_sandboxes: imbl::OrdSet::new(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        config::GpuModel,
        data_model::{
            ComputeOp,
            FunctionCall,
            FunctionCallId,
            FunctionRun,
            FunctionRunBuilder,
            FunctionRunStatus,
        },
        state_store::in_memory_state::FunctionRunKey,
    };

    #[test]
    fn test_function_run_key_ordering() {
        {
            let fn_run_key1 = FunctionRunKey("task1".to_string());
            let fn_run_key2 = FunctionRunKey("task2".to_string());
            let fn_run_key3 = FunctionRunKey("task3".to_string());
            let fn_run_key4 = FunctionRunKey("task4".to_string());
            let fn_run_key5 = FunctionRunKey("task5".to_string());

            assert!(fn_run_key1 < fn_run_key2);
            assert!(fn_run_key2 < fn_run_key3);
            assert!(fn_run_key3 < fn_run_key4);
            assert!(fn_run_key4 < fn_run_key5);
        }
    }

    #[test]
    fn test_function_run_catalog_indexing() {
        use std::collections::HashMap;

        use crate::{
            config::ExecutorCatalogEntry,
            data_model::{
                ApplicationEntryPoint,
                ApplicationState,
                ApplicationVersionBuilder,
                Function,
                FunctionResources,
                GPUResources,
                filter::{Expression, LabelsFilter, Operator},
                test_objects::tests::mock_data_payload,
            },
            in_memory_state_bootstrap,
            state_store::ExecutorCatalog,
        };

        // Create executor catalog entries with different resource capacities and labels
        let catalog_entry_small = ExecutorCatalogEntry {
            name: "small".to_string(),
            cpu_cores: 2, // 2000 ms/sec
            memory_gb: 4, // 4096 MB
            disk_gb: 10,  // 10240 MB
            gpu_model: None,
            labels: HashMap::new(),
        };

        let mut catalog_entry_large_labels = HashMap::new();
        catalog_entry_large_labels.insert("region".to_string(), "us-west".to_string());
        catalog_entry_large_labels.insert("tier".to_string(), "premium".to_string());
        let catalog_entry_large = ExecutorCatalogEntry {
            name: "large".to_string(),
            cpu_cores: 16, // 16000 ms/sec
            memory_gb: 64, // 65536 MB
            disk_gb: 500,  // 512000 MB
            gpu_model: None,
            labels: catalog_entry_large_labels,
        };

        let mut catalog_entry_gpu_labels = HashMap::new();
        catalog_entry_gpu_labels.insert("has_gpu".to_string(), "true".to_string());
        catalog_entry_gpu_labels.insert("region".to_string(), "us-east".to_string());
        let catalog_entry_gpu = ExecutorCatalogEntry {
            name: "gpu-node".to_string(),
            cpu_cores: 8,
            memory_gb: 32,
            disk_gb: 100,
            gpu_model: Some(GpuModel {
                name: "nvidia-a100".to_string(),
                count: 4,
            }),
            labels: catalog_entry_gpu_labels,
        };

        let executor_catalog = ExecutorCatalog {
            entries: vec![
                catalog_entry_small.clone(),
                catalog_entry_large.clone(),
                catalog_entry_gpu.clone(),
            ],
        };

        // Create functions with different resource requirements and placement
        // constraints
        let function_light = Function {
            name: "light".to_string(),
            placement_constraints: LabelsFilter::default(), // Matches all labels
            resources: FunctionResources {
                cpu_ms_per_sec: 1000, // 1 core
                memory_mb: 2048,      // 2 GB
                ephemeral_disk_mb: 5000,
                gpu_configs: vec![],
            },
            ..Default::default()
        };

        let function_heavy = Function {
            name: "heavy".to_string(),
            placement_constraints: LabelsFilter(vec![
                Expression {
                    key: "region".to_string(),
                    value: "us-west".to_string(),
                    operator: Operator::Eq,
                },
                Expression {
                    key: "tier".to_string(),
                    value: "premium".to_string(),
                    operator: Operator::Eq,
                },
            ]),
            resources: FunctionResources {
                cpu_ms_per_sec: 10000, // 10 cores - only fits on large
                memory_mb: 32768,      // 32 GB
                ephemeral_disk_mb: 100000,
                gpu_configs: vec![],
            },
            ..Default::default()
        };

        let function_gpu = Function {
            name: "gpu_task".to_string(),
            placement_constraints: LabelsFilter(vec![Expression {
                key: "region".to_string(),
                value: "us-east".to_string(),
                operator: Operator::Eq,
            }]),
            resources: FunctionResources {
                cpu_ms_per_sec: 2000,
                memory_mb: 8192,
                ephemeral_disk_mb: 10000,
                gpu_configs: vec![GPUResources {
                    count: 1,
                    model: "nvidia-a100".to_string(),
                }],
            },
            ..Default::default()
        };

        let mut functions = HashMap::new();
        functions.insert("light".to_string(), function_light);
        functions.insert("heavy".to_string(), function_heavy);
        functions.insert("gpu_task".to_string(), function_gpu);

        let app_version = ApplicationVersionBuilder::default()
            .namespace("test-ns".to_string())
            .name("test-app".to_string())
            .created_at(0)
            .version("1.0".to_string())
            .functions(functions)
            .edges(HashMap::new())
            .code(Some(mock_data_payload()))
            .entrypoint(Some(ApplicationEntryPoint {
                function_name: "light".to_string(),
                input_serializer: "cloudpickle".to_string(),
                inputs_base64: String::new(),
                output_serializer: "cloudpickle".to_string(),
                output_type_hints_base64: String::new(),
            }))
            .state(ApplicationState::Active)
            .build()
            .unwrap();

        let mut application_versions = imbl::OrdMap::new();
        application_versions.insert(app_version.key(), Box::new(app_version));

        // Create function runs
        let fn_run_light = create_test_function_run("test-ns", "test-app", "req-1", "light", "1.0");
        let fn_run_heavy = create_test_function_run("test-ns", "test-app", "req-2", "heavy", "1.0");
        let fn_run_gpu =
            create_test_function_run("test-ns", "test-app", "req-3", "gpu_task", "1.0");

        let mut function_runs = imbl::OrdMap::new();
        function_runs.insert(
            FunctionRunKey(fn_run_light.key()),
            Box::new(fn_run_light.clone()),
        );
        function_runs.insert(
            FunctionRunKey(fn_run_heavy.key()),
            Box::new(fn_run_heavy.clone()),
        );
        function_runs.insert(
            FunctionRunKey(fn_run_gpu.key()),
            Box::new(fn_run_gpu.clone()),
        );

        let mut state = in_memory_state_bootstrap! {
            clock: 1,
            application_versions: application_versions,
            function_runs: function_runs,
            executor_catalog: executor_catalog,
        };

        // Manually index all function runs
        let function_runs_to_index: Vec<_> = state.function_runs.values().cloned().collect();
        for function_run in function_runs_to_index {
            state.index_function_run_by_catalog(&function_run);
        }

        // Test 1: Light function (no label constraints) should be in small catalog only
        // (large and gpu have labels, but light has no constraints so matches empty
        // labels in small)
        let small_runs = state.function_runs_for_catalog_entry("small");
        assert_eq!(
            small_runs.len(),
            1,
            "Small catalog should have 1 function run (light)"
        );
        assert!(small_runs.iter().any(|fr| fr.id == fn_run_light.id));

        // Test 2: Large catalog should have light + heavy (labels match, resources fit)
        let large_runs = state.function_runs_for_catalog_entry("large");
        assert_eq!(
            large_runs.len(),
            2,
            "Large catalog should have 2 function runs (light + heavy)"
        );
        assert!(large_runs.iter().any(|fr| fr.id == fn_run_light.id));
        assert!(large_runs.iter().any(|fr| fr.id == fn_run_heavy.id));

        // Test 3: Heavy function should ONLY be in large catalog
        // - Labels match (region=us-west, tier=premium)
        // - Resources match (requires 10 cores, 32GB - only large has 16 cores, 64GB)
        assert!(
            !small_runs.iter().any(|fr| fr.id == fn_run_heavy.id),
            "Heavy function should not be in small catalog (insufficient resources)"
        );

        // Test 4: GPU function should ONLY be in gpu-node catalog
        // - Labels match (region=us-east)
        // - Resources match (requires GPU, only gpu-node has nvidia-a100)
        let gpu_runs = state.function_runs_for_catalog_entry("gpu-node");
        assert_eq!(
            gpu_runs.len(),
            2,
            "GPU catalog should have 2 function runs (light + gpu_task)"
        );
        assert!(gpu_runs.iter().any(|fr| fr.id == fn_run_gpu.id));
        assert!(gpu_runs.iter().any(|fr| fr.id == fn_run_light.id));

        // GPU task should not be in other catalogs (they don't have GPUs or don't match
        // labels)
        assert!(
            !small_runs.iter().any(|fr| fr.id == fn_run_gpu.id),
            "GPU function should not be in small catalog (no GPU)"
        );
        assert!(
            !large_runs.iter().any(|fr| fr.id == fn_run_gpu.id),
            "GPU function should not be in large catalog (no GPU)"
        );

        // Test 5: Query non-existent catalog entry
        let non_existent = state.function_runs_for_catalog_entry("non-existent");
        assert_eq!(
            non_existent.len(),
            0,
            "Non-existent catalog should return empty"
        );

        // Test 6: Get all function runs by catalog entry
        let all_by_catalog = state.all_function_runs_by_catalog_entry();
        assert_eq!(
            all_by_catalog.len(),
            3,
            "All three catalogs have at least one function"
        );
        assert_eq!(all_by_catalog.get("small").unwrap().len(), 1); // light
        assert_eq!(all_by_catalog.get("large").unwrap().len(), 2); // light + heavy
        assert_eq!(all_by_catalog.get("gpu-node").unwrap().len(), 2); // light + gpu_task
        assert!(all_by_catalog.contains_key("small"));
        assert!(all_by_catalog.contains_key("large"));
        assert!(all_by_catalog.contains_key("gpu-node"));

        // Test 7: Remove a function run and verify index update
        state.delete_function_runs(vec![fn_run_heavy.clone()]);
        let large_runs_after_delete = state.function_runs_for_catalog_entry("large");
        assert_eq!(
            large_runs_after_delete.len(),
            1,
            "After delete, large catalog should have 1 function run (light)"
        );
        assert!(
            !large_runs_after_delete
                .iter()
                .any(|fr| fr.id == fn_run_heavy.id),
            "Deleted function should not be in index"
        );
        assert!(
            large_runs_after_delete
                .iter()
                .any(|fr| fr.id == fn_run_light.id),
            "Other functions should remain in index"
        );
    }

    // Helper function to create a test function run
    fn create_test_function_run(
        namespace: &str,
        application: &str,
        request_id: &str,
        function_name: &str,
        version: &str,
    ) -> FunctionRun {
        FunctionRunBuilder::default()
            .id(FunctionCallId(format!(
                "{}-{}-{}-{}",
                namespace, application, request_id, function_name
            )))
            .request_id(request_id.to_string())
            .namespace(namespace.to_string())
            .application(application.to_string())
            .name(function_name.to_string())
            .version(version.to_string())
            .compute_op(ComputeOp::FunctionCall(FunctionCall {
                inputs: vec![],
                function_call_id: FunctionCallId(format!(
                    "{}-{}-{}-{}",
                    namespace, application, request_id, function_name
                )),
                fn_name: function_name.to_string(),
                call_metadata: bytes::Bytes::new(),
                parent_function_call_id: None,
            }))
            .status(FunctionRunStatus::Pending)
            .outcome(None)
            .input_args(vec![])
            .attempt_number(0)
            .call_metadata(bytes::Bytes::new())
            .build()
            .unwrap()
    }
}
