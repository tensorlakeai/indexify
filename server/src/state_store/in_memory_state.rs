use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::Arc,
};

use anyhow::{anyhow, Result};
use opentelemetry::{
    metrics::{Histogram, ObservableGauge},
    KeyValue,
};
use tokio::sync::RwLock;
use tracing::{debug, error, warn};

use crate::{
    data_model::{
        Allocation, Application, ApplicationState, ApplicationVersion, DataPayload, ExecutorId,
        ExecutorMetadata, ExecutorServerMetadata, FunctionExecutorId, FunctionExecutorResources,
        FunctionExecutorServerMetadata, FunctionExecutorState, FunctionResources, FunctionRun,
        FunctionRunStatus, FunctionURI, GraphInvocationCtx, GraphVersion, Namespace,
        NamespaceBuilder,
    },
    executor_api::executor_api_pb::DataPayloadEncoding,
    metrics::low_latency_boundaries,
    state_store::{
        requests::RequestPayload, scanner::StateReader, state_machine::IndexifyObjectsColumns,
        ExecutorCatalog,
    },
    utils::{get_elapsed_time, get_epoch_time_in_ms, TimeUnit},
};

#[derive(Debug, Clone)]
pub enum Error {
    ComputeGraphVersionNotFound {
        version: String,
        function_name: String,
    },
    ComputeFunctionNotFound {
        version: String,
        function_name: String,
    },
    ConstraintUnsatisfiable {
        reason: String,
        version: String,
        function_name: String,
    },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::ComputeGraphVersionNotFound { version, .. } => {
                write!(f, "Compute graph version not found: {version}")
            }
            Error::ComputeFunctionNotFound { function_name, .. } => {
                write!(f, "Compute function not found: {function_name}")
            }
            Error::ConstraintUnsatisfiable { reason, .. } => reason.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    pub fn version(&self) -> &str {
        match self {
            Error::ComputeGraphVersionNotFound { version, .. } => version,
            Error::ComputeFunctionNotFound { version, .. } => version,
            Error::ConstraintUnsatisfiable { version, .. } => version,
        }
    }

    pub fn function_name(&self) -> &str {
        match self {
            Error::ComputeGraphVersionNotFound { function_name, .. } => function_name,
            Error::ComputeFunctionNotFound { function_name, .. } => function_name,
            Error::ConstraintUnsatisfiable { function_name, .. } => function_name,
        }
    }
}

pub struct DesiredStateFunctionExecutor {
    pub function_executor: Box<FunctionExecutorServerMetadata>,
    pub resources: FunctionExecutorResources,
    pub secret_names: Vec<String>,
    pub initialization_timeout_ms: u32,
    pub code_payload: DataPayload,
}

pub struct DesiredExecutorState {
    #[allow(clippy::vec_box)]
    pub function_executors: Vec<Box<DesiredStateFunctionExecutor>>,
    #[allow(clippy::box_collection)]
    pub function_run_allocations: std::collections::HashMap<FunctionExecutorId, Vec<Allocation>>,
    pub clock: u64,
}

pub struct CandidateFunctionExecutors {
    #[allow(clippy::vec_box)]
    pub function_executors: Vec<Box<FunctionExecutorServerMetadata>>,
    pub num_pending_function_executors: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FunctionRunKey(String);

impl From<&FunctionRun> for FunctionRunKey {
    fn from(function_run: &FunctionRun) -> Self {
        FunctionRunKey(function_run.key())
    }
}

impl From<FunctionRun> for FunctionRunKey {
    fn from(function_run: FunctionRun) -> Self {
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
            "{}|{}|{}",
            allocation.namespace, allocation.application, allocation.function_call_id
        ))
    }
}

impl From<&Box<Allocation>> for FunctionRunKey {
    fn from(allocation: &Box<Allocation>) -> Self {
        FunctionRunKey(format!(
            "{}|{}|{}",
            allocation.namespace, allocation.application, allocation.function_call_id
        ))
    }
}

impl Display for FunctionRunKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RequestCtxKey(String);

impl From<String> for RequestCtxKey {
    fn from(key: String) -> Self {
        RequestCtxKey(key)
    }
}

impl From<&String> for RequestCtxKey {
    fn from(key: &String) -> Self {
        RequestCtxKey(key.clone())
    }
}

impl From<&GraphInvocationCtx> for RequestCtxKey {
    fn from(ctx: &GraphInvocationCtx) -> Self {
        RequestCtxKey(ctx.key())
    }
}

impl From<&FunctionRun> for RequestCtxKey {
    fn from(function_run: &FunctionRun) -> Self {
        RequestCtxKey(format!(
            "{}|{}|{}",
            function_run.namespace, function_run.application, function_run.request_id
        ))
    }
}

impl From<FunctionRun> for RequestCtxKey {
    fn from(function_run: FunctionRun) -> Self {
        RequestCtxKey(format!(
            "{}|{}|{}",
            function_run.namespace, function_run.application, function_run.request_id
        ))
    }
}

impl From<Box<FunctionRun>> for RequestCtxKey {
    fn from(function_run: Box<FunctionRun>) -> Self {
        RequestCtxKey(format!(
            "{}|{}|{}",
            function_run.namespace, function_run.application, function_run.request_id
        ))
    }
}

pub struct InMemoryState {
    // clock is the value of the state_id this in-memory state is at.
    pub clock: u64,

    pub namespaces: im::HashMap<String, Box<Namespace>>,

    // Namespace|CG Name -> ComputeGraph
    pub applications: im::HashMap<String, Box<Application>>,

    // Namespace|CG Name|Version -> ComputeGraph
    pub application_versions: im::OrdMap<String, Box<ApplicationVersion>>,

    // ExecutorId -> ExecutorMetadata
    // This is the metadata that executor is sending us, not the **Desired** state
    // from the perspective of the state store.
    pub executors: im::HashMap<ExecutorId, Box<ExecutorMetadata>>,

    // ExecutorId -> (FE ID -> List of Function Executors)
    pub executor_states: im::HashMap<ExecutorId, Box<ExecutorServerMetadata>>,

    pub function_executors_by_fn_uri: im::HashMap<
        FunctionURI,
        im::HashMap<FunctionExecutorId, Box<FunctionExecutorServerMetadata>>,
    >,

    // ExecutorId -> (FE ID -> List of Allocations)
    #[allow(clippy::vec_box)]
    pub allocations_by_executor:
        im::HashMap<ExecutorId, HashMap<FunctionExecutorId, Vec<Box<Allocation>>>>,

    // TaskKey -> Task
    pub unallocated_function_runs: im::OrdSet<FunctionRunKey>,

    // Function Run Key -> Function Run
    pub function_runs: im::OrdMap<FunctionRunKey, Box<FunctionRun>>,

    // Invocation Ctx
    pub invocation_ctx: im::OrdMap<RequestCtxKey, Box<GraphInvocationCtx>>,

    // Configured executor label sets
    pub executor_catalog: ExecutorCatalog,

    // Histogram metrics for task latency measurements for direct recording
    function_run_pending_latency: Histogram<f64>,
    allocation_running_latency: Histogram<f64>,
    allocation_completion_latency: Histogram<f64>,
}

/// InMemoryMetrics manages observable metrics for the InMemoryState
#[allow(dead_code)]
pub struct InMemoryMetrics {
    pub unallocated_function_runs: ObservableGauge<u64>,
    pub active_function_runs_gauge: ObservableGauge<u64>,
    pub active_invocations_gauge: ObservableGauge<u64>,
    pub active_allocations_gauge: ObservableGauge<u64>,
    pub max_invocation_age_gauge: ObservableGauge<f64>,
    pub max_function_run_age_gauge: ObservableGauge<f64>,
}

impl InMemoryMetrics {
    pub fn new(state: Arc<RwLock<InMemoryState>>) -> Self {
        let meter = opentelemetry::global::meter("state_store");

        // Create observable gauges with callbacks that clone needed data
        let unallocated_function_runs_gauge = {
            let state_clone = state.clone();
            meter
                .u64_observable_gauge("indexify.unallocated_function_runs")
                .with_description(
                    "Number of unallocated function runs, reported from in_memory_state",
                )
                .with_callback(move |observer| {
                    // Use a block scope to ensure the lock is dropped automatically
                    {
                        if let Ok(state) = state_clone.try_read() {
                            let function_run_count = state.unallocated_function_runs.len() as u64;
                            // Lock is automatically dropped at the end of this block
                            observer.observe(function_run_count, &[]);
                        } else {
                            debug!(
                                "Failed to acquire read lock for unallocated_function_runs metric"
                            );
                        }
                    }
                })
                .build()
        };

        let active_function_runs_gauge = {
            let state_clone = state.clone();
            meter
                .u64_observable_gauge("indexify.active_function_runs")
                .with_description("Number of active function runs, reported from in_memory_state")
                .with_callback(move |observer| {
                    if let Ok(state) = state_clone.try_read() {
                        let function_run_count = state
                            .function_runs
                            .iter()
                            // Filter out terminal function runs since they stick around until their
                            // invocation is completed.
                            .filter(|(_k, function_run)| function_run.outcome.is_some())
                            .count() as u64;
                        // Lock is automatically dropped at the end of this block
                        observer.observe(function_run_count, &[]);
                    } else {
                        debug!("Failed to acquire read lock for active_function_runs metric");
                    }
                })
                .build()
        };

        let active_invocations_gauge = {
            let state_clone = state.clone();
            meter
                .u64_observable_gauge("indexify.active_invocations_gauge")
                .with_description("Number of active invocations, reported from in_memory_state")
                .with_callback(move |observer| {
                    if let Ok(state) = state_clone.try_read() {
                        let invocation_count = state.invocation_ctx.len() as u64;
                        // Lock is automatically dropped at the end of this block
                        observer.observe(invocation_count, &[]);
                    } else {
                        debug!("Failed to acquire read lock for active_invocations metric");
                    }
                })
                .build()
        };

        let active_allocations_gauge = {
            let state_clone = state.clone();
            meter
                .u64_observable_gauge("indexify.active_allocations_gauge")
                .with_description("Number of active allocations, reported from in_memory_state")
                .with_callback(move |observer| {
                    // Clone data within a minimal scope to auto-drop the lock immediately
                    let allocations_by_executor = {
                        if let Ok(state) = state_clone.try_read() {
                            Some(state.allocations_by_executor.clone())
                        } else {
                            debug!("Failed to acquire read lock for active_allocations metric");
                            None
                        }
                    };

                    if let Some(allocations_by_executor) = allocations_by_executor {
                        // Process the cloned data outside the lock scope
                        for (executor_id, fn_map) in allocations_by_executor.iter() {
                            for (_, allocations) in fn_map.iter() {
                                observer.observe(
                                    allocations.len() as u64,
                                    &[KeyValue::new("executor_id", executor_id.to_string())],
                                );
                            }
                        }
                    }
                })
                .build()
        };

        // Add max invocation age metric
        let max_invocation_age_gauge = {
            let state_clone = state.clone();
            meter
                .f64_observable_gauge("indexify.max_invocation_age")
                .with_unit("s")
                .with_description("Maximum age of any non-completed invocation in seconds")
                .with_callback(move |observer| {
                    // Clone data within a minimal scope to auto-drop the lock immediately
                    let invocation_ctx = {
                        if let Ok(state) = state_clone.try_read() {
                            Some(state.invocation_ctx.clone())
                        } else {
                            debug!("Failed to acquire read lock for invocation_ctx metric");
                            None
                        }
                    };

                    let max_age = match invocation_ctx {
                        Some(invocation_ctx) => {
                            // Find the oldest non-completed invocation
                            invocation_ctx
                                .values()
                                .filter(|inv| inv.outcome.is_none())
                                .map(|inv| {
                                    get_elapsed_time(inv.created_at.into(), TimeUnit::Milliseconds)
                                })
                                .max_by(|a, b| {
                                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                                })
                                .unwrap_or(0.0) // Default to 0 if no
                                                // non-completed invocations
                        }
                        None => 0.0,
                    };

                    // Always report the max age (which may be 0)
                    observer.observe(max_age, &[]);
                })
                .build()
        };

        // Add max task age metric
        let max_function_run_age_gauge = {
            let state_clone = state.clone();
            meter
                .f64_observable_gauge("indexify.max_function_run_age")
                .with_unit("s")
                .with_description("Maximum age of any non-terminal function run in seconds")
                .with_callback(move |observer| {
                    // Clone data within a minimal scope to auto-drop the lock immediately
                    let tasks = {
                        if let Ok(state) = state_clone.try_read() {
                            Some(state.function_runs.clone())
                        } else {
                            debug!("Failed to acquire read lock for function_runs metric");
                            None
                        }
                    };

                    let max_age = match tasks {
                        Some(tasks) => {
                            // Find the oldest non-terminal task
                            tasks
                                .values()
                                .filter(|function_run| function_run.outcome.is_some())
                                .map(|function_run| {
                                    get_elapsed_time(
                                        function_run.creation_time_ns,
                                        TimeUnit::Nanoseconds,
                                    )
                                })
                                .max_by(|a, b| {
                                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                                })
                                .unwrap_or(0.0) // Default to 0 if no
                                                // non-terminal tasks
                        }
                        None => 0.0,
                    };

                    // Always report the max age (which may be 0)
                    observer.observe(max_age, &[]);
                })
                .build()
        };

        Self {
            unallocated_function_runs: unallocated_function_runs_gauge,
            active_function_runs_gauge,
            active_invocations_gauge,
            active_allocations_gauge,
            max_invocation_age_gauge,
            max_function_run_age_gauge,
        }
    }
}

impl InMemoryState {
    pub fn new(clock: u64, reader: StateReader, executor_catalog: ExecutorCatalog) -> Result<Self> {
        let meter = opentelemetry::global::meter("state_store");

        // Create histogram metrics for task latency measurements
        let task_pending_latency = meter
            .f64_histogram("indexify.function_run_pending_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time function runs spend from creation to running")
            .build();

        let allocation_running_latency = meter
            .f64_histogram("indexify.allocation_running_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time function runs spend from running to completion")
            .build();

        let allocation_completion_latency = meter
            .f64_histogram("indexify.allocation_completion_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("Time tasks spend from creation to completion")
            .build();

        // Creating Namespaces
        let mut namespaces = im::HashMap::new();
        let mut applications = im::HashMap::new();
        {
            let all_ns = reader.get_all_namespaces()?;
            for ns in &all_ns {
                // Creating Namespaces
                namespaces.insert(ns.name.clone(), Box::new(ns.clone()));

                // Creating Compute Graphs and Versions
                let cgs = reader.list_applications(&ns.name, None, None)?.0;
                for cg in cgs {
                    applications.insert(cg.key(), Box::new(cg));
                }
            }
        }

        let mut application_versions = im::OrdMap::new();
        {
            let all_cg_versions: Vec<(String, ApplicationVersion)> =
                reader.get_all_rows_from_cf(IndexifyObjectsColumns::ApplicationVersions)?;
            for (id, cg) in all_cg_versions {
                application_versions.insert(id, Box::new(cg));
            }
        }
        // Creating Allocated Tasks By Function by Executor
        let mut allocations_by_executor: im::HashMap<
            ExecutorId,
            HashMap<FunctionExecutorId, Vec<Box<Allocation>>>,
        > = im::HashMap::new();
        {
            let (allocations, _) = reader.get_rows_from_cf_with_limits::<Allocation>(
                &[],
                None,
                IndexifyObjectsColumns::Allocations,
                None,
            )?;
            for allocation in allocations {
                if allocation.is_terminal() {
                    continue;
                }
                allocations_by_executor
                    .entry(allocation.target.executor_id.clone())
                    .or_default()
                    .entry(allocation.target.function_executor_id.clone())
                    .or_default()
                    .push(Box::new(allocation));
            }
        }

        let mut invocation_ctx = im::OrdMap::new();
        {
            let all_graph_invocation_ctx: Vec<(String, GraphInvocationCtx)> =
                reader.get_all_rows_from_cf(IndexifyObjectsColumns::GraphInvocationCtx)?;
            for (_id, ctx) in all_graph_invocation_ctx {
                // Do not cache completed invocations
                if ctx.outcome.is_some() {
                    continue;
                }
                invocation_ctx.insert(ctx.key().into(), Box::new(ctx));
            }
        }

        let mut function_runs = im::OrdMap::new();
        let mut unallocated_function_runs = im::OrdSet::new();
        for ctx in invocation_ctx.values() {
            for function_run in ctx.function_runs.values() {
                if function_run.outcome.is_some() {
                    continue;
                }
                if function_run.status == FunctionRunStatus::Pending {
                    unallocated_function_runs.insert(function_run.clone().into());
                }
                function_runs.insert(function_run.clone().into(), Box::new(function_run.clone()));
            }
        }

        let in_memory_state = Self {
            clock,
            namespaces,
            applications,
            application_versions,
            executors: im::HashMap::new(),
            function_runs,
            unallocated_function_runs,
            invocation_ctx,
            allocations_by_executor,
            // function executors by executor are not known at startup
            executor_states: im::HashMap::new(),
            function_executors_by_fn_uri: im::HashMap::new(),
            executor_catalog,
            // metrics
            function_run_pending_latency: task_pending_latency,
            allocation_running_latency,
            allocation_completion_latency,
        };

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
            RequestPayload::InvokeComputeGraph(req) => {
                self.invocation_ctx
                    .insert(req.ctx.key().into(), Box::new(req.ctx.clone()));
                for function_run in req.ctx.function_runs.values() {
                    self.function_runs
                        .insert(function_run.clone().into(), Box::new(function_run.clone()));
                    self.unallocated_function_runs
                        .insert(function_run.clone().into());
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
            RequestPayload::CreateOrUpdateComputeGraph(req) => {
                self.applications
                    .insert(req.application.key(), Box::new(req.application.clone()));
                let req_version = req.application.to_version()?;
                let version = req_version.version.clone();

                self.application_versions
                    .insert(req_version.key(), Box::new(req_version));

                // FIXME - we should set this in the API and not here, so that these things are
                // not set in the state store
                if req.upgrade_requests_to_current_version {
                    // Update invocation ctxs and function runs
                    {
                        let mut invocation_ctx_to_update = vec![];
                        let invocation_ctx_key_prefix =
                            GraphInvocationCtx::key_prefix_for_application(
                                &req.namespace,
                                &req.application.name,
                            );
                        self.invocation_ctx
                            .range::<std::ops::RangeFrom<RequestCtxKey>, RequestCtxKey>(
                                invocation_ctx_key_prefix.clone().into()..,
                            )
                            .take_while(|(k, _v)| k.0.starts_with(&invocation_ctx_key_prefix))
                            .for_each(|(_k, v)| {
                                let mut ctx = v.clone();
                                ctx.application_version = version.clone();
                                invocation_ctx_to_update.push(ctx);
                            });

                        for ctx in invocation_ctx_to_update.iter_mut() {
                            for (_function_call_id, function_run) in
                                ctx.function_runs.clone().iter_mut()
                            {
                                if function_run.application_version != version {
                                    function_run.application_version = version.clone();
                                    ctx.function_runs
                                        .insert(function_run.id.clone(), function_run.clone());
                                }
                                self.function_runs
                                    .entry(function_run.clone().into())
                                    .and_modify(|existing_function_run| {
                                        *existing_function_run = Box::new(function_run.clone());
                                    });
                            }
                        }

                        for ctx in invocation_ctx_to_update {
                            self.invocation_ctx.insert(ctx.key().into(), ctx);
                        }
                    }
                }
            }
            RequestPayload::DeleteInvocationRequest((req, _)) => {
                self.delete_invocation(&req.namespace, &req.application, &req.invocation_id);
            }
            RequestPayload::DeleteComputeGraphRequest((req, _)) => {
                // Remove compute graph
                let key = Application::key_from(&req.namespace, &req.name);
                self.applications.remove(&key);

                // Remove compute graph versions
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

                // Remove invocation contexts
                {
                    let invocation_key_prefix =
                        GraphInvocationCtx::key_prefix_for_application(&req.namespace, &req.name);
                    let invocations_to_remove = self
                        .invocation_ctx
                        .range::<std::ops::RangeFrom<RequestCtxKey>, RequestCtxKey>(
                            invocation_key_prefix.clone().into()..,
                        )
                        .take_while(|(k, _v)| k.0.starts_with(&invocation_key_prefix))
                        .map(|(_k, v)| v.request_id.clone())
                        .collect::<Vec<String>>();
                    for k in invocations_to_remove {
                        self.delete_invocation(&req.namespace, &req.name, &k);
                    }
                }
            }
            RequestPayload::SchedulerUpdate((req, _)) => {
                for (ctx_key, function_call_ids) in &req.updated_function_runs {
                    for function_call_id in function_call_ids {
                        let Some(ctx) = req.updated_invocations_states.get(ctx_key).cloned() else {
                            error!(
                                ctx_key = ctx_key.clone(),
                                "invocation ctx not found for updated function runs"
                            );
                            continue;
                        };
                        let Some(function_run) = ctx.function_runs.get(function_call_id).cloned()
                        else {
                            error!(
                                ctx_key = ctx_key.clone(),
                                function_call_id = function_call_id.clone().to_string(),
                                "function run not found for updated function runs"
                            );
                            continue;
                        };
                        if function_run.status == FunctionRunStatus::Pending {
                            self.unallocated_function_runs
                                .insert(function_run.clone().into());
                        } else {
                            self.unallocated_function_runs
                                .remove(&function_run.clone().into());
                        }
                        self.function_runs
                            .insert(function_run.clone().into(), Box::new(function_run.clone()));
                    }
                }
                for (key, invocation_ctx) in &req.updated_invocations_states {
                    // Remove tasks for invocation ctx if completed
                    if invocation_ctx.outcome.is_some() {
                        self.delete_invocation(
                            &invocation_ctx.namespace,
                            &invocation_ctx.application_name,
                            &invocation_ctx.request_id,
                        );
                    } else {
                        self.invocation_ctx
                            .insert(key.clone().into(), Box::new(invocation_ctx.clone()));
                    }
                }

                for fe_meta in req.new_function_executors.clone() {
                    let Some(executor_state) = self.executor_states.get_mut(&fe_meta.executor_id)
                    else {
                        error!(
                            executor_id = fe_meta.executor_id.get(),
                            "executor not found for new function executor"
                        );
                        continue;
                    };
                    executor_state.function_executors.insert(
                        fe_meta.function_executor.id.clone(),
                        Box::new(fe_meta.clone()),
                    );

                    executor_state.resource_claims.insert(
                        fe_meta.function_executor.id.clone(),
                        fe_meta.function_executor.resources.clone(),
                    );

                    let fn_uri = FunctionURI::from(fe_meta.clone());
                    self.function_executors_by_fn_uri
                        .entry(fn_uri)
                        .or_default()
                        .insert(
                            fe_meta.function_executor.id.clone(),
                            Box::new(fe_meta.clone()),
                        );

                    // Executor has a new function executor
                    changed_executors.insert(fe_meta.executor_id.clone());
                }

                for allocation in &req.new_allocations {
                    if let Some(function_run) = self.function_runs.get(&allocation.into()) {
                        self.unallocated_function_runs
                            .remove(&function_run.clone().into());

                        self.allocations_by_executor
                            .entry(allocation.target.executor_id.clone())
                            .or_default()
                            .entry(allocation.target.function_executor_id.clone())
                            .or_default()
                            .push(Box::new(allocation.clone()));

                        // Record metrics
                        self.function_run_pending_latency.record(
                            get_elapsed_time(function_run.creation_time_ns, TimeUnit::Nanoseconds),
                            &[],
                        );

                        // Executor has a new allocation
                        changed_executors.insert(allocation.target.executor_id.clone());
                    } else {
                        error!(
                            namespace = &allocation.namespace,
                            graph = &allocation.application,
                            "fn" = &allocation.function,
                            executor_id = allocation.target.executor_id.get(),
                            allocation_id = %allocation.id,
                            invocation_id = &allocation.invocation_id,
                            task_id = allocation.function_call_id.to_string(),
                            "function run not found for new allocation"
                        );
                    }
                }

                for (executor_id, function_executors) in &req.remove_function_executors {
                    if let Some(fe_allocations) = self.allocations_by_executor.get_mut(executor_id)
                    {
                        fe_allocations
                            .retain(|fe_id, _allocations| !function_executors.contains(fe_id));
                    }

                    for function_executor_id in function_executors {
                        let fe =
                            self.executor_states
                                .get_mut(executor_id)
                                .and_then(|executor_state| {
                                    executor_state.resource_claims.remove(function_executor_id);
                                    executor_state
                                        .function_executors
                                        .remove(function_executor_id)
                                });

                        if let Some(fe) = fe {
                            let fn_uri = FunctionURI::from(fe.clone());
                            self.function_executors_by_fn_uri
                                .get_mut(&fn_uri)
                                .and_then(|fe_map| fe_map.remove(&fe.function_executor.id));
                        }
                        changed_executors.insert(executor_id.clone());
                    }
                }

                for executor_id in &req.remove_executors {
                    self.executors.remove(executor_id);
                    self.allocations_by_executor.remove(executor_id);
                    self.executor_states.remove(executor_id);

                    // Executor is removed
                    changed_executors.insert(executor_id.clone());
                }

                for (executor_id, free_resources) in &req.updated_executor_resources {
                    if let Some(executor) = self.executor_states.get_mut(executor_id) {
                        executor.free_resources = free_resources.clone();
                    }
                }
            }
            RequestPayload::UpsertExecutor(req) => {
                self.executors
                    .insert(req.executor.id.clone(), Box::new(req.executor.clone()));
                if self.executor_states.get(&req.executor.id).is_none() {
                    self.executor_states.insert(
                        req.executor.id.clone(),
                        Box::new(ExecutorServerMetadata {
                            executor_id: req.executor.id.clone(),
                            function_executors: HashMap::new(),
                            resource_claims: HashMap::new(),
                            free_resources: req.executor.host_resources.clone(),
                        }),
                    );
                }

                for allocation_output in &req.allocation_outputs {
                    // Remove the allocation
                    {
                        self.allocations_by_executor
                            .entry(allocation_output.executor_id.clone())
                            .and_modify(|fe_allocations| {
                                // TODO: This can be optimized by keeping a new index of task_id to
                                // FE,       we should measure the
                                // overhead.
                                fe_allocations.iter_mut().for_each(|(_, allocations)| {
                                    if let Some(index) = allocations
                                        .iter()
                                        .position(|a| a.id == allocation_output.allocation.id)
                                    {
                                        let allocation = &allocations[index];
                                        // Record metrics
                                        self.allocation_running_latency.record(
                                            get_elapsed_time(
                                                allocation.created_at,
                                                TimeUnit::Milliseconds,
                                            ),
                                            &[KeyValue::new(
                                                "outcome",
                                                allocation_output.allocation.outcome.to_string(),
                                            )],
                                        );

                                        // Remove the allocation
                                        allocations.remove(index);
                                    }
                                });

                                // Remove the function if no allocations left
                                fe_allocations.retain(|_, f| !f.is_empty());
                            });

                        // Executor's allocation is removed
                        changed_executors.insert(allocation_output.executor_id.clone());
                    }

                    // Record metrics
                    self.allocation_completion_latency.record(
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
            RequestPayload::DeregisterExecutor(req) => {
                let executor = self.executors.get_mut(&req.executor_id);
                if let Some(executor) = executor {
                    executor.tombstoned = true;
                }
            }
            _ => {}
        }

        Ok(changed_executors)
    }

    pub fn fe_resource_for_function_run(
        &self,
        function_run: &FunctionRun,
    ) -> Result<FunctionResources> {
        let application = self
            .application_versions
            .get(&ApplicationVersion::key_from(
                &function_run.namespace,
                &function_run.application,
                &function_run.application_version,
            ))
            .ok_or(anyhow!(
                "compute graph version: {} not found",
                function_run.application_version
            ))?;
        let compute_fn = application
            .nodes
            .get(&function_run.name)
            .ok_or(anyhow!("compute function: {} not found", function_run.name))?;
        Ok(compute_fn.resources.clone())
    }

    pub fn candidate_executors(
        &self,
        function_run: &FunctionRun,
    ) -> Result<Vec<ExecutorServerMetadata>> {
        let application = self
            .application_versions
            .get(&ApplicationVersion::key_from(
                &function_run.namespace,
                &function_run.application,
                &function_run.application_version,
            ))
            .ok_or_else(|| Error::ComputeGraphVersionNotFound {
                version: function_run.application_version.0.clone(),
                function_name: function_run.name.clone(),
            })?;

        // Check to see whether the compute graph state is marked as
        // active; if not, we do not schedule its tasks, even if there
        // are executors that could handle this particular task.
        if let ApplicationState::Disabled { reason } = &application.state {
            return Err(Error::ConstraintUnsatisfiable {
                version: application.version.to_string(),
                function_name: function_run.name.clone(),
                reason: reason.to_owned(),
            }
            .into());
        }

        let compute_fn = application.nodes.get(&function_run.name).ok_or_else(|| {
            Error::ComputeFunctionNotFound {
                version: function_run.application_version.0.clone(),
                function_name: function_run.name.clone(),
            }
        })?;

        let mut candidates = Vec::new();

        for (_, executor_state) in &self.executor_states {
            let Some(executor) = self.executors.get(&executor_state.executor_id) else {
                error!(
                    executor_id = executor_state.executor_id.get(),
                    "executor not found for candidate executors but was found in executor_states"
                );
                continue;
            };
            if executor.tombstoned || !executor.is_function_allowed(function_run) {
                continue;
            }

            // Check if this executor's labels matches the function's
            // placement constraints
            if !compute_fn.placement_constraints.matches(&executor.labels) {
                continue;
            }

            // TODO: Match functions to GPU models according to prioritized order in
            // gpu_configs.
            if executor_state
                .free_resources
                .can_handle_function_resources(&compute_fn.resources)
                .is_ok()
            {
                candidates.push(*executor_state.clone());
            }
        }

        Ok(candidates)
    }

    pub fn candidate_function_executors(
        &self,
        function_run: &FunctionRun,
        capacity_threshold: u32,
    ) -> Result<CandidateFunctionExecutors> {
        let mut candidates = Vec::new();
        let fn_uri = FunctionURI::from(function_run);
        let function_executors = self.function_executors_by_fn_uri.get(&fn_uri);
        let mut num_pending_function_executors = 0;
        if let Some(function_executors) = function_executors {
            for function_executor_kv in function_executors.iter() {
                let function_executor = function_executor_kv.1;
                if function_executor.function_executor.state == FunctionExecutorState::Pending
                    || function_executor.function_executor.state == FunctionExecutorState::Unknown
                {
                    num_pending_function_executors += 1;
                }
                if matches!(
                    function_executor.desired_state,
                    FunctionExecutorState::Terminated { .. }
                ) || matches!(
                    function_executor.function_executor.state,
                    FunctionExecutorState::Terminated { .. }
                ) {
                    continue;
                }
                // FIXME - Create a reverse index of fe_id -> # active allocations
                let allocation_count = self
                    .allocations_by_executor
                    .get(&function_executor.executor_id)
                    .and_then(|alloc_map| alloc_map.get(&function_executor.function_executor.id))
                    .map(|allocs| allocs.len())
                    .unwrap_or(0);
                if (allocation_count as u32)
                    < capacity_threshold * function_executor.function_executor.max_concurrency
                {
                    candidates.push(function_executor.clone());
                }
            }
        }
        Ok(CandidateFunctionExecutors {
            function_executors: candidates,
            num_pending_function_executors,
        })
    }

    pub fn delete_function_runs(&mut self, function_runs: Vec<FunctionRun>) {
        for function_run in function_runs.iter() {
            self.function_runs.remove(&function_run.clone().into());
            self.unallocated_function_runs
                .remove(&function_run.clone().into());
        }

        for (_executor, allocations_by_fe) in self.allocations_by_executor.iter_mut() {
            for (_fe_id, allocations) in allocations_by_fe.iter_mut() {
                allocations.retain(|allocation| {
                    !function_runs
                        .iter()
                        .any(|function_run| function_run.id == allocation.function_call_id)
                });
            }
        }
    }

    pub fn get_fe_resources_by_uri(
        &self,
        ns: &str,
        cg: &str,
        fn_name: &str,
        version: &GraphVersion,
    ) -> Option<FunctionResources> {
        let cg_version = self
            .application_versions
            .get(&ApplicationVersion::key_from(ns, cg, version))
            .cloned()?;
        cg_version
            .nodes
            .get(fn_name)
            .map(|node| node.resources.clone())
    }

    pub fn get_fe_max_concurrency_by_uri(
        &self,
        ns: &str,
        cg: &str,
        fn_name: &str,
        version: &GraphVersion,
    ) -> Option<u32> {
        let cg_version = self
            .application_versions
            .get(&ApplicationVersion::key_from(ns, cg, version))
            .cloned()?;
        cg_version
            .nodes
            .get(fn_name)
            .map(|node| node.max_concurrency)
    }

    pub fn delete_invocation(&mut self, namespace: &str, application: &str, invocation_id: &str) {
        // Remove invocation ctx
        self.invocation_ctx
            .remove(&GraphInvocationCtx::key_from(namespace, application, invocation_id).into());

        // Remove tasks
        let key_prefix = FunctionRun::key_prefix_for_request(namespace, application, invocation_id);
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

    #[tracing::instrument(skip(self))]
    pub fn vacuum_function_executors_candidates(
        &self,
        fe_resource: &FunctionResources,
    ) -> Result<Vec<FunctionExecutorServerMetadata>> {
        // For each executor in the system
        for (executor_id, executor) in &self.executors {
            if executor.tombstoned {
                continue;
            }

            // Get function executors for this executor from our in-memory state
            let function_executors = self
                .executor_states
                .get(executor_id)
                .cloned()
                .map(|executor_state| {
                    executor_state
                        .function_executors
                        .values()
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            // Start with the current free resources on this executor
            let mut available_resources = self
                .executor_states
                .get(executor_id)
                .map(|executor_state| executor_state.free_resources.clone())
                .unwrap_or_default();

            let mut function_executors_to_remove = Vec::new();
            for fe_metadata in function_executors.iter() {
                // Skip if the FE is already marked for termination
                if matches!(
                    fe_metadata.desired_state,
                    FunctionExecutorState::Terminated { .. }
                ) {
                    continue;
                }

                let fe = &fe_metadata.function_executor;
                let Some(executor) = self.executors.get(executor_id) else {
                    function_executors_to_remove.push(*fe_metadata.clone());
                    continue;
                };

                let Some(latest_cg_version) = self
                    .applications
                    .get(&Application::key_from(&fe.namespace, &fe.application_name))
                    .map(|cg| cg.version.clone())
                else {
                    function_executors_to_remove.push(*fe_metadata.clone());
                    continue;
                };

                let has_pending_tasks = self.has_pending_tasks(fe_metadata);

                let mut can_be_removed = false;
                if !has_pending_tasks {
                    let mut found_allowlist_match = false;
                    if let Some(allowlist) = executor.function_allowlist.as_ref() {
                        for allowlist_entry in allowlist.iter() {
                            if allowlist_entry.matches_function_executor(fe)
                                && fe.version == latest_cg_version
                            {
                                found_allowlist_match = true;
                                break;
                            }
                        }
                    }
                    if !found_allowlist_match {
                        debug!(
                            "Candidate for removal: outdated function executor {} from executor {} (version {} < latest {})",
                            fe.id.get(), executor_id.get(), fe.version, latest_cg_version
                        );
                        can_be_removed = true;
                    }
                }

                if can_be_removed {
                    let mut simulated_resources = available_resources.clone();
                    if simulated_resources
                        .free(&fe_metadata.function_executor.resources)
                        .is_err()
                    {
                        continue;
                    }

                    function_executors_to_remove.push(*fe_metadata.clone());
                    available_resources = simulated_resources;

                    if available_resources
                        .can_handle_function_resources(fe_resource)
                        .is_ok()
                    {
                        debug!(
                            "Found sufficient space on executor {} by removing {} function executors",
                            executor_id.get(),
                            function_executors_to_remove.len()
                        );
                        return Ok(function_executors_to_remove);
                    }
                }
            }
            debug!(
                "Could not find sufficient space on executor {} even after vacuuming",
                executor_id.get()
            );
        }

        Ok(Vec::new())
    }

    fn has_pending_tasks(&self, fe_meta: &FunctionExecutorServerMetadata) -> bool {
        let task_prefixes_for_fe = format!(
            "{}|{}|",
            fe_meta.function_executor.namespace, fe_meta.function_executor.application_name
        );
        self.function_runs
            .range(FunctionRunKey(task_prefixes_for_fe.clone())..)
            .take_while(|(k, _v)| k.0.starts_with(&task_prefixes_for_fe))
            .filter(|(_k, v)| {
                v.name == fe_meta.function_executor.function_name
                    && v.application_version == fe_meta.function_executor.version
            })
            .any(|(_k, v)| v.outcome.is_none())
    }

    pub fn desired_state(&self, executor_id: &ExecutorId) -> DesiredExecutorState {
        let active_function_executors = self
            .executor_states
            .get(executor_id)
            .cloned()
            .map(|executor_state| executor_state.function_executors.clone())
            .unwrap_or_default()
            .values()
            .filter(|fe_meta| {
                !matches!(
                    fe_meta.desired_state,
                    FunctionExecutorState::Terminated { .. }
                )
            })
            .cloned()
            .collect::<Vec<_>>();

        let mut function_executors = Vec::new();
        let mut task_allocations = std::collections::HashMap::new();
        for fe_meta in active_function_executors.iter() {
            let fe = &fe_meta.function_executor;
            let Some(cg_version) = self
                .application_versions
                .get(&ApplicationVersion::key_from(
                    &fe.namespace,
                    &fe.application_name,
                    &fe.version,
                ))
                .cloned()
            else {
                continue;
            };
            let Some(cg_node) = cg_version.nodes.get(&fe.function_name) else {
                continue;
            };
            function_executors.push(Box::new(DesiredStateFunctionExecutor {
                function_executor: fe_meta.clone(),
                resources: fe.resources.clone(),
                secret_names: cg_node.secret_names.clone().unwrap_or_default(),
                initialization_timeout_ms: cg_node.initialization_timeout.0,
                code_payload: DataPayload {
                    id: cg_version.code.id.clone(),
                    metadata_size: 0,
                    path: cg_version.code.path.clone(),
                    size: cg_version.code.size,
                    sha256_hash: cg_version.code.sha256_hash.clone(),
                    offset: 0, // Code always uses its full BLOB
                    encoding: DataPayloadEncoding::BinaryZip.as_str_name().to_string(),
                },
            }));

            let allocations = self
                .allocations_by_executor
                .get(executor_id)
                .and_then(|allocations| allocations.get(&fe_meta.function_executor.id.clone()))
                .unwrap_or(&Vec::new())
                .iter()
                .map(|allocation| *allocation.clone())
                .collect::<Vec<_>>();
            task_allocations.insert(fe_meta.function_executor.id.clone(), allocations);
        }

        DesiredExecutorState {
            function_executors,
            function_run_allocations: task_allocations,
            clock: self.clock,
        }
    }

    pub fn clone(&self) -> Arc<tokio::sync::RwLock<Self>> {
        Arc::new(tokio::sync::RwLock::new(InMemoryState {
            clock: self.clock,
            namespaces: self.namespaces.clone(),
            applications: self.applications.clone(),
            application_versions: self.application_versions.clone(),
            executors: self.executors.clone(),
            invocation_ctx: self.invocation_ctx.clone(),
            allocations_by_executor: self.allocations_by_executor.clone(),
            executor_states: self.executor_states.clone(),
            function_executors_by_fn_uri: self.function_executors_by_fn_uri.clone(),
            executor_catalog: self.executor_catalog.clone(),
            // metrics
            function_run_pending_latency: self.function_run_pending_latency.clone(),
            allocation_running_latency: self.allocation_running_latency.clone(),
            allocation_completion_latency: self.allocation_completion_latency.clone(),
            function_runs: self.function_runs.clone(),
            unallocated_function_runs: self.unallocated_function_runs.clone(),
        }))
    }

    #[allow(clippy::borrowed_box)]
    pub fn application_graph_version<'a>(
        &'a self,
        namespace: &str,
        application_name: &str,
        version: &GraphVersion,
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
                    application_name = application_name,
                    version = version.0,
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
                    task_id = function_run.id.to_string(),
                    invocation_id = function_run.request_id.to_string(),
                    namespace = function_run.namespace,
                    graph = function_run.application,
                    "fn" = function_run.name,
                    graph_version = function_run.application_version.0,
                    "application version not found",
                );
                None
            })
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
            use opentelemetry::global;
            Self {
                clock: 0,
                namespaces: im::HashMap::new(),
                applications: im::HashMap::new(),
                application_versions: im::OrdMap::new(),
                executors: im::HashMap::new(),
                executor_states: im::HashMap::new(),
                function_executors_by_fn_uri: im::HashMap::new(),
                allocations_by_executor: im::HashMap::new(),
                invocation_ctx: im::OrdMap::new(),
                executor_catalog: ExecutorCatalog::default(),
                function_run_pending_latency: global::meter("test").f64_histogram("test").build(),
                allocation_running_latency: global::meter("test").f64_histogram("test").build(),
                allocation_completion_latency: global::meter("test").f64_histogram("test").build(),
                function_runs: im::OrdMap::new(),
                unallocated_function_runs: im::OrdSet::new(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::{
        data_model::{
            ComputeOp, ExecutorId, FunctionCall, FunctionCallId, FunctionExecutorBuilder,
            FunctionExecutorId, FunctionExecutorResources, FunctionExecutorServerMetadata,
            FunctionExecutorState, FunctionRun, FunctionRunBuilder, FunctionRunFailureReason,
            FunctionRunOutcome, FunctionRunStatus, GraphVersion,
        },
        in_memory_state_bootstrap,
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
    fn test_has_pending_function_runs() {
        // Helper function to create a function run
        fn create_function_run(
            namespace: &str,
            application: &str,
            request_id: &str,
            compute_fn: &str,
            outcome: Option<FunctionRunOutcome>,
        ) -> FunctionRun {
            FunctionRunBuilder::default()
                .id(FunctionCallId(format!(
                    "{}-{}-{}-{}",
                    namespace, application, request_id, compute_fn
                )))
                .request_id(request_id.to_string())
                .namespace(namespace.to_string())
                .application(application.to_string())
                .name(compute_fn.to_string())
                .application_version(GraphVersion("1.0".to_string()))
                .compute_op(ComputeOp::FunctionCall(FunctionCall {
                    inputs: vec![],
                    function_call_id: FunctionCallId(format!(
                        "{}-{}-{}-{}",
                        namespace, application, request_id, compute_fn
                    )),
                    fn_name: compute_fn.to_string(),
                    call_metadata: Bytes::new(),
                }))
                .status(FunctionRunStatus::Pending)
                .outcome(outcome)
                .input_args(vec![])
                .attempt_number(0)
                .call_metadata(Bytes::new())
                .build()
                .unwrap()
        }

        // Create function executor metadata for testing
        let executor_id = ExecutorId::new("test-executor".to_string());
        let function_executor = FunctionExecutorBuilder::default()
            .id(FunctionExecutorId::new("test-fe".to_string()))
            .namespace("test-namespace".to_string())
            .application_name("test-graph".to_string())
            .function_name("test-function".to_string())
            .version(GraphVersion("1.0".to_string()))
            .state(FunctionExecutorState::Running)
            .resources(FunctionExecutorResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .max_concurrency(1)
            .build()
            .unwrap();

        let fe_metadata = FunctionExecutorServerMetadata {
            executor_id: executor_id.clone(),
            function_executor: function_executor.clone(),
            desired_state: FunctionExecutorState::Running,
        };

        // Test case 1: No tasks - should return false
        let mut state = in_memory_state_bootstrap! { clock: 1 };
        assert!(!state.has_pending_tasks(&fe_metadata));

        // Test case 2: Add a terminal function run (Success) - should return false
        let terminal_run = create_function_run(
            "test-namespace",
            "test-graph",
            "inv-1",
            "test-function",
            Some(FunctionRunOutcome::Success),
        );
        state
            .function_runs
            .insert(terminal_run.clone().into(), Box::new(terminal_run));
        assert!(!state.has_pending_tasks(&fe_metadata));

        // Test case 3: Add a terminal function run (Failure) - should return false
        let terminal_run2 = create_function_run(
            "test-namespace",
            "test-graph",
            "inv-2",
            "test-function",
            Some(FunctionRunOutcome::Failure(
                FunctionRunFailureReason::FunctionError,
            )),
        );
        state
            .function_runs
            .insert(terminal_run2.clone().into(), Box::new(terminal_run2));
        assert!(!state.has_pending_tasks(&fe_metadata));

        // Test case 4: Add a non-terminal function run (None outcome) - should return
        // true
        let pending_run = create_function_run(
            "test-namespace",
            "test-graph",
            "inv-3",
            "test-function",
            None,
        );
        state
            .function_runs
            .insert(pending_run.clone().into(), Box::new(pending_run));
        assert!(state.has_pending_tasks(&fe_metadata));

        // Test case 5: Add tasks for different namespace/graph - should not affect
        // result
        let different_run = create_function_run(
            "different-namespace",
            "different-graph",
            "inv-4",
            "test-function",
            None,
        );
        state
            .function_runs
            .insert(different_run.clone().into(), Box::new(different_run));
        assert!(state.has_pending_tasks(&fe_metadata));

        // Test case 6: Add tasks for same namespace/graph but different function -
        // should not affect result
        let different_fn_run = create_function_run(
            "test-namespace",
            "test-graph",
            "inv-5",
            "different-function",
            None,
        );
        state
            .function_runs
            .insert(different_fn_run.clone().into(), Box::new(different_fn_run));
        assert!(state.has_pending_tasks(&fe_metadata));

        // Test case 7: Add multiple pending tasks - should still return true
        let pending_run2 = create_function_run(
            "test-namespace",
            "test-graph",
            "inv-6",
            "test-function",
            None,
        );
        state
            .function_runs
            .insert(pending_run2.clone().into(), Box::new(pending_run2));
        assert!(state.has_pending_tasks(&fe_metadata));

        // Test case 8: Change all pending tasks to terminal - should return false
        let keys_to_update: Vec<FunctionRunKey> = state
            .function_runs
            .iter()
            .filter(|(key, function_run)| {
                key.0.starts_with("test-namespace|test-graph|")
                    && function_run.name == "test-function"
                    && function_run.outcome.is_none()
            })
            .map(|(key, _)| key.clone())
            .collect();

        for key in keys_to_update {
            if let Some(mut function_run) = state.function_runs.get(&key).cloned() {
                function_run.outcome = Some(FunctionRunOutcome::Success);
                state.function_runs.insert(key, function_run);
            }
        }
        assert!(!state.has_pending_tasks(&fe_metadata));

        let function_executor = FunctionExecutorBuilder::default()
            .id(FunctionExecutorId::new("test-fe-2".to_string()))
            .namespace("test-namespace".to_string())
            .application_name("test-graph".to_string())
            .function_name("different-function".to_string())
            .version(GraphVersion("1.0".to_string()))
            .state(FunctionExecutorState::Running)
            .resources(FunctionExecutorResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .max_concurrency(1)
            .build()
            .unwrap();

        // Test case 9: Test with different function executor metadata
        let fe_metadata2 = FunctionExecutorServerMetadata {
            executor_id: executor_id.clone(),
            function_executor,
            desired_state: FunctionExecutorState::Running,
        };
        assert!(state.has_pending_tasks(&fe_metadata2));

        // Test case 10: Change the different function task to terminal - should return
        // false
        let keys_to_update2: Vec<FunctionRunKey> = state
            .function_runs
            .iter()
            .filter(|(key, function_run)| {
                key.0.starts_with("test-namespace|test-graph|")
                    && function_run.name == "different-function"
                    && function_run.outcome.is_none()
            })
            .map(|(key, _)| key.clone())
            .collect();

        for key in keys_to_update2 {
            if let Some(mut function_run) = state.function_runs.get(&key).cloned() {
                function_run.outcome = Some(FunctionRunOutcome::Success);
                state.function_runs.insert(key, function_run);
            }
        }
        assert!(!state.has_pending_tasks(&fe_metadata2));
    }
}
