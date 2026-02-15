use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use prost::Message;
use proto_api::executor_api_pb::{
    DesiredExecutorState,
    ExecutorState,
    ExecutorStatus,
    ExecutorUpdate,
    GetDesiredExecutorStatesRequest,
    HostResources,
    ReportExecutorStateRequest,
    executor_api_client::ExecutorApiClient,
};
use sha2::{Digest, Sha256};
use tokio::{
    sync::{Mutex, Notify, mpsc},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::Instrument;

use crate::{
    blob_ops::BlobStore,
    code_cache::CodeCache,
    config::{DataplaneConfig, DriverConfig},
    driver::{DockerDriver, ForkExecDriver, ProcessDriver},
    function_container_manager::{DefaultImageResolver, FunctionContainerManager, ImageResolver},
    function_executor::controller::FESpawnConfig,
    http_proxy::run_http_proxy,
    metrics::DataplaneMetrics,
    monitoring::{MonitoringState, run_monitoring_server},
    resources::{probe_free_resources, probe_host_resources},
    state_file::StateFile,
    state_reconciler::StateReconciler,
    state_reporter::StateReporter,
    validation,
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(300); // 5 minutes

/// Heartbeat retry backoff parameters (matches Python executor's
/// state_reporter.py).
const HEARTBEAT_MIN_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const HEARTBEAT_MAX_RETRY_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes
const HEARTBEAT_BACKOFF_MULTIPLIER: u32 = 3;

pub struct Service {
    config: DataplaneConfig,
    channel: Channel,
    host_resources: HostResources,
    allowed_functions: Vec<proto_api::executor_api_pb::AllowedFunction>,
    container_manager: Arc<FunctionContainerManager>,
    metrics: Arc<DataplaneMetrics>,
    state_reconciler: Arc<Mutex<StateReconciler>>,
    state_reporter: Arc<StateReporter>,
    monitoring_state: Arc<MonitoringState>,
}

impl Service {
    pub async fn new(config: DataplaneConfig) -> Result<Self> {
        let channel = create_channel(&config).await?;
        let mut host_resources = probe_host_resources();

        // Apply resource overrides from config.
        if let Some(overrides) = &config.resource_overrides {
            if let Some(cpu) = overrides.cpu_count {
                host_resources.cpu_count = Some(cpu);
            }
            if let Some(mem) = overrides.memory_bytes {
                host_resources.memory_bytes = Some(mem);
            }
            if let Some(disk) = overrides.disk_bytes {
                host_resources.disk_bytes = Some(disk);
            }
            tracing::info!(
                cpu_count = ?host_resources.cpu_count,
                memory_bytes = ?host_resources.memory_bytes,
                disk_bytes = ?host_resources.disk_bytes,
                "Applied resource overrides from config"
            );
        }

        let metrics = Arc::new(DataplaneMetrics::new());

        let driver = create_process_driver(&config)?;
        let image_resolver: Arc<dyn ImageResolver> = Arc::new(DefaultImageResolver::new(
            config.default_function_image.clone(),
        ));

        let state_file = Arc::new(
            StateFile::new(&config.state_file)
                .await
                .context("Failed to initialize state file")?,
        );
        let container_manager = Arc::new(FunctionContainerManager::new(
            driver.clone(),
            image_resolver.clone(),
            metrics.clone(),
            state_file,
            config.executor_id.clone(),
        ));

        let blob_store = create_blob_store(&config, &metrics).await?;
        let code_cache = Arc::new(CodeCache::new(
            PathBuf::from(&config.function_executor.code_cache_path),
            blob_store.clone(),
            metrics.clone(),
        ));

        let (result_tx, result_rx) = mpsc::unbounded_channel();
        let state_change_notify = Arc::new(Notify::new());

        let spawn_config = FESpawnConfig {
            driver: driver.clone(),
            image_resolver,
            result_tx,
            server_channel: channel.clone(),
            blob_store,
            code_cache,
            executor_id: config.executor_id.clone(),
            fe_binary_path: config
                .function_executor
                .fe_binary_path
                .clone()
                .unwrap_or_else(|| "function-executor".to_string()),
            metrics: metrics.clone(),
        };

        let cancel_token = CancellationToken::new();
        let state_reconciler = Arc::new(Mutex::new(StateReconciler::new(
            container_manager.clone(),
            spawn_config,
            cancel_token,
            state_change_notify.clone(),
        )));

        let state_reporter = Arc::new(StateReporter::new(result_rx));

        let allowed_functions = config.parse_allowed_functions();
        if !allowed_functions.is_empty() {
            tracing::info!(
                count = allowed_functions.len(),
                "Function allowlist configured"
            );
        }

        let monitoring_state = Arc::new(MonitoringState::new(Arc::new(AtomicBool::new(false))));

        Ok(Self {
            config,
            channel,
            host_resources,
            allowed_functions,
            container_manager,
            metrics,
            state_reconciler,
            state_reporter,
            monitoring_state,
        })
    }

    pub async fn run(self) -> Result<()> {
        let executor_id = self.config.executor_id.clone();
        tracing::info!("Starting dataplane service");

        // Capture the current span (from start_dataplane's #[instrument]) which
        // contains executor_id. We'll propagate it to all spawned tasks so every
        // log line includes executor_id.
        let span = tracing::Span::current();

        // Recover containers from previous run
        let recovered = self.container_manager.recover().await;
        if recovered > 0 {
            tracing::info!(recovered, "Recovered containers from state file");
        }

        // Clean up orphaned containers (exist in Docker but not in state file)
        let cleaned = self.container_manager.cleanup_orphans().await;
        if cleaned > 0 {
            tracing::info!(cleaned, "Cleaned up orphaned containers");
        }

        let cancel_token = CancellationToken::new();
        let heartbeat_healthy = self.monitoring_state.heartbeat_healthy.clone();
        let stream_notify = Arc::new(Notify::new());

        let runtime = Arc::new(ServiceRuntime {
            channel: self.channel.clone(),
            identity: ExecutorIdentity {
                executor_id: executor_id.clone(),
                host_resources: self.host_resources,
                allowed_functions: self.allowed_functions.clone(),
                labels: self.config.labels.clone(),
                proxy_address: self.config.http_proxy.get_advertise_address(),
                server_addr: self.config.server_addr.clone(),
            },
            state_reconciler: self.state_reconciler.clone(),
            state_reporter: self.state_reporter.clone(),
            heartbeat_healthy: heartbeat_healthy.clone(),
            stream_notify: stream_notify.clone(),
            cancel_token: cancel_token.clone(),
            metrics: self.metrics.clone(),
            monitoring_state: self.monitoring_state.clone(),
        });

        let mut tasks = JoinSet::new();

        tasks.spawn({
            let span = span.clone();
            let rt = runtime.clone();
            async move { rt.run_heartbeat_loop().await }.instrument(span)
        });

        tasks.spawn({
            let span = span.clone();
            let rt = runtime.clone();
            async move { rt.run_desired_stream_loop().await }.instrument(span)
        });

        tasks.spawn({
            let span = span.clone();
            let container_manager = self.container_manager.clone();
            let cancel_token = cancel_token.clone();
            async move { container_manager.run_health_checks(cancel_token).await }.instrument(span)
        });

        tasks.spawn({
            let span = span.clone();
            let metrics = self.metrics.clone();
            let cancel_token = cancel_token.clone();
            async move { run_metrics_update_loop(metrics, cancel_token).await }.instrument(span)
        });

        tasks.spawn({
            let span = span.clone();
            let cancel_token = cancel_token.clone();
            let http_proxy_config = self.config.http_proxy.clone();
            let container_manager = self.container_manager.clone();
            let executor_id = executor_id.clone();
            async move {
                if let Err(e) = run_http_proxy(
                    http_proxy_config,
                    container_manager,
                    executor_id,
                    cancel_token,
                )
                .await
                {
                    tracing::error!(error = %e, "HTTP proxy server error");
                }
            }
            .instrument(span)
        });

        tasks.spawn({
            let span = span.clone();
            let cancel_token = cancel_token.clone();
            let monitoring_state = self.monitoring_state.clone();
            let monitoring_addr = self.config.monitoring.socket_addr();
            async move {
                run_monitoring_server(&monitoring_addr, monitoring_state, cancel_token).await;
            }
            .instrument(span)
        });

        tokio::select! {
            signal_name = wait_for_shutdown_signal() => {
                tracing::info!(signal = signal_name, "Shutdown signal received");
                cancel_token.cancel();
                self.state_reconciler.lock().await.shutdown().await;
            }
            Some(result) = tasks.join_next() => {
                if let Err(e) = result {
                    tracing::error!(error = %e, "Background task panicked");
                }
            }
        }

        Ok(())
    }
}

const METRICS_UPDATE_INTERVAL: Duration = Duration::from_secs(5);

/// Periodically update resource availability metrics.
async fn run_metrics_update_loop(metrics: Arc<DataplaneMetrics>, cancel_token: CancellationToken) {
    let mut interval = tokio::time::interval(METRICS_UPDATE_INTERVAL);

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                tracing::info!("Metrics update loop cancelled");
                return;
            }
            _ = interval.tick() => {
                let resources = probe_free_resources();
                metrics.update_resources(resources).await;
            }
        }
    }
}

/// Static identity fields for this executor, used in heartbeats and stream
/// requests.
struct ExecutorIdentity {
    executor_id: String,
    host_resources: HostResources,
    allowed_functions: Vec<proto_api::executor_api_pb::AllowedFunction>,
    labels: std::collections::HashMap<String, String>,
    proxy_address: String,
    server_addr: String,
}

/// Shared runtime context for heartbeat and stream loops.
struct ServiceRuntime {
    channel: Channel,
    identity: ExecutorIdentity,
    state_reconciler: Arc<Mutex<StateReconciler>>,
    state_reporter: Arc<StateReporter>,
    heartbeat_healthy: Arc<AtomicBool>,
    stream_notify: Arc<Notify>,
    cancel_token: CancellationToken,
    metrics: Arc<DataplaneMetrics>,
    monitoring_state: Arc<MonitoringState>,
}

impl ServiceRuntime {
    /// Build the executor state with all fields and a SHA-256 state hash.
    async fn build_executor_state(
        &self,
        function_executor_states: Vec<proto_api::executor_api_pb::FunctionExecutorState>,
        function_call_watches: Vec<proto_api::executor_api_pb::FunctionCallWatch>,
    ) -> ExecutorState {
        let system_hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "localhost".to_string());

        let mut executor_state = ExecutorState {
            executor_id: Some(self.identity.executor_id.clone()),
            hostname: Some(system_hostname),
            version: Some(env!("CARGO_PKG_VERSION").to_string()),
            status: Some(ExecutorStatus::Running.into()),
            total_resources: Some(self.identity.host_resources),
            total_function_executor_resources: Some(self.identity.host_resources),
            allowed_functions: self.identity.allowed_functions.clone(),
            function_executor_states,
            function_call_watches,
            proxy_address: Some(self.identity.proxy_address.clone()),
            labels: self.identity.labels.clone(),
            ..Default::default()
        };

        let serialized = executor_state.encode_to_vec();
        let mut hasher = Sha256::new();
        hasher.update(&serialized);
        let hash = hasher.finalize();
        executor_state.state_hash = Some(format!("{:x}", hash));

        executor_state
    }

    /// Build a heartbeat request with allocation results, fragmenting to fit
    /// within the 10 MB message size limit.
    ///
    /// Returns `(request, reported_allocation_ids, has_remaining_results)`.
    async fn build_heartbeat_request(
        &self,
        executor_state: ExecutorState,
    ) -> (ReportExecutorStateRequest, Vec<String>, bool) {
        // Calculate base message size (without allocation results) for fragmentation.
        let base_request = ReportExecutorStateRequest {
            executor_state: Some(executor_state.clone()),
            executor_update: Some(ExecutorUpdate {
                executor_id: Some(self.identity.executor_id.clone()),
                allocation_results: vec![],
            }),
        };
        let base_message_size = base_request.encoded_len();

        // Collect allocation results that fit within the 10 MB message size limit.
        // Results are NOT removed from the buffer yet — only after successful RPC.
        let (allocation_results, has_remaining) =
            self.state_reporter.collect_results(base_message_size).await;

        let reported_ids: Vec<String> = allocation_results
            .iter()
            .filter_map(|r| r.allocation_id.clone())
            .collect();

        let request = ReportExecutorStateRequest {
            executor_state: Some(executor_state),
            executor_update: Some(ExecutorUpdate {
                executor_id: Some(self.identity.executor_id.clone()),
                allocation_results,
            }),
        };

        (request, reported_ids, has_remaining)
    }

    async fn run_heartbeat_loop(&self) {
        let mut client = ExecutorApiClient::new(self.channel.clone());
        let mut retry_interval = HEARTBEAT_MIN_RETRY_INTERVAL;

        loop {
            if self.cancel_token.is_cancelled() {
                tracing::info!("Heartbeat loop cancelled");
                return;
            }

            // Get FE states and function call watches from reconciler
            let reconciler_guard = self.state_reconciler.lock().await;
            let function_executor_states = reconciler_guard.get_all_fe_states().await;
            let function_call_watches = reconciler_guard.get_function_call_watches().await;
            drop(reconciler_guard);

            let executor_state = self
                .build_executor_state(function_executor_states, function_call_watches)
                .await;
            let (request, reported_ids, has_remaining) =
                self.build_heartbeat_request(executor_state).await;

            // Record state report metrics
            let report_start = std::time::Instant::now();
            let request_size = prost::Message::encoded_len(&request);
            self.metrics.counters.state_report_rpcs.add(1, &[]);
            self.metrics
                .histograms
                .state_report_message_size_mb
                .record(request_size as f64 / (1024.0 * 1024.0), &[]);

            // Store reported state for monitoring endpoint
            *self.monitoring_state.last_reported_state.lock().await =
                Some(format!("{:#?}", request));

            if has_remaining {
                self.metrics
                    .counters
                    .state_report_message_fragmentations
                    .add(1, &[]);
            }

            match client.report_executor_state(request).await {
                Ok(_) => {
                    self.metrics
                        .histograms
                        .state_report_rpc_latency_seconds
                        .record(report_start.elapsed().as_secs_f64(), &[]);
                    self.metrics.counters.record_heartbeat(true);
                    retry_interval = HEARTBEAT_MIN_RETRY_INTERVAL; // Reset backoff on success

                    // Remove successfully reported results from the buffer
                    self.state_reporter
                        .remove_reported_results(&reported_ids)
                        .await;

                    // Mark monitoring as ready after first successful heartbeat
                    self.monitoring_state.ready.store(true, Ordering::SeqCst);

                    let was_healthy = self.heartbeat_healthy.swap(true, Ordering::SeqCst);
                    if !was_healthy {
                        tracing::info!("Heartbeat succeeded, notifying stream to start");
                        self.stream_notify.notify_one();
                    }

                    // If more results remain due to fragmentation, send immediately
                    if has_remaining {
                        tracing::debug!(
                            "More allocation results pending, sending next heartbeat immediately"
                        );
                        continue;
                    }

                    let results_notify = self.state_reporter.results_notify();
                    let reconciler = self.state_reconciler.lock().await;
                    let watcher_notify = reconciler.watcher_notify();
                    let state_change_notify = reconciler.state_change_notify();
                    drop(reconciler);

                    tokio::select! {
                        _ = self.cancel_token.cancelled() => {
                            tracing::info!("Heartbeat loop cancelled");
                            return;
                        }
                        _ = results_notify.notified() => {
                            // Results available, send heartbeat immediately
                        }
                        _ = watcher_notify.notified() => {
                            // New watches registered, send heartbeat immediately
                        }
                        _ = state_change_notify.notified() => {
                            // FE added/removed, send heartbeat immediately
                        }
                        _ = tokio::time::sleep(HEARTBEAT_INTERVAL) => {}
                    }
                }
                Err(e) => {
                    // Results NOT removed from buffer — will be retried in next heartbeat
                    self.metrics.counters.record_heartbeat(false);
                    self.metrics.counters.state_report_rpc_errors.add(1, &[]);
                    self.metrics
                        .histograms
                        .state_report_rpc_latency_seconds
                        .record(report_start.elapsed().as_secs_f64(), &[]);
                    tracing::warn!(
                        error = %e,
                        server_addr = %self.identity.server_addr,
                        retry_in_secs = retry_interval.as_secs(),
                        "Heartbeat failed, retrying with backoff"
                    );
                    self.heartbeat_healthy.store(false, Ordering::SeqCst);
                    tokio::select! {
                        _ = self.cancel_token.cancelled() => {
                            tracing::info!("Heartbeat loop cancelled");
                            return;
                        }
                        _ = tokio::time::sleep(retry_interval) => {}
                    }
                    // Exponential backoff: 5s → 15s → 45s → 135s → 300s (capped)
                    retry_interval = std::cmp::min(
                        retry_interval * HEARTBEAT_BACKOFF_MULTIPLIER,
                        HEARTBEAT_MAX_RETRY_INTERVAL,
                    );
                }
            }
        }
    }

    async fn run_desired_stream_loop(&self) {
        loop {
            if self.cancel_token.is_cancelled() {
                tracing::info!("Stream loop cancelled");
                return;
            }

            // Wait for heartbeat to be healthy before starting stream
            while !self.heartbeat_healthy.load(Ordering::SeqCst) {
                tracing::debug!("Waiting for heartbeat to be healthy");
                tokio::select! {
                    _ = self.cancel_token.cancelled() => {
                        tracing::info!("Stream loop cancelled");
                        return;
                    }
                    _ = self.stream_notify.notified() => {}
                }
            }

            tracing::info!("Starting desired executor states stream");
            if let Err(e) = self.run_desired_stream().await {
                self.metrics.counters.record_stream_disconnection("error");
                tracing::warn!(error = %e, "Desired stream ended");
            }

            // Small delay before reconnecting
            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    tracing::info!("Stream loop cancelled");
                    return;
                }
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }
}

impl ServiceRuntime {
    async fn run_desired_stream(&self) -> Result<()> {
        let mut client = ExecutorApiClient::new(self.channel.clone());

        let request = GetDesiredExecutorStatesRequest {
            executor_id: Some(self.identity.executor_id.clone()),
        };

        let response = client
            .get_desired_executor_states(request)
            .await
            .context("Failed to open desired states stream")?;

        let mut stream = response.into_inner();

        loop {
            // Check if cancelled
            if self.cancel_token.is_cancelled() {
                tracing::info!("Stream cancelled");
                return Ok(());
            }

            // Check if heartbeat is still healthy
            if !self.heartbeat_healthy.load(Ordering::SeqCst) {
                self.metrics
                    .counters
                    .record_stream_disconnection("heartbeat_unhealthy");
                tracing::warn!("Heartbeat unhealthy, disconnecting stream");
                return Ok(());
            }

            let message = tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    tracing::info!("Stream cancelled");
                    return Ok(());
                }
                result = tokio::time::timeout(STREAM_IDLE_TIMEOUT, stream.message()) => result
            };

            match message {
                Ok(Ok(Some(state))) => {
                    handle_desired_state(
                        state,
                        &self.state_reconciler,
                        &self.metrics,
                        &self.monitoring_state,
                    )
                    .await;
                }
                Ok(Ok(None)) => {
                    self.metrics
                        .counters
                        .record_stream_disconnection("server_closed");
                    tracing::info!("Stream closed by server");
                    return Ok(());
                }
                Ok(Err(e)) => {
                    return Err(e).context("Stream error");
                }
                Err(_) => {
                    self.metrics
                        .counters
                        .record_stream_disconnection("idle_timeout");
                    tracing::warn!("Stream idle timeout, reconnecting");
                    return Ok(());
                }
            }
        }
    }
}

/// Maximum retries for reconciliation (total attempts = 1 + this value).
/// Matches Python executor's state_reconciler.py _reconcile_state().
const RECONCILIATION_MAX_RETRIES: u32 = 2;
/// Delay between reconciliation retry attempts.
const RECONCILIATION_RETRY_DELAY: Duration = Duration::from_secs(5);

async fn handle_desired_state(
    state: DesiredExecutorState,
    state_reconciler: &Arc<Mutex<StateReconciler>>,
    metrics: &DataplaneMetrics,
    monitoring_state: &MonitoringState,
) {
    let reconcile_start = std::time::Instant::now();
    let num_fes = state.function_executors.len();
    let num_allocs = state.allocations.len();
    let num_fc_results = state.function_call_results.len();
    let clock = state.clock.unwrap_or(0);

    // Store desired state for monitoring endpoint
    *monitoring_state.last_desired_state.lock().await = Some(format!("{:#?}", state));

    // Record metrics for desired state received
    metrics
        .counters
        .record_desired_state(num_fes as u64, num_allocs as u64);
    metrics.counters.state_reconciliations.add(1, &[]);

    // Update gauge values for desired state
    {
        let mut state = metrics.state.lock().await;
        state.last_desired_state_allocations = num_allocs as u64;
        state.last_desired_state_function_executors = num_fes as u64;
    }

    tracing::info!(
        clock,
        num_function_executors = num_fes,
        num_allocations = num_allocs,
        num_function_call_results = num_fc_results,
        "Received desired executor state"
    );

    use crate::retry::{Backoff, retry_with_backoff};

    match retry_with_backoff(
        RECONCILIATION_MAX_RETRIES,
        Backoff::Fixed(RECONCILIATION_RETRY_DELAY),
        "reconciliation",
        || try_reconcile(&state, state_reconciler),
        |_: &anyhow::Error| true,
    )
    .await
    {
        Ok(()) => {
            metrics
                .histograms
                .state_reconciliation_latency_seconds
                .record(reconcile_start.elapsed().as_secs_f64(), &[]);
        }
        Err(e) => {
            metrics.counters.state_reconciliation_errors.add(1, &[]);
            metrics
                .histograms
                .state_reconciliation_latency_seconds
                .record(reconcile_start.elapsed().as_secs_f64(), &[]);
            tracing::error!(error = %e, "Reconciliation failed after all retry attempts");
        }
    }
}

async fn try_reconcile(
    state: &DesiredExecutorState,
    state_reconciler: &Arc<Mutex<StateReconciler>>,
) -> Result<()> {
    // Validate FE descriptions, skip invalid ones
    let valid_fes: Vec<_> = state
        .function_executors
        .iter()
        .filter(|fe| {
            if let Err(e) = validation::validate_fe_description(fe) {
                tracing::warn!(
                    fe_id = ?fe.id,
                    error = %e,
                    "Skipping invalid FunctionExecutorDescription"
                );
                false
            } else {
                true
            }
        })
        .cloned()
        .collect();

    let mut reconciler = state_reconciler.lock().await;
    reconciler.reconcile(valid_fes).await;

    // Route allocations to their FE controllers, skip invalid ones
    for allocation in &state.allocations {
        if let Err(e) = validation::validate_allocation(allocation) {
            tracing::warn!(
                allocation_id = ?allocation.allocation_id,
                error = %e,
                "Skipping invalid Allocation"
            );
            continue;
        }
        if let Some(fe_id) = &allocation.function_executor_id {
            reconciler.add_allocation(fe_id, allocation.clone());
        }
    }

    // Route function call results to registered watchers
    if !state.function_call_results.is_empty() {
        reconciler
            .deliver_function_call_results(&state.function_call_results)
            .await;
    }

    Ok(())
}

/// Wait for any shutdown signal (SIGINT, SIGTERM, SIGQUIT).
/// Returns the name of the signal received.
async fn wait_for_shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigterm =
            signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");
        let mut sigquit = signal(SignalKind::quit()).expect("Failed to install SIGQUIT handler");

        tokio::select! {
            _ = tokio::signal::ctrl_c() => "SIGINT",
            _ = sigterm.recv() => "SIGTERM",
            _ = sigquit.recv() => "SIGQUIT",
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl_c");
        "SIGINT"
    }
}

/// Create the process driver based on config (ForkExec or Docker).
fn create_process_driver(config: &DataplaneConfig) -> Result<Arc<dyn ProcessDriver>> {
    match &config.driver {
        DriverConfig::ForkExec => Ok(Arc::new(ForkExecDriver::new())),
        DriverConfig::Docker {
            address,
            runtime,
            network,
        } => match address {
            Some(addr) => Ok(Arc::new(DockerDriver::with_address(
                addr,
                runtime.clone(),
                network.clone(),
            )?)),
            None => Ok(Arc::new(DockerDriver::new(
                runtime.clone(),
                network.clone(),
            )?)),
        },
    }
}

/// Create the blob store from config (S3/GCS URL or local filesystem).
async fn create_blob_store(
    config: &DataplaneConfig,
    metrics: &Arc<DataplaneMetrics>,
) -> Result<Arc<BlobStore>> {
    match &config.function_executor.blob_store_url {
        Some(url) => Ok(Arc::new(
            BlobStore::from_uri(url, metrics.clone())
                .await
                .context("Failed to create blob store")?,
        )),
        None => Ok(Arc::new(BlobStore::new_local(metrics.clone()))),
    }
}

async fn create_channel(config: &DataplaneConfig) -> Result<Channel> {
    let mut endpoint =
        Endpoint::from_shared(config.server_addr.clone()).context("Invalid server address")?;

    if config.tls.enabled {
        let mut tls_config = ClientTlsConfig::new();

        if let Some(domain) = &config.tls.domain_name {
            tls_config = tls_config.domain_name(domain.clone());
        }

        if let Some(ca_path) = &config.tls.ca_cert_path {
            let ca_cert = tokio::fs::read(ca_path)
                .await
                .context("Failed to read CA certificate")?;
            let ca_cert = tonic::transport::Certificate::from_pem(ca_cert);
            tls_config = tls_config.ca_certificate(ca_cert);
        }

        if let (Some(cert_path), Some(key_path)) =
            (&config.tls.client_cert_path, &config.tls.client_key_path)
        {
            let client_cert = tokio::fs::read(cert_path)
                .await
                .context("Failed to read client certificate")?;
            let client_key = tokio::fs::read(key_path)
                .await
                .context("Failed to read client key")?;
            let identity = tonic::transport::Identity::from_pem(client_cert, client_key);
            tls_config = tls_config.identity(identity);
        }

        endpoint = endpoint.tls_config(tls_config)?;
    }

    // Use connect_lazy() so the dataplane can start even if the server is
    // unavailable. The connection will be established when the first request is
    // made (heartbeat).
    let channel = endpoint.connect_lazy();

    tracing::info!(server_addr = %config.server_addr, "Channel created");
    Ok(channel)
}
