use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use proto_api::executor_api_pb::{
    AllocationStreamRequest,
    ExecutorStatus,
    GetCommandStreamRequest,
    HostResources,
    executor_api_client::ExecutorApiClient,
};
use tokio::{
    sync::{Mutex, Notify, mpsc},
    task::JoinSet,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::Instrument;

use crate::{
    allocation_result_dispatcher::AllocationResultDispatcher,
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
    secrets::{NoopSecretsProvider, SecretsProvider},
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
    allocation_result_dispatcher: Arc<AllocationResultDispatcher>,
}

impl Service {
    /// Create a new dataplane service with default (no-op) providers.
    pub async fn new(config: DataplaneConfig) -> Result<Self> {
        let image_resolver: Arc<dyn ImageResolver> = Arc::new(DefaultImageResolver::new(
            config.default_function_image.clone(),
        ));
        let secrets_provider: Arc<dyn SecretsProvider> = Arc::new(NoopSecretsProvider::new());
        Self::with_providers(config, image_resolver, secrets_provider).await
    }

    /// Create a new dataplane service with custom image resolver and secrets
    /// provider.
    ///
    /// This is the extension point used by custom binaries (e.g.
    /// compute-engine-internal) to inject platform-specific implementations.
    pub async fn with_providers(
        config: DataplaneConfig,
        image_resolver: Arc<dyn ImageResolver>,
        secrets_provider: Arc<dyn SecretsProvider>,
    ) -> Result<Self> {
        let channel = create_channel(&config).await?;
        let discovered_gpus = crate::gpu_allocator::discover_gpus();
        let mut host_resources = probe_host_resources(&discovered_gpus);

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

        tracing::info!(
            cpu_count = ?host_resources.cpu_count,
            memory_bytes = ?host_resources.memory_bytes,
            disk_bytes = ?host_resources.disk_bytes,
            gpu_count = discovered_gpus.len(),
            gpu_model = ?host_resources.gpu.as_ref().and_then(|g| g.model),
            "Host resources discovered"
        );

        let metrics = Arc::new(DataplaneMetrics::new());

        let driver = create_process_driver(&config)?;

        let (result_tx, result_rx) = mpsc::unbounded_channel(); // CommandResponse (acks + container events)
        let (container_state_tx, container_state_rx) = mpsc::unbounded_channel(); // CommandResponse (ContainerTerminated)
        let (activity_tx, activity_rx) = mpsc::unbounded_channel(); // AllocationStreamRequest (completed/failed)

        let state_file = Arc::new(
            StateFile::new(&config.state_file)
                .await
                .context("Failed to initialize state file")?,
        );
        let container_manager = Arc::new(FunctionContainerManager::new(
            driver.clone(),
            image_resolver.clone(),
            secrets_provider.clone(),
            metrics.clone(),
            state_file,
            config.executor_id.clone(),
            container_state_tx.clone(),
        ));

        let blob_store = create_blob_store(&config, &metrics).await?;
        let code_cache = Arc::new(CodeCache::new(
            PathBuf::from(&config.function_executor.code_cache_path),
            blob_store.clone(),
            metrics.clone(),
        ));
        let state_change_notify = Arc::new(Notify::new());

        let gpu_allocator = Arc::new(crate::gpu_allocator::GpuAllocator::new(discovered_gpus));

        let spawn_config = FESpawnConfig {
            driver: driver.clone(),
            image_resolver,
            gpu_allocator,
            secrets_provider,
            result_tx,
            container_state_tx,
            activity_tx,
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

        let allocation_result_dispatcher = AllocationResultDispatcher::new();

        let cancel_token = CancellationToken::new();
        let state_reconciler = Arc::new(Mutex::new(StateReconciler::new(
            container_manager.clone(),
            spawn_config,
            cancel_token,
            state_change_notify.clone(),
            allocation_result_dispatcher.clone(),
        )));

        let state_reporter = Arc::new(StateReporter::new(
            result_rx,
            container_state_rx,
            activity_rx,
        ));

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
            allocation_result_dispatcher,
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
            command_stream_last_seq: Arc::new(AtomicU64::new(0)),
            allocation_result_dispatcher: self.allocation_result_dispatcher.clone(),
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
            async move { rt.run_command_stream_loop().await }.instrument(span)
        });

        tasks.spawn({
            let span = span.clone();
            let rt = runtime.clone();
            async move { rt.run_allocation_stream_loop().await }.instrument(span)
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
    /// Last processed command sequence number for the command stream.
    command_stream_last_seq: Arc<AtomicU64>,
    /// Dispatcher for routing allocation stream results to allocation runners.
    allocation_result_dispatcher: Arc<AllocationResultDispatcher>,
}

impl ServiceRuntime {
    /// Build a full state sync message (identity + all containers).
    fn build_full_state(
        &self,
        fe_states: &[proto_api::executor_api_pb::ContainerState],
    ) -> proto_api::executor_api_pb::DataplaneStateFullSync {
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "localhost".to_string());
        proto_api::executor_api_pb::DataplaneStateFullSync {
            hostname: Some(hostname),
            version: Some(env!("CARGO_PKG_VERSION").to_string()),
            total_resources: Some(self.identity.host_resources),
            total_container_resources: Some(self.identity.host_resources),
            allowed_functions: self.identity.allowed_functions.clone(),
            labels: self.identity.labels.clone(),
            catalog_entry_name: None,
            proxy_address: Some(self.identity.proxy_address.clone()),
            container_states: fe_states.to_vec(),
        }
    }

    async fn run_heartbeat_loop(&self) {
        let mut client = ExecutorApiClient::new(self.channel.clone());
        let mut retry_interval = HEARTBEAT_MIN_RETRY_INTERVAL;
        let mut send_full_state = false; // First heartbeat has no state; server asks for it

        loop {
            if self.cancel_token.is_cancelled() {
                tracing::info!("Heartbeat loop cancelled");
                return;
            }

            // Phase 1: Send pending command responses.
            //
            // collect_responses() clones items from the buffer. On success,
            // drain_sent() removes the sent count from the front. On failure,
            // items stay in the buffer for retry (no action needed).
            let mut results_failed = false;
            loop {
                let (responses, sent_count, has_remaining) =
                    self.state_reporter.collect_responses(0).await;

                if responses.is_empty() {
                    break;
                }

                let request = proto_api::executor_api_pb::ReportCommandResponsesRequest {
                    executor_id: self.identity.executor_id.clone(),
                    responses,
                };

                self.metrics.counters.state_report_rpcs.add(1, &[]);

                if has_remaining {
                    self.metrics
                        .counters
                        .state_report_message_fragmentations
                        .add(1, &[]);
                }

                match client.report_command_responses(request).await {
                    Ok(_) => {
                        // Remove the sent items from the front of the buffer.
                        self.state_reporter.drain_sent(sent_count).await;
                        self.metrics.counters.record_heartbeat(true);
                        retry_interval = HEARTBEAT_MIN_RETRY_INTERVAL;
                        self.heartbeat_healthy.store(true, Ordering::SeqCst);
                        if !has_remaining {
                            break;
                        }
                        // More responses pending, loop to send next batch
                    }
                    Err(e) => {
                        // Items stay in the buffer for retry — no action needed.
                        self.metrics.counters.record_heartbeat(false);
                        self.metrics.counters.state_report_rpc_errors.add(1, &[]);
                        tracing::warn!(
                            error = %e,
                            server_addr = %self.identity.server_addr,
                            "report_command_responses failed"
                        );
                        self.heartbeat_healthy.store(false, Ordering::SeqCst);
                        send_full_state = true;
                        results_failed = true;
                        break;
                    }
                }
            }

            if results_failed {
                tokio::select! {
                    _ = self.cancel_token.cancelled() => return,
                    _ = tokio::time::sleep(retry_interval) => {}
                }
                retry_interval = std::cmp::min(
                    retry_interval * HEARTBEAT_BACKOFF_MULTIPLIER,
                    HEARTBEAT_MAX_RETRY_INTERVAL,
                );
                continue;
            }

            // Allocation activities (AllocationCompleted/AllocationFailed and
            // CallFunction log entries) are sent via the separate
            // run_allocation_stream_loop bidi RPC, not the heartbeat loop.

            // Phase 2: Build and send heartbeat
            let report_start = std::time::Instant::now();

            // Build full_state payload if the server requested it
            let full_state = if send_full_state {
                tracing::info!("Sending full state in heartbeat");
                let reconciler_guard = self.state_reconciler.lock().await;
                let fe_states = reconciler_guard.get_all_fe_states().await;
                drop(reconciler_guard);
                Some(self.build_full_state(&fe_states))
            } else {
                None
            };

            let heartbeat_req = proto_api::executor_api_pb::HeartbeatRequest {
                executor_id: Some(self.identity.executor_id.clone()),
                status: Some(ExecutorStatus::Running.into()),
                full_state,
            };

            let request_size = prost::Message::encoded_len(&heartbeat_req);
            self.metrics.counters.state_report_rpcs.add(1, &[]);
            self.metrics
                .histograms
                .state_report_message_size_mb
                .record(request_size as f64 / (1024.0 * 1024.0), &[]);

            // Store reported state for monitoring endpoint
            *self.monitoring_state.last_reported_state.lock().await =
                Some(format!("{:#?}", heartbeat_req));

            match client.heartbeat(heartbeat_req).await {
                Ok(response) => {
                    self.metrics
                        .histograms
                        .state_report_rpc_latency_seconds
                        .record(report_start.elapsed().as_secs_f64(), &[]);
                    self.metrics.counters.record_heartbeat(true);
                    retry_interval = HEARTBEAT_MIN_RETRY_INTERVAL;
                    send_full_state = false;

                    // Check if server needs full state
                    let resp = response.into_inner();
                    let server_needs_state = resp.send_state.unwrap_or(false);
                    if server_needs_state {
                        tracing::info!("Server requested full state");
                        send_full_state = true;
                    }

                    // Mark monitoring as ready after first successful heartbeat
                    self.monitoring_state.ready.store(true, Ordering::SeqCst);

                    // Only mark healthy when the server knows our executor
                    // (send_state == false). When the server asks for full state,
                    // the executor isn't registered in runtime_data yet, so the
                    // command stream would be rejected with "executor not found".
                    if !server_needs_state {
                        let was_healthy = self.heartbeat_healthy.swap(true, Ordering::SeqCst);
                        if !was_healthy {
                            tracing::info!("Heartbeat succeeded, notifying stream to start");
                            self.stream_notify.notify_waiters();
                        }
                    }
                }
                Err(e) => {
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
                    send_full_state = true;
                    tokio::select! {
                        _ = self.cancel_token.cancelled() => return,
                        _ = tokio::time::sleep(retry_interval) => {}
                    }
                    retry_interval = std::cmp::min(
                        retry_interval * HEARTBEAT_BACKOFF_MULTIPLIER,
                        HEARTBEAT_MAX_RETRY_INTERVAL,
                    );
                    continue;
                }
            }

            // Phase 3: Wait for next trigger
            // If the server requested full state, send it immediately without
            // waiting for the heartbeat interval.
            if send_full_state {
                continue;
            }

            let results_notify = self.state_reporter.results_notify();
            let reconciler = self.state_reconciler.lock().await;
            let state_change_notify = reconciler.state_change_notify();
            drop(reconciler);

            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    tracing::info!("Heartbeat loop cancelled");
                    return;
                }
                _ = results_notify.notified() => {
                    // Results available, send immediately
                }
                _ = state_change_notify.notified() => {
                    // FE added/removed, send immediately
                }
                _ = tokio::time::sleep(HEARTBEAT_INTERVAL) => {}
            }
        }
    }

    /// Outer reconnect loop for the command stream.
    async fn run_command_stream_loop(&self) {
        loop {
            if self.cancel_token.is_cancelled() {
                tracing::info!("Command stream loop cancelled");
                return;
            }

            // Wait for heartbeat to be healthy before starting stream
            while !self.heartbeat_healthy.load(Ordering::SeqCst) {
                tracing::debug!("Command stream waiting for heartbeat to be healthy");
                tokio::select! {
                    _ = self.cancel_token.cancelled() => {
                        tracing::info!("Command stream loop cancelled");
                        return;
                    }
                    _ = self.stream_notify.notified() => {}
                }
            }

            tracing::info!("Starting command stream");
            if let Err(e) = self.run_command_stream().await {
                self.metrics.counters.record_stream_disconnection("error");
                tracing::warn!(error = %e, "Command stream ended");
            }

            // Small delay before reconnecting
            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    tracing::info!("Command stream loop cancelled");
                    return;
                }
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }

    /// Single connection for the command stream.
    async fn run_command_stream(&self) -> Result<()> {
        let mut client = ExecutorApiClient::new(self.channel.clone());
        let last_seq = self.command_stream_last_seq.load(Ordering::SeqCst);

        let request = GetCommandStreamRequest {
            executor_id: self.identity.executor_id.clone(),
            last_seq,
        };

        let response = client
            .command_stream(request)
            .await
            .context("Failed to open command stream")?;

        let mut stream = response.into_inner();

        loop {
            if self.cancel_token.is_cancelled() {
                tracing::info!("Command stream cancelled");
                return Ok(());
            }

            if !self.heartbeat_healthy.load(Ordering::SeqCst) {
                self.metrics
                    .counters
                    .record_stream_disconnection("heartbeat_unhealthy");
                tracing::warn!("Heartbeat unhealthy, disconnecting command stream");
                return Ok(());
            }

            let message = tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    tracing::info!("Command stream cancelled");
                    return Ok(());
                }
                result = tokio::time::timeout(STREAM_IDLE_TIMEOUT, stream.message()) => result
            };

            match message {
                Ok(Ok(Some(command))) => {
                    self.handle_command(command).await;
                }
                Ok(Ok(None)) => {
                    self.metrics
                        .counters
                        .record_stream_disconnection("server_closed");
                    tracing::info!("Command stream closed by server");
                    return Ok(());
                }
                Ok(Err(e)) => {
                    return Err(e).context("Command stream error");
                }
                Err(_) => {
                    self.metrics
                        .counters
                        .record_stream_disconnection("idle_timeout");
                    tracing::warn!("Command stream idle timeout, reconnecting");
                    return Ok(());
                }
            }
        }
    }

    // ---------------------------------------------------------------
    // Allocation stream (bidi RPC for allocation traffic)
    // ---------------------------------------------------------------

    /// Outer reconnect loop for the allocation stream.
    async fn run_allocation_stream_loop(&self) {
        loop {
            if self.cancel_token.is_cancelled() {
                tracing::info!("Allocation stream loop cancelled");
                return;
            }

            // Wait for heartbeat to be healthy before starting stream
            while !self.heartbeat_healthy.load(Ordering::SeqCst) {
                tracing::debug!("Allocation stream waiting for heartbeat to be healthy");
                tokio::select! {
                    _ = self.cancel_token.cancelled() => {
                        tracing::info!("Allocation stream loop cancelled");
                        return;
                    }
                    _ = self.stream_notify.notified() => {}
                }
            }

            tracing::info!("Starting allocation stream");
            if let Err(e) = self.run_allocation_stream().await {
                tracing::warn!(error = %e, "Allocation stream ended");
            }

            // Small delay before reconnecting
            tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    tracing::info!("Allocation stream loop cancelled");
                    return;
                }
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }

    /// Single connection for the allocation stream (bidi).
    ///
    /// The outbound side drains allocation activities (AllocationCompleted,
    /// AllocationFailed, CallFunction log entries) from the state reporter
    /// buffer and sends them to the server.
    ///
    /// The inbound side receives FunctionCallResult log entries from the
    /// server for delivery to allocation runners.
    async fn run_allocation_stream(&self) -> Result<()> {
        let mut client = ExecutorApiClient::new(self.channel.clone());

        // Create a channel for outbound messages. The drainer task sends
        // items here; the ReceiverStream feeds them to the bidi stream.
        let (outbound_tx, outbound_rx) = mpsc::channel::<AllocationStreamRequest>(64);

        // Open the bidi stream
        let response = client
            .allocation_stream(ReceiverStream::new(outbound_rx))
            .await
            .context("Failed to open allocation stream")?;

        let mut inbound = response.into_inner();

        // Spawn a task to drain activities from state_reporter and send
        // via outbound_tx. The task ends when the cancel token fires, the
        // outbound_tx is dropped (stream closed), or the receiver side of
        // outbound_tx is dropped (stream reconnecting).
        let drainer_cancel = self.cancel_token.clone();
        let drainer_reporter = self.state_reporter.clone();
        let drainer_executor_id = self.identity.executor_id.clone();
        let drainer_tx = outbound_tx.clone();
        let drainer_handle = tokio::spawn(async move {
            Self::drain_activities_to_stream(
                drainer_reporter,
                drainer_tx,
                drainer_executor_id,
                drainer_cancel,
            )
            .await;
        });

        // Main loop: read from inbound stream
        let result = loop {
            let message = tokio::select! {
                _ = self.cancel_token.cancelled() => {
                    break Ok(());
                }
                result = tokio::time::timeout(STREAM_IDLE_TIMEOUT, inbound.message()) => result
            };

            match message {
                Ok(Ok(Some(response))) => {
                    self.handle_allocation_stream_response(response);
                }
                Ok(Ok(None)) => {
                    tracing::info!("Allocation stream closed by server");
                    break Ok(());
                }
                Ok(Err(e)) => {
                    break Err(e).context("Allocation stream error");
                }
                Err(_) => {
                    tracing::warn!("Allocation stream idle timeout, reconnecting");
                    break Ok(());
                }
            }
        };

        // Drop the outbound sender to signal the drainer task to stop, and
        // wait for it to finish.
        drop(outbound_tx);
        let _ = drainer_handle.await;

        result
    }

    /// Background task: drain activities from the state reporter buffer and
    /// forward them via the outbound channel to the bidi stream.
    async fn drain_activities_to_stream(
        reporter: Arc<StateReporter>,
        tx: mpsc::Sender<AllocationStreamRequest>,
        executor_id: String,
        cancel_token: CancellationToken,
    ) {
        let activities_notify = reporter.activities_notify();

        loop {
            // Wait for activities to arrive
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    return;
                }
                _ = activities_notify.notified() => {}
            }

            // Drain all pending activities and send them
            let items = reporter.drain_all_activities().await;
            for mut item in items {
                // Ensure executor_id is set on every outbound message
                if item.executor_id.is_empty() {
                    item.executor_id.clone_from(&executor_id);
                }

                if tx.send(item).await.is_err() {
                    // Receiver dropped — stream is closing
                    return;
                }
            }
        }
    }

    /// Handle a single response from the allocation stream.
    ///
    /// Dispatches incoming `FunctionCallResult` entries to the allocation
    /// runner handling the target allocation via the shared
    /// `AllocationResultDispatcher`.
    fn handle_allocation_stream_response(
        &self,
        response: proto_api::executor_api_pb::AllocationStreamResponse,
    ) {
        if let Some(ref log_entry) = response.log_entry {
            let allocation_id = &log_entry.allocation_id;
            match &log_entry.entry {
                Some(
                    proto_api::executor_api_pb::allocation_log_entry::Entry::FunctionCallResult(
                        result,
                    ),
                ) => {
                    tracing::debug!(
                        allocation_id = %allocation_id,
                        function_call_id = ?result.function_call_id,
                        "Received FunctionCallResult from allocation stream"
                    );
                    let dispatcher = self.allocation_result_dispatcher.clone();
                    let alloc_id = allocation_id.clone();
                    let resp = response;
                    tokio::spawn(async move {
                        if !dispatcher.dispatch(&alloc_id, resp).await {
                            tracing::warn!(
                                allocation_id = %alloc_id,
                                "No allocation runner registered for result dispatch"
                            );
                        }
                    });
                }
                Some(proto_api::executor_api_pb::allocation_log_entry::Entry::CallFunction(_)) => {
                    tracing::warn!(
                        allocation_id = %allocation_id,
                        "Unexpected CallFunction entry from server on allocation stream"
                    );
                }
                None => {
                    tracing::warn!(
                        allocation_id = %allocation_id,
                        "Received empty log entry on allocation stream"
                    );
                }
            }
        }
    }

    /// Handle a single Command from the command stream.
    async fn handle_command(&self, command: proto_api::executor_api_pb::Command) {
        let seq = command.seq;

        let Some(cmd) = command.command else {
            tracing::warn!(seq, "Received command with no payload, skipping");
            self.command_stream_last_seq
                .fetch_max(seq, Ordering::SeqCst);
            return;
        };

        use proto_api::executor_api_pb::command::Command as Cmd;
        match cmd {
            Cmd::AddContainer(add) => {
                if let Some(container) = add.container {
                    if let Err(e) = validation::validate_fe_description(&container) {
                        tracing::warn!(
                            seq,
                            container_id = ?container.id,
                            namespace = ?container.function.as_ref().and_then(|f| f.namespace.as_deref()),
                            app = ?container.function.as_ref().and_then(|f| f.application_name.as_deref()),
                            "fn" = ?container.function.as_ref().and_then(|f| f.function_name.as_deref()),
                            sandbox_id = ?container.sandbox_metadata.as_ref().and_then(|m| m.sandbox_id.as_deref()),
                            pool_id = ?container.pool_id.as_deref(),
                            error = %e,
                            "Skipping invalid AddContainer command"
                        );
                    } else {
                        tracing::info!(
                            seq,
                            container_id = ?container.id,
                            namespace = ?container.function.as_ref().and_then(|f| f.namespace.as_deref()),
                            app = ?container.function.as_ref().and_then(|f| f.application_name.as_deref()),
                            "fn" = ?container.function.as_ref().and_then(|f| f.function_name.as_deref()),
                            sandbox_id = ?container.sandbox_metadata.as_ref().and_then(|m| m.sandbox_id.as_deref()),
                            pool_id = ?container.pool_id.as_deref(),
                            "AddContainer command"
                        );
                        let mut reconciler = self.state_reconciler.lock().await;
                        reconciler
                            .reconcile_containers(vec![container], vec![])
                            .await;
                    }
                }
            }
            Cmd::RemoveContainer(remove) => {
                tracing::info!(
                    seq,
                    container_id = %remove.container_id,
                    "RemoveContainer command"
                );
                let mut reconciler = self.state_reconciler.lock().await;
                reconciler
                    .reconcile_containers(vec![], vec![remove.container_id])
                    .await;
            }
            Cmd::RunAllocation(run) => {
                if let Some(allocation) = run.allocation {
                    if let Err(e) = validation::validate_allocation(&allocation) {
                        tracing::warn!(
                            seq,
                            allocation_id = ?allocation.allocation_id,
                            request_id = ?allocation.request_id,
                            container_id = ?allocation.container_id,
                            namespace = ?allocation.function.as_ref().and_then(|f| f.namespace.as_deref()),
                            "fn" = ?allocation.function.as_ref().and_then(|f| f.function_name.as_deref()),
                            error = %e,
                            "Skipping invalid RunAllocation command"
                        );
                    } else if let Some(fe_id) = allocation.container_id.clone() {
                        tracing::info!(
                            seq,
                            allocation_id = ?allocation.allocation_id,
                            request_id = ?allocation.request_id,
                            container_id = %fe_id,
                            namespace = ?allocation.function.as_ref().and_then(|f| f.namespace.as_deref()),
                            "fn" = ?allocation.function.as_ref().and_then(|f| f.function_name.as_deref()),
                            "RunAllocation command"
                        );
                        let mut reconciler = self.state_reconciler.lock().await;
                        reconciler
                            .reconcile_allocations(vec![(fe_id, allocation, seq)])
                            .await;
                    } else {
                        tracing::warn!(
                            seq,
                            allocation_id = ?allocation.allocation_id,
                            request_id = ?allocation.request_id,
                            namespace = ?allocation.function.as_ref().and_then(|f| f.namespace.as_deref()),
                            "fn" = ?allocation.function.as_ref().and_then(|f| f.function_name.as_deref()),
                            "RunAllocation missing container_id"
                        );
                    }
                }
            }
            Cmd::KillAllocation(kill) => {
                tracing::warn!(
                    seq,
                    allocation_id = %kill.allocation_id,
                    "KillAllocation command received (not yet implemented)"
                );
            }
            // DeliverResult was removed from Command — function call results
            // are now delivered via the AllocationEvent log.
            Cmd::UpdateContainerDescription(update) => {
                tracing::info!(
                    seq,
                    container_id = %update.container_id,
                    sandbox_id = ?update.sandbox_metadata.as_ref().and_then(|m| m.sandbox_id.as_deref()),
                    "UpdateContainerDescription command"
                );
                let mut reconciler = self.state_reconciler.lock().await;
                reconciler.update_container_description(update).await;
            }
        }

        self.command_stream_last_seq
            .fetch_max(seq, Ordering::SeqCst);
    }
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
            binds,
        } => match address {
            Some(addr) => Ok(Arc::new(DockerDriver::with_address(
                addr,
                runtime.clone(),
                network.clone(),
                binds.clone(),
            )?)),
            None => Ok(Arc::new(DockerDriver::new(
                runtime.clone(),
                network.clone(),
                binds.clone(),
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
