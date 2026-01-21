use std::{
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
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::{
    config::{DataplaneConfig, DriverConfig},
    driver::{DockerDriver, ForkExecDriver, ProcessDriver},
    function_container_manager::{DefaultImageResolver, FunctionContainerManager},
    resources::probe_host_resources,
};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const HEARTBEAT_RETRY_INTERVAL: Duration = Duration::from_secs(2);
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(300); // 5 minutes

pub struct Service {
    config: DataplaneConfig,
    channel: Channel,
    host_resources: HostResources,
    container_manager: Arc<FunctionContainerManager>,
}

impl Service {
    pub async fn new(config: DataplaneConfig) -> Result<Self> {
        let channel = create_channel(&config).await?;
        let host_resources = probe_host_resources();

        let driver: Arc<dyn ProcessDriver> = match &config.driver {
            DriverConfig::ForkExec => Arc::new(ForkExecDriver::new()),
            DriverConfig::Docker { socket_path } => match socket_path {
                Some(path) => Arc::new(DockerDriver::with_socket(path.clone())),
                None => Arc::new(DockerDriver::new()),
            },
        };

        let image_resolver = Arc::new(DefaultImageResolver);
        let container_manager = Arc::new(FunctionContainerManager::new(driver, image_resolver));

        Ok(Self {
            config,
            channel,
            host_resources,
            container_manager,
        })
    }

    pub async fn run(self) -> Result<()> {
        let executor_id = self.config.executor_id.clone();
        tracing::info!(%executor_id, "Starting dataplane service");

        let cancel_token = CancellationToken::new();
        let heartbeat_healthy = Arc::new(AtomicBool::new(false));
        let stream_notify = Arc::new(Notify::new());

        let heartbeat_handle = tokio::spawn({
            let channel = self.channel.clone();
            let executor_id = executor_id.clone();
            let heartbeat_healthy = heartbeat_healthy.clone();
            let stream_notify = stream_notify.clone();
            let host_resources = self.host_resources;
            let container_manager = self.container_manager.clone();
            let cancel_token = cancel_token.clone();
            async move {
                run_heartbeat_loop(
                    channel,
                    executor_id,
                    host_resources,
                    container_manager,
                    heartbeat_healthy,
                    stream_notify,
                    cancel_token,
                )
                .await
            }
        });

        let stream_handle = tokio::spawn({
            let channel = self.channel.clone();
            let executor_id = executor_id.clone();
            let heartbeat_healthy = heartbeat_healthy.clone();
            let stream_notify = stream_notify.clone();
            let container_manager = self.container_manager.clone();
            let cancel_token = cancel_token.clone();
            async move {
                run_desired_stream_loop(
                    channel,
                    executor_id,
                    container_manager,
                    heartbeat_healthy,
                    stream_notify,
                    cancel_token,
                )
                .await
            }
        });

        let health_check_handle = tokio::spawn({
            let container_manager = self.container_manager.clone();
            let cancel_token = cancel_token.clone();
            async move {
                container_manager.run_health_checks(cancel_token).await;
            }
        });

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Shutdown signal received, cancelling tasks");
                cancel_token.cancel();
            }
            result = heartbeat_handle => {
                if let Err(e) = result {
                    tracing::error!(error = %e, "Heartbeat task panicked");
                }
            }
            result = stream_handle => {
                if let Err(e) = result {
                    tracing::error!(error = %e, "Stream task panicked");
                }
            }
            result = health_check_handle => {
                if let Err(e) = result {
                    tracing::error!(error = %e, "Health check task panicked");
                }
            }
        }

        Ok(())
    }
}

async fn run_heartbeat_loop(
    channel: Channel,
    executor_id: String,
    host_resources: HostResources,
    container_manager: Arc<FunctionContainerManager>,
    heartbeat_healthy: Arc<AtomicBool>,
    stream_notify: Arc<Notify>,
    cancel_token: CancellationToken,
) {
    let mut client = ExecutorApiClient::new(channel);

    loop {
        if cancel_token.is_cancelled() {
            tracing::info!("Heartbeat loop cancelled");
            return;
        }

        let function_executor_states = container_manager.get_states().await;

        // Build executor state without state_hash first
        let mut executor_state = ExecutorState {
            executor_id: Some(executor_id.clone()),
            hostname: Some("localhost".to_string()),
            version: Some(env!("CARGO_PKG_VERSION").to_string()),
            status: Some(ExecutorStatus::Running.into()),
            total_resources: Some(host_resources),
            total_function_executor_resources: Some(host_resources),
            function_executor_states,
            ..Default::default()
        };

        // Compute state_hash by serializing and hashing
        let serialized = executor_state.encode_to_vec();
        let mut hasher = Sha256::new();
        hasher.update(&serialized);
        let hash = hasher.finalize();
        executor_state.state_hash = Some(format!("{:x}", hash));

        let request = ReportExecutorStateRequest {
            executor_state: Some(executor_state),
            executor_update: Some(ExecutorUpdate {
                executor_id: Some(executor_id.clone()),
                allocation_results: vec![],
            }),
        };

        match client.report_executor_state(request).await {
            Ok(_) => {
                let was_healthy = heartbeat_healthy.swap(true, Ordering::SeqCst);
                if !was_healthy {
                    tracing::info!("Heartbeat succeeded, notifying stream to start");
                    stream_notify.notify_one();
                }
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        tracing::info!("Heartbeat loop cancelled");
                        return;
                    }
                    _ = tokio::time::sleep(HEARTBEAT_INTERVAL) => {}
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "Heartbeat failed, retrying");
                heartbeat_healthy.store(false, Ordering::SeqCst);
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        tracing::info!("Heartbeat loop cancelled");
                        return;
                    }
                    _ = tokio::time::sleep(HEARTBEAT_RETRY_INTERVAL) => {}
                }
            }
        }
    }
}

async fn run_desired_stream_loop(
    channel: Channel,
    executor_id: String,
    container_manager: Arc<FunctionContainerManager>,
    heartbeat_healthy: Arc<AtomicBool>,
    stream_notify: Arc<Notify>,
    cancel_token: CancellationToken,
) {
    loop {
        if cancel_token.is_cancelled() {
            tracing::info!("Stream loop cancelled");
            return;
        }

        // Wait for heartbeat to be healthy before starting stream
        while !heartbeat_healthy.load(Ordering::SeqCst) {
            tracing::debug!("Waiting for heartbeat to be healthy");
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!("Stream loop cancelled");
                    return;
                }
                _ = stream_notify.notified() => {}
            }
        }

        tracing::info!("Starting desired executor states stream");
        if let Err(e) = run_desired_stream(
            &channel,
            &executor_id,
            &container_manager,
            &heartbeat_healthy,
            &cancel_token,
        )
        .await
        {
            tracing::warn!(error = %e, "Desired stream ended");
        }

        // Small delay before reconnecting
        tokio::select! {
            _ = cancel_token.cancelled() => {
                tracing::info!("Stream loop cancelled");
                return;
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

async fn run_desired_stream(
    channel: &Channel,
    executor_id: &str,
    container_manager: &FunctionContainerManager,
    heartbeat_healthy: &AtomicBool,
    cancel_token: &CancellationToken,
) -> Result<()> {
    let mut client = ExecutorApiClient::new(channel.clone());

    let request = GetDesiredExecutorStatesRequest {
        executor_id: Some(executor_id.to_string()),
    };

    let response = client
        .get_desired_executor_states(request)
        .await
        .context("Failed to open desired states stream")?;

    let mut stream = response.into_inner();

    loop {
        // Check if cancelled
        if cancel_token.is_cancelled() {
            tracing::info!("Stream cancelled");
            return Ok(());
        }

        // Check if heartbeat is still healthy
        if !heartbeat_healthy.load(Ordering::SeqCst) {
            tracing::warn!("Heartbeat unhealthy, disconnecting stream");
            return Ok(());
        }

        let message = tokio::select! {
            _ = cancel_token.cancelled() => {
                tracing::info!("Stream cancelled");
                return Ok(());
            }
            result = tokio::time::timeout(STREAM_IDLE_TIMEOUT, stream.message()) => result
        };

        match message {
            Ok(Ok(Some(state))) => {
                handle_desired_state(state, container_manager).await;
            }
            Ok(Ok(None)) => {
                tracing::info!("Stream closed by server");
                return Ok(());
            }
            Ok(Err(e)) => {
                return Err(e).context("Stream error");
            }
            Err(_) => {
                tracing::warn!("Stream idle timeout, reconnecting");
                return Ok(());
            }
        }
    }
}

async fn handle_desired_state(
    state: DesiredExecutorState,
    container_manager: &FunctionContainerManager,
) {
    let num_fes = state.function_executors.len();
    let num_allocs = state.allocations.len();
    let clock = state.clock.unwrap_or(0);

    tracing::info!(
        clock,
        num_function_executors = num_fes,
        num_allocations = num_allocs,
        "Received desired executor state"
    );

    // Sync containers with desired state
    container_manager.sync(state.function_executors).await;
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

    let channel = endpoint
        .connect()
        .await
        .context("Failed to connect to server")?;

    tracing::info!(server_addr = %config.server_addr, "Connected to server");
    Ok(channel)
}
