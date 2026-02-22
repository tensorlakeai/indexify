//! Container startup lifecycle.
//!
//! Handles image resolution, container creation, daemon connection,
//! network rule application, and post-startup result handling.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use proto_api::executor_api_pb::{
    CommandResponse,
    ContainerDescription,
    ContainerTerminationReason,
};
use tokio::sync::{RwLock, mpsc};

use super::{
    image_resolver::ImageResolver,
    types::{ContainerInfo, ContainerStore, container_type_str, update_container_counts},
};
use crate::{
    daemon_client::DaemonClient,
    driver::{ProcessConfig, ProcessDriver, ProcessHandle},
    metrics::DataplaneMetrics,
    secrets::SecretsProvider,
    snapshotter::Snapshotter,
    state_file::{PersistedContainer, StateFile},
};

pub(super) const DAEMON_READY_TIMEOUT: Duration = Duration::from_secs(60);

/// Start a container with the daemon and wait for it to be ready.
pub(super) async fn start_container_with_daemon(
    driver: &Arc<dyn ProcessDriver>,
    image_resolver: &Arc<dyn ImageResolver>,
    secrets_provider: &Arc<dyn SecretsProvider>,
    snapshotter: &Option<Arc<dyn Snapshotter>>,
    executor_id: &str,
    desc: &ContainerDescription,
    metrics: &Arc<DataplaneMetrics>,
) -> anyhow::Result<(ProcessHandle, DaemonClient)> {
    let info = ContainerInfo::from_description(desc, executor_id);

    // Check if this container should be restored from a snapshot
    let snapshot_uri = desc
        .sandbox_metadata
        .as_ref()
        .and_then(|m| m.snapshot_uri.clone())
        .filter(|u| !u.is_empty());

    let image = if let Some(ref uri) = snapshot_uri {
        // Restore from snapshot — download, decompress, import as Docker image
        let snapshotter = snapshotter.as_ref().ok_or_else(|| {
            anyhow::anyhow!("Snapshot restore requested but snapshotter not configured")
        })?;

        tracing::info!(
            container_id = %info.container_id,
            snapshot_uri = %uri,
            "Restoring container from snapshot"
        );

        let restore_result = snapshotter.restore_snapshot(uri).await?;
        restore_result.image
    } else if let Some(ref meta) = desc.sandbox_metadata &&
        let Some(ref img) = meta.image
    {
        // Prefer image from sandbox_metadata (server-provided)
        img.clone()
    } else if let Some(ref pool_id) = desc.pool_id {
        image_resolver
            .sandbox_image_for_pool(info.namespace, pool_id)
            .await?
    } else if let Some(sid) = info.sandbox_id {
        image_resolver.sandbox_image(info.namespace, sid).await?
    } else {
        anyhow::bail!("Cannot determine image: no sandbox_metadata.image, pool_id, or sandbox_id")
    };

    tracing::info!(
        container_id = %info.container_id,
        namespace = %info.namespace,
        image = %image,
        sandbox_id = ?info.sandbox_id,
        pool_id = ?desc.pool_id,
        "Image resolved for container"
    );

    // Extract resource limits from the function executor description.
    // Note: sandbox containers don't currently support GPU passthrough.
    // GPU allocation is handled by the FE controller for function containers.
    let resources = desc
        .resources
        .as_ref()
        .map(|r| crate::driver::ResourceLimits {
            cpu_millicores: r.cpu_ms_per_sec.map(|v| v as u64),
            memory_bytes: r.memory_bytes,
            gpu_device_ids: None,
        });

    // Start the container with the daemon as PID 1.
    // If entrypoint is provided in sandbox_metadata, pass it to the daemon to start
    // as a child process. Otherwise, daemon just waits for commands via its
    // HTTP API.
    let entrypoint = desc
        .sandbox_metadata
        .as_ref()
        .map(|m| m.entrypoint.clone())
        .unwrap_or_default();
    let (command, args) = if !entrypoint.is_empty() {
        let cmd = entrypoint[0].clone();
        let args: Vec<String> = entrypoint.iter().skip(1).cloned().collect();
        (cmd, args)
    } else {
        (String::new(), vec![])
    };

    // Build labels for container identification
    let container_type = container_type_str(desc);
    let labels = vec![
        ("indexify.managed".to_string(), "true".to_string()),
        ("indexify.type".to_string(), container_type.to_string()),
        ("indexify.namespace".to_string(), info.namespace.to_string()),
        ("indexify.application".to_string(), info.app.to_string()),
        ("indexify.function".to_string(), info.fn_name.to_string()),
        ("indexify.version".to_string(), info.app_version.to_string()),
        (
            "indexify.container_id".to_string(),
            info.container_id.to_string(),
        ),
    ];

    // Fetch secrets for this container
    let secrets = secrets_provider
        .fetch_secrets(executor_id, info.namespace, &desc.secret_names)
        .await?;
    let env: Vec<(String, String)> = secrets.into_iter().collect();

    let config = ProcessConfig {
        id: info.container_id.to_string(),
        process_type: crate::driver::ProcessType::Sandbox,
        image: Some(image),
        command,
        args,
        env,
        working_dir: None,
        resources,
        labels,
    };

    // Start the container (daemon will be PID 1)
    let handle = driver.start(config).await?;

    // Apply network firewall rules BEFORE daemon connection.
    // Container has IP now (cached in handle), but hasn't done any network requests
    // yet. This ensures network policy is enforced before any user code runs.
    if let Some(policy) = desc
        .sandbox_metadata
        .as_ref()
        .and_then(|m| m.network_policy.as_ref()) &&
        let Err(e) = crate::network_rules::apply_rules(&handle.id, &handle.container_ip, policy)
    {
        tracing::warn!(
            container_id = %info.container_id,
            executor_id = %info.executor_id,
            namespace = %info.namespace,
            container_handle_id = %handle.id,
            error = ?e,
            "Failed to apply network rules (continuing anyway)"
        );
        metrics.counters.sandbox_network_rules_errors.add(1, &[]);
        // Continue anyway - rules are defense-in-depth
    }

    // Get the daemon address from the handle
    let daemon_addr = handle
        .daemon_addr
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No daemon address available for container"))?;

    tracing::info!(
        container_id = %info.container_id,
        executor_id = %info.executor_id,
        namespace = %info.namespace,
        container_handle_id = %handle.id,
        daemon_addr = %daemon_addr,
        "Container started, connecting to daemon"
    );

    // Connect to the daemon with retry (container may take a moment to start)
    let connect_start = Instant::now();
    let daemon_result = async {
        let mut daemon_client =
            DaemonClient::connect_with_retry(daemon_addr, DAEMON_READY_TIMEOUT).await?;
        daemon_client.wait_for_ready(DAEMON_READY_TIMEOUT).await?;
        Ok::<_, anyhow::Error>(daemon_client)
    }
    .await;

    metrics
        .histograms
        .sandbox_daemon_connect_latency_seconds
        .record(connect_start.elapsed().as_secs_f64(), &[]);

    match daemon_result {
        Ok(daemon_client) => {
            tracing::info!(
                container_id = %info.container_id,
                executor_id = %info.executor_id,
                namespace = %info.namespace,
                container_handle_id = %handle.id,
                "Daemon ready, waiting for HTTP API commands"
            );
            Ok((handle, daemon_client))
        }
        Err(e) => {
            metrics.counters.sandbox_daemon_connect_errors.add(1, &[]);

            // Daemon failed to start — capture container logs for diagnostics
            let container_logs = match driver.get_logs(&handle, 50).await {
                Ok(logs) if !logs.is_empty() => logs,
                Ok(_) => "(no logs available)".to_string(),
                Err(log_err) => format!("(failed to fetch logs: {log_err})"),
            };

            tracing::error!(
                container_id = %info.container_id,
                executor_id = %info.executor_id,
                namespace = %info.namespace,
                container_handle_id = %handle.id,
                container_logs = %container_logs,
                error = ?e,
                "Daemon failed to start, killing container"
            );

            // Kill the orphaned container
            if let Err(kill_err) = driver.kill(&handle).await {
                tracing::warn!(
                    container_id = %info.container_id,
                    container_handle_id = %handle.id,
                    error = %kill_err,
                    "Failed to kill container after daemon startup failure"
                );
            }

            Err(e.context(format!(
                "Daemon failed to start. Container logs:\n{container_logs}"
            )))
        }
    }
}

/// Handle the result of a container startup attempt.
/// Called from the spawned lifecycle task after `start_container_with_daemon`
/// completes.
#[tracing::instrument(skip_all, fields(container_id = %id))]
pub(super) async fn handle_container_startup_result(
    id: String,
    desc: ContainerDescription,
    result: anyhow::Result<(ProcessHandle, DaemonClient)>,
    containers_ref: Arc<RwLock<ContainerStore>>,
    metrics: Arc<DataplaneMetrics>,
    state_file: Arc<StateFile>,
    container_state_tx: mpsc::UnboundedSender<CommandResponse>,
) {
    let mut containers = containers_ref.write().await;
    let Some(container) = containers.get_mut(&id) else {
        return;
    };

    let startup_duration_ms = container.created_at.elapsed().as_millis();
    let span = container.info().tracing_span();
    let container_type = container_type_str(&container.description);

    match result {
        Ok((handle, daemon_client)) => {
            metrics.counters.record_container_started(container_type);
            metrics
                .histograms
                .sandbox_startup_latency_seconds
                .record(startup_duration_ms as f64 / 1000.0, &[]);

            tracing::info!(
                parent: &span,
                handle_id = %handle.id,
                http_addr = ?handle.http_addr,
                container_type = %container_type,
                startup_duration_ms = %startup_duration_ms,
                event = "container_started",
                "Container started with daemon"
            );

            // Persist to state file for recovery after restart
            if let (Some(daemon_addr), Some(http_addr)) = (&handle.daemon_addr, &handle.http_addr) {
                let persisted = PersistedContainer {
                    container_id: id.clone(),
                    handle_id: handle.id.clone(),
                    daemon_addr: daemon_addr.clone(),
                    http_addr: http_addr.clone(),
                    container_ip: handle.container_ip.clone(),
                    started_at: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0),
                    description_proto: Some(PersistedContainer::encode_description(&desc)),
                };
                if let Err(e) = state_file.upsert(persisted).await {
                    tracing::warn!(
                        parent: &span,
                        error = ?e,
                        "Failed to persist container state"
                    );
                }
            }

            if let Err(e) = container.transition_to_running(handle, daemon_client) {
                tracing::warn!(parent: &span, error = ?e, "Invalid state transition on startup");
            } else {
                // Notify the server so it can promote sandbox status from
                // Pending to Running.
                super::FunctionContainerManager::send_container_started(&container_state_tx, &id);
            }

            update_container_counts(&containers, &metrics).await;
        }
        Err(e) => {
            metrics
                .counters
                .record_container_terminated(container_type, "startup_failed");
            metrics
                .histograms
                .sandbox_startup_latency_seconds
                .record(startup_duration_ms as f64 / 1000.0, &[]);

            tracing::error!(
                parent: &span,
                container_type = %container_type,
                startup_duration_ms = %startup_duration_ms,
                error = format!("{:#}", e),
                event = "container_startup_failed",
                "Failed to start container"
            );
            let reason = if e.downcast_ref::<crate::driver::ImageError>().is_some() {
                ContainerTerminationReason::StartupFailedBadImage
            } else {
                ContainerTerminationReason::StartupFailedInternalError
            };
            if container.transition_to_terminated(reason).is_ok() {
                super::FunctionContainerManager::send_container_terminated(
                    &container_state_tx,
                    &id,
                    reason,
                );
            }

            update_container_counts(&containers, &metrics).await;
        }
    }
}
