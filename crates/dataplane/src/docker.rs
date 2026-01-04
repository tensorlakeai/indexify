use std::collections::HashMap;

use anyhow::{Context, Result};
use async_trait::async_trait;
use bollard::models::ContainerCreateBody;
use bollard::query_parameters::{
    CreateContainerOptions, InspectContainerOptions, RemoveContainerOptions, StopContainerOptions,
};
use bollard::service::HostConfig;
use bollard::Docker as DockerClient;

use crate::container_driver::ContainerDriver;
use crate::objects::{ContainerState, FunctionExecutor};

pub struct Docker {
    client: DockerClient,
}

impl Docker {
    pub fn new() -> Result<Self> {
        let client = DockerClient::connect_with_local_defaults()
            .context("Failed to connect to Docker daemon")?;
        Ok(Self { client })
    }

    /// Generate a unique container name for a function executor
    fn container_name(function_executor: &FunctionExecutor) -> String {
        format!(
            "indexify-{}-{}-{}-{}",
            function_executor.namespace,
            function_executor.application_name,
            function_executor.function_name,
            function_executor.id.as_str()
        )
    }

    /// Generate the image name for a function executor
    fn image_name(function_executor: &FunctionExecutor) -> String {
        format!(
            "{}/{}:{}",
            function_executor.namespace,
            function_executor.application_name,
            function_executor.version
        )
    }
}

#[async_trait]
impl ContainerDriver for Docker {
    async fn start(&self, function_executor: &FunctionExecutor) -> Result<()> {
        let container_name = Self::container_name(function_executor);
        let image = Self::image_name(function_executor);

        tracing::info!(
            container_name = %container_name,
            image = %image,
            namespace = %function_executor.namespace,
            application = %function_executor.application_name,
            function = %function_executor.function_name,
            "Starting container for function executor"
        );

        // Configure resource limits
        let memory_limit = (function_executor.resources.memory_mb * 1024 * 1024) as i64;
        let cpu_period = 100_000i64; // 100ms
        let cpu_quota = (function_executor.resources.cpus as i64) * cpu_period;

        // Configure tmpfs mount for ephemeral disk at /tmp
        let ephemeral_disk_bytes = function_executor.resources.ephemeral_disk_mb * 1024 * 1024;
        let mut tmpfs = HashMap::new();
        tmpfs.insert("/tmp".to_string(), format!("size={}", ephemeral_disk_bytes));

        let host_config = HostConfig {
            memory: Some(memory_limit),
            cpu_period: Some(cpu_period),
            cpu_quota: Some(cpu_quota),
            tmpfs: Some(tmpfs),
            ..Default::default()
        };

        let mut labels = HashMap::new();
        labels.insert("indexify.namespace".to_string(), function_executor.namespace.clone());
        labels.insert("indexify.application".to_string(), function_executor.application_name.clone());
        labels.insert("indexify.function".to_string(), function_executor.function_name.clone());
        labels.insert("indexify.executor_id".to_string(), function_executor.id.as_str().to_string());

        let body = ContainerCreateBody {
            image: Some(image.clone()),
            hostname: Some(container_name.clone()),
            labels: Some(labels),
            host_config: Some(host_config),
            ..Default::default()
        };

        let options = CreateContainerOptions {
            name: Some(container_name.clone()),
            ..Default::default()
        };

        // Create the container
        self.client
            .create_container(Some(options), body)
            .await
            .context("Failed to create container")?;

        // Start the container
        self.client
            .start_container(&container_name, None::<bollard::query_parameters::StartContainerOptions>)
            .await
            .context("Failed to start container")?;

        tracing::info!(
            container_name = %container_name,
            "Container started successfully"
        );

        Ok(())
    }

    async fn stop(&self, function_executor: &FunctionExecutor) -> Result<()> {
        let container_name = Self::container_name(function_executor);

        tracing::info!(
            container_name = %container_name,
            namespace = %function_executor.namespace,
            application = %function_executor.application_name,
            function = %function_executor.function_name,
            "Stopping container for function executor"
        );

        // Stop the container with a 10 second timeout
        let stop_options = StopContainerOptions {
            t: Some(10),
            ..Default::default()
        };
        
        if let Err(e) = self.client.stop_container(&container_name, Some(stop_options)).await {
            tracing::warn!(
                container_name = %container_name,
                error = ?e,
                "Failed to stop container, it may already be stopped"
            );
        }

        // Remove the container
        let remove_options = RemoveContainerOptions {
            force: true,
            ..Default::default()
        };

        self.client
            .remove_container(&container_name, Some(remove_options))
            .await
            .context("Failed to remove container")?;

        tracing::info!(
            container_name = %container_name,
            "Container stopped and removed successfully"
        );

        Ok(())
    }

    async fn status(&self, function_executor: &FunctionExecutor) -> Result<ContainerState> {
        let container_name = Self::container_name(function_executor);

        tracing::debug!(
            container_name = %container_name,
            "Checking container status"
        );

        match self.client.inspect_container(&container_name, None::<InspectContainerOptions>).await {
            Ok(info) => {
                let state = info.state.as_ref();
                
                if let Some(state) = state {
                    if state.running.unwrap_or(false) {
                        return Ok(ContainerState::Running);
                    }
                    if state.dead.unwrap_or(false) || state.oom_killed.unwrap_or(false) {
                        return Ok(ContainerState::Killed);
                    }
                }
                
                Ok(ContainerState::Unknown)
            }
            Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => {
                // Container not found
                Ok(ContainerState::Unknown)
            }
            Err(e) => Err(e).context("Failed to inspect container"),
        }
    }
}
