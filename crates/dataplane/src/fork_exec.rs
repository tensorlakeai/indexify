use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::process::{Child, Command};
use tokio::sync::RwLock;

use crate::config::ForkExecConfig;
use crate::container_driver::ContainerDriver;
use crate::objects::{ContainerState, FunctionExecutor, FunctionExecutorId};

/// Tracks a running child process
struct ProcessHandle {
    child: Child,
    #[allow(dead_code)]
    function_executor_id: FunctionExecutorId,
}

/// Fork-exec based container driver that spawns processes directly
pub struct ForkExec {
    config: ForkExecConfig,
    /// Map of function executor ID to process handle
    processes: Arc<RwLock<HashMap<String, ProcessHandle>>>,
}

impl ForkExec {
    pub fn new(config: ForkExecConfig) -> Self {
        Self {
            config,
            processes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Generate a unique process identifier for a function executor
    fn process_id(function_executor: &FunctionExecutor) -> String {
        function_executor.id.as_str().to_string()
    }

    /// Get the binary path for a function executor
    fn binary_path(&self, function_executor: &FunctionExecutor) -> PathBuf {
        PathBuf::from(&self.config.bin_path)
            .join(&function_executor.namespace)
            .join(&function_executor.application_name)
            .join(&function_executor.function_name)
            .join(&function_executor.version)
            .join("executor")
    }

    /// Get the working directory for a function executor
    fn work_dir(&self, function_executor: &FunctionExecutor) -> PathBuf {
        PathBuf::from(&self.config.work_dir)
            .join(&function_executor.namespace)
            .join(&function_executor.application_name)
            .join(&function_executor.function_name)
            .join(function_executor.id.as_str())
    }
}

#[async_trait]
impl ContainerDriver for ForkExec {
    async fn start(&self, function_executor: &FunctionExecutor) -> Result<()> {
        let process_id = Self::process_id(function_executor);
        let binary_path = self.binary_path(function_executor);
        let work_dir = self.work_dir(function_executor);

        tracing::info!(
            process_id = %process_id,
            binary_path = %binary_path.display(),
            work_dir = %work_dir.display(),
            namespace = %function_executor.namespace,
            application = %function_executor.application_name,
            function = %function_executor.function_name,
            "Starting process for function executor"
        );

        // Ensure the working directory exists
        tokio::fs::create_dir_all(&work_dir)
            .await
            .context("Failed to create working directory")?;

        // Spawn the process
        let child = Command::new(&binary_path)
            .current_dir(&work_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("Failed to spawn process: {}", binary_path.display()))?;

        let pid = child.id();
        tracing::info!(
            process_id = %process_id,
            pid = ?pid,
            "Process spawned successfully"
        );

        // Store the process handle
        let handle = ProcessHandle {
            child,
            function_executor_id: function_executor.id.clone(),
        };

        let mut processes = self.processes.write().await;
        processes.insert(process_id, handle);

        Ok(())
    }

    async fn stop(&self, function_executor: &FunctionExecutor) -> Result<()> {
        let process_id = Self::process_id(function_executor);

        tracing::info!(
            process_id = %process_id,
            namespace = %function_executor.namespace,
            application = %function_executor.application_name,
            function = %function_executor.function_name,
            "Stopping process for function executor"
        );

        let mut processes = self.processes.write().await;
        
        if let Some(mut handle) = processes.remove(&process_id) {
            let pid = handle.child.id();
            
            // Try graceful shutdown first with SIGTERM
            #[cfg(unix)]
            if let Some(pid) = pid {
                unsafe {
                    libc::kill(pid as i32, libc::SIGTERM);
                }
            }

            // Wait a bit for graceful shutdown
            let timeout = tokio::time::Duration::from_secs(5);
            match tokio::time::timeout(timeout, handle.child.wait()).await {
                Ok(Ok(status)) => {
                    tracing::info!(
                        process_id = %process_id,
                        pid = ?pid,
                        status = ?status,
                        "Process terminated gracefully"
                    );
                }
                Ok(Err(e)) => {
                    tracing::warn!(
                        process_id = %process_id,
                        error = ?e,
                        "Error waiting for process"
                    );
                }
                Err(_) => {
                    // Timeout - force kill
                    tracing::warn!(
                        process_id = %process_id,
                        pid = ?pid,
                        "Process did not terminate gracefully, forcing kill"
                    );
                    
                    if let Err(e) = handle.child.kill().await {
                        tracing::error!(
                            process_id = %process_id,
                            error = ?e,
                            "Failed to kill process"
                        );
                    }
                }
            }

            // Clean up working directory
            let work_dir = self.work_dir(function_executor);
            if let Err(e) = tokio::fs::remove_dir_all(&work_dir).await {
                tracing::warn!(
                    work_dir = %work_dir.display(),
                    error = ?e,
                    "Failed to clean up working directory"
                );
            }
        } else {
            tracing::warn!(
                process_id = %process_id,
                "Process not found, may have already terminated"
            );
        }

        Ok(())
    }

    async fn status(&self, function_executor: &FunctionExecutor) -> Result<ContainerState> {
        let process_id = Self::process_id(function_executor);

        tracing::debug!(
            process_id = %process_id,
            "Checking process status"
        );

        let mut processes = self.processes.write().await;
        
        if let Some(handle) = processes.get_mut(&process_id) {
            // Try to check if process is still running
            match handle.child.try_wait() {
                Ok(Some(status)) => {
                    // Process has exited
                    tracing::debug!(
                        process_id = %process_id,
                        status = ?status,
                        "Process has exited"
                    );
                    
                    // Remove from tracking
                    processes.remove(&process_id);
                    
                    if status.success() {
                        Ok(ContainerState::Unknown) // Exited cleanly
                    } else {
                        Ok(ContainerState::Killed) // Exited with error
                    }
                }
                Ok(None) => {
                    // Process is still running
                    Ok(ContainerState::Running)
                }
                Err(e) => {
                    tracing::error!(
                        process_id = %process_id,
                        error = ?e,
                        "Error checking process status"
                    );
                    Ok(ContainerState::Unknown)
                }
            }
        } else {
            // Process not tracked
            Ok(ContainerState::Unknown)
        }
    }
}

