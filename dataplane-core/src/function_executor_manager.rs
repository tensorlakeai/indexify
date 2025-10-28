use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

use crate::executor_client::executor_api_pb::{
    Allocation, DesiredExecutorState, FunctionExecutorDescription, FunctionExecutorState,
    FunctionExecutorStatus,
};

/// Events that Function Executors send back to the manager
#[derive(Debug, Clone)]
pub enum FunctionExecutorEvent {
    /// Function Executor started successfully
    Started { function_executor_id: String },
    /// Function Executor failed to start
    StartFailed {
        function_executor_id: String,
        error: String,
    },
    /// Function Executor terminated
    Terminated { function_executor_id: String },
    /// Allocation completed
    AllocationCompleted {
        function_executor_id: String,
        allocation_id: String,
    },
    /// Allocation failed
    AllocationFailed {
        function_executor_id: String,
        allocation_id: String,
        error: String,
    },
}

/// A single Function Executor that manages a gRPC connection and executes allocations
pub struct FunctionExecutor {
    /// Function Executor ID
    id: String,
    /// Description from the server
    description: FunctionExecutorDescription,
    /// Current status
    status: FunctionExecutorStatus,
    /// Event channel to send updates to the manager
    event_tx: mpsc::UnboundedSender<FunctionExecutorEvent>,
}

impl FunctionExecutor {
    pub fn new(
        description: FunctionExecutorDescription,
        event_tx: mpsc::UnboundedSender<FunctionExecutorEvent>,
    ) -> Self {
        let id = description
            .id
            .clone()
            .unwrap_or_else(|| String::from("unknown"));

        info!(
            function_executor_id = %id,
            "Creating new Function Executor"
        );

        Self {
            id,
            description,
            status: FunctionExecutorStatus::Pending,
            event_tx,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn status(&self) -> FunctionExecutorStatus {
        self.status
    }

    /// Starts the Function Executor (spawns gRPC connection, etc.)
    pub async fn start(&mut self) -> anyhow::Result<()> {
        info!(
            function_executor_id = %self.id,
            "Starting Function Executor"
        );

        // TODO: Implement actual Function Executor startup
        // - Start gRPC server/client
        // - Initialize function runtime
        // - Set up allocation queue

        self.status = FunctionExecutorStatus::Running;

        // Notify manager that we started
        let _ = self.event_tx.send(FunctionExecutorEvent::Started {
            function_executor_id: self.id.clone(),
        });

        Ok(())
    }

    /// Runs an allocation on this Function Executor
    pub async fn run_allocation(&self, allocation: Allocation) -> anyhow::Result<()> {
        let allocation_id = allocation
            .allocation_id
            .clone()
            .unwrap_or_else(|| String::from("unknown"));

        info!(
            function_executor_id = %self.id,
            allocation_id = %allocation_id,
            "Running allocation on Function Executor"
        );

        // TODO: Implement actual allocation execution
        // - Prepare inputs
        // - Execute function
        // - Collect outputs
        // - Report results

        Ok(())
    }

    /// Shuts down the Function Executor
    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        info!(
            function_executor_id = %self.id,
            "Shutting down Function Executor"
        );

        // TODO: Implement shutdown
        // - Cancel running allocations
        // - Close gRPC connections
        // - Clean up resources

        self.status = FunctionExecutorStatus::Terminated;

        let _ = self.event_tx.send(FunctionExecutorEvent::Terminated {
            function_executor_id: self.id.clone(),
        });

        Ok(())
    }

    /// Returns the current state for reporting to the server
    pub fn get_state(&self) -> FunctionExecutorState {
        FunctionExecutorState {
            description: Some(self.description.clone()),
            status: Some(self.status as i32),
            allocation_ids_caused_termination: vec![],
            termination_reason: None,
        }
    }
}

/// Manager for all Function Executors and Allocations
/// This is the main coordinator that reconciles desired state from the server
pub struct FunctionExecutorManager {
    /// All function executors (function_executor_id -> FunctionExecutor)
    function_executors: Arc<RwLock<HashMap<String, FunctionExecutor>>>,
    /// All allocations (allocation_id -> Allocation)
    allocations: Arc<RwLock<HashMap<String, Allocation>>>,
    /// Channel for receiving events from Function Executors
    event_rx: mpsc::UnboundedReceiver<FunctionExecutorEvent>,
    /// Channel for sending events to Function Executors
    event_tx: mpsc::UnboundedSender<FunctionExecutorEvent>,
}

impl FunctionExecutorManager {
    /// Creates a new Function Executor Manager
    pub fn new() -> Self {
        info!(
            "creating function executor manager"
        );

        let (event_tx, event_rx) = mpsc::unbounded_channel();

        Self {
            function_executors: Arc::new(RwLock::new(HashMap::new())),
            allocations: Arc::new(RwLock::new(HashMap::new())),
            event_rx,
            event_tx,
        }
    }

    /// Reconciles the desired state from the server with the local state
    /// This is the main method that handles state synchronization
    pub async fn reconcile_desired_state(
        &self,
        desired_state: DesiredExecutorState,
    ) -> anyhow::Result<()> {
        debug!("Reconciling desired state with local state");

        // 1. Reconcile Function Executors
        self.reconcile_function_executors(&desired_state).await?;

        // 2. Reconcile Allocations
        self.reconcile_allocations(&desired_state).await?;

        Ok(())
    }

    /// Reconciles Function Executors (add new, remove deleted)
    async fn reconcile_function_executors(
        &self,
        desired_state: &DesiredExecutorState,
    ) -> anyhow::Result<()> {
        let mut function_executors = self.function_executors.write().await;
        // Find desired function executor IDs
        let desired_fe_ids: HashMap<String, FunctionExecutorDescription> = desired_state
            .function_executors
            .iter()
            .filter_map(|desc| {
                desc.id
                    .clone()
                    .map(|id| (id.clone(), desc.clone()))
            })
            .collect();

        // Find current function executor IDs
        let current_fe_ids: Vec<String> = function_executors.keys().cloned().collect();

        // Remove Function Executors that are no longer desired
        for fe_id in &current_fe_ids {
            if !desired_fe_ids.contains_key(fe_id) {
                info!(
                    function_executor_id = %fe_id,
                    "Removing Function Executor (no longer in desired state)"
                );

                if let Some(mut fe) = function_executors.remove(fe_id) {
                    // Spawn task to shut down the FE
                    tokio::spawn(async move {
                        if let Err(e) = fe.shutdown().await {
                            warn!(
                                function_executor_id = %fe.id(),
                                error = %e,
                                "Failed to shutdown Function Executor"
                            );
                        }
                    });
                }
            }
        }

        // Add new Function Executors
        for (fe_id, description) in desired_fe_ids {
            if !function_executors.contains_key(&fe_id) {
                info!(
                    function_executor_id = %fe_id,
                    "Creating new Function Executor"
                );

                let mut fe = FunctionExecutor::new(description, self.event_tx.clone());

                // Start the FE inline (it just updates status for now)
                if let Err(e) = fe.start().await {
                    warn!(
                        function_executor_id = %fe.id(),
                        error = %e,
                        "Failed to start Function Executor"
                    );
                }

                function_executors.insert(fe_id, fe);
            }
        }

        Ok(())
    }

    /// Reconciles Allocations (assign to Function Executors)
    async fn reconcile_allocations(
        &self,
        desired_state: &DesiredExecutorState,
    ) -> anyhow::Result<()> {
        let mut allocations = self.allocations.write().await;
        let function_executors = self.function_executors.read().await;
        // Get all desired allocations
        for allocation in &desired_state.allocations {
            let allocation_id = allocation
                .allocation_id
                .clone()
                .unwrap_or_else(|| String::from("unknown"));

            let fe_id = allocation
                .function_executor_id
                .clone()
                .unwrap_or_else(|| String::from("unknown"));

            // Check if this is a new allocation
            if !allocations.contains_key(&allocation_id) {
                info!(
                    allocation_id = %allocation_id,
                    function_executor_id = %fe_id,
                    "Adding new allocation"
                );

                // Find the Function Executor
                if let Some(fe) = function_executors.get(&fe_id) {
                    // Only run if FE is running
                    if fe.status() == FunctionExecutorStatus::Running {
                        let _allocation_clone = allocation.clone();
                        let allocation_id_clone = allocation_id.clone();
                        let fe_id_clone = fe_id.clone();

                        // Spawn task to run the allocation
                        tokio::spawn(async move {
                            // TODO: Get actual FE reference and run allocation
                            debug!(
                                allocation_id = %allocation_id_clone,
                                function_executor_id = %fe_id_clone,
                                "Running allocation (TODO)"
                            );
                        });
                    } else {
                        warn!(
                            allocation_id = %allocation_id,
                            function_executor_id = %fe_id,
                            "Cannot run allocation - Function Executor not running"
                        );
                    }
                } else {
                    warn!(
                        allocation_id = %allocation_id,
                        function_executor_id = %fe_id,
                        "Cannot run allocation - Function Executor not found"
                    );
                }

                allocations.insert(allocation_id.clone(), allocation.clone());
            }
        }

        // TODO: Handle allocation removal/cancellation

        Ok(())
    }

    /// Processes events from Function Executors
    pub async fn process_events(&mut self) {
        while let Some(event) = self.event_rx.recv().await {
            match event {
                FunctionExecutorEvent::Started {
                    function_executor_id,
                } => {
                    info!(
                        function_executor_id = %function_executor_id,
                        "Function Executor started"
                    );
                    // Update state if needed
                }
                FunctionExecutorEvent::StartFailed {
                    function_executor_id,
                    error,
                } => {
                    warn!(
                        function_executor_id = %function_executor_id,
                        error = %error,
                        "Function Executor failed to start"
                    );
                    // Handle failure
                }
                FunctionExecutorEvent::Terminated {
                    function_executor_id,
                } => {
                    info!(
                        function_executor_id = %function_executor_id,
                        "Function Executor terminated"
                    );
                    // Clean up state
                }
                FunctionExecutorEvent::AllocationCompleted {
                    function_executor_id,
                    allocation_id,
                } => {
                    info!(
                        function_executor_id = %function_executor_id,
                        allocation_id = %allocation_id,
                        "Allocation completed"
                    );
                    // Report result to heartbeat service
                }
                FunctionExecutorEvent::AllocationFailed {
                    function_executor_id,
                    allocation_id,
                    error,
                } => {
                    warn!(
                        function_executor_id = %function_executor_id,
                        allocation_id = %allocation_id,
                        error = %error,
                        "Allocation failed"
                    );
                    // Report failure to heartbeat service
                }
            }
        }
    }

    /// Gets all function executor states for reporting to the server
    pub async fn get_function_executor_states(&self) -> Vec<FunctionExecutorState> {
        let function_executors = self.function_executors.read().await;
        function_executors
            .values()
            .map(|fe| fe.get_state())
            .collect()
    }

    /// Returns the number of function executors
    pub async fn function_executor_count(&self) -> usize {
        let function_executors = self.function_executors.read().await;
        function_executors.len()
    }

    /// Returns the number of allocations
    pub async fn allocation_count(&self) -> usize {
        let allocations = self.allocations.read().await;
        allocations.len()
    }
}
