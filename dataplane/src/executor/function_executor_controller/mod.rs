use std::{collections::HashMap, path::PathBuf, sync::Arc};

use crate::executor::{
    blob_store::BlobStore,
    executor_api::{
        executor_api_pb::{
            Allocation, FunctionExecutorDescription, FunctionExecutorState, FunctionExecutorStatus,
        },
        ChannelManager, ExecutorStateReporter,
    },
    function_executor::{
        server_factory::SubprocessFunctionExecutorServerFactory, FunctionExecutor,
    },
    function_executor_controller::allocation::AllocationInfo,
};

pub mod allocation;
pub mod blob_utils;

pub enum FEControllerState {
    NotStarted,
    StartingUp,
    Running,
    Terminating,
    Terminated,
}

pub struct FunctionExecutorController {
    executor_id: String,
    fe_description: FunctionExecutorDescription,
    fe_server_factory: SubprocessFunctionExecutorServerFactory,
    channel_manager: Arc<ChannelManager>,
    state_reporter: ExecutorStateReporter,
    state_reconciler: ExecutorStateReconciler,
    blob_store: BlobStore,
    cache_path: PathBuf,
    fe: FunctionExecutor,
    internal_state: FEControllerState,
    reported_state: FunctionExecutorState,
    events: Vec<Box<dyn BaseEvent>>,
    allocations: HashMap<String, AllocationInfo>,
    runnable_allocations: Vec<AllocationInfo>,
    running_allocations: Vec<AllocationInfo>,
}

impl FunctionExecutorController {
    pub fn new(
        executor_id: String,
        fe_description: FunctionExecutorDescription,
        fe_server_factory: SubprocessFunctionExecutorServerFactory,
        channel_manager: Arc<ChannelManager>,
        state_reporter: ExecutorStateReporter,
        state_reconciler: ExecutorStateReconciler,
        blob_store: BlobStore,
        cache_path: PathBuf,
    ) -> Self {
        let fe = fe_server_factory.create_function_executor(
            executor_id.clone(),
            fe_description.clone(),
            channel_manager.clone(),
            state_reconciler.clone(),
            blob_store.clone(),
            cache_path.clone(),
        );

        Self {
            executor_id,
            fe_description,
            fe_server_factory,
            channel_manager,
            state_reporter,
            state_reconciler,
            blob_store,
            cache_path,
            fe,
            internal_state: FEControllerState::NotStarted,
            reported_state: FunctionExecutorState {
                description: fe_description,
                status: FunctionExecutorStatus::Unknown,
                termination_reason: None,
                allocation_ids_caused_termination: Vec::new(),
            },
            events: Vec::new(),
            allocations: HashMap::new(),
            runnable_allocations: Vec::new(),
            running_allocations: Vec::new(),
        }
    }

    pub fn function_executor_id(&self) -> &str {
        &self.fe_description.id()
    }

    pub fn add_allocation(&self, allocation: Allocation) {
        // TODO
    }

    pub fn has_allocation(&self, allocation_id: &str) -> bool {
        self.allocations.contains_key(allocation_id)
    }

    pub fn remove_allocation(&self, allocation_id: &str) {
        if let Some(alloc_info) = self.allocations.get(allocation_id) {
            if alloc_info.is_completed {
                return;
            }
            alloc_info.is_cancelled = true;
        }
    }
}
