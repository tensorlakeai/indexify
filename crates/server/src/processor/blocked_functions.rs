use imbl::HashMap;

use crate::{
    data_model::{FunctionContainerServerMetadata, FunctionRun},
    state_store::in_memory_state::FunctionRunKey,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionSignature {
    pub namespace: String,
    pub application: String,
    pub function_name: String,
    pub version: String,
}

impl From<&FunctionRun> for FunctionSignature {
    fn from(function_run: &FunctionRun) -> Self {
        Self {
            namespace: function_run.namespace.clone(),
            application: function_run.application.clone(),
            function_name: function_run.name.clone(),
            version: function_run.version.clone(),
        }
    }
}

impl From<&FunctionContainerServerMetadata> for FunctionSignature {
    fn from(fe_meta: &FunctionContainerServerMetadata) -> Self {
        Self {
            namespace: fe_meta.function_container.namespace.clone(),
            application: fe_meta.function_container.application_name.clone(),
            function_name: fe_meta.function_container.function_name.clone(),
            version: fe_meta.function_container.version.clone(),
        }
    }
}
pub struct BlockedFunctions {
    pub blocked_function_runs: HashMap<FunctionSignature, Vec<FunctionRunKey>>,
}

impl Default for BlockedFunctions {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockedFunctions {
    pub fn new() -> Self {
        Self {
            blocked_function_runs: HashMap::new(),
        }
    }

    pub fn block_function_run(&mut self, function_run: &FunctionRun) {
        let signature = FunctionSignature::from(function_run);
        self.blocked_function_runs
            .entry(signature.clone())
            .or_default()
            .push(function_run.into());
    }

    pub fn unblock_function_run(&mut self, function_run: &FunctionRun) {
        let signature = FunctionSignature::from(function_run);
        self.blocked_function_runs
            .entry(signature)
            .or_default()
            .retain(|key| *key != function_run.into());
    }

    /// Returns true if there are any blocked (pending) function runs for the
    /// given signature.
    pub fn has_pending(&self, signature: &FunctionSignature) -> bool {
        self.blocked_function_runs
            .get(signature)
            .is_some_and(|runs| !runs.is_empty())
    }

    /// Returns true if there are any blocked (pending) function runs for the
    /// given FE metadata.
    pub fn has_pending_for_fe(&self, fe_meta: &FunctionContainerServerMetadata) -> bool {
        self.has_pending(&FunctionSignature::from(fe_meta))
    }
}
