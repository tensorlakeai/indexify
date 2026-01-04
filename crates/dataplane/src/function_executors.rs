use std::{collections::{HashMap, HashSet}, sync::RwLock};

use anyhow::Result;

use crate::objects::{FunctionExecutor, FunctionExecutorId, FunctionExecutorState};

pub struct FunctionExecutors {
    function_executors: RwLock<HashMap<FunctionExecutorId, FunctionExecutor>>,
}

impl FunctionExecutors {
    pub fn new() -> Self {
        Self { function_executors: RwLock::new(HashMap::new()) }
    }

    pub fn sync(&mut self, new_function_executors: HashSet<FunctionExecutor>) -> Result<()> {
        let mut function_executors = self.function_executors.write().unwrap();
        for function_executor in &new_function_executors {
            if !function_executors.contains_key(&function_executor.id) {
                function_executors.insert(function_executor.id.clone(), function_executor.clone());
            }
        }
        let new_ids: HashSet<_> = new_function_executors.iter().map(|fe| &fe.id).collect();

        for fe in function_executors.values_mut() {
            if !new_ids.contains(&fe.id) {
                if fe.state != FunctionExecutorState::Terminated {
                    fe.state = FunctionExecutorState::Terminated;
                }
            }
        }

        Ok(())
    }
}