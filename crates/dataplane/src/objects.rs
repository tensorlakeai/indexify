#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionExecutorId(String);

impl FunctionExecutorId {
    pub fn new(id: String) -> Self {
        Self(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionExecutor {
    pub id: FunctionExecutorId,
    pub namespace: String,
    pub application_name: String,
    pub function_name: String,
    pub version: String,
    pub state: FunctionExecutorState,
    pub resources: FunctionExecutorResources,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionExecutorResources {
    pub cpus: u32,
    pub memory_mb: u64,
    pub ephemeral_disk_mb: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FunctionExecutorState {
    Pending,
    Running,
    Terminated,
}

pub enum ContainerState {
    Unknown,
    Killed,
    Running,
}

pub enum FEState {
    UNKNOWN,
    HEALTHY,
    UNHEALTHY,
}