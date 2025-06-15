use std::collections::HashMap;

use data_model::{TaskAnalytics, TaskOutcome};
use serde::{Deserialize, Serialize};

use crate::requests::IngestTaskOutputsRequest;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum InvocationStateChangeEvent {
    TaskCreated(TaskCreated),
    TaskAssigned(TaskAssigned),
    TaskCompleted(TaskCompleted),
    TaskMatchedCache(TaskMatchedCache),
    InvocationFinished(InvocationFinishedEvent),
}

impl InvocationStateChangeEvent {
    pub fn from_task_finished(event: IngestTaskOutputsRequest) -> Self {
        Self::TaskCompleted(TaskCompleted {
            invocation_id: event.invocation_id,
            fn_name: event.compute_fn,
            task_id: event.task.id.to_string(),
            outcome: event.allocation.outcome,
            allocation_id: event.allocation.id.to_string(),
        })
    }

    pub fn invocation_id(&self) -> String {
        match self {
            InvocationStateChangeEvent::InvocationFinished(InvocationFinishedEvent { id }) => {
                id.clone()
            }
            InvocationStateChangeEvent::TaskCreated(TaskCreated { invocation_id, .. }) => {
                invocation_id.clone()
            }
            InvocationStateChangeEvent::TaskAssigned(TaskAssigned { invocation_id, .. }) => {
                invocation_id.clone()
            }
            InvocationStateChangeEvent::TaskCompleted(TaskCompleted { invocation_id, .. }) => {
                invocation_id.clone()
            }
            InvocationStateChangeEvent::TaskMatchedCache(TaskMatchedCache {
                invocation_id,
                ..
            }) => invocation_id.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InvocationFinishedEvent {
    pub id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskCreated {
    pub invocation_id: String,
    pub fn_name: String,
    pub task_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DiagnosticMessage {
    pub invocation_id: String,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskAssigned {
    pub invocation_id: String,
    pub fn_name: String,
    pub task_id: String,
    pub allocation_id: String,
    pub executor_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskCompleted {
    pub invocation_id: String,
    pub fn_name: String,
    pub task_id: String,
    pub allocation_id: String,
    pub outcome: TaskOutcome,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskMatchedCache {
    pub invocation_id: String,
    pub fn_name: String,
    pub task_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InvocationFinished {
    pub namespace: String,
    pub compute_graph: String,
    pub invocation_id: String,
    pub analytics: HashMap<String, TaskAnalytics>,
}
