use serde::{Deserialize, Serialize};

use crate::{data_model::TaskOutcome, state_store::requests::AllocationOutput};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum InvocationStateChangeEvent {
    TaskCreated(TaskCreated),
    TaskAssigned(TaskAssigned),
    TaskCompleted(TaskCompleted),
    TaskMatchedCache(TaskMatchedCache),
    RequestCreated(RequestCreatedEvent),
    RequestFinished(RequestFinishedEvent),
}

impl InvocationStateChangeEvent {
    pub fn from_task_finished(event: AllocationOutput) -> Self {
        Self::TaskCompleted(TaskCompleted {
            request_id: event.invocation_id,
            fn_name: event.compute_fn,
            task_id: event.allocation.task_id.get().to_string(),
            outcome: (&event.allocation.outcome).into(),
            allocation_id: event.allocation.id.to_string(),
        })
    }

    pub fn invocation_id(&self) -> String {
        match self {
            InvocationStateChangeEvent::RequestCreated(RequestCreatedEvent {
                request_id: id,
                ..
            }) => id.clone(),
            InvocationStateChangeEvent::RequestFinished(RequestFinishedEvent {
                request_id: id,
            }) => id.clone(),
            InvocationStateChangeEvent::TaskCreated(TaskCreated {
                request_id: invocation_id,
                ..
            }) => invocation_id.clone(),
            InvocationStateChangeEvent::TaskAssigned(TaskAssigned {
                request_id: invocation_id,
                ..
            }) => invocation_id.clone(),
            InvocationStateChangeEvent::TaskCompleted(TaskCompleted {
                request_id: invocation_id,
                ..
            }) => invocation_id.clone(),
            InvocationStateChangeEvent::TaskMatchedCache(TaskMatchedCache {
                request_id: invocation_id,
                ..
            }) => invocation_id.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestCreatedEvent {
    pub request_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestFinishedEvent {
    pub request_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskCreated {
    pub request_id: String,
    pub fn_name: String,
    pub task_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskAssigned {
    pub request_id: String,
    pub fn_name: String,
    pub task_id: String,
    pub allocation_id: String,
    pub executor_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskOutcomeSummary {
    Unknown,
    Success,
    Failure,
}

impl From<&TaskOutcome> for TaskOutcomeSummary {
    fn from(outcome: &TaskOutcome) -> Self {
        match outcome {
            TaskOutcome::Unknown => TaskOutcomeSummary::Unknown,
            TaskOutcome::Success => TaskOutcomeSummary::Success,
            TaskOutcome::Failure(_) => TaskOutcomeSummary::Failure,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskCompleted {
    pub request_id: String,
    pub fn_name: String,
    pub task_id: String,
    pub allocation_id: String,
    pub outcome: TaskOutcomeSummary,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskMatchedCache {
    pub request_id: String,
    pub fn_name: String,
    pub task_id: String,
}
