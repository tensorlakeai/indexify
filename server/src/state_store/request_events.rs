use serde::{Deserialize, Serialize};

use crate::{data_model::FunctionRunOutcome, state_store::requests::AllocationOutput};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RequestStateChangeEvent {
    RequestStarted(RequestStartedEvent),
    FunctionRunCreated(FunctionRunCreated),
    FunctionRunAssigned(FunctionRunAssigned),
    FunctionRunCompleted(FunctionRunCompleted),
    FunctionRunMatchedCache(FunctionRunMatchedCache),
    RequestCreated(RequestCreatedEvent),
    RequestFinished(RequestFinishedEvent),
}

impl RequestStateChangeEvent {
    pub fn from_finished_function_run(event: AllocationOutput) -> Self {
        Self::FunctionRunCompleted(FunctionRunCompleted {
            request_id: event.request_id,
            fn_name: event.allocation.function,
            function_run_id: event.allocation.function_call_id.to_string(),
            outcome: (&event.allocation.outcome).into(),
            allocation_id: event.allocation.id.to_string(),
        })
    }

    pub fn request_id(&self) -> String {
        match self {
            RequestStateChangeEvent::RequestStarted(RequestStartedEvent {
                request_id: id, ..
            }) => id.clone(),
            RequestStateChangeEvent::RequestCreated(RequestCreatedEvent {
                request_id: id, ..
            }) => id.clone(),
            RequestStateChangeEvent::RequestFinished(RequestFinishedEvent { request_id: id }) => {
                id.clone()
            }
            RequestStateChangeEvent::FunctionRunCreated(FunctionRunCreated {
                request_id, ..
            }) => request_id.clone(),
            RequestStateChangeEvent::FunctionRunAssigned(FunctionRunAssigned {
                request_id,
                ..
            }) => request_id.clone(),
            RequestStateChangeEvent::FunctionRunCompleted(FunctionRunCompleted {
                request_id,
                ..
            }) => request_id.clone(),
            RequestStateChangeEvent::FunctionRunMatchedCache(FunctionRunMatchedCache {
                request_id,
                ..
            }) => request_id.clone(),
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
pub struct RequestStartedEvent {
    pub request_id: String,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FunctionRunCreated {
    pub request_id: String,
    pub fn_name: String,
    pub task_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FunctionRunAssigned {
    pub request_id: String,
    pub fn_name: String,
    pub task_id: String,
    pub allocation_id: String,
    pub executor_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FunctionRunOutcomeSummary {
    Unknown,
    Success,
    Failure,
}

impl From<&FunctionRunOutcome> for FunctionRunOutcomeSummary {
    fn from(outcome: &FunctionRunOutcome) -> Self {
        match outcome {
            FunctionRunOutcome::Unknown => FunctionRunOutcomeSummary::Unknown,
            FunctionRunOutcome::Success => FunctionRunOutcomeSummary::Success,
            FunctionRunOutcome::Failure(_) => FunctionRunOutcomeSummary::Failure,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FunctionRunCompleted {
    pub request_id: String,
    pub fn_name: String,
    pub function_run_id: String,
    pub allocation_id: String,
    pub outcome: FunctionRunOutcomeSummary,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FunctionRunMatchedCache {
    pub request_id: String,
    pub fn_name: String,
    pub function_run_id: String,
}
