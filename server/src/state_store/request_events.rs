use serde::{Deserialize, Serialize};

use crate::{data_model, state_store::requests::AllocationOutput};

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
            function_name: event.allocation.function,
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
    pub function_name: String,
    pub function_run_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FunctionRunAssigned {
    pub request_id: String,
    pub function_name: String,
    pub function_run_id: String,
    pub allocation_id: String,
    pub executor_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FunctionRunOutcome {
    Unknown,
    Success,
    Failure,
}

impl From<&data_model::FunctionRunOutcome> for FunctionRunOutcome {
    fn from(outcome: &data_model::FunctionRunOutcome) -> Self {
        match outcome {
            data_model::FunctionRunOutcome::Unknown => FunctionRunOutcome::Unknown,
            data_model::FunctionRunOutcome::Success => FunctionRunOutcome::Success,
            data_model::FunctionRunOutcome::Failure(_) => FunctionRunOutcome::Failure,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FunctionRunCompleted {
    pub request_id: String,
    pub function_name: String,
    pub function_run_id: String,
    pub allocation_id: String,
    pub outcome: FunctionRunOutcome,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FunctionRunMatchedCache {
    pub request_id: String,
    pub function_name: String,
    pub function_run_id: String,
}
