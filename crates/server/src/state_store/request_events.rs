use serde::{Deserialize, Serialize};

use crate::{
    data_model::{FunctionRunOutcome, RequestOutcome},
    state_store::requests::{AllocationOutput, RequestPayload, StateMachineUpdateRequest},
};

/// Unique identifier for a request state change event
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RequestStateChangeEventId(u64);

impl RequestStateChangeEventId {
    pub fn new(seq: u64) -> Self {
        Self(seq)
    }

    pub fn value(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for RequestStateChangeEventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub trait RequestEventMetadata {
    fn namespace(&self) -> &str;
    fn application_name(&self) -> &str;
    fn application_version(&self) -> &str;
    fn request_id(&self) -> &str;
}

/// A persisted request state change event with a unique ID
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PersistedRequestStateChangeEvent {
    pub id: RequestStateChangeEventId,
    pub event: RequestStateChangeEvent,
}

impl PersistedRequestStateChangeEvent {
    pub fn new(id: RequestStateChangeEventId, event: RequestStateChangeEvent) -> Self {
        Self { id, event }
    }

    /// Generate a key for storing in RocksDB
    /// Uses big endian bytes for proper lexicographic ordering
    pub fn key(&self) -> Vec<u8> {
        self.id.value().to_be_bytes().to_vec()
    }
}

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
    pub fn finished(
        namespace: &str,
        application: &str,
        application_version: &str,
        request_id: &str,
        outcome: RequestOutcome,
    ) -> Self {
        Self::RequestFinished(RequestFinishedEvent {
            namespace: namespace.to_string(),
            application_name: application.to_string(),
            application_version: application_version.to_string(),
            request_id: request_id.to_string(),
            outcome,
        })
    }

    pub fn from_finished_function_run(event: AllocationOutput) -> Self {
        Self::FunctionRunCompleted(FunctionRunCompleted {
            namespace: event.allocation.namespace.clone(),
            application_name: event.allocation.application.clone(),
            application_version: event.allocation.application_version.clone(),
            request_id: event.request_id,
            function_name: event.allocation.function,
            function_run_id: event.allocation.function_call_id.to_string(),
            outcome: (&event.allocation.outcome).into(),
            allocation_id: event.allocation.id.to_string(),
        })
    }

    pub fn namespace(&self) -> &str {
        match self {
            RequestStateChangeEvent::RequestStarted(event) => event.namespace(),
            RequestStateChangeEvent::RequestCreated(event) => event.namespace(),
            RequestStateChangeEvent::RequestFinished(event) => event.namespace(),
            RequestStateChangeEvent::FunctionRunCreated(event) => event.namespace(),
            RequestStateChangeEvent::FunctionRunAssigned(event) => event.namespace(),
            RequestStateChangeEvent::FunctionRunCompleted(event) => event.namespace(),
            RequestStateChangeEvent::FunctionRunMatchedCache(event) => event.namespace(),
        }
    }

    pub fn application_name(&self) -> &str {
        match self {
            RequestStateChangeEvent::RequestStarted(event) => event.application_name(),
            RequestStateChangeEvent::RequestCreated(event) => event.application_name(),
            RequestStateChangeEvent::RequestFinished(event) => event.application_name(),
            RequestStateChangeEvent::FunctionRunCreated(event) => event.application_name(),
            RequestStateChangeEvent::FunctionRunAssigned(event) => event.application_name(),
            RequestStateChangeEvent::FunctionRunCompleted(event) => event.application_name(),
            RequestStateChangeEvent::FunctionRunMatchedCache(event) => event.application_name(),
        }
    }

    pub fn application_version(&self) -> &str {
        match self {
            RequestStateChangeEvent::RequestStarted(event) => event.application_version(),
            RequestStateChangeEvent::RequestCreated(event) => event.application_version(),
            RequestStateChangeEvent::RequestFinished(event) => event.application_version(),
            RequestStateChangeEvent::FunctionRunCreated(event) => event.application_version(),
            RequestStateChangeEvent::FunctionRunAssigned(event) => event.application_version(),
            RequestStateChangeEvent::FunctionRunCompleted(event) => event.application_version(),
            RequestStateChangeEvent::FunctionRunMatchedCache(event) => event.application_version(),
        }
    }

    pub fn request_id(&self) -> &str {
        match self {
            RequestStateChangeEvent::RequestStarted(event) => event.request_id(),
            RequestStateChangeEvent::RequestCreated(event) => event.request_id(),
            RequestStateChangeEvent::RequestFinished(event) => event.request_id(),
            RequestStateChangeEvent::FunctionRunCreated(event) => event.request_id(),
            RequestStateChangeEvent::FunctionRunAssigned(event) => event.request_id(),
            RequestStateChangeEvent::FunctionRunCompleted(event) => event.request_id(),
            RequestStateChangeEvent::FunctionRunMatchedCache(event) => event.request_id(),
        }
    }

    pub fn message(&self) -> &str {
        match self {
            RequestStateChangeEvent::RequestStarted(_) => "Request Started",
            RequestStateChangeEvent::RequestCreated(_) => "Request Created",
            RequestStateChangeEvent::RequestFinished(_) => "Request Finished",
            RequestStateChangeEvent::FunctionRunCreated(_) => "Function Run Created",
            RequestStateChangeEvent::FunctionRunAssigned(_) => "Function Run Assigned",
            RequestStateChangeEvent::FunctionRunCompleted(_) => "Function Run Completed",
            RequestStateChangeEvent::FunctionRunMatchedCache(_) => {
                "Function Run Matched a Cached output"
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestCreatedEvent {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
}

impl RequestEventMetadata for RequestCreatedEvent {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn application_name(&self) -> &str {
        &self.application_name
    }

    fn application_version(&self) -> &str {
        &self.application_version
    }

    fn request_id(&self) -> &str {
        &self.request_id
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestFinishedEvent {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
    pub outcome: RequestOutcome,
}

impl RequestEventMetadata for RequestFinishedEvent {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn application_name(&self) -> &str {
        &self.application_name
    }

    fn application_version(&self) -> &str {
        &self.application_version
    }

    fn request_id(&self) -> &str {
        &self.request_id
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestStartedEvent {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
}

impl RequestEventMetadata for RequestStartedEvent {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn application_name(&self) -> &str {
        &self.application_name
    }

    fn application_version(&self) -> &str {
        &self.application_version
    }

    fn request_id(&self) -> &str {
        &self.request_id
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FunctionRunCreated {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
    pub function_name: String,
    pub function_run_id: String,
}

impl RequestEventMetadata for FunctionRunCreated {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn application_name(&self) -> &str {
        &self.application_name
    }

    fn application_version(&self) -> &str {
        &self.application_version
    }

    fn request_id(&self) -> &str {
        &self.request_id
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FunctionRunAssigned {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
    pub function_name: String,
    pub function_run_id: String,
    pub allocation_id: String,
    pub executor_id: String,
}

impl RequestEventMetadata for FunctionRunAssigned {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn application_name(&self) -> &str {
        &self.application_name
    }

    fn application_version(&self) -> &str {
        &self.application_version
    }

    fn request_id(&self) -> &str {
        &self.request_id
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FunctionRunCompleted {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
    pub function_name: String,
    pub function_run_id: String,
    pub allocation_id: String,
    pub outcome: FunctionRunOutcomeSummary,
}

impl RequestEventMetadata for FunctionRunCompleted {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn application_name(&self) -> &str {
        &self.application_name
    }

    fn application_version(&self) -> &str {
        &self.application_version
    }

    fn request_id(&self) -> &str {
        &self.request_id
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FunctionRunMatchedCache {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
    pub function_name: String,
    pub function_run_id: String,
}

impl RequestEventMetadata for FunctionRunMatchedCache {
    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn application_name(&self) -> &str {
        &self.application_name
    }

    fn application_version(&self) -> &str {
        &self.application_version
    }

    fn request_id(&self) -> &str {
        &self.request_id
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
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

/// Build request state change events from a state machine update request
pub fn build_request_state_change_events(
    update_request: &StateMachineUpdateRequest,
) -> Vec<RequestStateChangeEvent> {
    match &update_request.payload {
        RequestPayload::InvokeApplication(request) => {
            let mut events = vec![RequestStateChangeEvent::RequestStarted(
                RequestStartedEvent {
                    namespace: request.ctx.namespace.clone(),
                    application_name: request.ctx.application_name.clone(),
                    application_version: request.ctx.application_version.clone(),
                    request_id: request.ctx.request_id.clone(),
                },
            )];

            // Emit FunctionRunCreated for all function runs in the request context
            // (typically just the entrypoint function run)
            for function_run in request.ctx.function_runs.values() {
                events.push(RequestStateChangeEvent::FunctionRunCreated(
                    FunctionRunCreated {
                        namespace: function_run.namespace.clone(),
                        application_name: function_run.application.clone(),
                        application_version: function_run.version.clone(),
                        request_id: function_run.request_id.clone(),
                        function_name: function_run.name.clone(),
                        function_run_id: function_run.id.to_string(),
                    },
                ));
            }

            events
        }
        RequestPayload::UpsertExecutor(request) => request
            .allocation_outputs
            .iter()
            .map(|allocation_output| {
                RequestStateChangeEvent::from_finished_function_run(allocation_output.clone())
            })
            .collect::<Vec<_>>(),
        RequestPayload::SchedulerUpdate((sched_update, _)) => {
            let mut changes = Vec::new();

            // 1. FunctionRunCreated events first (runs must exist before being assigned)
            // Only emit for NEWLY created runs (created_at_clock is None means never
            // persisted)
            for (ctx_key, function_call_ids) in &sched_update.updated_function_runs {
                for function_call_id in function_call_ids {
                    let ctx = sched_update.updated_request_states.get(ctx_key).cloned();
                    let function_run =
                        ctx.and_then(|ctx| ctx.function_runs.get(function_call_id).cloned());
                    if let Some(function_run) = function_run {
                        // is_new() returns true if created_at_clock is None (never persisted)
                        if function_run.is_new() {
                            changes.push(RequestStateChangeEvent::FunctionRunCreated(
                                FunctionRunCreated {
                                    namespace: function_run.namespace.clone(),
                                    application_name: function_run.application.clone(),
                                    application_version: function_run.version.clone(),
                                    request_id: function_run.request_id.clone(),
                                    function_name: function_run.name.clone(),
                                    function_run_id: function_run.id.to_string(),
                                },
                            ));
                        }
                    }
                }
            }

            // 2. FunctionRunAssigned events (after runs are created)
            for allocation in &sched_update.new_allocations {
                changes.push(RequestStateChangeEvent::FunctionRunAssigned(
                    FunctionRunAssigned {
                        namespace: allocation.namespace.clone(),
                        application_name: allocation.application.clone(),
                        application_version: allocation.application_version.clone(),
                        request_id: allocation.request_id.clone(),
                        function_name: allocation.function.clone(),
                        function_run_id: allocation.function_call_id.to_string(),
                        executor_id: allocation.target.executor_id.get().to_string(),
                        allocation_id: allocation.id.to_string(),
                    },
                ));
            }

            // 3. RequestFinished events last
            for request_ctx in sched_update.updated_request_states.values() {
                if let Some(outcome) = &request_ctx.outcome {
                    changes.push(RequestStateChangeEvent::finished(
                        &request_ctx.namespace,
                        &request_ctx.application_name,
                        &request_ctx.application_version,
                        &request_ctx.request_id,
                        outcome.clone(),
                    ));
                }
            }

            changes
        }
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_created_event_metadata() {
        let event = RequestCreatedEvent {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "1.0.0".to_string(),
            request_id: "req-123".to_string(),
        };

        assert_eq!(event.namespace(), "test-ns");
        assert_eq!(event.application_name(), "test-app");
        assert_eq!(event.application_version(), "1.0.0");
        assert_eq!(event.request_id(), "req-123");

        let wrapped = RequestStateChangeEvent::RequestCreated(event);
        assert_eq!(wrapped.namespace(), "test-ns");
        assert_eq!(wrapped.application_name(), "test-app");
        assert_eq!(wrapped.application_version(), "1.0.0");
        assert_eq!(wrapped.request_id(), "req-123");
    }

    #[test]
    fn test_request_started_event_metadata() {
        let event = RequestStartedEvent {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "1.0.1".to_string(),
            request_id: "req-456".to_string(),
        };

        assert_eq!(event.namespace(), "test-ns");
        assert_eq!(event.application_name(), "test-app");
        assert_eq!(event.application_version(), "1.0.1");
        assert_eq!(event.request_id(), "req-456");

        let wrapped = RequestStateChangeEvent::RequestStarted(event);
        assert_eq!(wrapped.namespace(), "test-ns");
        assert_eq!(wrapped.application_name(), "test-app");
        assert_eq!(wrapped.application_version(), "1.0.1");
        assert_eq!(wrapped.request_id(), "req-456");
    }

    #[test]
    fn test_request_finished_event_metadata() {
        let event = RequestStateChangeEvent::finished(
            "test-ns",
            "test-app",
            "1.0.2",
            "req-789",
            RequestOutcome::Success,
        );

        assert_eq!(event.namespace(), "test-ns");
        assert_eq!(event.application_name(), "test-app");
        assert_eq!(event.application_version(), "1.0.2");
        assert_eq!(event.request_id(), "req-789");
    }

    #[test]
    fn test_function_run_created_event_metadata() {
        let event = FunctionRunCreated {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "2.0.0".to_string(),
            request_id: "req-001".to_string(),
            function_name: "my-function".to_string(),
            function_run_id: "run-123".to_string(),
        };

        assert_eq!(event.namespace(), "test-ns");
        assert_eq!(event.application_name(), "test-app");
        assert_eq!(event.application_version(), "2.0.0");
        assert_eq!(event.request_id(), "req-001");

        let wrapped = RequestStateChangeEvent::FunctionRunCreated(event);
        assert_eq!(wrapped.namespace(), "test-ns");
        assert_eq!(wrapped.application_name(), "test-app");
        assert_eq!(wrapped.application_version(), "2.0.0");
        assert_eq!(wrapped.request_id(), "req-001");
    }

    #[test]
    fn test_function_run_assigned_event_metadata() {
        let event = FunctionRunAssigned {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "2.0.1".to_string(),
            request_id: "req-002".to_string(),
            function_name: "my-function".to_string(),
            function_run_id: "run-456".to_string(),
            allocation_id: "alloc-789".to_string(),
            executor_id: "executor-001".to_string(),
        };

        assert_eq!(event.namespace(), "test-ns");
        assert_eq!(event.application_name(), "test-app");
        assert_eq!(event.application_version(), "2.0.1");
        assert_eq!(event.request_id(), "req-002");

        let wrapped = RequestStateChangeEvent::FunctionRunAssigned(event);
        assert_eq!(wrapped.namespace(), "test-ns");
        assert_eq!(wrapped.application_name(), "test-app");
        assert_eq!(wrapped.application_version(), "2.0.1");
        assert_eq!(wrapped.request_id(), "req-002");
    }

    #[test]
    fn test_function_run_completed_event_metadata() {
        let event = FunctionRunCompleted {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "2.0.2".to_string(),
            request_id: "req-003".to_string(),
            function_name: "my-function".to_string(),
            function_run_id: "run-789".to_string(),
            allocation_id: "alloc-456".to_string(),
            outcome: FunctionRunOutcomeSummary::Success,
        };

        assert_eq!(event.namespace(), "test-ns");
        assert_eq!(event.application_name(), "test-app");
        assert_eq!(event.application_version(), "2.0.2");
        assert_eq!(event.request_id(), "req-003");

        let wrapped = RequestStateChangeEvent::FunctionRunCompleted(event);
        assert_eq!(wrapped.namespace(), "test-ns");
        assert_eq!(wrapped.application_name(), "test-app");
        assert_eq!(wrapped.application_version(), "2.0.2");
        assert_eq!(wrapped.request_id(), "req-003");
    }

    #[test]
    fn test_function_run_matched_cache_event_metadata() {
        let event = FunctionRunMatchedCache {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "2.0.3".to_string(),
            request_id: "req-004".to_string(),
            function_name: "my-function".to_string(),
            function_run_id: "run-111".to_string(),
        };

        assert_eq!(event.namespace(), "test-ns");
        assert_eq!(event.application_name(), "test-app");
        assert_eq!(event.application_version(), "2.0.3");
        assert_eq!(event.request_id(), "req-004");

        let wrapped = RequestStateChangeEvent::FunctionRunMatchedCache(event);
        assert_eq!(wrapped.namespace(), "test-ns");
        assert_eq!(wrapped.application_name(), "test-app");
        assert_eq!(wrapped.application_version(), "2.0.3");
        assert_eq!(wrapped.request_id(), "req-004");
    }
}
