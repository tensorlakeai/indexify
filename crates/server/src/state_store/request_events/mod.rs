pub mod enrichment;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    data_model::{FunctionRunOutcome, FunctionRunStatus, RequestCtx, RequestOutcome},
    state_store::requests::{RequestPayload, StateMachineUpdateRequest},
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

impl From<u64> for RequestStateChangeEventId {
    fn from(seq: u64) -> Self {
        Self::new(seq)
    }
}

pub trait RequestEventMetadata {
    fn namespace(&self) -> &str;
    fn application_name(&self) -> &str;
    fn application_version(&self) -> &str;
    fn request_id(&self) -> &str;

    fn container_id(&self) -> Option<&str>;
    fn function_run_id(&self) -> Option<&str>;
    fn allocation_id(&self) -> Option<&str>;
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
    FunctionRunCompleted(FunctionRunCompleted),
    FunctionRunMatchedCache(FunctionRunMatchedCache),
    AllocationCreated(AllocationCreated),
    AllocationCompleted(AllocationCompleted),
    RequestFinished(RequestFinishedEvent),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RequestStateFinishedOutput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<serde_json::Value>,
    pub path: String,
    pub content_encoding: String,
}

impl RequestStateChangeEvent {
    pub fn finished(
        ctx: &RequestCtx,
        outcome: &RequestOutcome,
        output: Option<RequestStateFinishedOutput>,
    ) -> Self {
        Self::RequestFinished(RequestFinishedEvent {
            namespace: ctx.namespace.clone(),
            application_name: ctx.application_name.clone(),
            application_version: ctx.application_version.clone(),
            request_id: ctx.request_id.clone(),
            outcome: outcome.clone(),
            created_at: Utc::now(),
            output,
        })
    }

    pub fn namespace(&self) -> &str {
        match self {
            RequestStateChangeEvent::RequestStarted(event) => event.namespace(),
            RequestStateChangeEvent::RequestFinished(event) => event.namespace(),
            RequestStateChangeEvent::FunctionRunCreated(event) => event.namespace(),
            RequestStateChangeEvent::FunctionRunCompleted(event) => event.namespace(),
            RequestStateChangeEvent::FunctionRunMatchedCache(event) => event.namespace(),
            RequestStateChangeEvent::AllocationCreated(event) => event.namespace(),
            RequestStateChangeEvent::AllocationCompleted(event) => event.namespace(),
        }
    }

    pub fn application_name(&self) -> &str {
        match self {
            RequestStateChangeEvent::RequestStarted(event) => event.application_name(),
            RequestStateChangeEvent::RequestFinished(event) => event.application_name(),
            RequestStateChangeEvent::FunctionRunCreated(event) => event.application_name(),
            RequestStateChangeEvent::FunctionRunCompleted(event) => event.application_name(),
            RequestStateChangeEvent::FunctionRunMatchedCache(event) => event.application_name(),
            RequestStateChangeEvent::AllocationCreated(event) => event.application_name(),
            RequestStateChangeEvent::AllocationCompleted(event) => event.application_name(),
        }
    }

    pub fn application_version(&self) -> &str {
        match self {
            RequestStateChangeEvent::RequestStarted(event) => event.application_version(),
            RequestStateChangeEvent::RequestFinished(event) => event.application_version(),
            RequestStateChangeEvent::FunctionRunCreated(event) => event.application_version(),
            RequestStateChangeEvent::FunctionRunCompleted(event) => event.application_version(),
            RequestStateChangeEvent::FunctionRunMatchedCache(event) => event.application_version(),
            RequestStateChangeEvent::AllocationCreated(event) => event.application_version(),
            RequestStateChangeEvent::AllocationCompleted(event) => event.application_version(),
        }
    }

    pub fn request_id(&self) -> &str {
        match self {
            RequestStateChangeEvent::RequestStarted(event) => event.request_id(),
            RequestStateChangeEvent::RequestFinished(event) => event.request_id(),
            RequestStateChangeEvent::FunctionRunCreated(event) => event.request_id(),
            RequestStateChangeEvent::FunctionRunCompleted(event) => event.request_id(),
            RequestStateChangeEvent::FunctionRunMatchedCache(event) => event.request_id(),
            RequestStateChangeEvent::AllocationCreated(event) => event.request_id(),
            RequestStateChangeEvent::AllocationCompleted(event) => event.request_id(),
        }
    }

    pub fn message(&self) -> &str {
        match self {
            RequestStateChangeEvent::RequestStarted(_) => "Request Started",
            RequestStateChangeEvent::RequestFinished(_) => "Request Finished",
            RequestStateChangeEvent::FunctionRunCreated(_) => "Function Run Created",
            RequestStateChangeEvent::FunctionRunCompleted(_) => "Function Run Completed",
            RequestStateChangeEvent::FunctionRunMatchedCache(_) => {
                "Function Run Matched a Cached output"
            }
            RequestStateChangeEvent::AllocationCreated(_) => "Allocation Created",
            RequestStateChangeEvent::AllocationCompleted(_) => "Allocation Completed",
        }
    }

    pub fn container_id(&self) -> Option<&str> {
        match self {
            RequestStateChangeEvent::RequestStarted(event) => event.container_id(),
            RequestStateChangeEvent::RequestFinished(event) => event.container_id(),
            RequestStateChangeEvent::FunctionRunCreated(event) => event.container_id(),
            RequestStateChangeEvent::FunctionRunCompleted(event) => event.container_id(),
            RequestStateChangeEvent::FunctionRunMatchedCache(event) => event.container_id(),
            RequestStateChangeEvent::AllocationCreated(event) => event.container_id(),
            RequestStateChangeEvent::AllocationCompleted(event) => event.container_id(),
        }
    }

    pub fn function_run_id(&self) -> Option<&str> {
        match self {
            RequestStateChangeEvent::RequestStarted(event) => event.function_run_id(),
            RequestStateChangeEvent::RequestFinished(event) => event.function_run_id(),
            RequestStateChangeEvent::FunctionRunCreated(event) => event.function_run_id(),
            RequestStateChangeEvent::FunctionRunCompleted(event) => event.function_run_id(),
            RequestStateChangeEvent::FunctionRunMatchedCache(event) => event.function_run_id(),
            RequestStateChangeEvent::AllocationCreated(event) => event.function_run_id(),
            RequestStateChangeEvent::AllocationCompleted(event) => event.function_run_id(),
        }
    }

    pub fn allocation_id(&self) -> Option<&str> {
        match self {
            RequestStateChangeEvent::RequestStarted(event) => event.allocation_id(),
            RequestStateChangeEvent::RequestFinished(event) => event.allocation_id(),
            RequestStateChangeEvent::FunctionRunCreated(event) => event.allocation_id(),
            RequestStateChangeEvent::FunctionRunCompleted(event) => event.allocation_id(),
            RequestStateChangeEvent::FunctionRunMatchedCache(event) => event.allocation_id(),
            RequestStateChangeEvent::AllocationCreated(event) => event.allocation_id(),
            RequestStateChangeEvent::AllocationCompleted(event) => event.allocation_id(),
        }
    }
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestFinishedEvent {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
    pub outcome: RequestOutcome,
    #[serde(default)]
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub output: Option<RequestStateFinishedOutput>,
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

    fn container_id(&self) -> Option<&str> {
        None
    }

    fn function_run_id(&self) -> Option<&str> {
        None
    }

    fn allocation_id(&self) -> Option<&str> {
        None
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestStartedEvent {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
    #[serde(default)]
    pub created_at: DateTime<Utc>,
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

    fn container_id(&self) -> Option<&str> {
        None
    }

    fn function_run_id(&self) -> Option<&str> {
        None
    }

    fn allocation_id(&self) -> Option<&str> {
        None
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
    #[serde(default)]
    pub created_at: DateTime<Utc>,
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

    fn container_id(&self) -> Option<&str> {
        None
    }

    fn function_run_id(&self) -> Option<&str> {
        Some(&self.function_run_id)
    }

    fn allocation_id(&self) -> Option<&str> {
        None
    }
}

/// Event emitted when a function run reaches its final outcome (after all
/// retries exhausted or success)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FunctionRunCompleted {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
    pub function_name: String,
    pub function_run_id: String,
    pub outcome: FunctionRunOutcomeSummary,
    #[serde(default)]
    pub created_at: DateTime<Utc>,
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

    fn container_id(&self) -> Option<&str> {
        None
    }

    fn function_run_id(&self) -> Option<&str> {
        Some(&self.function_run_id)
    }

    fn allocation_id(&self) -> Option<&str> {
        None
    }
}

/// Event emitted when an allocation (execution attempt) is created and assigned
/// to an executor
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AllocationCreated {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
    pub function_name: String,
    pub function_run_id: String,
    pub allocation_id: String,
    pub executor_id: String,
    #[serde(default)]
    pub container_id: String,
    #[serde(default)]
    pub created_at: DateTime<Utc>,
}

impl RequestEventMetadata for AllocationCreated {
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

    fn container_id(&self) -> Option<&str> {
        Some(&self.container_id)
    }

    fn function_run_id(&self) -> Option<&str> {
        Some(&self.function_run_id)
    }

    fn allocation_id(&self) -> Option<&str> {
        Some(&self.allocation_id)
    }
}

/// Event emitted when an allocation (execution attempt) completes with an
/// outcome
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AllocationCompleted {
    pub namespace: String,
    pub application_name: String,
    pub application_version: String,
    pub request_id: String,
    pub function_name: String,
    pub function_run_id: String,
    pub allocation_id: String,
    pub outcome: FunctionRunOutcomeSummary,
    #[serde(default)]
    pub container_id: String,
    #[serde(default)]
    pub created_at: DateTime<Utc>,
}

impl RequestEventMetadata for AllocationCompleted {
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

    fn container_id(&self) -> Option<&str> {
        Some(&self.container_id)
    }

    fn function_run_id(&self) -> Option<&str> {
        Some(&self.function_run_id)
    }

    fn allocation_id(&self) -> Option<&str> {
        Some(&self.allocation_id)
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
    #[serde(default)]
    pub created_at: DateTime<Utc>,
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

    fn container_id(&self) -> Option<&str> {
        None
    }

    fn function_run_id(&self) -> Option<&str> {
        Some(&self.function_run_id)
    }

    fn allocation_id(&self) -> Option<&str> {
        None
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
                    created_at: Utc::now(),
                },
            )];

            // Emit FunctionRunCreated for all function runs in the request context
            for function_run in request.ctx.function_runs.values() {
                events.push(RequestStateChangeEvent::FunctionRunCreated(
                    FunctionRunCreated {
                        namespace: function_run.namespace.clone(),
                        application_name: function_run.application.clone(),
                        application_version: function_run.version.clone(),
                        request_id: function_run.request_id.clone(),
                        function_name: function_run.name.clone(),
                        function_run_id: function_run.id.to_string(),
                        created_at: Utc::now(),
                    },
                ));
            }

            events
        }
        RequestPayload::UpsertExecutor(_) => vec![],
        RequestPayload::SchedulerUpdate(payload) => {
            let sched_update = &payload.update;
            let mut changes = Vec::new();

            // 1. FunctionRunCreated events first (runs must exist before being assigned)
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
                                    created_at: Utc::now(),
                                },
                            ));
                        }
                    }
                }
            }

            for allocation in &sched_update.new_allocations {
                changes.push(RequestStateChangeEvent::AllocationCreated(
                    AllocationCreated {
                        namespace: allocation.namespace.clone(),
                        application_name: allocation.application.clone(),
                        application_version: allocation.application_version.clone(),
                        request_id: allocation.request_id.clone(),
                        function_name: allocation.function.clone(),
                        function_run_id: allocation.function_call_id.to_string(),
                        executor_id: allocation.target.executor_id.get().to_string(),
                        container_id: allocation.target.container_id.get().to_string(),
                        allocation_id: allocation.id.to_string(),
                        created_at: Utc::now(),
                    },
                ));
            }

            for allocation in &sched_update.updated_allocations {
                if !matches!(allocation.outcome, FunctionRunOutcome::Unknown) {
                    changes.push(RequestStateChangeEvent::AllocationCompleted(
                        AllocationCompleted {
                            namespace: allocation.namespace.clone(),
                            application_name: allocation.application.clone(),
                            application_version: allocation.application_version.clone(),
                            request_id: allocation.request_id.clone(),
                            function_name: allocation.function.clone(),
                            function_run_id: allocation.function_call_id.to_string(),
                            allocation_id: allocation.id.to_string(),
                            outcome: (&allocation.outcome).into(),
                            container_id: allocation.target.container_id.get().to_string(),
                            created_at: Utc::now(),
                        },
                    ));
                }
            }

            for (ctx_key, function_call_ids) in &sched_update.updated_function_runs {
                if let Some(ctx) = sched_update.updated_request_states.get(ctx_key) {
                    for function_call_id in function_call_ids {
                        // Only emit FunctionRunCompleted when the run has reached its final state
                        if let Some(function_run) = ctx.function_runs.get(function_call_id) &&
                            matches!(function_run.status, FunctionRunStatus::Completed) &&
                            let Some(outcome) = &function_run.outcome
                        {
                            changes.push(RequestStateChangeEvent::FunctionRunCompleted(
                                FunctionRunCompleted {
                                    namespace: function_run.namespace.clone(),
                                    application_name: function_run.application.clone(),
                                    application_version: function_run.version.clone(),
                                    request_id: function_run.request_id.clone(),
                                    function_name: function_run.name.clone(),
                                    function_run_id: function_run.id.to_string(),
                                    outcome: outcome.into(),
                                    created_at: Utc::now(),
                                },
                            ));
                        }
                    }
                }
            }

            for request_ctx in sched_update.updated_request_states.values() {
                if let Some(outcome) = &request_ctx.outcome {
                    changes.push(RequestStateChangeEvent::finished(
                        request_ctx,
                        outcome,
                        None,
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
    use crate::data_model::RequestCtxBuilder;

    #[test]
    fn test_request_started_event_metadata() {
        let event = RequestStartedEvent {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "1.0.1".to_string(),
            request_id: "req-456".to_string(),
            created_at: Utc::now(),
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
        let ctx = RequestCtxBuilder::default()
            .namespace("test-ns".to_string())
            .application_name("test-app".to_string())
            .application_version("1.0.2".to_string())
            .request_id("req-789".to_string())
            .outcome(Some(RequestOutcome::Success))
            .function_calls(Default::default())
            .function_runs(Default::default())
            .build()
            .unwrap();
        let event = RequestStateChangeEvent::finished(&ctx, &RequestOutcome::Success, None);

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
            created_at: Utc::now(),
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
    fn test_function_run_completed_event_metadata() {
        let event = FunctionRunCompleted {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "2.0.2".to_string(),
            request_id: "req-003".to_string(),
            function_name: "my-function".to_string(),
            function_run_id: "run-789".to_string(),
            outcome: FunctionRunOutcomeSummary::Success,
            created_at: Utc::now(),
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
    fn test_allocation_created_event_metadata() {
        let event = AllocationCreated {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "2.0.1".to_string(),
            request_id: "req-002".to_string(),
            function_name: "my-function".to_string(),
            function_run_id: "run-456".to_string(),
            allocation_id: "alloc-789".to_string(),
            executor_id: "executor-001".to_string(),
            container_id: "container-001".to_string(),
            created_at: Utc::now(),
        };

        assert_eq!(event.namespace(), "test-ns");
        assert_eq!(event.application_name(), "test-app");
        assert_eq!(event.application_version(), "2.0.1");
        assert_eq!(event.request_id(), "req-002");

        let wrapped = RequestStateChangeEvent::AllocationCreated(event);
        assert_eq!(wrapped.namespace(), "test-ns");
        assert_eq!(wrapped.application_name(), "test-app");
        assert_eq!(wrapped.application_version(), "2.0.1");
        assert_eq!(wrapped.request_id(), "req-002");
    }

    #[test]
    fn test_allocation_completed_event_metadata() {
        let event = AllocationCompleted {
            namespace: "test-ns".to_string(),
            application_name: "test-app".to_string(),
            application_version: "2.0.2".to_string(),
            request_id: "req-003".to_string(),
            function_name: "my-function".to_string(),
            function_run_id: "run-789".to_string(),
            allocation_id: "alloc-456".to_string(),
            outcome: FunctionRunOutcomeSummary::Success,
            container_id: "container-001".to_string(),
            created_at: Utc::now(),
        };

        assert_eq!(event.namespace(), "test-ns");
        assert_eq!(event.application_name(), "test-app");
        assert_eq!(event.application_version(), "2.0.2");
        assert_eq!(event.request_id(), "req-003");

        let wrapped = RequestStateChangeEvent::AllocationCompleted(event);
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
            created_at: Utc::now(),
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
