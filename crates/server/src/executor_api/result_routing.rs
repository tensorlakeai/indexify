use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use super::{
    BlobStorageRegistry,
    ExecutorId,
    IndexifyState,
    RequestPayload,
    StateMachineUpdateRequest,
    executor_api_pb,
};
use crate::{data_model::FunctionCallId, proto_convert::to_internal_compute_op};

struct PendingFunctionCall {
    parent_allocation_id: String,
    /// The function_call_id from the original CallFunction that the parent FE
    /// registered its watcher under. Preserved through re-registrations so
    /// the final result is delivered with the ID the FE expects.
    original_function_call_id: String,
    executor_id: ExecutorId,
}

/// Routes function call results from child allocation completions
/// back to the parent executor's pending_results buffer via ExecutorConnection.
pub struct FunctionCallResultRouter {
    pending: RwLock<HashMap<String, PendingFunctionCall>>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum RegistrationOutcome {
    Inserted,
    Replaced,
    Unchanged,
}

impl FunctionCallResultRouter {
    pub fn new() -> Self {
        Self {
            pending: RwLock::new(HashMap::new()),
        }
    }

    /// Always overwrite the existing route for `function_call_id`.
    ///
    /// Used by tail-call chaining (`try_route_result`) where ownership must
    /// move to the downstream root function call ID.
    pub async fn register(
        &self,
        function_call_id: String,
        parent_allocation_id: String,
        original_function_call_id: String,
        executor_id: ExecutorId,
    ) {
        self.pending.write().await.insert(
            function_call_id,
            PendingFunctionCall {
                parent_allocation_id,
                original_function_call_id,
                executor_id,
            },
        );
    }

    /// Register a route coming from parent `CallFunction` logs.
    ///
    /// If the entry already exists for the same `(executor_id,
    /// parent_allocation_id)` chain, keep it unchanged so a prior tail-call
    /// registration preserves `original_function_call_id`.
    ///
    /// If the parent allocation or executor changed, replace the entry. This
    /// makes retries/reschedules safe when `function_call_id` is stable across
    /// attempts.
    async fn register_from_parent(
        &self,
        function_call_id: String,
        parent_allocation_id: String,
        original_function_call_id: String,
        executor_id: ExecutorId,
    ) -> RegistrationOutcome {
        use std::collections::hash_map::Entry;
        let mut pending = self.pending.write().await;
        match pending.entry(function_call_id) {
            Entry::Occupied(mut occ) => {
                let should_replace = {
                    let existing = occ.get();
                    existing.parent_allocation_id != parent_allocation_id ||
                        existing.executor_id != executor_id
                };

                if should_replace {
                    occ.insert(PendingFunctionCall {
                        parent_allocation_id,
                        original_function_call_id,
                        executor_id,
                    });
                    RegistrationOutcome::Replaced
                } else {
                    RegistrationOutcome::Unchanged
                }
            }
            Entry::Vacant(v) => {
                v.insert(PendingFunctionCall {
                    parent_allocation_id,
                    original_function_call_id,
                    executor_id,
                });
                RegistrationOutcome::Inserted
            }
        }
    }

    pub async fn purge_executor(&self, executor_id: &ExecutorId) -> usize {
        let mut pending = self.pending.write().await;
        let before = pending.len();
        pending.retain(|_, route| &route.executor_id != executor_id);
        before.saturating_sub(pending.len())
    }

    async fn take(&self, function_call_id: &str) -> Option<PendingFunctionCall> {
        self.pending.write().await.remove(function_call_id)
    }

    #[cfg(test)]
    pub(crate) async fn pending_len(&self) -> usize {
        self.pending.read().await.len()
    }
}

/// Process an allocation log entry received via heartbeat.
/// Adapted from the old `handle_log_entry` -- uses `executor_id` for router
/// registration instead of a direct `result_tx` channel.
pub(super) async fn handle_log_entry(
    log_entry: &executor_api_pb::AllocationLogEntry,
    executor_id: &ExecutorId,
    router: &Arc<FunctionCallResultRouter>,
    indexify_state: &Arc<IndexifyState>,
    blob_storage_registry: &Arc<BlobStorageRegistry>,
) -> Result<()> {
    let allocation_id = &log_entry.allocation_id;

    match &log_entry.entry {
        Some(executor_api_pb::allocation_log_entry::Entry::CallFunction(call)) => {
            // Register in router so results get pushed back to this executor.
            // We register each individual function call ID from the updates,
            // not just the root_function_call_id. When a CallFunction contains
            // multiple function calls (e.g. a .map() that fans out), each child
            // allocation completes with its own function_call_id. The router
            // must match on those individual IDs to route results back.
            if let Some(ref updates) = call.updates {
                for update in &updates.updates {
                    if let Some(ref op) = update.op &&
                        let executor_api_pb::execution_plan_update::Op::FunctionCall(fc) = op &&
                        let Some(ref individual_fc_id) = fc.id
                    {
                        match router
                            .register_from_parent(
                                individual_fc_id.clone(),
                                allocation_id.clone(),
                                individual_fc_id.clone(),
                                executor_id.clone(),
                            )
                            .await
                        {
                            RegistrationOutcome::Inserted => {
                                debug!(
                                    executor_id = executor_id.get(),
                                    allocation_id = %allocation_id,
                                    function_call_id = %individual_fc_id,
                                    "heartbeat: registered individual function call in router"
                                );
                            }
                            RegistrationOutcome::Replaced => {
                                info!(
                                    executor_id = executor_id.get(),
                                    allocation_id = %allocation_id,
                                    function_call_id = %individual_fc_id,
                                    "heartbeat: replaced stale function call router entry for \
                                     rescheduled attempt"
                                );
                            }
                            RegistrationOutcome::Unchanged => {
                                debug!(
                                    executor_id = executor_id.get(),
                                    allocation_id = %allocation_id,
                                    function_call_id = %individual_fc_id,
                                    "heartbeat: function call already registered for same \
                                     parent chain, keeping existing route"
                                );
                            }
                        }
                    }
                }
            }

            // Extract namespace/application/request_id from the proto message
            let namespace = call
                .namespace
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("CallFunction missing namespace"))?
                .clone();
            let application = call
                .application
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("CallFunction missing application"))?
                .clone();
            let request_id = call
                .request_id
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("CallFunction missing request_id"))?
                .clone();
            let source_function_call_id = call
                .source_function_call_id
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("CallFunction missing source_function_call_id"))?
                .clone();

            let updates = call
                .updates
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("CallFunction missing updates"))?;

            let root_function_call_id = updates
                .root_function_call_id
                .clone()
                .map(FunctionCallId::from)
                .unwrap_or_else(|| FunctionCallId::from(nanoid::nanoid!()));

            let mut compute_ops = Vec::new();
            for update in &updates.updates {
                compute_ops.push(to_internal_compute_op(
                    update.clone(),
                    blob_storage_registry,
                    Some(source_function_call_id.clone()),
                )?);
            }

            info!(
                executor_id = executor_id.get(),
                allocation_id = %allocation_id,
                namespace = %namespace,
                app = %application,
                request_id = %request_id,
                source_function_call_id = %source_function_call_id,
                "heartbeat: dispatching CallFunction to state machine"
            );

            let request = StateMachineUpdateRequest {
                payload: RequestPayload::CreateFunctionCall(
                    crate::state_store::requests::FunctionCallRequest {
                        namespace: namespace.clone(),
                        application_name: application.clone(),
                        request_id: request_id.clone(),
                        graph_updates: crate::state_store::requests::RequestUpdates {
                            request_updates: compute_ops,
                            output_function_call_id: root_function_call_id,
                        },
                        source_function_call_id: FunctionCallId::from(source_function_call_id),
                    },
                ),
            };

            indexify_state.write(request).await?;
        }
        Some(executor_api_pb::allocation_log_entry::Entry::FunctionCallResult(_)) => {
            // Server -> Executor direction only -- should not be received from client
            warn!(
                executor_id = executor_id.get(),
                allocation_id = %allocation_id,
                "heartbeat: unexpected FunctionCallResult from client"
            );
        }
        None => {
            warn!(
                executor_id = executor_id.get(),
                allocation_id = %allocation_id,
                "heartbeat: empty log entry"
            );
        }
    }
    Ok(())
}

/// After processing a child AllocationCompleted, check if any parent is waiting
/// for this function call result and push it into their pending_results buffer.
///
/// When the child returns a direct `Value`, the result is pushed immediately
/// to the parent. When the child returns `Updates` (graph updates that spawn
/// downstream function calls), the pending entry is re-registered under the
/// downstream `root_function_call_id` so the final value in the chain gets
/// routed to the parent.
pub(super) async fn try_route_result(
    router: &Arc<FunctionCallResultRouter>,
    function_call_id: &str,
    completed: &executor_api_pb::AllocationCompleted,
    indexify_state: &Arc<IndexifyState>,
) {
    if let Some(pending) = router.take(function_call_id).await {
        match &completed.return_value {
            Some(executor_api_pb::allocation_completed::ReturnValue::Value(dp)) => {
                debug!(
                    function_call_id = %function_call_id,
                    parent_allocation_id = %pending.parent_allocation_id,
                    "try_route_result: routing Value result to parent"
                );

                let log_entry = build_result_log_entry(
                    pending.parent_allocation_id,
                    pending.original_function_call_id.clone(),
                    completed,
                    Some(dp.clone()),
                );

                let connections = indexify_state.executor_connections.read().await;
                if let Some(conn) = connections.get(&pending.executor_id) {
                    conn.push_result(log_entry).await;
                } else {
                    warn!(
                        function_call_id = %function_call_id,
                        executor_id = pending.executor_id.get(),
                        "try_route_result: executor connection not found, dropping result"
                    );
                }
            }
            Some(executor_api_pb::allocation_completed::ReturnValue::Updates(updates)) => {
                // The child returned graph updates that will spawn downstream
                // function calls. Re-register the pending entry under the
                // downstream root_function_call_id so that when the final
                // function in the chain completes with a Value, the result
                // gets routed back to the parent.
                if let Some(ref downstream_fc_id) = updates.root_function_call_id {
                    info!(
                        function_call_id = %function_call_id,
                        downstream_function_call_id = %downstream_fc_id,
                        parent_allocation_id = %pending.parent_allocation_id,
                        original_function_call_id = %pending.original_function_call_id,
                        "try_route_result: child returned Updates, re-registering \
                         pending entry for downstream function call"
                    );
                    router
                        .register(
                            downstream_fc_id.clone(),
                            pending.parent_allocation_id,
                            pending.original_function_call_id,
                            pending.executor_id,
                        )
                        .await;
                } else {
                    warn!(
                        function_call_id = %function_call_id,
                        "try_route_result: child returned Updates without \
                         root_function_call_id, cannot re-register"
                    );
                }
            }
            None => {
                // No return value at all -- send success with empty payload
                debug!(
                    function_call_id = %function_call_id,
                    parent_allocation_id = %pending.parent_allocation_id,
                    "try_route_result: routing empty result to parent"
                );

                let log_entry = build_result_log_entry(
                    pending.parent_allocation_id,
                    pending.original_function_call_id.clone(),
                    completed,
                    None,
                );

                let connections = indexify_state.executor_connections.read().await;
                if let Some(conn) = connections.get(&pending.executor_id) {
                    conn.push_result(log_entry).await;
                } else {
                    warn!(
                        function_call_id = %function_call_id,
                        executor_id = pending.executor_id.get(),
                        "try_route_result: executor connection not found, dropping result"
                    );
                }
            }
        }
    } else {
        debug!(
            function_call_id = %function_call_id,
            allocation_id = %completed.allocation_id,
            "try_route_result: no match in router"
        );
    }
}

pub(super) async fn try_route_failure(
    router: &Arc<FunctionCallResultRouter>,
    function_call_id: &str,
    failed: &executor_api_pb::AllocationFailed,
    indexify_state: &Arc<IndexifyState>,
) {
    if let Some(pending) = router.take(function_call_id).await {
        let log_entry = executor_api_pb::AllocationLogEntry {
            allocation_id: pending.parent_allocation_id,
            clock: 0,
            entry: Some(
                executor_api_pb::allocation_log_entry::Entry::FunctionCallResult(
                    executor_api_pb::FunctionCallResult {
                        namespace: failed.function.as_ref().and_then(|f| f.namespace.clone()),
                        request_id: failed.request_id.clone(),
                        function_call_id: Some(pending.original_function_call_id.clone()),
                        outcome_code: Some(executor_api_pb::AllocationOutcomeCode::Failure.into()),
                        failure_reason: Some(failed.reason),
                        return_value: None,
                        request_error: failed.request_error.clone(),
                    },
                ),
            ),
        };

        let connections = indexify_state.executor_connections.read().await;
        if let Some(conn) = connections.get(&pending.executor_id) {
            conn.push_result(log_entry).await;
        } else {
            warn!(
                function_call_id = %function_call_id,
                executor_id = pending.executor_id.get(),
                "try_route_failure: executor connection not found, dropping result"
            );
        }
    }
}

/// Build a `FunctionCallResult` log entry to route back to the parent executor.
///
/// Shared by the `Value`/`None` arms of `try_route_result` to avoid
/// duplication.
fn build_result_log_entry(
    parent_allocation_id: String,
    original_function_call_id: String,
    completed: &executor_api_pb::AllocationCompleted,
    return_value: Option<executor_api_pb::DataPayload>,
) -> executor_api_pb::AllocationLogEntry {
    executor_api_pb::AllocationLogEntry {
        allocation_id: parent_allocation_id,
        clock: 0, // TODO: server-assigned monotonic clock
        entry: Some(
            executor_api_pb::allocation_log_entry::Entry::FunctionCallResult(
                executor_api_pb::FunctionCallResult {
                    namespace: completed
                        .function
                        .as_ref()
                        .and_then(|f| f.namespace.clone()),
                    request_id: completed.request_id.clone(),
                    function_call_id: Some(original_function_call_id),
                    outcome_code: Some(executor_api_pb::AllocationOutcomeCode::Success.into()),
                    failure_reason: None,
                    return_value,
                    request_error: None,
                },
            ),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_from_parent_replaces_stale_entry_on_reschedule() {
        let router = FunctionCallResultRouter::new();
        let executor_a = ExecutorId::new("executor-a".to_string());
        let executor_b = ExecutorId::new("executor-b".to_string());

        let first = router
            .register_from_parent(
                "fc-1".to_string(),
                "alloc-a".to_string(),
                "fc-1".to_string(),
                executor_a,
            )
            .await;
        assert_eq!(first, RegistrationOutcome::Inserted);

        let second = router
            .register_from_parent(
                "fc-1".to_string(),
                "alloc-b".to_string(),
                "fc-1".to_string(),
                executor_b.clone(),
            )
            .await;
        assert_eq!(second, RegistrationOutcome::Replaced);

        let entry = router.take("fc-1").await.expect("entry should exist");
        assert_eq!(entry.parent_allocation_id, "alloc-b");
        assert_eq!(entry.executor_id, executor_b);
        assert_eq!(entry.original_function_call_id, "fc-1");
    }

    #[tokio::test]
    async fn test_register_from_parent_keeps_existing_for_same_chain() {
        let router = FunctionCallResultRouter::new();
        let executor = ExecutorId::new("executor-a".to_string());

        // Simulate tail-call registration that already preserved the original
        // function_call_id for this chain.
        router
            .register(
                "fc-1".to_string(),
                "alloc-a".to_string(),
                "root-fc".to_string(),
                executor.clone(),
            )
            .await;

        let outcome = router
            .register_from_parent(
                "fc-1".to_string(),
                "alloc-a".to_string(),
                "fc-1".to_string(),
                executor,
            )
            .await;
        assert_eq!(outcome, RegistrationOutcome::Unchanged);

        let entry = router.take("fc-1").await.expect("entry should exist");
        assert_eq!(entry.parent_allocation_id, "alloc-a");
        assert_eq!(entry.original_function_call_id, "root-fc");
    }

    #[tokio::test]
    async fn test_purge_executor_removes_only_target_executor_entries() {
        let router = FunctionCallResultRouter::new();
        let executor_a = ExecutorId::new("executor-a".to_string());
        let executor_b = ExecutorId::new("executor-b".to_string());

        router
            .register(
                "fc-a1".to_string(),
                "alloc-a1".to_string(),
                "fc-a1".to_string(),
                executor_a.clone(),
            )
            .await;
        router
            .register(
                "fc-a2".to_string(),
                "alloc-a2".to_string(),
                "fc-a2".to_string(),
                executor_a,
            )
            .await;
        router
            .register(
                "fc-b1".to_string(),
                "alloc-b1".to_string(),
                "fc-b1".to_string(),
                executor_b.clone(),
            )
            .await;

        let purged = router.purge_executor(&executor_b).await;
        assert_eq!(purged, 1);
        assert!(router.take("fc-b1").await.is_none());
        assert!(router.take("fc-a1").await.is_some());
        assert!(router.take("fc-a2").await.is_some());
    }
}
