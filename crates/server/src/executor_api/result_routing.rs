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

#[derive(Debug)]
struct MissingExecutorConnectionError;

impl std::fmt::Display for MissingExecutorConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "executor connection not found")
    }
}

impl std::error::Error for MissingExecutorConnectionError {}

fn is_missing_executor_connection_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<MissingExecutorConnectionError>()
        .is_some()
}

#[derive(Clone)]
struct PendingFunctionCall {
    parent_allocation_id: String,
    /// The function_call_id from the original CallFunction that the parent FE
    /// registered its watcher under. Preserved through re-registrations so
    /// the final result is delivered with the ID the FE expects.
    original_function_call_id: String,
    executor_id: ExecutorId,
}

#[derive(Clone, Copy)]
struct ParentRequestContext<'a> {
    namespace: &'a str,
    application: &'a str,
    request_id: &'a str,
}

/// Routes function call results from child allocation completions
/// back to the parent executor's pending_results buffer via ExecutorConnection.
pub struct FunctionCallResultRouter {
    pending: RwLock<HashMap<String, PendingFunctionCall>>,
    indexify_state: Arc<IndexifyState>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum RegistrationOutcome {
    Inserted,
    Replaced,
    Unchanged,
}

impl FunctionCallResultRouter {
    pub fn new(indexify_state: Arc<IndexifyState>) -> Self {
        Self {
            pending: RwLock::new(HashMap::new()),
            indexify_state,
        }
    }

    fn from_persisted(
        route: crate::state_store::PersistedFunctionCallRoute,
    ) -> PendingFunctionCall {
        PendingFunctionCall {
            parent_allocation_id: route.parent_allocation_id,
            original_function_call_id: route.original_function_call_id,
            executor_id: route.executor_id,
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
    ) -> Result<()> {
        self.indexify_state
            .upsert_function_call_route(
                &function_call_id,
                &parent_allocation_id,
                &original_function_call_id,
                &executor_id,
            )
            .await?;
        self.pending.write().await.insert(
            function_call_id,
            PendingFunctionCall {
                parent_allocation_id,
                original_function_call_id,
                executor_id,
            },
        );
        Ok(())
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
        request_ctx: ParentRequestContext<'_>,
    ) -> Result<RegistrationOutcome> {
        let existing = {
            let pending = self.pending.read().await;
            pending.get(&function_call_id).cloned()
        };

        let existing = match existing {
            Some(route) => Some(route),
            None => self
                .indexify_state
                .get_function_call_route(&function_call_id)
                .await?
                .map(Self::from_persisted),
        };

        let new_route = PendingFunctionCall {
            parent_allocation_id: parent_allocation_id.clone(),
            original_function_call_id: original_function_call_id.clone(),
            executor_id: executor_id.clone(),
        };

        match existing {
            Some(existing_route)
                if existing_route.parent_allocation_id == parent_allocation_id &&
                    existing_route.executor_id == executor_id =>
            {
                self.pending
                    .write()
                    .await
                    .entry(function_call_id)
                    .or_insert(existing_route);
                Ok(RegistrationOutcome::Unchanged)
            }
            Some(existing_route)
                if existing_route.original_function_call_id != function_call_id =>
            {
                // Tail-call route ownership has already been rebound by
                // try_route_result() to a parent allocation that is waiting
                // on this downstream function_call_id. Do not allow a
                // subsequently ingested child CallFunction log to clobber that
                // live parent route.
                let existing_parent_alive = self
                    .is_allocation_alive_for_request(
                        request_ctx.namespace,
                        request_ctx.application,
                        request_ctx.request_id,
                        &existing_route.parent_allocation_id,
                    )
                    .await?;
                if existing_parent_alive {
                    self.pending
                        .write()
                        .await
                        .entry(function_call_id)
                        .or_insert(existing_route);
                    return Ok(RegistrationOutcome::Unchanged);
                }

                // Existing tail-call owner is stale/dead. Rebind to the new
                // parent allocation attempt while preserving the original
                // function_call_id expected by the parent FE watcher.
                let preserved_original = existing_route.original_function_call_id;
                self.indexify_state
                    .upsert_function_call_route(
                        &function_call_id,
                        &parent_allocation_id,
                        &preserved_original,
                        &executor_id,
                    )
                    .await?;
                self.pending.write().await.insert(
                    function_call_id,
                    PendingFunctionCall {
                        parent_allocation_id,
                        original_function_call_id: preserved_original,
                        executor_id,
                    },
                );
                Ok(RegistrationOutcome::Replaced)
            }
            Some(_) => {
                self.indexify_state
                    .upsert_function_call_route(
                        &function_call_id,
                        &parent_allocation_id,
                        &original_function_call_id,
                        &executor_id,
                    )
                    .await?;
                self.pending
                    .write()
                    .await
                    .insert(function_call_id, new_route);
                Ok(RegistrationOutcome::Replaced)
            }
            None => {
                self.indexify_state
                    .upsert_function_call_route(
                        &function_call_id,
                        &parent_allocation_id,
                        &original_function_call_id,
                        &executor_id,
                    )
                    .await?;
                self.pending
                    .write()
                    .await
                    .insert(function_call_id, new_route);
                Ok(RegistrationOutcome::Inserted)
            }
        }
    }

    async fn is_allocation_alive_for_request(
        &self,
        namespace: &str,
        application: &str,
        request_id: &str,
        allocation_id: &str,
    ) -> Result<bool> {
        let key = crate::data_model::Allocation::key_from(
            namespace,
            application,
            request_id,
            allocation_id,
        );
        let allocation = self.indexify_state.reader().get_allocation(&key).await?;
        Ok(matches!(allocation, Some(a) if !a.is_terminal()))
    }

    pub async fn purge_executor(&self, executor_id: &ExecutorId) -> Result<usize> {
        let mut pending = self.pending.write().await;
        let before = pending.len();
        pending.retain(|_, route| &route.executor_id != executor_id);
        let in_mem_purged = before.saturating_sub(pending.len());
        drop(pending);

        let persisted_purged = self
            .indexify_state
            .purge_function_call_routes_for_executor(executor_id)
            .await?;
        Ok(persisted_purged.max(in_mem_purged))
    }

    async fn take(&self, function_call_id: &str) -> Result<Option<PendingFunctionCall>> {
        if let Some(route) = self.pending.write().await.remove(function_call_id) {
            if let Err(err) = self
                .indexify_state
                .delete_function_call_route(function_call_id)
                .await
            {
                warn!(
                    function_call_id = %function_call_id,
                    error = ?err,
                    "failed to delete persisted function call route after in-memory take"
                );
            }
            return Ok(Some(route));
        }

        Ok(self
            .indexify_state
            .take_function_call_route(function_call_id)
            .await?
            .map(Self::from_persisted))
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
            // Extract namespace/application/request_id from the proto message.
            // Required by both routing registration and state-machine updates.
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
                                ParentRequestContext {
                                    namespace: &namespace,
                                    application: &application,
                                    request_id: &request_id,
                                },
                            )
                            .await?
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
                                    "heartbeat: function call already registered, keeping \
                                     existing route"
                                );
                            }
                        }
                    }
                }
            }

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
) -> Result<()> {
    let pending = match router.take(function_call_id).await {
        Ok(route) => route,
        Err(err) => {
            warn!(
                function_call_id = %function_call_id,
                error = ?err,
                "try_route_result: failed to load/take function call route"
            );
            return Err(err);
        }
    };

    if let Some(pending) = pending {
        if !is_parent_allocation_alive(
            indexify_state,
            &pending.parent_allocation_id,
            completed.function.as_ref(),
            completed.request_id.as_deref(),
        )
        .await?
        {
            info!(
                function_call_id = %function_call_id,
                parent_allocation_id = %pending.parent_allocation_id,
                executor_id = pending.executor_id.get(),
                "try_route_result: parent allocation not alive; dropping routed result and route"
            );
            return Ok(());
        }

        match &completed.return_value {
            Some(executor_api_pb::allocation_completed::ReturnValue::Value(dp)) => {
                debug!(
                    function_call_id = %function_call_id,
                    parent_allocation_id = %pending.parent_allocation_id,
                    "try_route_result: routing Value result to parent"
                );

                let log_entry = build_result_log_entry(
                    pending.parent_allocation_id.clone(),
                    pending.original_function_call_id.clone(),
                    completed,
                    Some(dp.clone()),
                );
                if let Err(err) = push_result_to_executor(
                    indexify_state,
                    &pending.executor_id,
                    function_call_id,
                    log_entry,
                )
                .await
                {
                    if is_missing_executor_connection_error(&err) {
                        warn!(
                            function_call_id = %function_call_id,
                            executor_id = pending.executor_id.get(),
                            "try_route_result: parent executor connection missing, dropping routed result"
                        );
                        return Ok(());
                    }
                    if let Err(recover_err) = router
                        .register(
                            function_call_id.to_string(),
                            pending.parent_allocation_id,
                            pending.original_function_call_id,
                            pending.executor_id,
                        )
                        .await
                    {
                        warn!(
                            function_call_id = %function_call_id,
                            error = ?recover_err,
                            "try_route_result: failed to restore route after routing error"
                        );
                    }
                    return Err(err);
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
                    if let Err(err) = router
                        .register(
                            downstream_fc_id.clone(),
                            pending.parent_allocation_id.clone(),
                            pending.original_function_call_id.clone(),
                            pending.executor_id.clone(),
                        )
                        .await
                    {
                        if let Err(recover_err) = router
                            .register(
                                function_call_id.to_string(),
                                pending.parent_allocation_id,
                                pending.original_function_call_id,
                                pending.executor_id,
                            )
                            .await
                        {
                            warn!(
                                function_call_id = %function_call_id,
                                downstream_function_call_id = %downstream_fc_id,
                                error = ?recover_err,
                                "try_route_result: failed to restore route after downstream \
                                 registration error"
                            );
                        }
                        return Err(err);
                    }
                } else {
                    warn!(
                        function_call_id = %function_call_id,
                        "try_route_result: child returned Updates without \
                         root_function_call_id, cannot re-register"
                    );
                    if let Err(recover_err) = router
                        .register(
                            function_call_id.to_string(),
                            pending.parent_allocation_id,
                            pending.original_function_call_id,
                            pending.executor_id,
                        )
                        .await
                    {
                        warn!(
                            function_call_id = %function_call_id,
                            error = ?recover_err,
                            "try_route_result: failed to restore route after missing \
                             root_function_call_id"
                        );
                    }
                    return Err(anyhow::anyhow!(
                        "AllocationCompleted Updates missing root_function_call_id"
                    ));
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
                    pending.parent_allocation_id.clone(),
                    pending.original_function_call_id.clone(),
                    completed,
                    None,
                );
                if let Err(err) = push_result_to_executor(
                    indexify_state,
                    &pending.executor_id,
                    function_call_id,
                    log_entry,
                )
                .await
                {
                    if is_missing_executor_connection_error(&err) {
                        warn!(
                            function_call_id = %function_call_id,
                            executor_id = pending.executor_id.get(),
                            "try_route_result: parent executor connection missing, dropping routed result"
                        );
                        return Ok(());
                    }
                    if let Err(recover_err) = router
                        .register(
                            function_call_id.to_string(),
                            pending.parent_allocation_id,
                            pending.original_function_call_id,
                            pending.executor_id,
                        )
                        .await
                    {
                        warn!(
                            function_call_id = %function_call_id,
                            error = ?recover_err,
                            "try_route_result: failed to restore route after routing error"
                        );
                    }
                    return Err(err);
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
    Ok(())
}

pub(super) async fn try_route_failure(
    router: &Arc<FunctionCallResultRouter>,
    function_call_id: &str,
    failed: &executor_api_pb::AllocationFailed,
    indexify_state: &Arc<IndexifyState>,
) -> Result<()> {
    let pending = match router.take(function_call_id).await {
        Ok(route) => route,
        Err(err) => {
            warn!(
                function_call_id = %function_call_id,
                error = ?err,
                "try_route_failure: failed to load/take function call route"
            );
            return Err(err);
        }
    };

    if let Some(pending) = pending {
        if !is_parent_allocation_alive(
            indexify_state,
            &pending.parent_allocation_id,
            failed.function.as_ref(),
            failed.request_id.as_deref(),
        )
        .await?
        {
            info!(
                function_call_id = %function_call_id,
                parent_allocation_id = %pending.parent_allocation_id,
                executor_id = pending.executor_id.get(),
                "try_route_failure: parent allocation not alive; dropping routed failure and route"
            );
            return Ok(());
        }

        let log_entry = executor_api_pb::AllocationLogEntry {
            allocation_id: pending.parent_allocation_id.clone(),
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

        if let Err(err) = push_result_to_executor(
            indexify_state,
            &pending.executor_id,
            function_call_id,
            log_entry,
        )
        .await
        {
            if is_missing_executor_connection_error(&err) {
                warn!(
                    function_call_id = %function_call_id,
                    executor_id = pending.executor_id.get(),
                    "try_route_failure: parent executor connection missing, dropping routed failure"
                );
                return Ok(());
            }
            if let Err(recover_err) = router
                .register(
                    function_call_id.to_string(),
                    pending.parent_allocation_id,
                    pending.original_function_call_id,
                    pending.executor_id,
                )
                .await
            {
                warn!(
                    function_call_id = %function_call_id,
                    error = ?recover_err,
                    "try_route_failure: failed to restore route after routing error"
                );
            }
            return Err(err);
        }
    }
    Ok(())
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

async fn push_result_to_executor(
    indexify_state: &Arc<IndexifyState>,
    executor_id: &ExecutorId,
    function_call_id: &str,
    log_entry: executor_api_pb::AllocationLogEntry,
) -> Result<()> {
    let conn = {
        let connections = indexify_state.executor_connections.read().await;
        connections.get(executor_id).cloned()
    };
    let Some(conn) = conn else {
        warn!(
            function_call_id = %function_call_id,
            executor_id = executor_id.get(),
            "missing executor connection while routing result"
        );
        return Err(anyhow::Error::new(MissingExecutorConnectionError));
    };

    conn.push_result(log_entry).await?;
    Ok(())
}

async fn is_parent_allocation_alive(
    indexify_state: &Arc<IndexifyState>,
    parent_allocation_id: &str,
    function: Option<&executor_api_pb::FunctionRef>,
    request_id: Option<&str>,
) -> Result<bool> {
    let Some(function) = function else {
        return Ok(false);
    };
    let Some(namespace) = function.namespace.as_deref() else {
        return Ok(false);
    };
    let Some(application) = function.application_name.as_deref() else {
        return Ok(false);
    };
    let Some(request_id) = request_id else {
        return Ok(false);
    };

    let parent_key = crate::data_model::Allocation::key_from(
        namespace,
        application,
        request_id,
        parent_allocation_id,
    );
    let parent = indexify_state.reader().get_allocation(&parent_key).await?;

    Ok(matches!(parent, Some(allocation) if !allocation.is_terminal()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data_model::{AllocationBuilder, AllocationTarget, ContainerId, FunctionCallId},
        state_store::{
            requests::{
                RequestPayload,
                SchedulerUpdatePayload,
                SchedulerUpdateRequest,
                StateMachineUpdateRequest,
            },
            test_state_store::TestStateStore,
        },
    };

    async fn insert_allocation(
        store: &TestStateStore,
        executor: &ExecutorId,
        namespace: &str,
        application: &str,
        request_id: &str,
        function_call_id: &str,
        outcome: crate::data_model::FunctionRunOutcome,
    ) -> String {
        let allocation = AllocationBuilder::default()
            .target(AllocationTarget::new(
                executor.clone(),
                ContainerId::new(format!("container-{function_call_id}")),
            ))
            .function_call_id(FunctionCallId::from(function_call_id.to_string()))
            .namespace(namespace.to_string())
            .application(application.to_string())
            .application_version("v1".to_string())
            .function("fn".to_string())
            .request_id(request_id.to_string())
            .outcome(outcome)
            .input_args(vec![])
            .call_metadata(bytes::Bytes::new())
            .build()
            .unwrap();
        let allocation_id = allocation.id.to_string();
        let mut update = SchedulerUpdateRequest::default();
        update.new_allocations.push(allocation);
        store
            .indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(update)),
            })
            .await
            .unwrap();
        allocation_id
    }

    fn parent_request_context() -> ParentRequestContext<'static> {
        ParentRequestContext {
            namespace: "ns",
            application: "app",
            request_id: "req-1",
        }
    }

    #[tokio::test]
    async fn test_register_from_parent_replaces_stale_entry_on_reschedule() {
        let store = TestStateStore::new().await.unwrap();
        let router = FunctionCallResultRouter::new(store.indexify_state.clone());
        let executor_a = ExecutorId::new("executor-a".to_string());
        let executor_b = ExecutorId::new("executor-b".to_string());

        let first = router
            .register_from_parent(
                "fc-1".to_string(),
                "alloc-a".to_string(),
                "fc-1".to_string(),
                executor_a,
                parent_request_context(),
            )
            .await
            .unwrap();
        assert_eq!(first, RegistrationOutcome::Inserted);

        let second = router
            .register_from_parent(
                "fc-1".to_string(),
                "alloc-b".to_string(),
                "fc-1".to_string(),
                executor_b.clone(),
                parent_request_context(),
            )
            .await
            .unwrap();
        assert_eq!(second, RegistrationOutcome::Replaced);

        let entry = router
            .take("fc-1")
            .await
            .unwrap()
            .expect("entry should exist");
        assert_eq!(entry.parent_allocation_id, "alloc-b");
        assert_eq!(entry.executor_id, executor_b);
        assert_eq!(entry.original_function_call_id, "fc-1");
    }

    #[tokio::test]
    async fn test_register_from_parent_keeps_existing_for_same_chain() {
        let store = TestStateStore::new().await.unwrap();
        let router = FunctionCallResultRouter::new(store.indexify_state.clone());
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
            .await
            .unwrap();

        let outcome = router
            .register_from_parent(
                "fc-1".to_string(),
                "alloc-a".to_string(),
                "fc-1".to_string(),
                executor,
                parent_request_context(),
            )
            .await
            .unwrap();
        assert_eq!(outcome, RegistrationOutcome::Unchanged);

        let entry = router
            .take("fc-1")
            .await
            .unwrap()
            .expect("entry should exist");
        assert_eq!(entry.parent_allocation_id, "alloc-a");
        assert_eq!(entry.original_function_call_id, "root-fc");
    }

    #[tokio::test]
    async fn test_register_from_parent_does_not_clobber_live_tail_call_route() {
        let store = TestStateStore::new().await.unwrap();
        let router = FunctionCallResultRouter::new(store.indexify_state.clone());
        let executor = ExecutorId::new("executor-a".to_string());
        let namespace = "ns";
        let application = "app";
        let request_id = "req-1";

        let parent_allocation_id = insert_allocation(
            &store,
            &executor,
            namespace,
            application,
            request_id,
            "parent-fc",
            crate::data_model::FunctionRunOutcome::Unknown,
        )
        .await;
        let child_allocation_id = insert_allocation(
            &store,
            &executor,
            namespace,
            application,
            request_id,
            "child-fc",
            crate::data_model::FunctionRunOutcome::Unknown,
        )
        .await;

        router
            .register(
                "downstream-fc".to_string(),
                parent_allocation_id.clone(),
                "root-fc".to_string(),
                executor.clone(),
            )
            .await
            .unwrap();

        let outcome = router
            .register_from_parent(
                "downstream-fc".to_string(),
                child_allocation_id,
                "downstream-fc".to_string(),
                executor,
                ParentRequestContext {
                    namespace,
                    application,
                    request_id,
                },
            )
            .await
            .unwrap();
        assert_eq!(outcome, RegistrationOutcome::Unchanged);

        let entry = router
            .take("downstream-fc")
            .await
            .unwrap()
            .expect("entry should exist");
        assert_eq!(entry.parent_allocation_id, parent_allocation_id);
        assert_eq!(entry.original_function_call_id, "root-fc");
    }

    #[tokio::test]
    async fn test_register_from_parent_rebinds_stale_tail_call_owner_preserving_original() {
        let store = TestStateStore::new().await.unwrap();
        let router = FunctionCallResultRouter::new(store.indexify_state.clone());
        let executor = ExecutorId::new("executor-a".to_string());
        let namespace = "ns";
        let application = "app";
        let request_id = "req-1";

        let stale_parent_allocation_id = insert_allocation(
            &store,
            &executor,
            namespace,
            application,
            request_id,
            "stale-parent-fc",
            crate::data_model::FunctionRunOutcome::Success,
        )
        .await;
        let replacement_parent_allocation_id = insert_allocation(
            &store,
            &executor,
            namespace,
            application,
            request_id,
            "replacement-parent-fc",
            crate::data_model::FunctionRunOutcome::Unknown,
        )
        .await;

        router
            .register(
                "downstream-fc".to_string(),
                stale_parent_allocation_id,
                "root-fc".to_string(),
                executor.clone(),
            )
            .await
            .unwrap();

        let outcome = router
            .register_from_parent(
                "downstream-fc".to_string(),
                replacement_parent_allocation_id.clone(),
                "downstream-fc".to_string(),
                executor,
                ParentRequestContext {
                    namespace,
                    application,
                    request_id,
                },
            )
            .await
            .unwrap();
        assert_eq!(outcome, RegistrationOutcome::Replaced);

        let entry = router
            .take("downstream-fc")
            .await
            .unwrap()
            .expect("entry should exist");
        assert_eq!(entry.parent_allocation_id, replacement_parent_allocation_id);
        assert_eq!(entry.original_function_call_id, "root-fc");
    }

    #[tokio::test]
    async fn test_purge_executor_removes_only_target_executor_entries() {
        let store = TestStateStore::new().await.unwrap();
        let router = FunctionCallResultRouter::new(store.indexify_state.clone());
        let executor_a = ExecutorId::new("executor-a".to_string());
        let executor_b = ExecutorId::new("executor-b".to_string());

        router
            .register(
                "fc-a1".to_string(),
                "alloc-a1".to_string(),
                "fc-a1".to_string(),
                executor_a.clone(),
            )
            .await
            .unwrap();
        router
            .register(
                "fc-a2".to_string(),
                "alloc-a2".to_string(),
                "fc-a2".to_string(),
                executor_a,
            )
            .await
            .unwrap();
        router
            .register(
                "fc-b1".to_string(),
                "alloc-b1".to_string(),
                "fc-b1".to_string(),
                executor_b.clone(),
            )
            .await
            .unwrap();

        let purged = router.purge_executor(&executor_b).await.unwrap();
        assert_eq!(purged, 1);
        assert!(router.take("fc-b1").await.unwrap().is_none());
        assert!(router.take("fc-a1").await.unwrap().is_some());
        assert!(router.take("fc-a2").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_take_falls_back_to_persisted_route_after_router_restart() {
        let store = TestStateStore::new().await.unwrap();
        let executor = ExecutorId::new("executor-a".to_string());

        {
            let router = FunctionCallResultRouter::new(store.indexify_state.clone());
            router
                .register(
                    "fc-1".to_string(),
                    "alloc-a".to_string(),
                    "fc-1".to_string(),
                    executor.clone(),
                )
                .await
                .unwrap();
        }

        // Simulate process/router restart: in-memory map is empty, only
        // persisted route remains.
        let router = FunctionCallResultRouter::new(store.indexify_state.clone());
        let entry = router
            .take("fc-1")
            .await
            .unwrap()
            .expect("entry should exist");
        assert_eq!(entry.parent_allocation_id, "alloc-a");
        assert_eq!(entry.executor_id, executor);

        let none = router.take("fc-1").await.unwrap();
        assert!(none.is_none());
    }

    #[tokio::test]
    async fn test_try_route_result_drops_when_parent_allocation_not_alive_and_clears_route() {
        let store = TestStateStore::new().await.unwrap();
        let router = Arc::new(FunctionCallResultRouter::new(store.indexify_state.clone()));
        let executor = ExecutorId::new("executor-a".to_string());
        let function_call_id = "fc-drop-parent-missing";

        store
            .indexify_state
            .register_executor_connection(&executor)
            .await;

        router
            .register(
                function_call_id.to_string(),
                "parent-missing".to_string(),
                function_call_id.to_string(),
                executor.clone(),
            )
            .await
            .unwrap();

        let completed = executor_api_pb::AllocationCompleted {
            allocation_id: "child-alloc-1".to_string(),
            function: Some(executor_api_pb::FunctionRef {
                namespace: Some("ns".to_string()),
                application_name: Some("app".to_string()),
                function_name: Some("child-fn".to_string()),
                application_version: Some("v1".to_string()),
            }),
            function_call_id: Some(function_call_id.to_string()),
            request_id: Some("req-1".to_string()),
            return_value: None,
            execution_duration_ms: None,
        };

        try_route_result(&router, function_call_id, &completed, &store.indexify_state)
            .await
            .unwrap();

        let conn = {
            let connections = store.indexify_state.executor_connections.read().await;
            connections.get(&executor).cloned().unwrap()
        };
        assert!(
            conn.clone_results().await.is_empty(),
            "parent-missing route should be dropped without enqueuing a result"
        );
        assert!(
            router.take(function_call_id).await.unwrap().is_none(),
            "route should be removed after dropping due to missing parent allocation"
        );
        assert!(
            store
                .indexify_state
                .get_function_call_route(function_call_id)
                .await
                .unwrap()
                .is_none(),
            "persisted route should also be cleared"
        );
    }

    #[tokio::test]
    async fn test_try_route_failure_drops_when_parent_allocation_not_alive_and_clears_route() {
        let store = TestStateStore::new().await.unwrap();
        let router = Arc::new(FunctionCallResultRouter::new(store.indexify_state.clone()));
        let executor = ExecutorId::new("executor-a".to_string());
        let function_call_id = "fc-drop-parent-missing-failure";

        store
            .indexify_state
            .register_executor_connection(&executor)
            .await;

        router
            .register(
                function_call_id.to_string(),
                "parent-missing".to_string(),
                function_call_id.to_string(),
                executor.clone(),
            )
            .await
            .unwrap();

        let failed = executor_api_pb::AllocationFailed {
            allocation_id: "child-alloc-2".to_string(),
            function: Some(executor_api_pb::FunctionRef {
                namespace: Some("ns".to_string()),
                application_name: Some("app".to_string()),
                function_name: Some("child-fn".to_string()),
                application_version: Some("v1".to_string()),
            }),
            function_call_id: Some(function_call_id.to_string()),
            request_id: Some("req-1".to_string()),
            reason: executor_api_pb::AllocationFailureReason::FunctionError.into(),
            execution_duration_ms: None,
            request_error: None,
            container_id: None,
        };

        try_route_failure(&router, function_call_id, &failed, &store.indexify_state)
            .await
            .unwrap();

        let conn = {
            let connections = store.indexify_state.executor_connections.read().await;
            connections.get(&executor).cloned().unwrap()
        };
        assert!(
            conn.clone_results().await.is_empty(),
            "parent-missing route should be dropped without enqueuing a failure"
        );
        assert!(
            router.take(function_call_id).await.unwrap().is_none(),
            "route should be removed after dropping due to missing parent allocation"
        );
        assert!(
            store
                .indexify_state
                .get_function_call_route(function_call_id)
                .await
                .unwrap()
                .is_none(),
            "persisted route should also be cleared"
        );
    }
}
