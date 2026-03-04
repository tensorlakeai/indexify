use super::*;

fn function_ref_labels(
    function: Option<&executor_api_pb::FunctionRef>,
) -> (&str, &str, &str, &str) {
    let Some(function) = function else {
        return ("", "", "", "");
    };
    (
        function.namespace.as_deref().unwrap_or(""),
        function.application_name.as_deref().unwrap_or(""),
        function.function_name.as_deref().unwrap_or(""),
        function.application_version.as_deref().unwrap_or(""),
    )
}

fn call_function_target_labels(call: &executor_api_pb::FunctionCallRequest) -> (&str, &str) {
    if let Some(updates) = call.updates.as_ref() {
        for update in &updates.updates {
            if let Some(executor_api_pb::execution_plan_update::Op::FunctionCall(fc)) =
                update.op.as_ref() &&
                let Some(target) = fc.target.as_ref()
            {
                return (
                    target.function_name.as_deref().unwrap_or(""),
                    target.application_version.as_deref().unwrap_or(""),
                );
            }
        }
    }
    ("", "")
}

fn allocation_log_labels(
    log_entry: &executor_api_pb::AllocationLogEntry,
) -> (&str, &str, &str, &str, &str, &str) {
    let allocation_id = log_entry.allocation_id.as_str();
    if let Some(executor_api_pb::allocation_log_entry::Entry::CallFunction(call)) =
        log_entry.entry.as_ref()
    {
        let namespace = call.namespace.as_deref().unwrap_or("");
        let app = call.application.as_deref().unwrap_or("");
        let request_id = call.request_id.as_deref().unwrap_or("");
        let (fn_name, version) = call_function_target_labels(call);
        return (request_id, fn_name, namespace, app, version, allocation_id);
    }
    ("", "", "", "", "", allocation_id)
}

impl ExecutorAPIService {
    /// Process full-state sync if present and return whether the executor is
    /// known to the server after processing.
    pub(super) async fn resolve_executor_known(
        &self,
        executor_id: &ExecutorId,
        full_state: Option<executor_api_pb::DataplaneStateFullSync>,
    ) -> Result<bool, Status> {
        if let Some(full_state) = full_state {
            info!(
                executor_id = executor_id.get(),
                "processing full state sync"
            );
            self.handle_full_state(executor_id, full_state).await?;
            return Ok(true);
        }

        let runtime_data = self.executor_manager.runtime_data_read().await;
        Ok(runtime_data.contains_key(executor_id))
    }

    pub(super) async fn process_heartbeat_reports(
        &self,
        executor_id: &ExecutorId,
        executor_known: bool,
        command_responses: Vec<executor_api_pb::CommandResponse>,
        allocation_outcomes: Vec<executor_api_pb::AllocationOutcome>,
        allocation_log_entries: Vec<executor_api_pb::AllocationLogEntry>,
    ) -> Result<(), Status> {
        if !executor_known {
            let has_reports = !command_responses.is_empty() ||
                !allocation_outcomes.is_empty() ||
                !allocation_log_entries.is_empty();
            if has_reports {
                warn!(
                    executor_id = executor_id.get(),
                    command_responses = command_responses.len(),
                    allocation_outcomes = allocation_outcomes.len(),
                    allocation_log_entries = allocation_log_entries.len(),
                    "dropping reports from unknown executor"
                );
            }
            return Ok(());
        }

        let mut failed_items = 0usize;

        if !command_responses.is_empty() {
            match process_command_responses(&self.indexify_state, executor_id, command_responses)
                .await
            {
                Ok(command_response_failures) => {
                    failed_items = failed_items.saturating_add(command_response_failures);
                }
                Err(e) => {
                    warn!(
                        executor_id = executor_id.get(),
                        error = %e,
                        "heartbeat: process_command_responses failed"
                    );
                    failed_items = failed_items.saturating_add(1);
                }
            }
        }

        failed_items = failed_items.saturating_add(
            self.process_allocation_log_entries(executor_id, allocation_log_entries)
                .await?,
        );
        // Process log entries before outcomes.
        //
        // CallFunction log entries register result-routing ownership and
        // enqueue downstream graph updates. If outcomes are ingested first,
        // same-heartbeat completions can be dropped as unroutable and newly
        // created child runs can miss the only capacity-change trigger in the
        // batch.
        failed_items = failed_items.saturating_add(
            self.process_allocation_outcomes(executor_id, allocation_outcomes)
                .await?,
        );

        if failed_items > 0 {
            return Err(Status::internal(format!(
                "heartbeat ingestion failed for {failed_items} report item(s)"
            )));
        }

        Ok(())
    }

    async fn process_allocation_outcomes(
        &self,
        executor_id: &ExecutorId,
        allocation_outcomes: Vec<executor_api_pb::AllocationOutcome>,
    ) -> Result<usize, Status> {
        let mut failed_items = 0usize;
        for item in allocation_outcomes {
            match item.outcome {
                Some(executor_api_pb::allocation_outcome::Outcome::Completed(completed)) => {
                    // Ingest first, then route. Continue processing the batch
                    // and report failures at the end so good items still get
                    // applied in this pass.
                    let completed_for_logging = completed.clone();
                    let completed_for_routing = completed_for_logging
                        .function_call_id
                        .as_ref()
                        .map(|_| completed_for_logging.clone());
                    match process_allocation_completed(
                        &self.indexify_state,
                        &self.blob_storage_registry,
                        executor_id,
                        completed,
                    )
                    .await
                    {
                        Ok(AllocationIngestDisposition::Applied) => {
                            if let Some(completed) = &completed_for_routing &&
                                let Some(fc_id) = completed.function_call_id.as_deref() &&
                                let Err(e) = try_route_result(
                                    &self.function_call_result_router,
                                    fc_id,
                                    completed,
                                    &self.indexify_state,
                                )
                                .await
                            {
                                let (namespace, app, fn_name, version) =
                                    function_ref_labels(completed.function.as_ref());
                                let request_id = completed.request_id.as_deref().unwrap_or("");
                                let allocation_id = completed.allocation_id.as_str();
                                if super::is_malformed_payload_error(&e) {
                                    warn!(
                                        executor_id = executor_id.get(),
                                        request_id = %request_id,
                                        "fn" = %fn_name,
                                        namespace = %namespace,
                                        app = %app,
                                        version = %version,
                                        allocation_id = %allocation_id,
                                        function_call_id = %fc_id,
                                        error = %e,
                                        "heartbeat: malformed allocation_completed routing \
                                         payload; skipping"
                                    );
                                    continue;
                                }
                                warn!(
                                    executor_id = executor_id.get(),
                                    request_id = %request_id,
                                    "fn" = %fn_name,
                                    namespace = %namespace,
                                    app = %app,
                                    version = %version,
                                    allocation_id = %allocation_id,
                                    function_call_id = %fc_id,
                                    error = %e,
                                    "heartbeat: failed to route allocation_completed result"
                                );
                                failed_items = failed_items.saturating_add(1);
                            }
                        }
                        Ok(AllocationIngestDisposition::SkippedNoop) => {
                            // Duplicate/out-of-order outcomes must be no-op for
                            // ingest and routing to preserve pending routes.
                        }
                        Err(e) => {
                            let (namespace, app, fn_name, version) =
                                function_ref_labels(completed_for_logging.function.as_ref());
                            let request_id =
                                completed_for_logging.request_id.as_deref().unwrap_or("");
                            let allocation_id = completed_for_logging.allocation_id.as_str();
                            if super::is_malformed_payload_error(&e) {
                                warn!(
                                    executor_id = executor_id.get(),
                                    request_id = %request_id,
                                    "fn" = %fn_name,
                                    namespace = %namespace,
                                    app = %app,
                                    version = %version,
                                    allocation_id = %allocation_id,
                                    error = %e,
                                    "heartbeat: malformed allocation_completed; skipping"
                                );
                                continue;
                            }
                            warn!(
                                executor_id = executor_id.get(),
                                error = %e,
                                "heartbeat: process_allocation_completed failed"
                            );
                            failed_items = failed_items.saturating_add(1);
                            continue;
                        }
                    }
                }
                Some(executor_api_pb::allocation_outcome::Outcome::Failed(failed)) => {
                    // Ingest first, then route. Continue processing the batch
                    // and report failures at the end so good items still get
                    // applied in this pass.
                    let failed_for_logging = failed.clone();
                    let failed_for_routing = failed_for_logging
                        .function_call_id
                        .as_ref()
                        .map(|_| failed_for_logging.clone());
                    match process_allocation_failed(
                        &self.indexify_state,
                        &self.blob_storage_registry,
                        executor_id,
                        failed,
                    )
                    .await
                    {
                        Ok(AllocationIngestDisposition::Applied) => {
                            if let Some(failed) = &failed_for_routing &&
                                let Some(fc_id) = failed.function_call_id.as_deref() &&
                                let Err(e) = try_route_failure(
                                    &self.function_call_result_router,
                                    fc_id,
                                    failed,
                                    &self.indexify_state,
                                )
                                .await
                            {
                                let (namespace, app, fn_name, version) =
                                    function_ref_labels(failed.function.as_ref());
                                let request_id = failed.request_id.as_deref().unwrap_or("");
                                let allocation_id = failed.allocation_id.as_str();
                                if super::is_malformed_payload_error(&e) {
                                    warn!(
                                        executor_id = executor_id.get(),
                                        request_id = %request_id,
                                        "fn" = %fn_name,
                                        namespace = %namespace,
                                        app = %app,
                                        version = %version,
                                        allocation_id = %allocation_id,
                                        function_call_id = %fc_id,
                                        error = %e,
                                        "heartbeat: malformed allocation_failed routing \
                                         payload; skipping"
                                    );
                                    continue;
                                }
                                warn!(
                                    executor_id = executor_id.get(),
                                    request_id = %request_id,
                                    "fn" = %fn_name,
                                    namespace = %namespace,
                                    app = %app,
                                    version = %version,
                                    allocation_id = %allocation_id,
                                    function_call_id = %fc_id,
                                    error = %e,
                                    "heartbeat: failed to route allocation_failed result"
                                );
                                failed_items = failed_items.saturating_add(1);
                            }
                        }
                        Ok(AllocationIngestDisposition::SkippedNoop) => {
                            // Duplicate/out-of-order outcomes must be no-op for
                            // ingest and routing to preserve pending routes.
                        }
                        Err(e) => {
                            let (namespace, app, fn_name, version) =
                                function_ref_labels(failed_for_logging.function.as_ref());
                            let request_id = failed_for_logging.request_id.as_deref().unwrap_or("");
                            let allocation_id = failed_for_logging.allocation_id.as_str();
                            if super::is_malformed_payload_error(&e) {
                                warn!(
                                    executor_id = executor_id.get(),
                                    request_id = %request_id,
                                    "fn" = %fn_name,
                                    namespace = %namespace,
                                    app = %app,
                                    version = %version,
                                    allocation_id = %allocation_id,
                                    error = %e,
                                    "heartbeat: malformed allocation_failed; skipping"
                                );
                                continue;
                            }
                            warn!(
                                executor_id = executor_id.get(),
                                error = %e,
                                "heartbeat: process_allocation_failed failed"
                            );
                            failed_items = failed_items.saturating_add(1);
                            continue;
                        }
                    }
                }
                None => {}
            }
        }

        Ok(failed_items)
    }

    async fn process_allocation_log_entries(
        &self,
        executor_id: &ExecutorId,
        allocation_log_entries: Vec<executor_api_pb::AllocationLogEntry>,
    ) -> Result<usize, Status> {
        let mut failed_items = 0usize;
        for log_entry in allocation_log_entries {
            if let Err(e) = handle_log_entry(
                &log_entry,
                executor_id,
                &self.function_call_result_router,
                &self.indexify_state,
                &self.blob_storage_registry,
            )
            .await
            {
                let (request_id, fn_name, namespace, app, version, allocation_id) =
                    allocation_log_labels(&log_entry);
                if super::is_malformed_payload_error(&e) {
                    warn!(
                        executor_id = executor_id.get(),
                        request_id = %request_id,
                        "fn" = %fn_name,
                        namespace = %namespace,
                        app = %app,
                        version = %version,
                        allocation_id = %allocation_id,
                        error = %e,
                        "heartbeat: malformed allocation_log_entry; skipping"
                    );
                    continue;
                }
                warn!(
                    executor_id = executor_id.get(),
                    request_id = %request_id,
                    "fn" = %fn_name,
                    namespace = %namespace,
                    app = %app,
                    version = %version,
                    allocation_id = %allocation_id,
                    error = %e,
                    "heartbeat: handle_log_entry_v2 failed"
                );
                failed_items = failed_items.saturating_add(1);
                continue;
            }
        }

        Ok(failed_items)
    }

    pub(super) async fn maybe_deregister_stopped_executor(
        &self,
        executor_id: &ExecutorId,
        reported_status: Option<executor_api_pb::ExecutorStatus>,
    ) -> Result<bool, Status> {
        if !matches!(
            reported_status,
            Some(executor_api_pb::ExecutorStatus::Stopped)
        ) {
            return Ok(false);
        }

        info!(
            executor_id = executor_id.get(),
            "executor reported stopped status; deregistering immediately"
        );
        if let Err(e) = self
            .executor_manager
            .deregister_executor(executor_id.clone(), "executor reported stopped")
            .await
        {
            warn!(
                executor_id = executor_id.get(),
                error = %e,
                "failed to deregister stopped executor"
            );
            return Err(Status::internal(format!(
                "failed to deregister stopped executor: {e}"
            )));
        }
        Ok(true)
    }
}
