use std::{sync::Arc, vec};

use anyhow::Result;
use executor_api_pb::executor_api_server::ExecutorApi;
pub use proto_api::executor_api_pb;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

mod heartbeat_helpers;
mod polling;
mod report_processing;
mod result_routing;

use polling::{long_poll_commands, long_poll_results};
pub use report_processing::{
    AllocationIngestDisposition,
    process_allocation_completed,
    process_allocation_failed,
    process_command_responses,
};
pub use result_routing::FunctionCallResultRouter;
use result_routing::{handle_log_entry, try_route_failure, try_route_result};

use crate::{
    blob_store::registry::BlobStorageRegistry,
    data_model::{self, ExecutorId, ExecutorMetadataBuilder, FunctionAllowlist},
    executors::ExecutorManager,
    state_store::{
        IndexifyState,
        requests::{RequestPayload, StateMachineUpdateRequest, UpsertExecutorRequest},
    },
};

// Proto <-> internal conversion impls live in crate::proto_convert.

/// Register an executor's full state --- the shared business logic behind
/// both the v2 heartbeat RPC (with `full_state`) and the test
/// infrastructure.
///
/// Always writes an `UpsertExecutor` to the state machine. In the delta
/// model (v2), full state only arrives on first registration or after a
/// FullSync (e.g. post-deletion), so there is no benefit to hashing and
/// skipping writes.
///
/// Callers are responsible for calling `heartbeat_v2()` for liveness
/// before or after this function.
pub async fn sync_executor_full_state(
    executor_manager: &ExecutorManager,
    indexify_state: Arc<IndexifyState>,
    executor: data_model::ExecutorMetadata,
) -> Result<()> {
    // Register runtime data (marks executor as "known" for delta processing)
    executor_manager.register_executor(executor.clone()).await?;

    let upsert_request =
        UpsertExecutorRequest::build(executor, vec![], true, indexify_state.clone())?;

    let sm_req = StateMachineUpdateRequest {
        payload: RequestPayload::UpsertExecutor(upsert_request),
    };
    indexify_state.write(sm_req).await?;

    Ok(())
}

pub struct ExecutorAPIService {
    indexify_state: Arc<IndexifyState>,
    executor_manager: Arc<ExecutorManager>,
    blob_storage_registry: Arc<BlobStorageRegistry>,
    function_call_result_router: Arc<FunctionCallResultRouter>,
}

impl ExecutorAPIService {
    pub fn new(
        indexify_state: Arc<IndexifyState>,
        executor_manager: Arc<ExecutorManager>,
        blob_storage_registry: Arc<BlobStorageRegistry>,
    ) -> Self {
        let function_call_result_router =
            Arc::new(FunctionCallResultRouter::new(indexify_state.clone()));
        executor_manager.set_function_call_result_router(function_call_result_router.clone());

        Self {
            indexify_state,
            executor_manager,
            blob_storage_registry,
            function_call_result_router,
        }
    }

    async fn handle_full_state(
        &self,
        executor_id: &ExecutorId,
        full_state: executor_api_pb::DataplaneStateFullSync,
    ) -> Result<(), Status> {
        // --- Proto -> internal conversion ---

        let mut executor_metadata = ExecutorMetadataBuilder::default();
        executor_metadata.id(executor_id.clone());
        executor_metadata.addr(full_state.hostname.clone().unwrap_or_default());
        executor_metadata.executor_version(full_state.version.unwrap_or_default());
        if let Some(catalog_name) = full_state.catalog_entry_name {
            executor_metadata.catalog_name(Some(catalog_name));
        }
        if let Some(proxy_address) = full_state.proxy_address {
            executor_metadata.proxy_address(Some(proxy_address));
        }

        let host_resources = full_state
            .total_container_resources
            .map(data_model::HostResources::try_from)
            .transpose()
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?
            .unwrap_or_default();
        executor_metadata.host_resources(host_resources);

        let allowed_functions: Vec<FunctionAllowlist> = full_state
            .allowed_functions
            .into_iter()
            .filter_map(|f| FunctionAllowlist::try_from(f).ok())
            .collect();
        if allowed_functions.is_empty() {
            executor_metadata.function_allowlist(None);
        } else {
            executor_metadata.function_allowlist(Some(allowed_functions));
        }
        executor_metadata.labels(full_state.labels.into());
        executor_metadata.state(data_model::ExecutorState::Running);
        executor_metadata.state_hash(String::new());

        let mut containers = imbl::HashMap::new();
        for fe_state in full_state.container_states {
            match data_model::Container::try_from(fe_state) {
                Ok(container) => {
                    containers.insert(container.id.clone(), container);
                }
                Err(e) => {
                    warn!(
                        executor_id = executor_id.get(),
                        error = %e,
                        "skipping container in full state sync"
                    );
                }
            }
        }
        executor_metadata.containers(containers);

        let executor = executor_metadata
            .build()
            .map_err(|e| Status::internal(e.to_string()))?;

        // --- Shared registration logic ---
        sync_executor_full_state(
            &self.executor_manager,
            self.indexify_state.clone(),
            executor,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        // Ensure an ExecutorConnection exists so events can buffer even
        // before the command stream connects.
        self.indexify_state
            .register_executor_connection(executor_id)
            .await;

        // Emit an immediate full-sync command batch for this executor.
        self.executor_manager
            .emit_commands_for_executor(executor_id, true)
            .await;

        Ok(())
    }
}

/// Convert a `data_model::NetworkPolicy` to the proto `NetworkPolicy`.
pub(crate) fn network_policy_to_pb(
    np: &data_model::NetworkPolicy,
) -> executor_api_pb::NetworkPolicy {
    executor_api_pb::NetworkPolicy {
        allow_internet_access: Some(np.allow_internet_access),
        allow_out: np.allow_out.clone(),
        deny_out: np.deny_out.clone(),
    }
}

#[tonic::async_trait]
impl ExecutorApi for ExecutorAPIService {
    async fn heartbeat(
        &self,
        request: Request<executor_api_pb::HeartbeatRequest>,
    ) -> Result<Response<executor_api_pb::HeartbeatResponse>, Status> {
        let executor_api_pb::HeartbeatRequest {
            executor_id: raw_executor_id,
            status,
            full_state,
            command_responses,
            allocation_outcomes,
            allocation_log_entries,
        } = request.into_inner();

        let executor_id: ExecutorId = raw_executor_id
            .ok_or(Status::invalid_argument("executor_id required"))?
            .into();
        let reported_status =
            status.and_then(|s| executor_api_pb::ExecutorStatus::try_from(s).ok());

        // Touch the executor liveness
        self.executor_manager
            .heartbeat_v2(&executor_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let executor_known = self
            .resolve_executor_known(&executor_id, full_state)
            .await?;
        let report_result = self
            .process_heartbeat_reports(
                &executor_id,
                executor_known,
                command_responses,
                allocation_outcomes,
                allocation_log_entries,
            )
            .await;

        let stopped = self
            .maybe_deregister_stopped_executor(&executor_id, reported_status)
            .await?;

        if stopped {
            // Preserve report-ingest error semantics, but always honor
            // explicit stop signals by attempting deregistration first.
            report_result?;
            // Executor is intentionally shutting down; do not request full
            // state re-registration.
            return Ok(Response::new(executor_api_pb::HeartbeatResponse {
                send_state: Some(false),
            }));
        }
        report_result?;

        let mut send_state = !executor_known;
        if executor_known && !send_state {
            let connections = self.indexify_state.executor_connections.read().await;
            if let Some(conn) = connections.get(&executor_id) &&
                conn.take_full_state_request()
            {
                info!(
                    executor_id = executor_id.get(),
                    "requesting full state after command ack regression"
                );
                send_state = true;
            }
        }

        Ok(Response::new(executor_api_pb::HeartbeatResponse {
            send_state: Some(send_state),
        }))
    }

    async fn poll_commands(
        &self,
        request: Request<executor_api_pb::PollCommandsRequest>,
    ) -> Result<Response<executor_api_pb::PollCommandsResponse>, Status> {
        let req = request.into_inner();
        let executor_id = ExecutorId::new(req.executor_id);

        self.executor_manager
            .heartbeat_v2(&executor_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Verify the executor is registered before entering long-poll
        {
            let connections = self.indexify_state.executor_connections.read().await;
            if !connections.contains_key(&executor_id) {
                return Err(Status::not_found("executor not registered"));
            }
        }

        let commands =
            long_poll_commands(&self.indexify_state, &executor_id, req.acked_command_seq).await;

        Ok(Response::new(executor_api_pb::PollCommandsResponse {
            commands,
        }))
    }

    async fn poll_allocation_results(
        &self,
        request: Request<executor_api_pb::PollAllocationResultsRequest>,
    ) -> Result<Response<executor_api_pb::PollAllocationResultsResponse>, Status> {
        let req = request.into_inner();
        let executor_id = ExecutorId::new(req.executor_id);

        self.executor_manager
            .heartbeat_v2(&executor_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Verify the executor is registered before entering long-poll
        {
            let connections = self.indexify_state.executor_connections.read().await;
            if !connections.contains_key(&executor_id) {
                return Err(Status::not_found("executor not registered"));
            }
        }

        let results =
            long_poll_results(&self.indexify_state, &executor_id, req.acked_result_seq).await;

        Ok(Response::new(
            executor_api_pb::PollAllocationResultsResponse { results },
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use executor_api_pb::{
        ContainerDescription,
        ContainerResources,
        ContainerState,
        ContainerStatus,
        ContainerTerminationReason as TerminationReasonPb,
        ContainerType as ContainerTypePb,
        FunctionRef,
        SandboxMetadata,
    };
    use proto_api::executor_api_pb;
    use tonic::Request;

    use super::{executor_api_pb::executor_api_server::ExecutorApi, process_command_responses};
    use crate::{
        data_model::{self, ContainerTerminationReason, ContainerType},
        executors::EXECUTOR_TIMEOUT,
        testing::TestService,
    };

    /// Build a minimal sandbox ContainerState proto with the given
    /// sandbox_id and termination status, simulating what the dataplane reports
    /// when a sandbox container fails to start (e.g. image pull failure).
    fn sandbox_fe_state_proto(
        container_id: &str,
        sandbox_id: &str,
        status: ContainerStatus,
        termination_reason: Option<TerminationReasonPb>,
    ) -> ContainerState {
        ContainerState {
            description: Some(ContainerDescription {
                id: Some(container_id.to_string()),
                function: Some(FunctionRef {
                    namespace: Some("test-ns".to_string()),
                    application_name: Some("".to_string()),
                    function_name: Some(container_id.to_string()),
                    application_version: Some("".to_string()),
                }),
                resources: Some(ContainerResources {
                    cpu_ms_per_sec: Some(100),
                    memory_bytes: Some(256 * 1024 * 1024),
                    disk_bytes: Some(1024 * 1024 * 1024),
                    gpu: None,
                }),
                max_concurrency: Some(1),
                container_type: Some(ContainerTypePb::Sandbox.into()),
                sandbox_metadata: Some(SandboxMetadata {
                    image: Some("ubuntu".to_string()),
                    timeout_secs: Some(600),
                    entrypoint: vec![],
                    network_policy: None,
                    sandbox_id: Some(sandbox_id.to_string()),
                    snapshot_uri: None,
                }),
                secret_names: vec![],
                initialization_timeout_ms: None,
                application: None,
                allocation_timeout_ms: None,
                pool_id: None,
            }),
            status: Some(status.into()),
            termination_reason: termination_reason.map(|r| r.into()),
        }
    }

    #[test]
    fn test_terminated_sandbox_container_preserves_sandbox_id() {
        // Simulates the dataplane reporting a sandbox container that failed to
        // start (e.g. Docker image pull failure). The proto->Container conversion
        // must preserve sandbox_id so the reconciler can find and terminate the
        // associated sandbox.
        let fe_state = sandbox_fe_state_proto(
            "sb-container-123",
            "sb-container-123",
            ContainerStatus::Terminated,
            Some(TerminationReasonPb::StartupFailedInternalError),
        );

        let container = data_model::Container::try_from(fe_state).unwrap();

        assert_eq!(container.container_type, ContainerType::Sandbox);
        assert!(
            matches!(
                container.state,
                data_model::ContainerState::Terminated {
                    reason: ContainerTerminationReason::StartupFailedInternalError,
                    ..
                }
            ),
            "Container should be Terminated with StartupFailedInternalError, got: {:?}",
            container.state
        );
        assert_eq!(
            container.sandbox_id.as_ref().map(|s| s.get()),
            Some("sb-container-123"),
            "sandbox_id must be preserved through proto conversion"
        );
    }

    #[test]
    fn test_running_sandbox_container_preserves_sandbox_id() {
        let fe_state = sandbox_fe_state_proto(
            "sb-container-456",
            "sb-container-456",
            ContainerStatus::Running,
            None,
        );

        let container = data_model::Container::try_from(fe_state).unwrap();

        assert_eq!(
            container.sandbox_id.as_ref().map(|s| s.get()),
            Some("sb-container-456"),
        );
        assert_eq!(container.state, data_model::ContainerState::Running);
    }

    #[test]
    fn test_function_container_has_no_sandbox_id() {
        // Function containers don't have sandbox_metadata, so sandbox_id should
        // be None.
        let fe_state = ContainerState {
            description: Some(ContainerDescription {
                id: Some("fn-container-1".to_string()),
                function: Some(FunctionRef {
                    namespace: Some("test-ns".to_string()),
                    application_name: Some("app".to_string()),
                    function_name: Some("process".to_string()),
                    application_version: Some("v1".to_string()),
                }),
                resources: Some(ContainerResources {
                    cpu_ms_per_sec: Some(100),
                    memory_bytes: Some(256 * 1024 * 1024),
                    disk_bytes: Some(1024 * 1024 * 1024),
                    gpu: None,
                }),
                max_concurrency: Some(1),
                container_type: Some(ContainerTypePb::Function.into()),
                sandbox_metadata: None,
                secret_names: vec![],
                initialization_timeout_ms: None,
                application: None,
                allocation_timeout_ms: None,
                pool_id: None,
            }),
            status: Some(ContainerStatus::Running.into()),
            termination_reason: None,
        };

        let container = data_model::Container::try_from(fe_state).unwrap();

        assert_eq!(container.container_type, ContainerType::Function);
        assert!(
            container.sandbox_id.is_none(),
            "Function containers should not have sandbox_id"
        );
    }

    fn make_command(seq: u64) -> executor_api_pb::Command {
        executor_api_pb::Command { seq, command: None }
    }

    fn make_call_function_log_entry(
        parent_allocation_id: &str,
        child_function_call_id: &str,
    ) -> executor_api_pb::AllocationLogEntry {
        executor_api_pb::AllocationLogEntry {
            allocation_id: parent_allocation_id.to_string(),
            clock: 0,
            entry: Some(executor_api_pb::allocation_log_entry::Entry::CallFunction(
                executor_api_pb::FunctionCallRequest {
                    namespace: Some("ns".to_string()),
                    application: Some("app".to_string()),
                    request_id: Some("req-1".to_string()),
                    source_function_call_id: Some("source-fc".to_string()),
                    updates: Some(executor_api_pb::ExecutionPlanUpdates {
                        updates: vec![executor_api_pb::ExecutionPlanUpdate {
                            op: Some(executor_api_pb::execution_plan_update::Op::FunctionCall(
                                executor_api_pb::FunctionCall {
                                    id: Some(child_function_call_id.to_string()),
                                    target: Some(FunctionRef {
                                        namespace: Some("ns".to_string()),
                                        application_name: Some("app".to_string()),
                                        function_name: Some("child-fn".to_string()),
                                        application_version: Some("v1".to_string()),
                                    }),
                                    args: vec![],
                                    call_metadata: None,
                                },
                            )),
                        }],
                        root_function_call_id: Some(child_function_call_id.to_string()),
                        start_at: None,
                    }),
                },
            )),
        }
    }

    fn make_call_function_log_entry_missing_op(
        parent_allocation_id: &str,
    ) -> executor_api_pb::AllocationLogEntry {
        executor_api_pb::AllocationLogEntry {
            allocation_id: parent_allocation_id.to_string(),
            clock: 0,
            entry: Some(executor_api_pb::allocation_log_entry::Entry::CallFunction(
                executor_api_pb::FunctionCallRequest {
                    namespace: Some("ns".to_string()),
                    application: Some("app".to_string()),
                    request_id: Some("req-1".to_string()),
                    source_function_call_id: Some("source-fc".to_string()),
                    updates: Some(executor_api_pb::ExecutionPlanUpdates {
                        updates: vec![executor_api_pb::ExecutionPlanUpdate { op: None }],
                        root_function_call_id: Some("root-fc".to_string()),
                        start_at: None,
                    }),
                },
            )),
        }
    }

    fn make_completed_outcome(
        child_function_call_id: &str,
        allocation_id: &str,
    ) -> executor_api_pb::AllocationOutcome {
        executor_api_pb::AllocationOutcome {
            outcome: Some(executor_api_pb::allocation_outcome::Outcome::Completed(
                executor_api_pb::AllocationCompleted {
                    allocation_id: allocation_id.to_string(),
                    function: Some(FunctionRef {
                        namespace: Some("ns".to_string()),
                        application_name: Some("app".to_string()),
                        function_name: Some("child-fn".to_string()),
                        application_version: Some("v1".to_string()),
                    }),
                    function_call_id: Some(child_function_call_id.to_string()),
                    request_id: Some("req-1".to_string()),
                    return_value: None,
                    execution_duration_ms: None,
                },
            )),
        }
    }

    #[tokio::test]
    async fn test_reconnect_full_state_resets_command_outbox() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );
        let executor_id = crate::data_model::ExecutorId::from("executor-reconnect-reset");

        // Initial register.
        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        // Seed outbox with persisted commands and ack progression.
        test_service
            .service
            .indexify_state
            .enqueue_executor_commands(&executor_id, vec![make_command(0), make_command(0)])
            .await
            .unwrap();
        let _ = ExecutorApi::poll_commands(
            &api,
            Request::new(executor_api_pb::PollCommandsRequest {
                executor_id: executor_id.get().to_string(),
                acked_command_seq: Some(1),
            }),
        )
        .await
        .unwrap();

        // Reconnect with full state (same executor_id): this should reset
        // command outbox/cursor before re-syncing.
        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        let connections = test_service
            .service
            .indexify_state
            .executor_connections
            .read()
            .await;
        let conn = connections
            .get(&executor_id)
            .expect("executor connection should exist");
        assert!(
            conn.clone_commands().await.is_empty(),
            "reconnect full state should clear stale pending commands"
        );

        // A new command after reconnect should start from seq=1
        // (cursor reset), not continue the old stream.
        test_service
            .service
            .indexify_state
            .enqueue_executor_commands(&executor_id, vec![make_command(0)])
            .await
            .unwrap();
        let response = ExecutorApi::poll_commands(
            &api,
            Request::new(executor_api_pb::PollCommandsRequest {
                executor_id: executor_id.get().to_string(),
                acked_command_seq: None,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(response.commands.len(), 1);
        assert_eq!(
            response.commands[0].seq, 1,
            "reconnect full state should reset outbox sequence cursor"
        );
    }

    #[tokio::test]
    async fn test_long_poll_survives_reconnect_and_receives_new_commands() {
        let test_service = TestService::new().await.unwrap();
        let api = Arc::new(super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        ));
        let executor_id = crate::data_model::ExecutorId::from("executor-reconnect-long-poll");

        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        let poll_executor_id = executor_id.get().to_string();
        let poll_api = api.clone();
        let poll_task = tokio::spawn(async move {
            ExecutorApi::poll_commands(
                poll_api.as_ref(),
                Request::new(executor_api_pb::PollCommandsRequest {
                    executor_id: poll_executor_id,
                    acked_command_seq: None,
                }),
            )
            .await
            .unwrap()
            .into_inner()
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Reconnect with the same executor ID (full-state sync path).
        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        test_service
            .service
            .indexify_state
            .enqueue_executor_commands(&executor_id, vec![make_command(0)])
            .await
            .unwrap();

        let response = tokio::time::timeout(Duration::from_secs(2), poll_task)
            .await
            .expect("long poll should be released by new command")
            .unwrap();
        assert_eq!(
            response.commands.len(),
            1,
            "long poll should receive command after reconnect"
        );
    }

    #[tokio::test]
    async fn test_stopped_heartbeat_deregisters_executor_immediately() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );
        let executor_id = crate::data_model::ExecutorId::from("executor-stopped-heartbeat");

        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        {
            let connections = test_service
                .service
                .indexify_state
                .executor_connections
                .read()
                .await;
            assert!(connections.contains_key(&executor_id));
        }
        {
            let runtime_data = test_service
                .service
                .executor_manager
                .runtime_data_read()
                .await;
            assert!(runtime_data.contains_key(&executor_id));
        }

        let response = ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_id.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Stopped.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![],
                allocation_log_entries: vec![],
            }),
        )
        .await
        .unwrap()
        .into_inner();

        assert_eq!(response.send_state, Some(false));

        {
            let connections = test_service
                .service
                .indexify_state
                .executor_connections
                .read()
                .await;
            assert!(!connections.contains_key(&executor_id));
        }
        {
            let runtime_data = test_service
                .service
                .executor_manager
                .runtime_data_read()
                .await;
            assert!(!runtime_data.contains_key(&executor_id));
        }
    }

    #[tokio::test]
    async fn test_stopped_heartbeat_deregisters_even_when_report_ingestion_fails() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );
        let executor_id =
            crate::data_model::ExecutorId::from("executor-stopped-heartbeat-report-failure");

        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        let result = ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_id.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Stopped.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![],
                allocation_log_entries: vec![make_call_function_log_entry_missing_op(
                    "parent-stopped-error",
                )],
            }),
        )
        .await;

        assert!(
            result.is_err(),
            "heartbeat should still surface report-ingestion errors"
        );

        {
            let connections = test_service
                .service
                .indexify_state
                .executor_connections
                .read()
                .await;
            assert!(!connections.contains_key(&executor_id));
        }
        {
            let runtime_data = test_service
                .service
                .executor_manager
                .runtime_data_read()
                .await;
            assert!(!runtime_data.contains_key(&executor_id));
        }
    }

    #[tokio::test]
    async fn test_ack_regression_requests_full_state_on_next_heartbeat() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );
        let executor_id = crate::data_model::ExecutorId::from("executor-ack-regression");

        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        // Seed one pending command so poll_commands returns immediately.
        {
            let connections = test_service
                .service
                .indexify_state
                .executor_connections
                .read()
                .await;
            let conn = connections
                .get(&executor_id)
                .expect("executor connection should exist");
            conn.push_commands(vec![make_command(10)]).await;
        }

        // First poll reports a non-zero ack baseline.
        let _ = ExecutorApi::poll_commands(
            &api,
            Request::new(executor_api_pb::PollCommandsRequest {
                executor_id: executor_id.get().to_string(),
                acked_command_seq: Some(5),
            }),
        )
        .await
        .unwrap();

        let hb1 = ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_id.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![],
                allocation_log_entries: vec![],
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(hb1.send_state, Some(false));

        // Second poll regresses ack to None (0), which should trigger
        // a one-shot full-state request on the next heartbeat.
        let _ = ExecutorApi::poll_commands(
            &api,
            Request::new(executor_api_pb::PollCommandsRequest {
                executor_id: executor_id.get().to_string(),
                acked_command_seq: None,
            }),
        )
        .await
        .unwrap();

        let hb2 = ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_id.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![],
                allocation_log_entries: vec![],
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(
            hb2.send_state,
            Some(true),
            "ack regression should request full state on next heartbeat"
        );

        // One-shot: subsequent heartbeat should clear the flag.
        let hb3 = ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_id.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![],
                allocation_log_entries: vec![],
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(hb3.send_state, Some(false));
    }

    #[tokio::test]
    async fn test_result_routing_prefers_latest_parent_after_reschedule() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );

        let executor_a = crate::data_model::ExecutorId::from("executor-route-a");
        let executor_b = crate::data_model::ExecutorId::from("executor-route-b");
        let child_fc_id = "child-fc-stable";

        // Register both executors.
        api.handle_full_state(
            &executor_a,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();
        api.handle_full_state(
            &executor_b,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        // Attempt A registers a child call route first.
        ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_a.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![],
                allocation_log_entries: vec![make_call_function_log_entry(
                    "parent-alloc-a",
                    child_fc_id,
                )],
            }),
        )
        .await
        .unwrap();

        // Rescheduled attempt B reports the same function_call_id with a new
        // parent allocation.
        ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_b.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![],
                allocation_log_entries: vec![make_call_function_log_entry(
                    "parent-alloc-b",
                    child_fc_id,
                )],
            }),
        )
        .await
        .unwrap();

        // Seed the child allocation so outcome ingestion succeeds.
        let child_allocation = data_model::AllocationBuilder::default()
            .target(data_model::AllocationTarget::new(
                executor_b.clone(),
                data_model::ContainerId::new("child-container".to_string()),
            ))
            .function_call_id(data_model::FunctionCallId::from(child_fc_id))
            .namespace("ns".to_string())
            .application("app".to_string())
            .application_version("v1".to_string())
            .function("child-fn".to_string())
            .request_id("req-1".to_string())
            .outcome(data_model::FunctionRunOutcome::Unknown)
            .input_args(vec![])
            .call_metadata(bytes::Bytes::new())
            .build()
            .expect("child allocation should build");
        let child_allocation_id = child_allocation.id.to_string();
        let mut update = crate::state_store::requests::SchedulerUpdateRequest::default();
        update.new_allocations.push(child_allocation);
        test_service
            .service
            .indexify_state
            .write(crate::state_store::requests::StateMachineUpdateRequest {
                payload: crate::state_store::requests::RequestPayload::SchedulerUpdate(
                    crate::state_store::requests::SchedulerUpdatePayload::new(update),
                ),
            })
            .await
            .unwrap();

        // Child completion arrives; result must route to the latest parent
        // (executor B / parent-alloc-b), not stale attempt A.
        ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_b.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![make_completed_outcome(
                    child_fc_id,
                    &child_allocation_id,
                )],
                allocation_log_entries: vec![],
            }),
        )
        .await
        .unwrap();

        let (results_a, results_b) = {
            let connections = test_service
                .service
                .indexify_state
                .executor_connections
                .read()
                .await;
            let conn_a = connections
                .get(&executor_a)
                .expect("executor A connection should exist");
            let conn_b = connections
                .get(&executor_b)
                .expect("executor B connection should exist");
            (conn_a.clone_results().await, conn_b.clone_results().await)
        };

        assert!(
            results_a.is_empty(),
            "stale executor should not receive routed result"
        );
        assert_eq!(
            results_b.len(),
            1,
            "latest parent executor should receive exactly one routed result"
        );

        let routed = results_b[0]
            .entry
            .as_ref()
            .expect("sequenced result should include log entry");
        assert_eq!(routed.allocation_id, "parent-alloc-b");

        match routed
            .entry
            .as_ref()
            .expect("log entry payload should be present")
        {
            executor_api_pb::allocation_log_entry::Entry::FunctionCallResult(result) => {
                assert_eq!(
                    result.function_call_id.as_deref(),
                    Some(child_fc_id),
                    "routed result should preserve original function_call_id"
                );
                assert_eq!(
                    result.outcome_code,
                    Some(executor_api_pb::AllocationOutcomeCode::Success.into())
                );
            }
            _ => panic!("expected FunctionCallResult entry"),
        }
    }

    #[tokio::test]
    async fn test_lapsed_deregistration_purges_router_entries() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );

        let executor_id = crate::data_model::ExecutorId::from("executor-lapsed-router-purge");
        let child_fc_id = "child-fc-timeout-purge";

        // Deterministic lapsed-heartbeat timing.
        tokio::time::pause();

        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        // Register a pending router entry for this executor.
        ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_id.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![],
                allocation_log_entries: vec![make_call_function_log_entry(
                    "parent-timeout-alloc",
                    child_fc_id,
                )],
            }),
        )
        .await
        .unwrap();

        assert_eq!(
            api.function_call_result_router.pending_len().await,
            1,
            "expected one pending route before timeout deregistration"
        );

        // Let heartbeat deadline lapse and run timeout cleanup.
        tokio::time::advance(EXECUTOR_TIMEOUT + Duration::from_secs(1)).await;
        api.executor_manager
            .process_lapsed_executors()
            .await
            .unwrap();

        assert_eq!(
            api.function_call_result_router.pending_len().await,
            0,
            "lapsed deregistration should purge router entries"
        );
    }

    #[tokio::test]
    async fn test_lapsed_deregistration_purges_router_entries_under_load() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );

        let executor_id = crate::data_model::ExecutorId::from("executor-lapsed-router-purge-load");
        tokio::time::pause();

        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        let mut entries = Vec::new();
        for idx in 0..64 {
            entries.push(make_call_function_log_entry(
                &format!("parent-timeout-alloc-{idx}"),
                &format!("child-fc-timeout-purge-{idx}"),
            ));
        }

        ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_id.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![],
                allocation_log_entries: entries,
            }),
        )
        .await
        .unwrap();

        assert_eq!(
            api.function_call_result_router.pending_len().await,
            64,
            "expected all router entries to be registered before timeout"
        );

        tokio::time::advance(EXECUTOR_TIMEOUT + Duration::from_secs(1)).await;
        api.executor_manager
            .process_lapsed_executors()
            .await
            .unwrap();

        assert_eq!(
            api.function_call_result_router.pending_len().await,
            0,
            "lapsed deregistration should purge all stale router entries"
        );
    }

    #[tokio::test]
    async fn test_missing_allocation_outcome_is_idempotent_noop_and_does_not_route_result() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );

        let executor_id = crate::data_model::ExecutorId::from("executor-route-retry-on-error");
        let child_fc_id = "child-fc-retry-on-error";

        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        // Register route entry first.
        ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_id.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![],
                allocation_log_entries: vec![make_call_function_log_entry(
                    "parent-retry-alloc",
                    child_fc_id,
                )],
            }),
        )
        .await
        .unwrap();

        assert_eq!(
            api.function_call_result_router.pending_len().await,
            1,
            "expected one pending route before ingest failure"
        );

        // Send a completion that references a missing allocation. The server
        // should log and skip the bad item but keep processing the batch.
        ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_id.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![make_completed_outcome(
                    child_fc_id,
                    "missing-allocation",
                )],
                allocation_log_entries: vec![],
            }),
        )
        .await
        .expect("heartbeat should not fail due to one bad outcome");

        assert_eq!(
            api.function_call_result_router.pending_len().await,
            1,
            "route should remain pending when outcome ingest is a no-op"
        );
    }

    #[tokio::test]
    async fn test_outcome_noop_does_not_block_log_entries_in_same_heartbeat() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );

        let executor_id = crate::data_model::ExecutorId::from("executor-batch-continue-on-error");
        let routed_fc_id = "child-fc-batch-continue";

        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        // The first outcome references a missing allocation (idempotent no-op).
        // The log entry in the same heartbeat must still be processed.
        ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_id.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![make_completed_outcome(
                    "child-fc-missing-allocation",
                    "missing-allocation",
                )],
                allocation_log_entries: vec![make_call_function_log_entry(
                    "parent-batch-continue",
                    routed_fc_id,
                )],
            }),
        )
        .await
        .expect("heartbeat should continue processing remaining batch items");

        assert_eq!(
            api.function_call_result_router.pending_len().await,
            1,
            "valid log entry should still register a route after a no-op outcome"
        );
    }

    #[tokio::test]
    async fn test_process_command_responses_skips_malformed_items() {
        let test_service = TestService::new().await.unwrap();
        let executor_id = crate::data_model::ExecutorId::from("executor-command-response-counts");

        let bad_snapshot_completed = executor_api_pb::CommandResponse {
            command_seq: Some(1),
            response: Some(
                executor_api_pb::command_response::Response::SnapshotCompleted(
                    executor_api_pb::SnapshotCompleted {
                        container_id: "container-1".to_string(),
                        snapshot_id: "".to_string(),
                        snapshot_uri: "".to_string(),
                        size_bytes: 0,
                    },
                ),
            ),
        };
        let bad_snapshot_failed = executor_api_pb::CommandResponse {
            command_seq: Some(2),
            response: Some(executor_api_pb::command_response::Response::SnapshotFailed(
                executor_api_pb::SnapshotFailed {
                    container_id: "container-2".to_string(),
                    snapshot_id: "".to_string(),
                    error_message: "failed".to_string(),
                },
            )),
        };

        let failures = process_command_responses(
            &test_service.service.indexify_state,
            &executor_id,
            vec![bad_snapshot_completed, bad_snapshot_failed],
        )
        .await
        .expect("batch should continue and skip malformed command responses");

        assert_eq!(
            failures, 0,
            "malformed command responses should be skipped without failing heartbeat"
        );
    }

    #[tokio::test]
    async fn test_malformed_log_entry_does_not_block_following_log_entries() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );

        let executor_id =
            crate::data_model::ExecutorId::from("executor-log-entry-continue-on-error");
        let routed_fc_id = "child-fc-log-entry-continue";

        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        let malformed_log_entry = executor_api_pb::AllocationLogEntry {
            allocation_id: "parent-malformed".to_string(),
            clock: 0,
            entry: Some(executor_api_pb::allocation_log_entry::Entry::CallFunction(
                executor_api_pb::FunctionCallRequest {
                    namespace: Some("ns".to_string()),
                    application: Some("app".to_string()),
                    request_id: Some("req-1".to_string()),
                    source_function_call_id: Some("source-fc".to_string()),
                    updates: None,
                },
            )),
        };

        ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_id.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![],
                allocation_outcomes: vec![],
                allocation_log_entries: vec![
                    malformed_log_entry,
                    make_call_function_log_entry("parent-valid", routed_fc_id),
                ],
            }),
        )
        .await
        .expect("heartbeat should skip malformed log entry and continue");

        assert_eq!(
            api.function_call_result_router.pending_len().await,
            1,
            "valid log entries should still register routes despite one malformed entry"
        );
    }

    #[tokio::test]
    async fn test_command_response_failure_still_processes_other_heartbeat_reports() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );

        let executor_id = crate::data_model::ExecutorId::from("executor-command-response-error");
        let routed_fc_id = "child-fc-command-response-error";

        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        // SnapshotCompleted with empty snapshot_id is invalid and should fail
        // command-response ingestion.
        let bad_response = executor_api_pb::CommandResponse {
            command_seq: Some(1),
            response: Some(
                executor_api_pb::command_response::Response::SnapshotCompleted(
                    executor_api_pb::SnapshotCompleted {
                        container_id: "container-1".to_string(),
                        snapshot_id: "".to_string(),
                        snapshot_uri: "".to_string(),
                        size_bytes: 0,
                    },
                ),
            ),
        };

        ExecutorApi::heartbeat(
            &api,
            Request::new(executor_api_pb::HeartbeatRequest {
                executor_id: Some(executor_id.get().to_string()),
                status: Some(executor_api_pb::ExecutorStatus::Running.into()),
                full_state: None,
                command_responses: vec![bad_response],
                allocation_outcomes: vec![],
                allocation_log_entries: vec![make_call_function_log_entry(
                    "parent-command-response-error",
                    routed_fc_id,
                )],
            }),
        )
        .await
        .expect("heartbeat should skip malformed command response and continue");

        assert_eq!(
            api.function_call_result_router.pending_len().await,
            1,
            "other heartbeat reports should still be processed before returning error"
        );
    }

    #[tokio::test]
    async fn test_scheduler_update_emits_commands_without_wake_loop() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );
        let executor_id = crate::data_model::ExecutorId::from("executor-update-driven-emission");

        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        // Process queued UpsertExecutor so scheduler has executor metadata.
        test_service.process_all_state_changes().await.unwrap();

        let container = crate::data_model::ContainerBuilder::default()
            .id(crate::data_model::ContainerId::new(
                "container-update-driven".to_string(),
            ))
            .namespace("ns".to_string())
            .application_name("app".to_string())
            .function_name("fn".to_string())
            .version("v1".to_string())
            .state(crate::data_model::ContainerState::Pending)
            .resources(crate::data_model::ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .max_concurrency(1)
            .container_type(crate::data_model::ContainerType::Function)
            .build()
            .unwrap();
        let mut executor_state = crate::data_model::ExecutorServerMetadata {
            executor_id: executor_id.clone(),
            function_container_ids: imbl::HashSet::new(),
            free_resources: crate::data_model::HostResources {
                cpu_ms_per_sec: 8_000,
                memory_bytes: 16 * 1024 * 1024 * 1024,
                disk_bytes: 100 * 1024 * 1024 * 1024,
                gpu: None,
            },
            resource_claims: imbl::HashMap::new(),
        };
        executor_state
            .add_container(&container)
            .expect("container should fit executor resources");

        let container_meta = crate::data_model::ContainerServerMetadata::new(
            executor_id.clone(),
            container.clone(),
            crate::data_model::ContainerState::Pending,
        );

        let mut update = crate::state_store::requests::SchedulerUpdateRequest::default();
        update
            .updated_executor_states
            .insert(executor_id.clone(), Box::new(executor_state));
        update
            .containers
            .insert(container.id.clone(), Box::new(container_meta));

        // Mirror scheduler publish path (ArcSwap update), then emit commands
        // directly from the scheduler update.
        let current = test_service.service.indexify_state.app_state.load_full();
        let mut indexes = current.indexes.clone();
        let mut scheduler = current.scheduler.clone();
        let clock = indexes.clock;
        indexes
            .apply_scheduler_update(clock, &update, "test_scheduler_update")
            .unwrap();
        scheduler.apply_container_update(&update);
        test_service
            .service
            .executor_manager
            .rebuild_scheduler_command_intents(&mut update, &indexes);
        test_service
            .service
            .indexify_state
            .write_scheduler_output(crate::state_store::requests::StateMachineUpdateRequest {
                payload: crate::state_store::requests::RequestPayload::SchedulerUpdate(
                    crate::state_store::requests::SchedulerUpdatePayload::new(update.clone()),
                ),
            })
            .await
            .unwrap();
        test_service
            .service
            .indexify_state
            .app_state
            .store(std::sync::Arc::new(crate::state_store::AppState {
                indexes,
                scheduler,
            }));

        test_service
            .service
            .executor_manager
            .drain_and_emit_scheduler_command_intents()
            .await;

        let response = ExecutorApi::poll_commands(
            &api,
            Request::new(executor_api_pb::PollCommandsRequest {
                executor_id: executor_id.get().to_string(),
                acked_command_seq: None,
            }),
        )
        .await
        .unwrap()
        .into_inner();

        let has_add = response.commands.iter().any(|c| {
            matches!(
                c.command,
                Some(executor_api_pb::command::Command::AddContainer(_))
            )
        });
        assert!(
            has_add,
            "expected AddContainer command after scheduler update"
        );
    }

    #[tokio::test]
    async fn test_scheduler_update_emits_remove_before_add_for_replacement() {
        let test_service = TestService::new().await.unwrap();
        let executor_id = crate::data_model::ExecutorId::from("executor-remove-before-add");

        let old_container = crate::data_model::ContainerBuilder::default()
            .id(crate::data_model::ContainerId::new(
                "container-old-remove-first".to_string(),
            ))
            .namespace("ns".to_string())
            .application_name("app".to_string())
            .function_name("fn-old".to_string())
            .version("v1".to_string())
            .state(crate::data_model::ContainerState::Running)
            .resources(crate::data_model::ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .max_concurrency(1)
            .container_type(crate::data_model::ContainerType::Function)
            .build()
            .unwrap();
        let new_container = crate::data_model::ContainerBuilder::default()
            .id(crate::data_model::ContainerId::new(
                "container-new-add-second".to_string(),
            ))
            .namespace("ns".to_string())
            .application_name("app".to_string())
            .function_name("fn-new".to_string())
            .version("v1".to_string())
            .state(crate::data_model::ContainerState::Pending)
            .resources(crate::data_model::ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .max_concurrency(1)
            .container_type(crate::data_model::ContainerType::Function)
            .build()
            .unwrap();

        let old_meta = crate::data_model::ContainerServerMetadata::new(
            executor_id.clone(),
            old_container.clone(),
            crate::data_model::ContainerState::Terminated {
                reason: crate::data_model::ContainerTerminationReason::FunctionCancelled,
            },
        );
        let new_meta = crate::data_model::ContainerServerMetadata::new(
            executor_id.clone(),
            new_container.clone(),
            crate::data_model::ContainerState::Pending,
        );

        let mut update = crate::state_store::requests::SchedulerUpdateRequest::default();
        update
            .containers
            .insert(old_container.id.clone(), Box::new(old_meta));
        update
            .containers
            .insert(new_container.id.clone(), Box::new(new_meta));

        let indexes_guard = test_service.service.indexify_state.app_state.load();
        test_service
            .service
            .executor_manager
            .rebuild_scheduler_command_intents(&mut update, &indexes_guard.indexes);

        assert_eq!(
            update.scheduler_command_intents.len(),
            2,
            "expected one RemoveContainer and one AddContainer intent"
        );
        assert!(
            matches!(
                update.scheduler_command_intents[0].command.command,
                Some(executor_api_pb::command::Command::RemoveContainer(_))
            ),
            "expected first command to be RemoveContainer, got {:?}",
            update.scheduler_command_intents[0].command
        );
        assert!(
            matches!(
                update.scheduler_command_intents[1].command.command,
                Some(executor_api_pb::command::Command::AddContainer(_))
            ),
            "expected second command to be AddContainer, got {:?}",
            update.scheduler_command_intents[1].command
        );
    }

    #[tokio::test]
    async fn test_scheduler_update_emits_update_for_claimed_warm_sandbox() {
        let test_service = TestService::new().await.unwrap();
        let api = super::ExecutorAPIService::new(
            test_service.service.indexify_state.clone(),
            test_service.service.executor_manager.clone(),
            test_service.service.blob_storage_registry.clone(),
        );
        let executor_id = crate::data_model::ExecutorId::from("executor-warm-claim-update");

        api.handle_full_state(
            &executor_id,
            executor_api_pb::DataplaneStateFullSync::default(),
        )
        .await
        .unwrap();

        // Process queued UpsertExecutor so scheduler has executor metadata.
        test_service.process_all_state_changes().await.unwrap();

        let container_id =
            crate::data_model::ContainerId::new("container-warm-claim-update".to_string());
        let sandbox_id = crate::data_model::SandboxId::new("sandbox-claimed-update".to_string());
        let pool_id = crate::data_model::ContainerPoolId::new("pool-claimed-update");

        let container = crate::data_model::ContainerBuilder::default()
            .id(container_id.clone())
            .namespace("ns".to_string())
            .application_name("app".to_string())
            .function_name("sandbox-fn".to_string())
            .version("v1".to_string())
            .state(crate::data_model::ContainerState::Running)
            .resources(crate::data_model::ContainerResources {
                cpu_ms_per_sec: 1000,
                memory_mb: 512,
                ephemeral_disk_mb: 1024,
                gpu: None,
            })
            .max_concurrency(1)
            .container_type(crate::data_model::ContainerType::Sandbox)
            .image(Some("ubuntu:latest".to_string()))
            .timeout_secs(600)
            .sandbox_id(Some(sandbox_id.clone()))
            .pool_id(Some(pool_id.clone()))
            .build()
            .unwrap();
        let mut executor_state = crate::data_model::ExecutorServerMetadata {
            executor_id: executor_id.clone(),
            function_container_ids: imbl::HashSet::new(),
            free_resources: crate::data_model::HostResources {
                cpu_ms_per_sec: 8_000,
                memory_bytes: 16 * 1024 * 1024 * 1024,
                disk_bytes: 100 * 1024 * 1024 * 1024,
                gpu: None,
            },
            resource_claims: imbl::HashMap::new(),
        };
        executor_state
            .add_container(&container)
            .expect("container should fit executor resources");

        let container_meta = crate::data_model::ContainerServerMetadata::new(
            executor_id.clone(),
            container.clone(),
            crate::data_model::ContainerState::Running,
        );
        let sandbox = crate::data_model::SandboxBuilder::default()
            .id(sandbox_id.clone())
            .namespace("ns".to_string())
            .image("ubuntu:latest".to_string())
            .status(crate::data_model::SandboxStatus::Running)
            .resources(container.resources.clone())
            .timeout_secs(600)
            .executor_id(Some(executor_id.clone()))
            .pool_id(Some(pool_id.clone()))
            .container_id(Some(container_id.clone()))
            .build()
            .unwrap();

        let mut update = crate::state_store::requests::SchedulerUpdateRequest::default();
        update
            .updated_executor_states
            .insert(executor_id.clone(), Box::new(executor_state));
        update
            .containers
            .insert(container.id.clone(), Box::new(container_meta));
        update
            .updated_sandboxes
            .insert(crate::data_model::SandboxKey::from(&sandbox), sandbox);

        let current = test_service.service.indexify_state.app_state.load_full();
        let mut indexes = current.indexes.clone();
        let mut scheduler = current.scheduler.clone();
        let clock = indexes.clock;
        indexes
            .apply_scheduler_update(clock, &update, "test_scheduler_update")
            .unwrap();
        scheduler.apply_container_update(&update);
        test_service
            .service
            .executor_manager
            .rebuild_scheduler_command_intents(&mut update, &indexes);
        test_service
            .service
            .indexify_state
            .write_scheduler_output(crate::state_store::requests::StateMachineUpdateRequest {
                payload: crate::state_store::requests::RequestPayload::SchedulerUpdate(
                    crate::state_store::requests::SchedulerUpdatePayload::new(update.clone()),
                ),
            })
            .await
            .unwrap();
        test_service
            .service
            .indexify_state
            .app_state
            .store(std::sync::Arc::new(crate::state_store::AppState {
                indexes,
                scheduler,
            }));

        test_service
            .service
            .executor_manager
            .drain_and_emit_scheduler_command_intents()
            .await;

        let response = ExecutorApi::poll_commands(
            &api,
            Request::new(executor_api_pb::PollCommandsRequest {
                executor_id: executor_id.get().to_string(),
                acked_command_seq: None,
            }),
        )
        .await
        .unwrap()
        .into_inner();

        let update_cmd = response
            .commands
            .iter()
            .find_map(|command| match &command.command {
                Some(executor_api_pb::command::Command::UpdateContainerDescription(update)) => {
                    Some(update)
                }
                _ => None,
            });
        let Some(update_cmd) = update_cmd else {
            panic!(
                "expected UpdateContainerDescription command after warm-sandbox claim update: {:?}",
                response.commands
            );
        };
        assert_eq!(update_cmd.container_id, container_id.get().to_string());
        assert_eq!(
            update_cmd
                .sandbox_metadata
                .as_ref()
                .and_then(|m| m.sandbox_id.as_deref()),
            Some(sandbox_id.get()),
            "expected claimed sandbox metadata to be propagated"
        );
    }
}
