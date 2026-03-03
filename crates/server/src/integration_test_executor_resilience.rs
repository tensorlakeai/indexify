#![cfg(test)]

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use tonic::Request;

use crate::{
    blob_store::BlobStorageConfig,
    config::ServerConfig,
    data_model,
    executor_api::{
        ExecutorAPIService,
        executor_api_pb::{
            self,
            AllocationCompleted,
            AllocationLogEntry,
            AllocationOutcome,
            ContainerDescription,
            ContainerResources,
            ContainerState,
            ContainerStatus,
            ContainerType,
            DataplaneStateFullSync,
            ExecutionPlanUpdate,
            ExecutionPlanUpdates,
            FunctionCall,
            FunctionCallRequest,
            FunctionRef,
            HeartbeatRequest,
            PollAllocationResultsRequest,
            allocation_log_entry,
            allocation_outcome,
            execution_plan_update,
            executor_api_server::ExecutorApi,
        },
    },
    executors::{EXECUTOR_TIMEOUT, STARTUP_EXECUTOR_TIMEOUT},
    service::Service,
    state_store::{
        driver::rocksdb::RocksDBConfig,
        executor_connection::MAX_POLL_RESPONSE_BYTES,
        requests::{
            RequestPayload,
            SchedulerUpdatePayload,
            SchedulerUpdateRequest,
            StateMachineUpdateRequest,
        },
    },
    testing::TestService,
};

fn make_call_function_log_entry(
    parent_allocation_id: &str,
    child_function_call_id: &str,
) -> AllocationLogEntry {
    AllocationLogEntry {
        allocation_id: parent_allocation_id.to_string(),
        clock: 0,
        entry: Some(allocation_log_entry::Entry::CallFunction(
            FunctionCallRequest {
                namespace: Some("ns".to_string()),
                application: Some("app".to_string()),
                request_id: Some("req-1".to_string()),
                source_function_call_id: Some("source-fc".to_string()),
                updates: Some(ExecutionPlanUpdates {
                    updates: vec![ExecutionPlanUpdate {
                        op: Some(execution_plan_update::Op::FunctionCall(FunctionCall {
                            id: Some(child_function_call_id.to_string()),
                            target: Some(FunctionRef {
                                namespace: Some("ns".to_string()),
                                application_name: Some("app".to_string()),
                                function_name: Some("child-fn".to_string()),
                                application_version: Some("v1".to_string()),
                            }),
                            args: vec![],
                            call_metadata: None,
                        })),
                    }],
                    root_function_call_id: Some(child_function_call_id.to_string()),
                    start_at: None,
                }),
            },
        )),
    }
}

fn function_fe_state_proto(container_id: &str) -> ContainerState {
    ContainerState {
        description: Some(ContainerDescription {
            id: Some(container_id.to_string()),
            function: Some(FunctionRef {
                namespace: Some("ns".to_string()),
                application_name: Some("app".to_string()),
                function_name: Some("fn".to_string()),
                application_version: Some("v1".to_string()),
            }),
            resources: Some(ContainerResources {
                cpu_ms_per_sec: Some(100),
                memory_bytes: Some(256 * 1024 * 1024),
                disk_bytes: Some(1024 * 1024 * 1024),
                gpu: None,
            }),
            max_concurrency: Some(1),
            container_type: Some(ContainerType::Function.into()),
            sandbox_metadata: None,
            secret_names: vec![],
            initialization_timeout_ms: None,
            application: None,
            allocation_timeout_ms: None,
            pool_id: None,
        }),
        status: Some(ContainerStatus::Running.into()),
        termination_reason: None,
    }
}

fn make_completed_outcome(
    child_function_call_id: &str,
    allocation_id: &str,
) -> executor_api_pb::AllocationOutcome {
    AllocationOutcome {
        outcome: Some(allocation_outcome::Outcome::Completed(
            AllocationCompleted {
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

fn make_completed_updates_missing_root_outcome(
    child_function_call_id: &str,
    allocation_id: &str,
) -> executor_api_pb::AllocationOutcome {
    AllocationOutcome {
        outcome: Some(allocation_outcome::Outcome::Completed(
            AllocationCompleted {
                allocation_id: allocation_id.to_string(),
                function: Some(FunctionRef {
                    namespace: Some("ns".to_string()),
                    application_name: Some("app".to_string()),
                    function_name: Some("child-fn".to_string()),
                    application_version: Some("v1".to_string()),
                }),
                function_call_id: Some(child_function_call_id.to_string()),
                request_id: Some("req-1".to_string()),
                return_value: Some(executor_api_pb::allocation_completed::ReturnValue::Updates(
                    ExecutionPlanUpdates {
                        updates: vec![],
                        root_function_call_id: None,
                        start_at: None,
                    },
                )),
                execution_duration_ms: None,
            },
        )),
    }
}

async fn heartbeat_full_state(api: &ExecutorAPIService, executor_id: &str) -> Result<()> {
    ExecutorApi::heartbeat(
        api,
        Request::new(HeartbeatRequest {
            executor_id: Some(executor_id.to_string()),
            status: Some(executor_api_pb::ExecutorStatus::Running.into()),
            full_state: Some(DataplaneStateFullSync::default()),
            command_responses: vec![],
            allocation_outcomes: vec![],
            allocation_log_entries: vec![],
        }),
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn test_oversized_command_enqueue_rejected() -> Result<()> {
    let test_service = TestService::new().await?;
    let api = ExecutorAPIService::new(
        test_service.service.indexify_state.clone(),
        test_service.service.executor_manager.clone(),
        test_service.service.blob_storage_registry.clone(),
    );
    let executor_id = "executor-oversized-command";
    let executor_dm_id = data_model::ExecutorId::from(executor_id);

    heartbeat_full_state(&api, executor_id).await?;

    let conn = {
        let connections = test_service
            .service
            .indexify_state
            .executor_connections
            .read()
            .await;
        connections
            .get(&executor_dm_id)
            .expect("executor connection should exist")
            .clone()
    };

    let oversized = executor_api_pb::Command {
        seq: 1,
        command: Some(executor_api_pb::command::Command::KillAllocation(
            executor_api_pb::KillAllocation {
                allocation_id: "x".repeat(MAX_POLL_RESPONSE_BYTES + 256),
            },
        )),
    };
    let err = conn.push_commands(vec![oversized]).await.unwrap_err();
    assert!(
        err.to_string().contains("exceeds max poll item size"),
        "expected oversized command rejection error, got {err:?}"
    );
    assert!(conn.clone_commands().await.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_oversized_result_enqueue_rejected_without_seq_advance() -> Result<()> {
    let test_service = TestService::new().await?;
    let api = ExecutorAPIService::new(
        test_service.service.indexify_state.clone(),
        test_service.service.executor_manager.clone(),
        test_service.service.blob_storage_registry.clone(),
    );
    let executor_id = "executor-oversized-result";
    let executor_dm_id = data_model::ExecutorId::from(executor_id);

    heartbeat_full_state(&api, executor_id).await?;

    let conn = {
        let connections = test_service
            .service
            .indexify_state
            .executor_connections
            .read()
            .await;
        connections
            .get(&executor_dm_id)
            .expect("executor connection should exist")
            .clone()
    };

    let err = conn
        .push_result(AllocationLogEntry {
            allocation_id: "x".repeat(MAX_POLL_RESPONSE_BYTES + 256),
            clock: 0,
            entry: None,
        })
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("exceeds max poll item size"),
        "expected oversized result rejection error, got {err:?}"
    );
    assert_eq!(conn.next_result_seq(), 1);
    assert!(conn.clone_results().await.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_reconnect_full_state_clears_stale_pending_results() -> Result<()> {
    let test_service = TestService::new().await?;
    let api = ExecutorAPIService::new(
        test_service.service.indexify_state.clone(),
        test_service.service.executor_manager.clone(),
        test_service.service.blob_storage_registry.clone(),
    );
    let executor_id = "executor-reconnect-result-reset";
    let executor_dm_id = data_model::ExecutorId::from(executor_id);

    heartbeat_full_state(&api, executor_id).await?;

    let conn = {
        let connections = test_service
            .service
            .indexify_state
            .executor_connections
            .read()
            .await;
        connections
            .get(&executor_dm_id)
            .expect("executor connection should exist")
            .clone()
    };
    conn.push_result(AllocationLogEntry {
        allocation_id: "stale-result-allocation".to_string(),
        clock: 0,
        entry: None,
    })
    .await?;

    // Simulate dataplane reconnect with same executor ID.
    heartbeat_full_state(&api, executor_id).await?;

    let results = {
        let connections = test_service
            .service
            .indexify_state
            .executor_connections
            .read()
            .await;
        let conn = connections
            .get(&executor_dm_id)
            .expect("executor connection should still exist");
        conn.clone_results().await
    };
    assert!(
        results.is_empty(),
        "reconnect full-state should clear stale pending results"
    );

    Ok(())
}

#[tokio::test]
async fn test_routed_result_dropped_when_parent_connection_missing() -> Result<()> {
    let test_service = TestService::new().await?;
    let api = ExecutorAPIService::new(
        test_service.service.indexify_state.clone(),
        test_service.service.executor_manager.clone(),
        test_service.service.blob_storage_registry.clone(),
    );
    let executor_id = "executor-route-connection-gap";
    let executor_dm_id = data_model::ExecutorId::from(executor_id);
    let child_fc = "child-fc-connection-gap";

    heartbeat_full_state(&api, executor_id).await?;

    ExecutorApi::heartbeat(
        &api,
        Request::new(HeartbeatRequest {
            executor_id: Some(executor_id.to_string()),
            status: Some(executor_api_pb::ExecutorStatus::Running.into()),
            full_state: None,
            command_responses: vec![],
            allocation_outcomes: vec![],
            allocation_log_entries: vec![make_call_function_log_entry(
                "parent-alloc-connection-gap",
                child_fc,
            )],
        }),
    )
    .await?;

    assert!(
        test_service
            .service
            .indexify_state
            .get_function_call_route(child_fc)
            .await?
            .is_some(),
        "route should exist before child outcome arrives"
    );

    // Seed child allocation so completion ingestion succeeds.
    let child_allocation = data_model::AllocationBuilder::default()
        .target(data_model::AllocationTarget::new(
            data_model::ExecutorId::from(executor_id),
            data_model::ContainerId::new("child-container-connection-gap".to_string()),
        ))
        .function_call_id(data_model::FunctionCallId::from(child_fc))
        .namespace("ns".to_string())
        .application("app".to_string())
        .application_version("v1".to_string())
        .function("child-fn".to_string())
        .request_id("req-1".to_string())
        .outcome(data_model::FunctionRunOutcome::Unknown)
        .input_args(vec![])
        .call_metadata(bytes::Bytes::new())
        .build()?;
    let child_allocation_id = child_allocation.id.to_string();
    let mut update = SchedulerUpdateRequest::default();
    update.new_allocations.push(child_allocation);
    test_service
        .service
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(update)),
        })
        .await?;

    // Simulate a transient parent connection loss exactly when child outcome
    // arrives.
    test_service
        .service
        .indexify_state
        .deregister_executor_connection(&executor_dm_id)
        .await;

    ExecutorApi::heartbeat(
        &api,
        Request::new(HeartbeatRequest {
            executor_id: Some(executor_id.to_string()),
            status: Some(executor_api_pb::ExecutorStatus::Running.into()),
            full_state: None,
            command_responses: vec![],
            allocation_outcomes: vec![make_completed_outcome(child_fc, &child_allocation_id)],
            allocation_log_entries: vec![],
        }),
    )
    .await?;

    // Parent connection is missing, so routed result must not be delivered and
    // no connection should be recreated implicitly.
    assert!(
        test_service
            .service
            .indexify_state
            .executor_connections
            .read()
            .await
            .get(&executor_dm_id)
            .is_none(),
        "routing should not recreate missing parent executor connection"
    );
    assert!(
        test_service
            .service
            .indexify_state
            .get_function_call_route(child_fc)
            .await?
            .is_none(),
        "route should be consumed to avoid repeated retries for undeliverable result"
    );

    Ok(())
}

#[tokio::test]
async fn test_malformed_updates_routing_does_not_fail_heartbeat() -> Result<()> {
    let test_service = TestService::new().await?;
    let api = ExecutorAPIService::new(
        test_service.service.indexify_state.clone(),
        test_service.service.executor_manager.clone(),
        test_service.service.blob_storage_registry.clone(),
    );
    let executor_id = "executor-malformed-updates-route";
    let child_fc = "child-fc-malformed-updates-route";

    heartbeat_full_state(&api, executor_id).await?;
    ExecutorApi::heartbeat(
        &api,
        Request::new(HeartbeatRequest {
            executor_id: Some(executor_id.to_string()),
            status: Some(executor_api_pb::ExecutorStatus::Running.into()),
            full_state: None,
            command_responses: vec![],
            allocation_outcomes: vec![],
            allocation_log_entries: vec![make_call_function_log_entry(
                "parent-alloc-malformed-updates",
                child_fc,
            )],
        }),
    )
    .await?;

    let child_allocation = data_model::AllocationBuilder::default()
        .target(data_model::AllocationTarget::new(
            data_model::ExecutorId::from(executor_id),
            data_model::ContainerId::new("child-container-malformed-updates".to_string()),
        ))
        .function_call_id(data_model::FunctionCallId::from(child_fc))
        .namespace("ns".to_string())
        .application("app".to_string())
        .application_version("v1".to_string())
        .function("child-fn".to_string())
        .request_id("req-1".to_string())
        .outcome(data_model::FunctionRunOutcome::Unknown)
        .input_args(vec![])
        .call_metadata(bytes::Bytes::new())
        .build()?;
    let child_allocation_id = child_allocation.id.to_string();
    let mut update = SchedulerUpdateRequest::default();
    update.new_allocations.push(child_allocation);
    test_service
        .service
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(update)),
        })
        .await?;

    let heartbeat = ExecutorApi::heartbeat(
        &api,
        Request::new(HeartbeatRequest {
            executor_id: Some(executor_id.to_string()),
            status: Some(executor_api_pb::ExecutorStatus::Running.into()),
            full_state: None,
            command_responses: vec![],
            allocation_outcomes: vec![make_completed_updates_missing_root_outcome(
                child_fc,
                &child_allocation_id,
            )],
            allocation_log_entries: vec![],
        }),
    )
    .await;
    assert!(
        heartbeat.is_ok(),
        "malformed updates routing should be logged and skipped, not fail the heartbeat"
    );

    Ok(())
}

#[tokio::test]
async fn test_full_state_ignores_malformed_container_entries() -> Result<()> {
    let test_service = TestService::new().await?;
    let api = ExecutorAPIService::new(
        test_service.service.indexify_state.clone(),
        test_service.service.executor_manager.clone(),
        test_service.service.blob_storage_registry.clone(),
    );
    let executor_id = "executor-full-state-mixed-malformed";

    let heartbeat = ExecutorApi::heartbeat(
        &api,
        Request::new(HeartbeatRequest {
            executor_id: Some(executor_id.to_string()),
            status: Some(executor_api_pb::ExecutorStatus::Running.into()),
            full_state: Some(DataplaneStateFullSync {
                container_states: vec![
                    function_fe_state_proto("valid-function-container"),
                    ContainerState {
                        description: None,
                        status: Some(ContainerStatus::Running.into()),
                        termination_reason: None,
                    },
                ],
                ..Default::default()
            }),
            command_responses: vec![],
            allocation_outcomes: vec![],
            allocation_log_entries: vec![],
        }),
    )
    .await;

    assert!(
        heartbeat.is_ok(),
        "heartbeat with mixed valid/malformed full_state entries should succeed"
    );

    Ok(())
}

#[tokio::test]
async fn test_lapsed_executor_purges_persisted_function_call_routes() -> Result<()> {
    tokio::time::pause();
    let test_service = TestService::new().await?;
    let api = ExecutorAPIService::new(
        test_service.service.indexify_state.clone(),
        test_service.service.executor_manager.clone(),
        test_service.service.blob_storage_registry.clone(),
    );
    let executor_id = "executor-timeout-route-purge";
    let child_fc = "child-fc-timeout-persisted";

    heartbeat_full_state(&api, executor_id).await?;
    ExecutorApi::heartbeat(
        &api,
        Request::new(HeartbeatRequest {
            executor_id: Some(executor_id.to_string()),
            status: Some(executor_api_pb::ExecutorStatus::Running.into()),
            full_state: None,
            command_responses: vec![],
            allocation_outcomes: vec![],
            allocation_log_entries: vec![make_call_function_log_entry(
                "parent-timeout-alloc",
                child_fc,
            )],
        }),
    )
    .await?;

    assert!(
        test_service
            .service
            .indexify_state
            .get_function_call_route(child_fc)
            .await?
            .is_some(),
        "route should be persisted before timeout purge"
    );

    tokio::time::advance(EXECUTOR_TIMEOUT + Duration::from_secs(1)).await;
    test_service
        .service
        .executor_manager
        .process_lapsed_executors()
        .await?;

    assert!(
        test_service
            .service
            .indexify_state
            .get_function_call_route(child_fc)
            .await?
            .is_none(),
        "lapsed deregistration should purge persisted routes"
    );

    Ok(())
}

#[tokio::test]
async fn test_router_recreation_still_routes_results_from_persisted_routes() -> Result<()> {
    let test_service = TestService::new().await?;
    let api1 = ExecutorAPIService::new(
        test_service.service.indexify_state.clone(),
        test_service.service.executor_manager.clone(),
        test_service.service.blob_storage_registry.clone(),
    );
    let executor_id = "executor-route-restart-sim";
    let child_fc = "child-fc-router-restart-sim";

    heartbeat_full_state(&api1, executor_id).await?;
    ExecutorApi::heartbeat(
        &api1,
        Request::new(HeartbeatRequest {
            executor_id: Some(executor_id.to_string()),
            status: Some(executor_api_pb::ExecutorStatus::Running.into()),
            full_state: None,
            command_responses: vec![],
            allocation_outcomes: vec![],
            allocation_log_entries: vec![make_call_function_log_entry(
                "parent-alloc-restart-sim",
                child_fc,
            )],
        }),
    )
    .await?;

    assert!(
        test_service
            .service
            .indexify_state
            .get_function_call_route(child_fc)
            .await?
            .is_some(),
        "route should be persisted before router recreation"
    );

    // Seed child allocation so completion ingestion succeeds.
    let child_allocation = data_model::AllocationBuilder::default()
        .target(data_model::AllocationTarget::new(
            data_model::ExecutorId::from(executor_id),
            data_model::ContainerId::new("child-container".to_string()),
        ))
        .function_call_id(data_model::FunctionCallId::from(child_fc))
        .namespace("ns".to_string())
        .application("app".to_string())
        .application_version("v1".to_string())
        .function("child-fn".to_string())
        .request_id("req-1".to_string())
        .outcome(data_model::FunctionRunOutcome::Unknown)
        .input_args(vec![])
        .call_metadata(bytes::Bytes::new())
        .build()?;
    let child_allocation_id = child_allocation.id.to_string();
    let mut update = SchedulerUpdateRequest::default();
    update.new_allocations.push(child_allocation);
    test_service
        .service
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(update)),
        })
        .await?;

    // Recreate the API service/router over the same IndexifyState to simulate
    // control-plane process restart with persistent routes intact.
    let api2 = Arc::new(ExecutorAPIService::new(
        test_service.service.indexify_state.clone(),
        test_service.service.executor_manager.clone(),
        test_service.service.blob_storage_registry.clone(),
    ));

    ExecutorApi::heartbeat(
        api2.as_ref(),
        Request::new(HeartbeatRequest {
            executor_id: Some(executor_id.to_string()),
            status: Some(executor_api_pb::ExecutorStatus::Running.into()),
            full_state: None,
            command_responses: vec![],
            allocation_outcomes: vec![make_completed_outcome(child_fc, &child_allocation_id)],
            allocation_log_entries: vec![],
        }),
    )
    .await?;

    let results = ExecutorApi::poll_allocation_results(
        api2.as_ref(),
        Request::new(PollAllocationResultsRequest {
            executor_id: executor_id.to_string(),
            acked_result_seq: None,
        }),
    )
    .await?
    .into_inner()
    .results;
    assert_eq!(
        results.len(),
        1,
        "expected routed result after router recreation"
    );

    assert!(
        test_service
            .service
            .indexify_state
            .get_function_call_route(child_fc)
            .await?
            .is_none(),
        "route should be removed after successful routing"
    );

    Ok(())
}

#[tokio::test]
async fn test_service_restart_still_routes_results_from_persisted_routes() -> Result<()> {
    tokio::time::pause();
    let temp_dir = tempfile::tempdir()?;
    let state_store_path = temp_dir
        .path()
        .join("state_store")
        .to_str()
        .unwrap()
        .to_string();
    let blob_store_path = format!(
        "file://{}",
        temp_dir.path().join("blob_store").to_str().unwrap()
    );
    let executor_id = "executor-route-restart-real";
    let child_fc = "child-fc-route-restart-real";

    let service1 = Service::new(ServerConfig {
        state_store_path: state_store_path.clone(),
        rocksdb_config: RocksDBConfig::default(),
        blob_storage: BlobStorageConfig {
            path: blob_store_path.clone(),
            region: None,
        },
        ..Default::default()
    })
    .await?;
    // Let startup cleanup task install its sleep before we advance virtual
    // time later, so it can fully complete and release DB resources.
    tokio::task::yield_now().await;
    let api1 = ExecutorAPIService::new(
        service1.indexify_state.clone(),
        service1.executor_manager.clone(),
        service1.blob_storage_registry.clone(),
    );

    heartbeat_full_state(&api1, executor_id).await?;
    ExecutorApi::heartbeat(
        &api1,
        Request::new(HeartbeatRequest {
            executor_id: Some(executor_id.to_string()),
            status: Some(executor_api_pb::ExecutorStatus::Running.into()),
            full_state: None,
            command_responses: vec![],
            allocation_outcomes: vec![],
            allocation_log_entries: vec![make_call_function_log_entry(
                "parent-alloc-restart-real",
                child_fc,
            )],
        }),
    )
    .await?;

    assert!(
        service1
            .indexify_state
            .get_function_call_route(child_fc)
            .await?
            .is_some(),
        "route should be persisted before service restart"
    );

    // Seed child allocation so completion ingestion succeeds.
    let child_allocation = data_model::AllocationBuilder::default()
        .target(data_model::AllocationTarget::new(
            data_model::ExecutorId::from(executor_id),
            data_model::ContainerId::new("child-container-restart-real".to_string()),
        ))
        .function_call_id(data_model::FunctionCallId::from(child_fc))
        .namespace("ns".to_string())
        .application("app".to_string())
        .application_version("v1".to_string())
        .function("child-fn".to_string())
        .request_id("req-1".to_string())
        .outcome(data_model::FunctionRunOutcome::Unknown)
        .input_args(vec![])
        .call_metadata(bytes::Bytes::new())
        .build()?;
    let child_allocation_id = child_allocation.id.to_string();
    let mut update = SchedulerUpdateRequest::default();
    update.new_allocations.push(child_allocation);
    service1
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(update)),
        })
        .await?;

    drop(api1);
    drop(service1);
    tokio::time::advance(STARTUP_EXECUTOR_TIMEOUT + Duration::from_secs(1)).await;
    tokio::task::yield_now().await;

    let service2 = Service::new(ServerConfig {
        state_store_path,
        rocksdb_config: RocksDBConfig::default(),
        blob_storage: BlobStorageConfig {
            path: blob_store_path,
            region: None,
        },
        ..Default::default()
    })
    .await?;
    assert!(
        service2
            .indexify_state
            .get_function_call_route(child_fc)
            .await?
            .is_some(),
        "route should be recoverable from persisted state after restart"
    );
    let api2 = Arc::new(ExecutorAPIService::new(
        service2.indexify_state.clone(),
        service2.executor_manager.clone(),
        service2.blob_storage_registry.clone(),
    ));
    heartbeat_full_state(api2.as_ref(), executor_id).await?;

    ExecutorApi::heartbeat(
        api2.as_ref(),
        Request::new(HeartbeatRequest {
            executor_id: Some(executor_id.to_string()),
            status: Some(executor_api_pb::ExecutorStatus::Running.into()),
            full_state: None,
            command_responses: vec![],
            allocation_outcomes: vec![make_completed_outcome(child_fc, &child_allocation_id)],
            allocation_log_entries: vec![],
        }),
    )
    .await?;

    let results = ExecutorApi::poll_allocation_results(
        api2.as_ref(),
        Request::new(PollAllocationResultsRequest {
            executor_id: executor_id.to_string(),
            acked_result_seq: None,
        }),
    )
    .await?
    .into_inner()
    .results;
    assert_eq!(
        results.len(),
        1,
        "expected routed result after full service restart"
    );
    assert!(
        service2
            .indexify_state
            .get_function_call_route(child_fc)
            .await?
            .is_none(),
        "route should be removed after successful routing"
    );

    Ok(())
}

#[tokio::test]
async fn test_service_restart_replays_persisted_scheduler_command_intents() -> Result<()> {
    tokio::time::pause();
    let temp_dir = tempfile::tempdir()?;
    let state_store_path = temp_dir
        .path()
        .join("state_store")
        .to_str()
        .unwrap()
        .to_string();
    let blob_store_path = format!(
        "file://{}",
        temp_dir.path().join("blob_store").to_str().unwrap()
    );
    let executor_id = data_model::ExecutorId::new("executor-intent-replay".to_string());

    let service1 = Service::new(ServerConfig {
        state_store_path: state_store_path.clone(),
        rocksdb_config: RocksDBConfig::default(),
        blob_storage: BlobStorageConfig {
            path: blob_store_path.clone(),
            region: None,
        },
        ..Default::default()
    })
    .await?;
    // Let startup cleanup install its timer before advancing virtual time.
    tokio::task::yield_now().await;

    // Build a scheduler update that creates one allocation targeting this
    // executor and persist matching command intents in the same transaction.
    let allocation = data_model::AllocationBuilder::default()
        .target(data_model::AllocationTarget::new(
            executor_id.clone(),
            data_model::ContainerId::new("intent-replay-container".to_string()),
        ))
        .function_call_id(data_model::FunctionCallId::from(
            "intent-replay-fc".to_string(),
        ))
        .namespace("ns".to_string())
        .application("app".to_string())
        .application_version("v1".to_string())
        .function("fn".to_string())
        .request_id("intent-replay-req".to_string())
        .outcome(data_model::FunctionRunOutcome::Unknown)
        .input_args(vec![])
        .call_metadata(bytes::Bytes::new())
        .build()?;
    let mut update = SchedulerUpdateRequest::default();
    update.new_allocations.push(allocation.clone());

    let indexes_guard = service1.indexify_state.app_state.load();
    service1
        .executor_manager
        .rebuild_scheduler_command_intents(&mut update, &indexes_guard.indexes);
    drop(indexes_guard);
    assert!(
        !update.scheduler_command_intents.is_empty(),
        "expected scheduler command intents for new allocation"
    );
    service1
        .indexify_state
        .write_scheduler_output(StateMachineUpdateRequest {
            payload: RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(update)),
        })
        .await?;

    // Simulate crash right after persistence and before intent drain.
    drop(service1);
    tokio::time::advance(STARTUP_EXECUTOR_TIMEOUT + Duration::from_secs(1)).await;
    tokio::task::yield_now().await;

    let service2 = Service::new(ServerConfig {
        state_store_path,
        rocksdb_config: RocksDBConfig::default(),
        blob_storage: BlobStorageConfig {
            path: blob_store_path,
            region: None,
        },
        ..Default::default()
    })
    .await?;

    service2
        .indexify_state
        .register_executor_connection(&executor_id)
        .await;
    service2
        .executor_manager
        .drain_and_emit_scheduler_command_intents()
        .await;

    let commands = {
        let connections = service2.indexify_state.executor_connections.read().await;
        let conn = connections
            .get(&executor_id)
            .expect("executor connection should exist");
        conn.clone_commands().await
    };
    assert!(
        commands.iter().any(|command| matches!(
            command.command,
            Some(executor_api_pb::command::Command::RunAllocation(_))
        )),
        "expected replayed RunAllocation command after restart"
    );

    let none_left = service2
        .indexify_state
        .pending_scheduler_command_intents_count()
        .await?;
    assert!(
        none_left == 0,
        "all persisted scheduler intents should be drained"
    );

    Ok(())
}
