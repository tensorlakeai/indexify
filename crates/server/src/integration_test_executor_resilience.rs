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
    let intents = service1
        .executor_manager
        .build_scheduler_command_intents(&update, &indexes_guard.indexes);
    drop(indexes_guard);
    assert!(
        !intents.is_empty(),
        "expected scheduler command intents for new allocation"
    );

    service1
        .indexify_state
        .write_scheduler_output_with_intents(
            StateMachineUpdateRequest {
                payload: RequestPayload::SchedulerUpdate(SchedulerUpdatePayload::new(update)),
            },
            &intents,
        )
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
        .take_scheduler_command_intents(1)
        .await?;
    assert!(
        none_left.is_empty(),
        "all persisted scheduler intents should be drained"
    );

    Ok(())
}
