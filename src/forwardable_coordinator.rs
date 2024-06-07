use std::collections::HashMap;

use indexify_proto::indexify_coordinator;

use crate::{coordinator_client::CoordinatorClient, state::store::ExecutorId};

pub struct ForwardableCoordinator {
    coordinator_client: CoordinatorClient,
}

impl ForwardableCoordinator {
    pub fn new(coordinator_client: CoordinatorClient) -> Self {
        Self { coordinator_client }
    }

    pub async fn register_ingestion_server(
        &self,
        leader_addr: &str,
        ingestion_server_id: &str,
    ) -> Result<(), anyhow::Error> {
        let req = indexify_coordinator::RegisterIngestionServerRequest {
            ingestion_server_id: ingestion_server_id.to_string(),
        };

        let mut client = self.coordinator_client.get_coordinator(leader_addr).await?;

        client.register_ingestion_server(req).await?;

        Ok(())
    }

    pub async fn executors_heartbeat(
        &self,
        leader_addr: &str,
        executors: HashMap<ExecutorId, std::time::SystemTime>,
    ) -> Result<(), anyhow::Error> {
        let req = indexify_coordinator::ExecutorsHeartbeatRequest {
            executors: executors
                .into_iter()
                .map(|(executor_id, last_heartbeat)| {
                    let duration_since_epoch = last_heartbeat
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("Time went backwards");

                    let last_heartbeat = ::prost_wkt_types::Timestamp {
                        seconds: duration_since_epoch.as_secs() as i64,
                        nanos: duration_since_epoch.subsec_nanos() as i32,
                    };
                    (executor_id.to_string(), last_heartbeat)
                })
                .collect(),
        };

        let mut client = self.coordinator_client.get_coordinator(leader_addr).await?;

        client.executors_heartbeat(req).await?;

        Ok(())
    }

    pub async fn remove_ingestion_server(
        &self,
        leader_addr: &str,
        ingestion_server_id: &str,
    ) -> Result<(), anyhow::Error> {
        let req = indexify_coordinator::RemoveIngestionServerRequest {
            ingestion_server_id: ingestion_server_id.to_string(),
        };

        let mut client = self.coordinator_client.get_coordinator(leader_addr).await?;

        client.remove_ingestion_server(req).await?;

        Ok(())
    }

    pub async fn create_gc_tasks(
        &self,
        leader_addr: &str,
        state_change: indexify_internal_api::StateChange,
    ) -> Result<(), anyhow::Error> {
        let state_change: indexify_coordinator::StateChange = state_change.try_into()?;
        let req = indexify_coordinator::CreateGcTasksRequest {
            state_change: Some(state_change),
        };

        let mut client = self.coordinator_client.get_coordinator(leader_addr).await?;

        client.create_gc_tasks(req).await?;

        Ok(())
    }
}
