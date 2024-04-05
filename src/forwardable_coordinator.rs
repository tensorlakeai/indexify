use indexify_proto::indexify_coordinator;

use crate::coordinator_client::CoordinatorClient;

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
        content_id: &str,
    ) -> Result<(), anyhow::Error> {
        let req = indexify_coordinator::CreateGcTasksRequest {
            content_id: content_id.to_string(),
        };

        let mut client = self.coordinator_client.get_coordinator(leader_addr).await?;

        client.create_gc_tasks(req).await?;

        Ok(())
    }
}
