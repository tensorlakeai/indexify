use anyhow::Result;
use tonic::transport::Channel;

// Include the generated proto code
pub mod executor_api_pb {
    tonic::include_proto!("executor_api_pb");
}

use executor_api_pb::{
    executor_api_client::ExecutorApiClient, DesiredExecutorState, GetDesiredExecutorStatesRequest,
    ReportExecutorStateRequest, ReportExecutorStateResponse,
};

/// gRPC client for the ExecutorAPI service
pub struct ExecutorClient {
    client: ExecutorApiClient<Channel>,
}

impl ExecutorClient {
    /// Create a new ExecutorClient by connecting to the server
    pub async fn connect(server_addr: String) -> Result<Self> {
        let client = ExecutorApiClient::connect(server_addr).await?;
        Ok(Self { client })
    }

    /// Report executor state to the server
    pub async fn report_executor_state(
        &mut self,
        request: ReportExecutorStateRequest,
    ) -> Result<ReportExecutorStateResponse> {
        let response = self.client.report_executor_state(request).await?;
        Ok(response.into_inner())
    }

    /// Get desired executor states stream
    pub async fn get_desired_executor_states(
        &mut self,
        executor_id: String,
    ) -> Result<tonic::Streaming<DesiredExecutorState>> {
        let request = GetDesiredExecutorStatesRequest {
            executor_id: Some(executor_id),
        };
        let response = self.client.get_desired_executor_states(request).await?;
        Ok(response.into_inner())
    }
}
