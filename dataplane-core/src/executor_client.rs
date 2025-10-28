use anyhow::Result;
use tonic::transport::{Channel, ClientTlsConfig};

use crate::config::Config;

pub mod executor_api_pb {
    tonic::include_proto!("executor_api_pb");
}

use executor_api_pb::{
    DesiredExecutorState,
    GetDesiredExecutorStatesRequest,
    ReportExecutorStateRequest,
    ReportExecutorStateResponse,
    executor_api_client::ExecutorApiClient,
};

pub struct ExecutorClient {
    client: ExecutorApiClient<Channel>,
}

impl ExecutorClient {
    pub async fn connect(config: &Config) -> Result<Self> {
        let server_addr = config.server_grpc_addr.clone();

        let channel = if let Some(tls_config) = &config.tls {
            // Build TLS configuration
            let cert = std::fs::read(&tls_config.cert_path)?;
            let key = std::fs::read(&tls_config.key_path)?;
            let identity = tonic::transport::Identity::from_pem(cert, key);

            let mut tls = ClientTlsConfig::new().identity(identity);

            // Add CA bundle if provided
            if let Some(ca_bundle_path) = &tls_config.ca_bundle_path {
                let ca_cert = std::fs::read(ca_bundle_path)?;
                let ca = tonic::transport::Certificate::from_pem(ca_cert);
                tls = tls.ca_certificate(ca);
            }

            Channel::from_shared(format!("https://{}", server_addr))?
                .tls_config(tls)?
                .connect()
                .await?
        } else {
            // No TLS, connect normally
            Channel::from_shared(format!("http://{}", server_addr))?
                .connect()
                .await?
        };

        let client = ExecutorApiClient::new(channel);
        Ok(Self { client })
    }

    pub async fn report_executor_state(
        &mut self,
        request: ReportExecutorStateRequest,
    ) -> Result<ReportExecutorStateResponse> {
        let response = self.client.report_executor_state(request).await?;
        Ok(response.into_inner())
    }

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
