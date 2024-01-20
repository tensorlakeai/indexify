use std::sync::Arc;

use anyhow::Error;

use self::coordinator_service::CoordinatorServer;
use crate::server_config::ServerConfig;

pub mod coordinator;
pub mod coordinator_service;

pub(super) async fn get_coordinator_server(
    config: Arc<ServerConfig>,
) -> Result<CoordinatorServer, Error> {
    CoordinatorServer::new(config).await
}
