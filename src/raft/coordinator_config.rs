use openraft::Raft;
use crate::server_config::CoordinatorConfig;

use super::{memstore::{MemNodeId, Config as CoordinatorRaftConfig, MemStore}, network::IndexifyRaftNetwork};
use std::sync::Arc;

pub type CoordinatorRaft = Raft<CoordinatorRaftConfig, IndexifyRaftNetwork, Arc<MemStore>>;

#[derive(Clone)]
pub struct CoordinatorRaftApp {
	pub id: MemNodeId,
	pub addr: String,
	pub raft: CoordinatorRaft,
	pub store: Arc<MemStore>,
	pub config: Arc<openraft::Config>,
}

impl CoordinatorRaftApp {
	pub async fn new(id: MemNodeId, addr: String, raft_config: openraft::Config, coordinator_config: CoordinatorConfig) -> anyhow::Result<Self> {
		let config = Arc::new(raft_config.clone());
		raft_config.validate()?;

		let store = Arc::new(MemStore::new(Arc::new(coordinator_config)).await?);

		let network = IndexifyRaftNetwork {};

		let raft = Raft::new(id, config.clone(), network, store.clone()).await?;

		Ok(Self {
			id,
			addr,
			raft,
			store,
			config,
		})
	}
}