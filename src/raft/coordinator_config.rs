use openraft::Raft;
use super::{memstore::{MemNodeId, Config as CoordinatorRaftConfig, MemStore}, raft_network::IndexifyRaftNetwork};
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