use axum::{Extension, Router};
use indexify::raft::{raft_api, management, memstore::{MemNodeId, MemStore}, raft_network::IndexifyRaftNetwork, coordinator_config::CoordinatorRaftApp};
use openraft::{Raft, Config};
use std::sync::Arc;

async fn start_raft_node(node_id: MemNodeId, http_addr: String) -> anyhow::Result<()> {
	let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        ..Default::default()
	};

	let config = Arc::new(config.validate()?);

	let store = Arc::new(MemStore::default());

	let network = IndexifyRaftNetwork {};

	let raft = Raft::new(node_id, config.clone(), network, store.clone()).await?;

	let app_data = CoordinatorRaftApp {
		id: node_id,
		addr: http_addr.clone(),
		raft,
		store,
		config,
	};

	let router = Router::new()
	// raft internal RPC
		.route("/raft/append", axum::routing::post(raft_api::append))
		.route("/raft/snapshot", axum::routing::post(raft_api::snapshot))
		.route("/raft/vote", axum::routing::post(raft_api::vote))
		// admin API
		.route("/management/init", axum::routing::post(management::init))
		.route("/management/add_learner", axum::routing::post(management::add_learner))
		.route("/management/change_membership", axum::routing::post(management::change_membership))
		.route("/management/metrics", axum::routing::get(management::metrics))
		// application API
		// TODO: ADD THE COORDINATOR API HANDLERS HERE
		.layer(Extension(app_data));

	// run the server
	let addr = http_addr.parse().unwrap();
	tracing::info!("Listening on {}", addr);
	let server = axum::Server::bind(&addr).serve(router.into_make_service());
	let _ = server.await;
	Ok(())
}

use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    #[clap(long)]
    pub count: u64,

    #[clap(long)]
    pub http_addr: String,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Setup the logger
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_ids(true)
        .with_level(true)
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Parse the parameters passed by arguments.
    let options = Opt::parse();

	// create multiple nodes in different tokio threads
	let mut handles = vec![];
	for i in 0..options.count {
		// create a new ID for each node
		let node_id = i;
		// run on multiple ports on localhost
		let http_addr = format!("localhost:{}", 8080 + i);
		let handle = tokio::spawn(start_raft_node(node_id, http_addr));
		handles.push(handle);
	}

	// wait for all nodes to finish
	for handle in handles {
		handle.await?;
	}

	Ok(())
}
