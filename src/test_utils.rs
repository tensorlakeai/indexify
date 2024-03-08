use std::{
    collections::BTreeMap,
    fs,
    sync::Arc,
    time::{Duration, Instant},
};

use indexify_internal_api::Index;

use crate::{
    server_config::{ServerConfig, StateStoreConfig},
    state::{
        store::requests::{Request, RequestPayload},
        App,
        NodeId,
    },
};

#[cfg(test)]
pub struct RaftTestCluster {
    nodes: BTreeMap<NodeId, Arc<App>>,
    pub seed_node_id: NodeId,
}

#[cfg(test)]
impl RaftTestCluster {
    /// Helper function to create raft configs for as many nodes as required
    fn create_test_raft_configs(
        node_count: usize,
    ) -> Result<Vec<Arc<ServerConfig>>, anyhow::Error> {
        let append = nanoid::nanoid!();
        let base_port = 18950;
        let mut configs = Vec::new();
        let seed_node = format!("localhost:{}", base_port + 1); //  use the first node as the seed node

        // Generate configurations and peer information
        for i in 0..node_count {
            let port = (base_port + i * 2) as u64;

            let config = Arc::new(ServerConfig {
                node_id: i as u64,
                coordinator_port: port,
                coordinator_addr: format!("localhost:{}", port),
                raft_port: port + 1,
                state_store: StateStoreConfig {
                    path: Some(format!("/tmp/indexify-test/raft/{}/{}", append, i)),
                },
                seed_node: seed_node.clone(),
                ..Default::default()
            });

            configs.push(config.clone());
        }

        Ok(configs)
    }

    /// This checks whether a node has been initialized by comparing the
    /// number of nodes in the cluster according to the node passed in
    /// and the number of nodes that should be present by reading from the
    /// BTreeMap of nodes
    fn is_node_initialized(&self, node: Arc<App>) -> bool {
        let num_of_nodes_in_cluster = node
            .raft
            .metrics()
            .borrow()
            .membership_config
            .nodes()
            .count();
        let expected_num_of_nodes_in_cluster = self.nodes.len();
        num_of_nodes_in_cluster == expected_num_of_nodes_in_cluster
    }

    /// Use this method to get which node is the current leader in the
    /// cluster. This will use the `get_leader()` method on the seed
    /// node to get the node id of the current leader
    async fn get_current_leader(&self) -> anyhow::Result<Arc<App>> {
        let seed_node = self
            .nodes
            .get(&self.seed_node_id)
            .expect("Expect seed node to be present");

        let current_leader_id = seed_node
            .raft
            .current_leader()
            .await
            .ok_or(anyhow::anyhow!("Error getting leader"))?;

        let current_leader = self.nodes.get(&current_leader_id).expect(
            format!(
                "Expect node {} to be present in the cluster",
                current_leader_id
            )
            .as_str(),
        );
        Ok(Arc::clone(current_leader))
    }

    /// Get a handle on a node that is not currently the leader in the
    /// cluster. In a single node cluster, this will return the handle of
    /// the leader
    async fn get_non_leader_node(&self) -> Arc<App> {
        let leader = self
            .get_current_leader()
            .await
            .expect("Error getting leader");
        if self.nodes.len() == 1 {
            return leader;
        }
        let non_leader = self
            .nodes
            .iter()
            .find(|(id, _)| **id != leader.id)
            .expect("Expect non leader to be present");
        Arc::clone(non_leader.1)
    }

    /// Send the current write to the leader of the cluster
    async fn send_write_to_leader(&self, request: Request) -> anyhow::Result<()> {
        let leader = self.get_current_leader().await?;
        leader.raft.client_write(request).await?;
        Ok(())
    }

    async fn wait_until_future<F, Fut>(
        &self,
        mut condition: F,
        timeout: Duration,
    ) -> anyhow::Result<()>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<bool>>,
    {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if condition().await? {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(100)).await;
        }
        Err(anyhow::anyhow!("Timeout waiting for condition"))
    }

    /// Create and return a new instance of the TestRaftCluster. The size of
    /// the cluster will be determined by the number of nodes passed in
    pub async fn new(num_of_nodes: usize) -> anyhow::Result<Self> {
        let server_configs = RaftTestCluster::create_test_raft_configs(num_of_nodes)?;
        let seed_node_id = server_configs.get(0).unwrap().node_id; //  the seed node will always be the first node in the list
        let mut nodes = BTreeMap::new();
        for config in server_configs {
            let _ = fs::remove_dir_all(config.state_store.clone().path.unwrap());
            let shared_state = App::new(config.clone()).await?;
            nodes.insert(config.node_id, shared_state);
        }
        Ok(Self {
            nodes,
            seed_node_id,
        })
    }

    /// Initialize the TestRaftCluster. This will always initialize the seed
    /// node as that must always be the first node initialized
    pub async fn initialize(&self, timeout: Duration) -> anyhow::Result<()> {
        let seed_node = self
            .nodes
            .get(&self.seed_node_id)
            .expect("Seed node not found");

        seed_node
            .initialize_raft()
            .await
            .map_err(|e| anyhow::anyhow!("Error initializing raft: {}", e))?;

        let start = tokio::time::Instant::now();
        loop {
            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!(format!(
                    "Timeout error: Raft cluster failed to initialize within {:#?} seconds",
                    timeout
                )));
            }

            if self.is_node_initialized(Arc::clone(seed_node)) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// This function will send a write request to the cluster and then
    /// check if the write can be read back from any node it takes a
    /// to_leader value to indicate whether this write should go to the
    /// leader or not NOTE: Currently, this is exclusive to creating
    /// and reading an index. Need to generalise it using generics
    pub async fn read_own_write(&self, _to_leader: bool) -> anyhow::Result<()> {
        let request = Request {
            payload: RequestPayload::CreateIndex {
                index: Index::default(),
                namespace: "namespace".into(),
                id: "id".into(),
            },
            new_state_changes: vec![],
            state_changes_processed: vec![],
        };
        self.send_write_to_leader(request).await?;
        tokio::time::sleep(Duration::from_secs(2)).await;

        self.wait_until_future(
            || async {
                let non_leader_node = self.get_non_leader_node().await;
                match non_leader_node.get_index("id").await {
                    Ok(read_result) if read_result == Index::default() => Ok(true),
                    Ok(_) => Ok(false),
                    Err(e) => Err(e),
                }
            },
            Duration::from_secs(2),
        )
        .await?;

        Ok(())
    }

    pub async fn assert_is_leader(&self, node_id: NodeId) -> bool {
        let node = self
            .nodes
            .get(&node_id)
            .expect(&format!("Could not find {} in node list", node_id));

        match node.raft.ensure_linearizable().await {
            Ok(_) => return true,
            Err(_) => return false,
        }
    }

    pub async fn force_current_leader_abdication(&self) -> anyhow::Result<()> {
        let current_leader = self.get_current_leader().await?;
        current_leader.raft.runtime_config().heartbeat(false);
        tokio::time::sleep(Duration::from_secs(1)).await; //  wait for long enough that election timeout occurs
        Ok(())
    }

    pub async fn promote_node_to_leader(&self, node_id: NodeId) -> anyhow::Result<()> {
        let node_to_promote = self
            .nodes
            .get(&node_id)
            .expect(&format!("Could not find {} in node list", node_id));
        node_to_promote.raft.trigger().elect().await?;
        tokio::time::sleep(Duration::from_secs(5)).await; //  TODO: Why do I need a timeout here at all?
        self.wait_until_future(
            || async {
                if let Ok(current_leader) = self.get_current_leader().await {
                    if current_leader.id == node_to_promote.id {
                        return Ok(true);
                    }
                }
                return Ok(false); //  expected leader not found, keep looping
            },
            Duration::from_secs(5),
        )
        .await?;
        Ok(())
    }
}
