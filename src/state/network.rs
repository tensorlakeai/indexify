use std::{error::Error, fmt::Display, path::PathBuf, sync::Arc};

use anyerror::AnyError;
use futures::{Future, FutureExt};
use indexify_proto::indexify_raft::{InstallSnapshotRequest, SnapshotFileChunkRequest};
use openraft::{
    error::{Fatal, NetworkError, RemoteError, ReplicationClosed, StreamingError, Unreachable},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest,
        AppendEntriesResponse,
        SnapshotResponse,
        VoteRequest,
        VoteResponse,
    },
    BasicNode,
    ErrorVerb,
    OptionalSend,
    StorageError,
    Vote,
};
use tokio::{fs, fs::File, io::AsyncReadExt};
use tonic::IntoRequest;
use tracing::{info, warn};

use super::store::requests::StateMachineUpdateResponse;
use crate::{
    grpc_helper::GrpcHelper,
    metrics::raft_metrics::{self},
    state::{
        openraft::Snapshot,
        raft_client::RaftClient,
        store::requests::{RequestPayload, StateMachineUpdateRequest},
        typ::{RPCError, RaftError},
        NodeId,
        TypeConfig,
    },
};
pub struct Network {
    raft_client: Arc<RaftClient>,
}

impl Default for Network {
    fn default() -> Self {
        let raft_client = Arc::new(RaftClient::new());
        Self::new(raft_client)
    }
}

impl Clone for Network {
    fn clone(&self) -> Self {
        Network {
            raft_client: Arc::clone(&self.raft_client),
        }
    }
}

impl Network {
    pub fn new(raft_client: Arc<RaftClient>) -> Self {
        Self { raft_client }
    }

    /// This method is used when a state machine request was received by a
    /// non-leader node to forward it to a leader node
    pub async fn forward(
        &self,
        target_addr: &str,
        request: StateMachineUpdateRequest,
    ) -> Result<StateMachineUpdateResponse, anyhow::Error> {
        let mut client = self
            .raft_client
            .get(target_addr)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get raft client: {}", e))?;

        let tonic_request = GrpcHelper::encode_raft_request(&request)?.into_request();

        let bytes_sent = tonic_request.get_ref().data.len() as u64;
        raft_metrics::network::incr_sent_bytes(target_addr, bytes_sent);

        let response = client
            .forward(tonic_request)
            .await
            .map_err(|e| GrpcHelper::internal_err(e.to_string()))?;

        let result: Result<StateMachineUpdateResponse, _> =
            serde_json::from_str(&response.into_inner().data);
        let reply = result.map_err(|e| {
            raft_metrics::network::incr_sent_failures(target_addr);
            anyhow::anyhow!(format!(
                "Failed to parse the response received from forwarding a state machine request: {}",
                e.to_string()
            ))
        })?;

        Ok(reply)
    }

    /// This method is used to allow a node to try to join the main cluster
    /// after it comes up. The node makes this request periodically
    pub async fn join_cluster(
        &self,
        node_id: NodeId,
        node_addr: &str,
        coordinator_addr: &str,
        target_addr: &str,
    ) -> Result<StateMachineUpdateResponse, anyhow::Error> {
        let mut client = self
            .raft_client
            .get(target_addr)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get raft client: {}", e))?;

        let request = GrpcHelper::encode_raft_request(&StateMachineUpdateRequest {
            payload: RequestPayload::JoinCluster {
                node_id,
                address: node_addr.into(),
                coordinator_addr: coordinator_addr.into(),
            },
            new_state_changes: vec![],
            state_changes_processed: vec![],
        })?
        .into_request();

        let bytes_sent = request.get_ref().data.len() as u64;
        raft_metrics::network::incr_sent_bytes(target_addr, bytes_sent);

        let response = client
            .join_cluster(request)
            .await
            .map_err(|e| GrpcHelper::internal_err(e.to_string()))?;

        let reply = serde_json::from_str::<StateMachineUpdateResponse>(&response.into_inner().data)
            .map_err(|e| {
                raft_metrics::network::incr_sent_failures(target_addr);
                anyhow::anyhow!(
                    "Failed to parse the response received from sending a join_cluster request: {}",
                    e.to_string()
                )
            })?;

        Ok(reply)
    }
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            target,
            target_node: node.clone(),
            raft_client: self.raft_client.clone(),
        }
    }
}

pub struct NetworkConnection {
    target: NodeId,
    target_node: BasicNode,
    raft_client: Arc<RaftClient>,
}

fn anyhow_to_unreachable(e: anyhow::Error) -> Unreachable {
    Unreachable::new(&Into::<AnyError>::into(e))
}

async fn snapshot_files(path: &PathBuf) -> Result<Vec<fs::DirEntry>, std::io::Error> {
    let mut dir = fs::read_dir(path).await?;
    let mut entries = Vec::new();
    while let Some(entry) = dir.next_entry().await? {
        entries.push(entry);
    }
    Ok(entries)
}

impl NetworkConnection {
    fn status_to_unreachable<E>(&self, status: tonic::Status) -> RPCError<RaftError<E>>
    where
        E: Error,
    {
        RPCError::Unreachable(Unreachable::new(&status))
    }

    /// Wrap a RaftError with RPCError
    pub(crate) fn to_rpc_err<E: Error>(&self, e: RaftError<E>) -> RPCError<RaftError<E>> {
        let remote_err = RemoteError::new_with_node(self.target, self.target_node.clone(), e);
        RPCError::RemoteError(remote_err)
    }
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<RaftError>> {
        let mut client = self
            .raft_client
            .clone()
            .get(&self.target_node.addr)
            .await
            .map_err(|e| self.status_to_unreachable(tonic::Status::aborted(e.to_string())))?;

        let raft_req = GrpcHelper::encode_raft_request(&req).map_err(|e| Unreachable::new(&e))?;
        let req = GrpcHelper::into_req(raft_req);

        let bytes_sent = req.get_ref().data.len() as u64;
        raft_metrics::network::incr_sent_bytes(&self.target_node.addr, bytes_sent);

        let grpc_res = client.append_entries(req).await;

        let resp = grpc_res.map_err(|e| {
            raft_metrics::network::incr_sent_failures(&self.target_node.addr);
            self.status_to_unreachable(e)
        })?;

        let raft_res = GrpcHelper::parse_raft_reply(resp)
            .map_err(|serde_err| new_net_err(&serde_err, || "parse append_entries reply"))?;

        raft_res.map_err(|e| self.to_rpc_err(e))
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<NodeId>,
        snapshot: Snapshot<TypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<NodeId>, StreamingError<TypeConfig, Fatal<NodeId>>> {
        info!(
            "full snapshot path {:?} target {:?}",
            snapshot.snapshot.snapshot_dir, self.target_node.addr,
        );
        let mut client = self
            .raft_client
            .clone()
            .get(&self.target_node.addr)
            .await
            .map_err(|e| StreamingError::Unreachable(anyhow_to_unreachable(e)))?;

        let entries = snapshot_files(&snapshot.snapshot.snapshot_dir)
            .await
            .map_err(|e| {
                StorageError::from_io_error(
                    openraft::ErrorSubject::Snapshot(None),
                    ErrorVerb::Read,
                    e,
                )
            })?;

        let mut c = std::pin::pin!(_cancel);
        for entry in entries {
            let mut file = File::open(entry.path()).await.map_err(|e| {
                StorageError::from_io_error(
                    openraft::ErrorSubject::Snapshot(None),
                    ErrorVerb::Read,
                    e,
                )
            })?;
            let mut offset = 0;
            info!("send snapshot file {:?}", entry.file_name());
            loop {
                // Check for cancellation.
                if let Some(err) = c.as_mut().now_or_never() {
                    return Err(err.into());
                }

                let mut buf = Vec::with_capacity(1024 * 1024);
                let n = file.read_buf(&mut buf).await.map_err(|e| {
                    StorageError::from_io_error(
                        openraft::ErrorSubject::Snapshot(None),
                        ErrorVerb::Read,
                        e,
                    )
                })?;
                if n == 0 {
                    break;
                }
                let req = SnapshotFileChunkRequest {
                    snapshot_id: snapshot.meta.snapshot_id.clone(),

                    vote: serde_json::to_string(&vote)
                        .map_err(|e| new_net_err(&e, || "serialize error"))?,
                    name: entry.file_name().into_string().map_err(|e| {
                        let err = std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to convert OsString to String: {:?}", e),
                        );
                        new_net_err(&err, || "serialize error")
                    })?,
                    data: buf,
                    offset,
                };

                raft_metrics::network::incr_sent_bytes(&self.target_node.addr, n as u64);

                let ret = client
                    .transfer_snapshot(req.into_request())
                    .await
                    .map_err(|e| Unreachable::new(&e))?
                    .into_inner();
                if ret.error != "" {
                    let err: Fatal<NodeId> = serde_json::from_str(&ret.error)
                        .map_err(|e| new_net_err(&e, || "deserialize snapshot error"))?;
                    return Err(RemoteError::new(self.target, err).into());
                } else {
                    let reply_vote: Vote<u64> = serde_json::from_str(&ret.data)
                        .map_err(|e| new_net_err(&e, || "deserialize vote"))?;
                    if reply_vote > vote {
                        return Err(ReplicationClosed::new(format!(
                            "vote changed: my {:?} receiver {:?}",
                            vote, reply_vote
                        ))
                        .into());
                    }
                }

                offset += n as u64;
            }
        }

        let vote =
            serde_json::to_string(&vote).map_err(|e| new_net_err(&e, || "serialize vote"))?;
        let snapshot_meta = serde_json::to_string(&snapshot.meta)
            .map_err(|e| new_net_err(&e, || "serialize snapshot"))?;
        let req = InstallSnapshotRequest {
            vote,
            snapshot_meta,
        };

        let grpc_res = client.install_snapshot(req.into_request()).await;

        let resp = grpc_res
            .map_err(|e| {
                raft_metrics::network::incr_sent_failures(&self.target_node.addr);
                Unreachable::new(&e)
            })?
            .into_inner();
        if resp.error != "" {
            let err: Fatal<NodeId> = serde_json::from_str(&resp.error)
                .map_err(|e| new_net_err(&e, || "deserialize install snapshot error"))?;
            warn!(
                "snapshot target {:?} received error {:?}",
                self.target_node.addr, err
            );
            return Err(RemoteError::new(self.target, err).into());
        } else {
            let reply: SnapshotResponse<u64> = serde_json::from_str(&resp.data)
                .map_err(|e| new_net_err(&e, || "deserialize vote"))?;
            info!(
                "snapshot target {:?} received response {:?}",
                self.target_node.addr, reply.vote
            );
            Ok(reply)
        }
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<RaftError>> {
        let mut client = self
            .raft_client
            .get(&self.target_node.addr)
            .await
            .map_err(|e| self.status_to_unreachable(tonic::Status::aborted(e.to_string())))?;

        let raft_req = GrpcHelper::encode_raft_request(&req).map_err(|e| Unreachable::new(&e))?;

        let req = GrpcHelper::into_req(raft_req);

        let bytes_sent = req.get_ref().data.len() as u64;
        raft_metrics::network::incr_sent_bytes(&self.target_node.addr, bytes_sent);

        let grpc_res = client.vote(req).await;

        let resp = grpc_res.map_err(|e| {
            raft_metrics::network::incr_sent_failures(&self.target_node.addr);
            self.status_to_unreachable(e)
        })?;

        let raft_res = GrpcHelper::parse_raft_reply(resp)
            .map_err(|serde_err| new_net_err(&serde_err, || "parse vote reply"))?;

        raft_res.map_err(|e| self.to_rpc_err(e))
    }
}

fn new_net_err<D: Display>(e: &(impl Error + 'static), msg: impl FnOnce() -> D) -> NetworkError {
    NetworkError::new(&AnyError::new(e).add_context(msg))
}
