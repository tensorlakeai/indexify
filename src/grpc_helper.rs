//! Helper functions for handling grpc.

use indexify_proto::indexify_raft::{RaftReply, RaftRequest};

use crate::state::typ::RaftError;

pub struct GrpcHelper;

impl GrpcHelper {
    pub fn into_req<T>(t: T) -> tonic::Request<T> {
        tonic::Request::new(t)
    }

    pub fn encode_raft_request<T>(v: &T) -> Result<RaftRequest, serde_json::Error>
    where
        T: serde::Serialize + 'static,
    {
        let data = serde_json::to_string(&v)?;
        Ok(RaftRequest { data })
    }

    pub fn parse_raft_reply<T, E>(
        reply: tonic::Response<RaftReply>,
    ) -> Result<Result<T, RaftError<E>>, serde_json::Error>
    where
        T: serde::de::DeserializeOwned,
        E: serde::Serialize + serde::de::DeserializeOwned,
    {
        let raft_reply = reply.into_inner();

        if !raft_reply.error.is_empty() {
            let e: RaftError<E> = serde_json::from_str(&raft_reply.error)?;
            Ok(Err(e))
        } else {
            let d: T = serde_json::from_str(&raft_reply.data)?;
            Ok(Ok(d))
        }
    }

    /// Parse tonic::Request and decode it into required type.
    pub fn parse_req<T>(request: tonic::Request<RaftRequest>) -> Result<T, tonic::Status>
    where
        T: serde::de::DeserializeOwned,
    {
        let raft_req = request.into_inner();
        Self::parse(&raft_req.data)
    }

    /// Create an Ok response for raft API.
    pub fn ok_response<D>(d: D) -> Result<tonic::Response<RaftReply>, tonic::Status>
    where
        D: serde::Serialize,
    {
        let data = serde_json::to_string(&d).expect("fail to serialize resp");
        let reply = RaftReply {
            data,
            error: "".to_string(),
        };
        Ok(tonic::Response::new(reply))
    }

    pub fn err_response<E>(e: E) -> Result<tonic::Response<RaftReply>, tonic::Status>
    where
        E: serde::Serialize,
    {
        let error = serde_json::to_string(&e).map_err(|e| {
            tonic::Status::invalid_argument(format!("fail to serialize resp: {}", e))
        })?;
        let reply = RaftReply {
            data: "".to_string(),
            error,
        };
        Ok(tonic::Response::new(reply))
    }

    pub fn result_response<T, E>(
        d: Result<T, E>,
    ) -> Result<tonic::Response<RaftReply>, tonic::Status>
    where
        T: serde::Serialize,
        E: serde::Serialize,
    {
        match d {
            Ok(data) => Self::ok_response(data),
            Err(e) => Self::err_response(e),
        }
    }

    /// Parse string and decode it into required type.
    pub fn parse<T>(s: &str) -> Result<T, tonic::Status>
    where
        T: serde::de::DeserializeOwned,
    {
        let req: T = serde_json::from_str(s).map_err(Self::invalid_arg)?;
        Ok(req)
    }

    /// Create a tonic::Status with invalid argument error.
    pub fn invalid_arg(e: impl ToString) -> tonic::Status {
        tonic::Status::invalid_argument(e.to_string())
    }

    /// Create a tonic::Status with internal error.
    pub fn internal_err(e: impl Into<String>) -> tonic::Status {
        tonic::Status::internal(e.into())
    }
}
