use std::{net::SocketAddr, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use tokio::{
    net::TcpStream,
    process::{Child, Command},
    sync::Mutex,
    time::{timeout, Duration},
};
use tracing::{error, info, warn};

use super::function_executor_server::function_executor_pb;
use crate::{
    agent::function_executor_server::FunctionExecutorServer,
    executor_api::executor_api_pb,
    blob_store::BlobStorage,
};

#[async_trait]
pub trait FunctionExecutor: Send + Sync {
    fn id(&self) -> &str;
    fn state(&self) -> crate::executor_api::executor_api_pb::FunctionExecutorState;
    fn server(&self) -> &FunctionExecutorServer;
    fn set_desired_status(&mut self, status: executor_api_pb::FunctionExecutorStatus);
    fn get_current_status(&self) -> executor_api_pb::FunctionExecutorStatus;
    async fn stop(&self) -> anyhow::Result<()>;
    async fn is_healthy(&self) -> anyhow::Result<bool>;
}

pub struct SubProcessFunctionExecutor {
    id: String,
    address: String,
    port: u16,
    child: Mutex<Child>,
    server: FunctionExecutorServer,
    description: executor_api_pb::FunctionExecutorDescription,
    status: executor_api_pb::FunctionExecutorStatus,
}

#[async_trait]
impl FunctionExecutor for SubProcessFunctionExecutor {
    fn set_desired_status(&mut self, status: executor_api_pb::FunctionExecutorStatus) {
        self.status = status;
    }

    fn get_current_status(&self) -> executor_api_pb::FunctionExecutorStatus {
        self.status
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn state(&self) -> crate::executor_api::executor_api_pb::FunctionExecutorState {
        crate::executor_api::executor_api_pb::FunctionExecutorState {
            description: Some(self.description.clone()),
            status: Some(self.status),
            termination_reason: None,
            allocation_ids_caused_termination: vec![],
        }
    }

    fn server(&self) -> &FunctionExecutorServer { &self.server }

    async fn stop(&self) -> anyhow::Result<()> {
        let mut child = self.child.lock().await;
        if let Err(e) = child.start_kill() {
            warn!(fe_id=%self.id, error=?e, "failed to send kill to FE child");
        }
        let _ = child.wait().await;
        Ok(())
    }

    async fn is_healthy(&self) -> anyhow::Result<bool> {
        // Prefer gRPC health check through server, otherwise TCP connect fallback
        if let Ok(resp) = timeout(Duration::from_secs(1), self.server.check_health()).await {
            if let Ok(r) = resp {
                return Ok(r.healthy.unwrap_or(false));
            }
        }
        let addr: SocketAddr = self.address.parse()?;
        let res = timeout(Duration::from_secs(1), TcpStream::connect(addr)).await;
        Ok(matches!(res, Ok(Ok(_))))
    }
}

#[async_trait]
pub trait FunctionExecutorFactory: Send + Sync {
    async fn create(
        &self,
        executor_id: String,
        verbose_logs: bool,
        description: executor_api_pb::FunctionExecutorDescription,
    ) -> Result<Arc<dyn FunctionExecutor>>;
}

pub struct SubprocessFactory;

#[async_trait]
impl FunctionExecutorFactory for SubprocessFactory {
    async fn create(
        &self,
        executor_id: String,
        verbose_logs: bool,
        description: executor_api_pb::FunctionExecutorDescription,
    ) -> Result<Arc<dyn FunctionExecutor>> {
        let port = find_free_local_port()?;
        let address = format!("127.0.0.1:{port}");
        // FE id must be present in description
        let fe_id = description
            .id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("FunctionExecutorDescription.id is required"))?;
        let mut args = vec![
            format!("--executor-id={}", executor_id),
            format!("--function-executor-id={}", fe_id),
            "--address".to_string(),
            address.clone(),
        ];
        if verbose_logs {
            args.push("--dev".to_string());
        }
        info!(fe_id=%fe_id, port=%port, "starting function-executor subprocess");
        let mut cmd = Command::new("function-executor");
        cmd.args(args);
        let mut child = cmd.spawn()?;
        // Connect gRPC channel
        let server = FunctionExecutorServer::connect(address.clone()).await?;
        // get_info
        let _ = server.get_info().await?;
        // Build InitializeRequest from description + downloaded application code
        let init_req = build_initialize_request_with_code(&description).await?;
        // initialize with timeout
        let init_timeout_ms: u64 = description
            .initialization_timeout_ms
            .map(|v| v as u64)
            .unwrap_or(60_000);
        let init_res = server
            .initialize(init_req, Duration::from_millis(init_timeout_ms))
            .await;
        match init_res {
            Ok(resp) => {
                if resp.outcome_code.unwrap_or(0) != 1 {
                    error!(fe_id=%fe_id, "FE initialization failed");
                    let _ = child.kill().await;
                    return Err(anyhow::anyhow!("FE initialization failed"));
                }
            }
            Err(_) => {
                error!(fe_id=%fe_id, "FE initialization timed out");
                let _ = child.kill().await;
                return Err(anyhow::anyhow!("FE initialization timed out"));
            }
        }
        // Success state with ID and Running status
        Ok(std::sync::Arc::new(SubProcessFunctionExecutor {
            id: fe_id,
            address,
            port,
            child: Mutex::new(child),
            server,
            description,
            status: executor_api_pb::FunctionExecutorStatus::Running as i32,
        }))
    }
}

fn build_initialize_request(
    desc: &executor_api_pb::FunctionExecutorDescription,
) -> function_executor_pb::InitializeRequest {
    let function_ref = desc
        .function
        .as_ref()
        .map(|f| function_executor_pb::FunctionRef {
            namespace: f.namespace.clone(),
            application_name: f.application_name.clone(),
            function_name: f.function_name.clone(),
            application_version: f.application_version.clone(),
        });
    function_executor_pb::InitializeRequest { function: function_ref, application_code: None }
}

async fn build_initialize_request_with_code(
    desc: &executor_api_pb::FunctionExecutorDescription,
) -> Result<function_executor_pb::InitializeRequest> {
    let mut req = build_initialize_request(desc);
    let application = desc
        .application
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("missing application payload in description"))?;
    // Download bytes
    let data = BlobStorage::read_uri(&application.uri.clone().unwrap_or_default()).await?;
    // Build manifest mapping
    let encoding = application.encoding.unwrap_or(0);
    let fe_encoding = match encoding {
        x if x == executor_api_pb::DataPayloadEncoding::Utf8Json as i32 => function_executor_pb::SerializedObjectEncoding::Utf8Json as i32,
        x if x == executor_api_pb::DataPayloadEncoding::Utf8Text as i32 => function_executor_pb::SerializedObjectEncoding::Utf8Text as i32,
        x if x == executor_api_pb::DataPayloadEncoding::BinaryPickle as i32 => function_executor_pb::SerializedObjectEncoding::BinaryPickle as i32,
        x if x == executor_api_pb::DataPayloadEncoding::BinaryZip as i32 => function_executor_pb::SerializedObjectEncoding::BinaryZip as i32,
        x if x == executor_api_pb::DataPayloadEncoding::Raw as i32 => function_executor_pb::SerializedObjectEncoding::Raw as i32,
        _ => function_executor_pb::SerializedObjectEncoding::Unknown as i32,
    };
    let manifest = function_executor_pb::SerializedObjectManifest {
        encoding: Some(fe_encoding),
        encoding_version: application.encoding_version.map(|v| v as u64),
        size: application.size.map(|v| v as u64),
        metadata_size: application.metadata_size.map(|v| v as u64),
        sha256_hash: application.sha256_hash.clone(),
        content_type: application.content_type.clone(),
        source_function_call_id: application.source_function_call_id.clone(),
    };
    let so = function_executor_pb::SerializedObject { manifest: Some(manifest), data: Some(data.into()) };
    req.application_code = Some(so);
    Ok(req)
}

fn find_free_local_port() -> anyhow::Result<u16> {
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
    let port = listener.local_addr()?.port();
    // Drop the listener; port should be free for immediate reuse.
    drop(listener);
    Ok(port)
}
