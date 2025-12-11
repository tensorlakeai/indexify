use std::{sync::Arc, time::Duration};

use anyhow::Result;
use futures::future::BoxFuture;
use tokio::{sync::Mutex, task::JoinHandle};
use tonic::transport::Channel;
use tracing::error;

use crate::executor::function_executor::function_executor_service::{
    function_executor_client::FunctionExecutorClient, HealthCheckRequest,
};

const HEALTH_CHECK_POLL_PERIOD_SEC: u32 = 5;

type HealthCheckFailedCallback =
    Box<dyn Fn(HealthCheckResult) -> BoxFuture<'static, ()> + Send + Sync>;

pub struct HealthCheckResult {
    is_healthy: bool,
    pub reason: String,
}

impl HealthCheckResult {
    pub fn new(is_healthy: bool, reason: String) -> Self {
        Self { is_healthy, reason }
    }

    pub fn healthy(&self) -> bool {
        self.is_healthy
    }
}

pub struct HealthChecker {
    channel: Channel,
    task: Arc<Mutex<Option<JoinHandle<()>>>>,
    callback: Arc<Mutex<Option<HealthCheckFailedCallback>>>,
}

impl HealthChecker {
    pub fn new(channel: Channel) -> Self {
        Self {
            channel,
            task: Arc::new(Mutex::new(None)),
            callback: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn check(&self) -> Result<HealthCheckResult> {
        if std::env::var("INDEXIFY_DISABLE_FUNCTION_EXECUTOR_HEALTH_CHECKS").unwrap_or_default()
            == "1"
        {
            Ok(HealthCheckResult::new(true, "Function Executor health checks are disabled using INDEXIFY_DISABLE_FUNCTION_EXECUTOR_HEALTH_CHECKS env var.".to_string()))
        } else {
            let mut client = FunctionExecutorClient::new(self.channel.clone());
            let response = client
                .check_health(HealthCheckRequest {})
                .await?
                .into_inner();
            // TODO: set labels
            Ok(HealthCheckResult::new(
                response.healthy(),
                response.status_message().to_string(),
            ))
        }
    }

    pub async fn start(self: Arc<Self>, callback: HealthCheckFailedCallback) -> Result<()> {
        let task = self.task.lock().await;
        if task.is_some() {
            return Ok(());
        }
        let mut cb = self.callback.lock().await;
        *cb = Some(callback);
        let _self = Arc::clone(&self);
        let handle = tokio::spawn(async move {
            loop {
                match _self.check().await {
                    Ok(result) => {
                        let cb_opt = {
                            let mut cb_guard = _self.callback.lock().await;
                            cb_guard.take()
                        };

                        if let Some(cb) = cb_opt {
                            tokio::spawn(cb(result));
                        }
                        break;
                    }
                    Err(err) => {
                        error!("Health check RPC failed, ignoring error: {err:?}")
                    }
                }
                tokio::time::sleep(Duration::from_secs(HEALTH_CHECK_POLL_PERIOD_SEC as u64)).await;
            }
        });
        let mut task = self.task.lock().await;
        *task = Some(handle);
        Ok(())
    }

    pub async fn stop(self: Arc<Self>) {
        let mut task = self.task.lock().await;
        if let Some(handle) = task.take() {
            handle.abort();
        }
        self.callback.lock().await.take();
    }
}
