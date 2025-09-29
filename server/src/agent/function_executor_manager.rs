use std::{collections::{HashMap, HashSet}, sync::Arc};

use anyhow::Result;
use parking_lot::Mutex;
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{sleep, Duration},
};
use tracing::{error, info};

use super::fe_driver::{FunctionExecutor, FunctionExecutorFactory};
use crate::executor_api::executor_api_pb;

pub struct FunctionExecutorManager {
    executor_id: String,
    executors: Mutex<HashMap<String, Arc<dyn FunctionExecutor>>>,
    factory: Arc<dyn FunctionExecutorFactory>,
}

impl FunctionExecutorManager {
    pub fn new(executor_id: String, factory: Arc<dyn FunctionExecutorFactory>) -> Self {
        Self {
            executor_id,
            executors: Mutex::new(HashMap::new()),
            factory,
        }
    }

    pub async fn sync_function_executors(&self, fe_descriptions: Vec<executor_api_pb::FunctionExecutorDescription>) {
        let executor_ids = self.executors.lock().keys().cloned().collect::<HashSet<_>>();
        let server_fes = fe_descriptions.into_iter().map(|fe| fe.id).filter(|id| id.is_some()).map(|id| id.unwrap()).collect::<HashSet<_>>();
        let fes_to_remove = executor_ids.difference(&server_fes).cloned().collect::<Vec<_>>();
        let fes_to_add = server_fes.difference(&executor_ids).cloned().collect::<Vec<_>>();
        for fe_id in fes_to_remove {
            self.terminate(&fe_id, executor_api_pb::FunctionExecutorTerminationReason::FunctionCancelled).await;
        }
        for fe_id in fes_to_add {
            let fe_description = fe_descriptions.iter().find(|fe| fe.id == Some(fe_id.clone())).unwrap().clone();
            let fe = self.factory.create(self.executor_id.clone(), false, fe_description).await;
            self.executors.lock().insert(fe_id, fe.unwrap());
        }
    }

    // FE lifecycle (start/stop) will be wired to create/remove executors via the
    // factory.

    pub async fn terminate(
        &self,
        _fe_id: &str,
        _reason: executor_api_pb::FunctionExecutorTerminationReason,
    ) -> Result<()> {
        info!(fe_id=%_fe_id, reason=%_reason.as_str_name(), "terminating function executor");
        let Some(exec) = self.executors.lock().get(_fe_id).cloned() else {
            error!(fe_id=%_fe_id, "function executor not found");
            return Ok(());
        };
        if let Err(e) = exec.stop().await {
            error!(error=?e, "failed to stop function executor");
            return Err(e);
        }
        self.executors.lock().remove(_fe_id);
        Ok(())
    }

    pub fn states(&self) -> Vec<executor_api_pb::FunctionExecutorState> {
        self.executors.lock().values().map(|e| e.state()).collect()
    }

    pub fn spawn_health_loop(
        manager: Arc<tokio::sync::RwLock<FunctionExecutorManager>>,
        mut shutdown_rx: watch::Receiver<()>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            const PERIOD: Duration = Duration::from_secs(5);
            loop {
                if shutdown_rx.has_changed().unwrap_or(false) {
                    break;
                }

                // Snapshot executor IDs and instances; process each outside the lock
                let fe_list: Vec<Arc<dyn FunctionExecutor>> = manager
                    .read()
                    .await
                    .executors
                    .lock()
                    .values()
                    .cloned()
                    .collect();
                for exec in fe_list {
                    let status = exec.state().status.unwrap_or_default();
                    let is_live = status == executor_api_pb::FunctionExecutorStatus::Running as i32 ||
                        status == executor_api_pb::FunctionExecutorStatus::Pending as i32;
                    if !is_live {
                        continue;
                    }
                    let unhealthy = !exec.is_healthy().await.unwrap_or(false);
                    if unhealthy {
                        let mgr = manager.write().await;
                        if let Err(e) = mgr
                            .terminate(
                                &exec.id(),
                                executor_api_pb::FunctionExecutorTerminationReason::Unhealthy,
                            )
                            .await
                        {
                            error!(error=?e, "failed to terminate function executor");
                        }
                    }
                }

                tokio::select! {
                    _ = sleep(PERIOD) => {},
                    _ = shutdown_rx.changed() => { break; }
                }
            }
        })
    }
}
