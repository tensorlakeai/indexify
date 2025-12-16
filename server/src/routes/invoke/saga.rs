// This is a saga to manage invoking an application
// It tracks the compensation steps needed to rollback in case of failure of a
// transaction. It is not designed to be general for all sagas, but specifically
// for invoking an application.

use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use derive_builder::Builder;
use tokio::{runtime::Handle, sync::RwLock, task::block_in_place};
use tracing::{error, info};

use crate::{
    blob_store::{BlobStorage, PutResult},
    data_model::{Application, RequestCtx},
    state_store::{
        EnsureIdempotencyError,
        IndexifyState,
        requests::{InvokeApplicationRequest, RequestPayload, StateMachineUpdateRequest},
    },
};

#[derive(Clone)]
enum CompensationAction {
    Idempotency {
        namespace: String,
        application_name: String,
    },
    StoreBody,
    WriteFnCall,
}

#[derive(Builder)]
pub struct InvokeApplicationSaga {
    #[builder(default)]
    compensation_actions: Arc<RwLock<Vec<CompensationAction>>>,
    blob_storage: Arc<BlobStorage>,
    state: Arc<IndexifyState>,
    request_id: String,
}

impl InvokeApplicationSaga {
    async fn compensate(&self) {
        let mut actions = self.compensation_actions.write().await;
        if actions.is_empty() {
            return;
        }
        info!(
            "Running {} compensation action(s) for request {}",
            actions.len(),
            self.request_id
        );
        for action in actions.iter().rev() {
            match action {
                CompensationAction::Idempotency {
                    namespace,
                    application_name,
                } => self.rollback_idempotency(namespace, application_name).await,
                CompensationAction::StoreBody => self.rollback_store_body().await,
                CompensationAction::WriteFnCall => self.rollback_write_fn_call().await,
            }
        }
        actions.clear();
    }

    pub async fn commit(self) {
        info!("Committing saga for {}", self.request_id);
        let mut actions = self.compensation_actions.write().await;
        actions.clear();
    }

    pub async fn ensure_idempotent(
        &self,
        application: &Application,
    ) -> Result<(), EnsureIdempotencyError> {
        let namespace = application.namespace.clone();
        let application_name = application.name.clone();

        let result = self
            .ensure_idempotent_internal(&namespace, &application_name)
            .await;

        if result.is_err() {
            self.compensate().await;
        } else {
            let mut actions = self.compensation_actions.write().await;
            actions.push(CompensationAction::Idempotency {
                namespace,
                application_name,
            });
        }

        result
    }

    async fn ensure_idempotent_internal(
        &self,
        namespace: &str,
        application_name: &str,
    ) -> Result<(), EnsureIdempotencyError> {
        info!(
            "Ensuring idempotency for invoke application request {} for application {namespace}/{application_name}",
            self.request_id
        );

        self.state
            .ensure_invoke_request_idempotency(namespace, application_name, &self.request_id)
            .await
            .inspect_err(|error| match error {
                EnsureIdempotencyError::Error(error) => {
                    error!("failed to ensure idempotency: {:?}", error)
                }
                EnsureIdempotencyError::AlreadyExists => info!(
                    "duplicate invoke application request detected for request id {}",
                    self.request_id
                ),
            })
    }

    async fn rollback_idempotency(&self, namespace: &str, application_name: &str) {
        let _ = self
            .state
            .delete_invoke_request_idempotency(namespace, application_name, &self.request_id)
            .await
            .inspect_err(|error| {
                error!(
                    "failed to rollback idempotency for request {}: {:?}",
                    self.request_id, error
                )
            });
    }

    pub async fn store_body(
        &self,
        body: impl futures::Stream<Item = Result<Bytes>> + Send + Unpin,
    ) -> Result<PutResult, anyhow::Error> {
        let result = self
            .store_body_internal(body)
            .await
            .inspect_err(|err| error!("failed to write to blob store: {:?}", err));

        if result.is_err() {
            self.compensate().await;
        } else {
            let mut actions = self.compensation_actions.write().await;
            actions.push(CompensationAction::StoreBody);
        }

        result
    }

    async fn store_body_internal(
        &self,
        body: impl futures::Stream<Item = Result<Bytes>> + Send + Unpin,
    ) -> Result<PutResult, anyhow::Error> {
        self.blob_storage.put(&self.request_id, body).await
    }

    async fn rollback_store_body(&self) {
        let result = self.blob_storage.delete(&self.request_id).await;
        match result {
            Ok(_) => info!(
                "Successfully rolled back stored body for {}",
                self.request_id
            ),
            Err(error) => error!(
                ?error,
                "Failed to rollback stored body for {}", self.request_id
            ),
        }
    }

    pub async fn write_fn_call(&self, request_ctx: RequestCtx) -> Result<(), anyhow::Error> {
        let result = self.write_fn_call_internal(request_ctx).await;

        if result.is_err() {
            self.compensate().await;
        } else {
            let mut actions = self.compensation_actions.write().await;
            actions.push(CompensationAction::WriteFnCall);
        }

        result
    }

    async fn write_fn_call_internal(&self, request_ctx: RequestCtx) -> Result<(), anyhow::Error> {
        let payload = RequestPayload::InvokeApplication(InvokeApplicationRequest {
            namespace: request_ctx.namespace.clone(),
            application_name: request_ctx.application_name.clone(),
            ctx: request_ctx.clone(),
        });

        self.state
            .write(StateMachineUpdateRequest { payload })
            .await
    }

    async fn rollback_write_fn_call(&self) {
        // No specific rollback logic as this is the terminal action
    }
}

// TODO: replace with async drop when stabilized
// https://doc.rust-lang.org/std/future/trait.AsyncDrop.html
impl Drop for InvokeApplicationSaga {
    fn drop(&mut self) {
        block_in_place(|| Handle::current().block_on(self.compensate()));
    }
}
