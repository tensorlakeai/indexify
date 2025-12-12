// This is a saga to manage invoking an application
// It tracks the compensation steps needed to rollback in case of failure of a
// transaction. It is not designed to be general for all sagas, but specifically
// for invoking an application.

use std::{cell::RefCell, rc::Rc, sync::Arc};

use anyhow::Result;
use bytes::Bytes;
use derive_builder::Builder;
use futures::executor::block_on;
use tracing::{error, info};

use crate::blob_store::{BlobStorage, PutResult};

enum InvokeApplicationSagaCompensationStep {
    Idempotency,
    StoreBody,
    WriteInvokeApplication,
}

#[derive(Builder)]
pub struct InvokeApplicationSaga {
    #[builder(default)]
    compensation_steps: Rc<RefCell<Vec<InvokeApplicationSagaCompensationStep>>>,
    blob_storage: Arc<BlobStorage>,
    request_id: String,
}

impl InvokeApplicationSaga {
    async fn run_compensations(&self) {
        let compensation_steps = self.compensation_steps.take();
        info!(
            "Running {} compensation(s) for request {}",
            compensation_steps.len(),
            self.request_id
        );
        for step in compensation_steps.into_iter().rev() {
            match step {
                InvokeApplicationSagaCompensationStep::Idempotency => {
                    self.rollback_idempotency().await
                }
                InvokeApplicationSagaCompensationStep::StoreBody => {
                    self.rollback_store_body().await
                }
                InvokeApplicationSagaCompensationStep::WriteInvokeApplication => {
                    self.rollback_write_invoke_application().await
                }
            }
        }
    }

    pub fn commit(self) {
        info!("Committing saga for {}", self.request_id);
        self.compensation_steps.borrow_mut().clear();
    }

    pub async fn check_idempotency(&self) -> Result<(), anyhow::Error> {
        let result = self.check_idempotency_internal().await;

        if result.is_err() {
            self.run_compensations().await;
        } else {
            self.compensation_steps
                .borrow_mut()
                .push(InvokeApplicationSagaCompensationStep::Idempotency);
        }

        result
    }

    async fn check_idempotency_internal(&self) -> Result<(), anyhow::Error> {
        // Simulate idempotency check logic
        if true {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Idempotency check failed for request {}",
                self.request_id
            ))
        }
    }

    async fn rollback_idempotency(&self) {
        println!("Rollback idempotency for request {}", self.request_id);
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
            self.run_compensations().await;
        } else {
            self.compensation_steps
                .borrow_mut()
                .push(InvokeApplicationSagaCompensationStep::StoreBody);
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
        println!("Rollback store payload for request {}", self.request_id);
    }

    pub async fn write_invoke_application(&self) -> Result<(), anyhow::Error> {
        let result = self.write_invoke_application_internal().await;

        if result.is_err() {
            self.run_compensations().await;
        } else {
            self.compensation_steps
                .borrow_mut()
                .push(InvokeApplicationSagaCompensationStep::WriteInvokeApplication);
        }

        result
    }

    async fn write_invoke_application_internal(&self) -> Result<(), anyhow::Error> {
        // Simulate writing invoke application logic
        if true {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Write invoke application failed for request {}",
                self.request_id
            ))
        }
    }

    async fn rollback_write_invoke_application(&self) {
        // No specific rollback logic for write invoke application
    }
}

// TODO: replace with async drop when stabilized
// https://doc.rust-lang.org/std/future/trait.AsyncDrop.html
impl Drop for InvokeApplicationSaga {
    fn drop(&mut self) {
        if !self.compensation_steps.borrow().is_empty() {
            info!("Aborting saga for {}", self.request_id);
            block_on(async {
                self.run_compensations().await;
            })
        }
    }
}
