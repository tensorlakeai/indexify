use core::panic;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use data_model::ProcessorId;
use metrics::{processors_metrics, Timer};
use opentelemetry::KeyValue;
use tokio::sync::{Mutex, MutexGuard};
use tracing::error;

use crate::dispatcher::DispatchedRequest;

const MAX_BATCHED_REQUESTS: usize = 32;
const MAX_REQUEST_QUEUE_LENGTH: usize = 128;

struct Inner {
    request_rx: tokio::sync::mpsc::Receiver<DispatchedRequest>,
}

/// processor logic trait leveraged by processor runner so that each processor
/// does not have to implement any channel management.
#[async_trait]
pub trait ProcessorLogic: Send + Sync {
    fn processor_id(&self) -> ProcessorId;
    async fn process(&self, requests: Vec<DispatchedRequest>) -> Result<()>;
}

/// Processor runner that manages a single processor's request queue and state
/// changes. It is responsible for receiving requests and state changes and
/// dispatching them to the processor logic implementation.
pub struct ProcessorRunner<T: ProcessorLogic> {
    state_change_tx: tokio::sync::watch::Sender<()>,
    state_change_rx: tokio::sync::watch::Receiver<()>,
    request_tx: tokio::sync::mpsc::Sender<DispatchedRequest>,
    inner: Arc<Mutex<Inner>>,
    processor: Arc<T>,
    metrics: Arc<processors_metrics::Metrics>,
}

impl<T: ProcessorLogic> ProcessorRunner<T> {
    pub fn new(processor: Arc<T>, metrics: Arc<processors_metrics::Metrics>) -> Self {
        let (state_change_tx, state_change_rx) = tokio::sync::watch::channel(());
        let (request_tx, request_rx) =
            tokio::sync::mpsc::channel::<DispatchedRequest>(MAX_REQUEST_QUEUE_LENGTH);
        Self {
            state_change_tx,
            state_change_rx,
            request_tx,
            inner: Arc::new(Mutex::new(Inner { request_rx })),
            processor,
            metrics,
        }
    }

    pub fn get_request_tx(&self) -> tokio::sync::mpsc::Sender<DispatchedRequest> {
        self.request_tx.clone()
    }

    pub fn get_state_change_tx(&self) -> tokio::sync::watch::Sender<()> {
        self.state_change_tx.clone()
    }

    pub fn processor_id(&self) -> ProcessorId {
        self.processor.processor_id()
    }

    /// start the processor runner which will process queues requests and state
    /// changes continuously.
    pub async fn start(&self, mut shutdown_rx: tokio::sync::watch::Receiver<()>) {
        let mut rx = self.state_change_rx.clone();

        // mark the rx as changed so that the first iteration will process possible
        // existing state changes.
        rx.mark_changed();

        let mut inner_guard = self.inner.lock().await;
        loop {
            let mut requests = Vec::with_capacity(MAX_BATCHED_REQUESTS);
            tokio::select! {
                _ = rx.changed() => {
                    rx.borrow_and_update();
                    self.try_recv_queued_requests(&mut requests, &mut inner_guard).await;
                },
                _ = inner_guard.request_rx.recv_many(&mut requests, MAX_BATCHED_REQUESTS) => {
                },
                _ = shutdown_rx.changed() => {
                    break
                }
            }

            if let Err(err) = self.delegate_process(requests).await {
                error!("error processing work: {:?}", err);
            }
        }
    }

    /// process the currently queued requests and state changes.
    pub async fn process(&self) -> Result<()> {
        let mut requests = Vec::with_capacity(MAX_BATCHED_REQUESTS);
        let mut inner_guard = self.inner.lock().await;
        self.try_recv_queued_requests(&mut requests, &mut inner_guard)
            .await;
        self.delegate_process(requests).await
    }

    /// try to receive queued requests from the channel so that we always try to
    /// process requests for every state change.
    async fn try_recv_queued_requests(
        &self,
        requests: &mut Vec<DispatchedRequest>,
        inner_guard: &mut MutexGuard<'_, Inner>,
    ) {
        loop {
            if requests.len() == MAX_BATCHED_REQUESTS {
                break;
            }
            if let std::result::Result::Ok(request) = inner_guard.request_rx.try_recv() {
                requests.push(request);
            } else {
                break;
            }
        }
    }

    /// delegate the processing of the requests to the processor logic.
    async fn delegate_process(&self, requests: Vec<DispatchedRequest>) -> Result<()> {
        // metrics setup
        let timer_kvs = &[KeyValue::new("processor", self.processor_id().to_string())];
        let _timer = Timer::start_with_labels(&self.metrics.processors_process_duration, timer_kvs);
        for req in &requests {
            self.metrics.requests_queue_duration.record(
                req.queued_at.elapsed().as_secs_f64(),
                &[KeyValue::new(
                    "processor",
                    self.processor.processor_id().to_string(),
                )],
            );
        }

        self.processor.process(requests).await
    }
}
