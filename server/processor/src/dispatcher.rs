use core::panic;
use std::{sync::Arc, time::Instant};

use anyhow::{Ok, Result};
use dashmap::DashMap;
use data_model::{ChangeType, ProcessorId, ProcessorType, StateChange};
use itertools::Itertools;
use metrics::processors_metrics;
use opentelemetry::KeyValue;
use state_store::{requests::RequestPayload, StateChangeDispatcher};
use tracing::{trace, warn};

use crate::runner::{ProcessorLogic, ProcessorRunner};

#[derive(Debug)]
pub struct DispatchedRequest {
    pub request: RequestPayload,
    pub result: tokio::sync::oneshot::Sender<Result<()>>,
    pub queued_at: Instant,
}

#[derive(Debug, Clone)]
struct ProcessorHandle {
    state_change_tx: tokio::sync::watch::Sender<()>,
    request_tx: tokio::sync::mpsc::Sender<DispatchedRequest>,
}

pub struct Dispatcher {
    processor_handles: DashMap<ProcessorId, ProcessorHandle>,
    metrics: Arc<processors_metrics::Metrics>,
}

impl Dispatcher {
    pub fn new(metrics: Arc<processors_metrics::Metrics>) -> Result<Self> {
        let processor_handles: DashMap<ProcessorId, ProcessorHandle> = DashMap::new();

        let d = Dispatcher {
            processor_handles,
            metrics,
        };

        Ok(d)
    }

    /// Add a processor to the dispatcher.
    ///
    /// This function allows adding processor after the dispatcher is created to
    /// support adding processor who depend on the dispatcher, therefore
    /// preventing circular dependencies.
    pub fn add_processor<T: ProcessorLogic>(&self, processor: Arc<ProcessorRunner<T>>) {
        let state_change_tx = processor.get_state_change_tx();
        let request_tx = processor.get_request_tx();
        self.processor_handles.insert(
            processor.processor_id(),
            ProcessorHandle {
                state_change_tx,
                request_tx,
            },
        );
    }

    /// Dispatches a request to the appropriate processor based on the request
    /// type.
    pub async fn dispatch_requests(&self, request: RequestPayload) -> Result<()> {
        let processor_id = self.request_to_processor_id(request.clone());

        let (tx, rx) = tokio::sync::oneshot::channel();
        let dr = DispatchedRequest {
            request: request.clone(),
            result: tx,
            queued_at: Instant::now(),
        };

        let handle = match self.processor_handles.get(&processor_id) {
            Some(tx) => tx.value().clone(),
            None => panic!("processor not found: {:?}", request),
        };

        trace!(
            "sending request to processor: {} in {:?}",
            request.to_string(),
            processor_id.clone(),
        );
        let processor_type_str = processor_id.to_string();
        self.metrics
            .requests_inflight
            .add(1, &[KeyValue::new("processor", processor_type_str.clone())]);
        handle.request_tx.send(dr).await?;

        let res = match rx.await {
            std::result::Result::Ok(res) => res,
            Err(err) => {
                warn!("error processing request: {:?}", err);
                Err(anyhow::anyhow!("error processing request: {:?}", err))
            }
        };

        self.metrics
            .requests_inflight
            .add(-1, &[KeyValue::new("processor", processor_type_str)]);

        res
    }

    fn request_to_processor_id(&self, request: RequestPayload) -> ProcessorId {
        match request {
            RequestPayload::InvokeComputeGraph(_) |
            RequestPayload::ReplayComputeGraph(_) |
            RequestPayload::ReplayInvocations(_) |
            RequestPayload::FinalizeTask(_) |
            RequestPayload::CreateNameSpace(_) |
            RequestPayload::CreateOrUpdateComputeGraph(_) |
            RequestPayload::DeleteComputeGraph(_) |
            RequestPayload::DeleteInvocation(_) |
            RequestPayload::NamespaceProcessorUpdate(_) |
            RequestPayload::RemoveGcUrls(_) |
            RequestPayload::UpdateSystemTask(_) |
            RequestPayload::RemoveSystemTask(_) => ProcessorId::new(ProcessorType::Namespace),
            RequestPayload::TaskAllocationProcessorUpdate(_) |
            RequestPayload::RegisterExecutor(_) |
            RequestPayload::MutateClusterTopology(_) |
            RequestPayload::DeregisterExecutor(_) => ProcessorId::new(ProcessorType::TaskAllocator),
        }
    }
}

impl StateChangeDispatcher for Dispatcher {
    /// Dispatches state changes to the appropriate processors based on the
    /// change types.
    ///
    /// Examples:
    /// - [InvokeComputeGraph, InvokeComputeGraph] will result in a single
    ///   processor being triggered.
    /// - [ExecutorAdded, TaskCreated] will result in a single processor being
    ///   triggered.
    fn dispatch_state_change(&self, changes: Vec<StateChange>) -> Result<()> {
        changes
            .iter()
            // get all processor ids that are subscribed to the state change
            .flat_map(|sc| self.processor_ids_for_state_change(sc.clone()))
            // dedupe the processor ids
            .unique()
            // dispatch the state change to each processor
            .try_for_each(|processor_id| -> Result<()> {
                let handle = match self.processor_handles.get(&processor_id) {
                    Some(tx) => Ok(tx.value().clone()),
                    None => Err(anyhow::anyhow!("processor not found: {:?}", processor_id)),
                }?;
                handle.state_change_tx.send(())?;
                Ok(())
            })?;

        Ok(())
    }

    /// Returns the processor ids that need to be notified of a state change.
    fn processor_ids_for_state_change(&self, change: StateChange) -> Vec<ProcessorId> {
        match change.change_type {
            ChangeType::InvokeComputeGraph(_) | ChangeType::TaskFinished(_) => {
                vec![ProcessorId::new(ProcessorType::Namespace)]
            }
            ChangeType::TombstoneInvocation(_) | ChangeType::TombstoneComputeGraph(_) => {
                vec![
                    ProcessorId::new(ProcessorType::Namespace),
                    ProcessorId::new(ProcessorType::TaskAllocator),
                ]
            }
            ChangeType::ExecutorAdded |
            ChangeType::ExecutorRemoved(_) |
            ChangeType::TaskCreated(_) => {
                vec![ProcessorId::new(ProcessorType::TaskAllocator)]
            }
        }
    }
}
