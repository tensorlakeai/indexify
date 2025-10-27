use std::{sync::Arc, time::Duration};

use anyhow::Result;
use derive_builder::Builder;
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use serde::{Deserialize, Serialize};
use strum::Display;
use tokio::sync::Notify;
use tracing::{error, info, instrument};

use crate::{
    data_model::AllocationUsage,
    metrics::{Timer, low_latency_boundaries},
    queue::Queue,
    state_store::{IndexifyState, driver::Writer, state_machine},
};

pub struct UsageProcessor {
    indexify_state: Arc<IndexifyState>,
    queue: Arc<Option<Queue>>,
    processing_latency: Histogram<f64>,
    usage_events_counter: Counter<u64>,
    max_attempts: u8,
}

impl UsageProcessor {
    pub async fn new(
        queue: Arc<Option<Queue>>,
        indexify_state: Arc<IndexifyState>,
    ) -> Result<Self> {
        let meter = opentelemetry::global::meter("usage_processor_metrics");

        let processing_latency = meter
            .f64_histogram("indexify.usage.processing_latency")
            .with_unit("s")
            .with_boundaries(low_latency_boundaries())
            .with_description("usage processor event processing latency in seconds")
            .build();

        let usage_events_counter = meter
            .u64_counter("indexify.usage.events_total")
            .with_description("total number of processed usage events")
            .build();

        Ok(Self {
            indexify_state,
            processing_latency,
            usage_events_counter,
            max_attempts: 10,
            queue,
        })
    }

    #[instrument(skip_all)]
    pub async fn start(&self, mut shutdown_rx: tokio::sync::watch::Receiver<()>) {
        let mut usage_events_rx = self.indexify_state.usage_events_rx.clone();
        let mut cursor: Option<Vec<u8>> = None;

        let notify = Arc::new(Notify::new());
        loop {
            tokio::select! {
                _ = usage_events_rx.changed() => {
                    usage_events_rx.borrow_and_update();

                    if let Err(error) = self.process_allocation_usage_events(&mut cursor, &notify).await {
                        error!(
                            %error,
                            "error processing allocation usage events"
                        );
                    }

                },
                _ = notify.notified() => {
                    if let Err(error) = self.process_allocation_usage_events(&mut cursor, &notify).await {
                        error!(
                            %error,
                            "error processing allocation usage events"
                        );
                    }
                },
                _ = shutdown_rx.changed() => {
                    info!("usage processor shutting down");
                    break;
                }
            }
        }
    }

    #[instrument(skip_all)]
    async fn process_allocation_usage_events(
        &self,
        cursor: &mut Option<Vec<u8>>,
        notify: &Arc<Notify>,
    ) -> Result<()> {
        let timer_kvs = &[KeyValue::new("op", "process_allocation_usage_events")];
        Timer::start_with_labels(&self.processing_latency, timer_kvs);

        let (events, new_cursor) = self
            .indexify_state
            .reader()
            .allocation_usage(cursor.clone().as_ref())?;

        if events.is_empty() {
            return Ok(());
        }

        self.usage_events_counter.add(events.len() as u64, &[]);

        if let Some(c) = new_cursor {
            cursor.replace(c);
        };

        let mut failed_submission_cursor: Option<Vec<u8>> = None;
        let mut processed_events = Vec::new();
        for event in events {
            if let Err(error) = self.send_to_queue(event.clone()).await {
                error!(
                    %error,
                    namespace = %event.namespace,
                    allocation_id = %event.allocation_id,
                    application = %event.application,
                    request_id = %event.request_id,
                    "error processing allocation usage event"
                );

                if failed_submission_cursor.is_none() {
                    failed_submission_cursor = Some(event.key().to_vec());
                }

                break;
            } else {
                processed_events.push(event);
            }
        }

        if !processed_events.is_empty() {
            self.remove_and_commit_with_backoff(processed_events)
                .await?;
        }

        if let Some(failed_cursor) = failed_submission_cursor {
            cursor.replace(failed_cursor);
        }

        notify.notify_one();

        Ok(())
    }

    async fn remove_and_commit_with_backoff(
        &self,
        processed_events: Vec<AllocationUsage>,
    ) -> Result<()> {
        for attempt in 1..=self.max_attempts {
            let txn = self.indexify_state.db.transaction();

            if let Err(error) =
                state_machine::remove_allocation_usage_events(&txn, processed_events.as_slice())
            {
                error!(
                    %error,
                    attempt,
                    "error removing processed allocation usage events, retrying..."
                );

                if attempt == self.max_attempts {
                    return Err(error);
                }

                let delay = Duration::from_secs(attempt as u64);
                tokio::time::sleep(delay).await;
            }

            match txn.commit() {
                Ok(_) => return Ok(()),
                Err(commit_error) => {
                    error!(
                        %commit_error,
                        attempt,
                        "error committing transaction to remove processed allocation usage events, retrying..."
                    );

                    if attempt == self.max_attempts {
                        return Err(anyhow::Error::new(commit_error));
                    }

                    let delay = Duration::from_secs(attempt as u64);
                    tokio::time::sleep(delay).await;
                }
            }
        }

        Err(anyhow::anyhow!("Failed to remove and commit events"))
    }

    async fn send_to_queue(&self, event: AllocationUsage) -> Result<()> {
        let queue = match self.queue.as_ref() {
            Some(q) => q,
            None => {
                return Ok(());
            }
        };

        let usage_event = UsageEvent::try_from(event)?;
        queue.send_json(&usage_event).await?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder)]
pub struct UsageEvent {
    pub service: String,
    #[serde(default)]
    pub application_usage: Vec<ApplicationResourceUsage>,
    pub timestamp: String,
    pub event_id: String,
    pub project_id: String,
}

impl TryFrom<AllocationUsage> for UsageEvent {
    type Error = anyhow::Error;

    fn try_from(allocation_usage: AllocationUsage) -> Result<Self, Self::Error> {
        let now = chrono::Utc::now();

        let mut usage_event_builder = UsageEventBuilder::default();

        usage_event_builder
            .project_id(allocation_usage.namespace)
            .event_id(allocation_usage.allocation_id.to_string())
            .service("applications".to_string())
            .timestamp(now.to_rfc3339());

        let mut usage_entries = Vec::new();

        let cpu_amount =
            allocation_usage.cpu_ms_per_second as u64 * allocation_usage.execution_duration_ms;
        let cpu_amount = cpu_amount / 1000;

        let cpu_usage = ApplicationResourceUsageBuilder::default()
            .resource(ApplicationsResourceType::Cpu)
            .application(allocation_usage.application.clone())
            .allocation_id(allocation_usage.allocation_id.to_string())
            .request_id(allocation_usage.request_id.clone())
            .amount(cpu_amount)
            .build()?;

        usage_entries.push(cpu_usage);
        let disk_amount = allocation_usage.disk_mb * allocation_usage.execution_duration_ms;
        let disk_amount = disk_amount / 1000;

        let disk_usage = ApplicationResourceUsageBuilder::default()
            .resource(ApplicationsResourceType::DiskMb)
            .application(allocation_usage.application.clone())
            .allocation_id(allocation_usage.allocation_id.to_string())
            .request_id(allocation_usage.request_id.clone())
            .amount(disk_amount)
            .build()?;

        usage_entries.push(disk_usage);

        let memory_amount = allocation_usage.memory_mb * allocation_usage.execution_duration_ms;
        let memory_amount = memory_amount / 1000;

        let memory_usage = ApplicationResourceUsageBuilder::default()
            .resource(ApplicationsResourceType::MemoryMb)
            .application(allocation_usage.application.clone())
            .allocation_id(allocation_usage.allocation_id.to_string())
            .request_id(allocation_usage.request_id.clone())
            .amount(memory_amount)
            .build()?;

        usage_entries.push(memory_usage);

        if !allocation_usage.gpu_used.is_empty() {
            let gpu_amount = allocation_usage.execution_duration_ms / 1000;

            let mut gpu_usage_builder = ApplicationResourceUsageBuilder::default();

            gpu_usage_builder
                .application(allocation_usage.application.clone())
                .allocation_id(allocation_usage.allocation_id.to_string())
                .request_id(allocation_usage.request_id.clone())
                .amount(gpu_amount);

            let mut gpu_models = Vec::new();

            for gpu in allocation_usage.gpu_used.iter() {
                for _ in 0..gpu.count {
                    let gpu_model = GpuModel::from(gpu.model.as_str());
                    gpu_models.push(gpu_model);
                }
            }

            gpu_usage_builder.resource(ApplicationsResourceType::Gpu(gpu_models));

            let gpu_usage = gpu_usage_builder.build()?;

            usage_entries.push(gpu_usage);
        };

        let usage_event = usage_event_builder
            .application_usage(usage_entries)
            .build()?;

        Ok(usage_event)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Builder)]
pub struct ApplicationResourceUsage {
    pub resource: ApplicationsResourceType,
    pub amount: u64,

    pub application: String,
    pub allocation_id: String,
    pub request_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Display, PartialEq, Eq, Hash)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum ApplicationsResourceType {
    Cpu,
    DiskMb,
    Gpu(Vec<GpuModel>),
    MemoryMb,
}

#[derive(Debug, Clone, Serialize, Deserialize, Display, PartialEq, Eq, Hash)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum GpuModel {
    NvidiaA100_40GB,
    NvidiaA100_80GB,
    NvidiaH100_80GB,
    NvidiaTeslaT4,
    NvidiaA6000,
    NvidiaA10,
    Unknown,
}

impl From<&str> for GpuModel {
    fn from(value: &str) -> Self {
        match value {
            "GPU_MODEL_NVIDIA_A100_40GB" => GpuModel::NvidiaA100_40GB,
            "GPU_MODEL_NVIDIA_A100_80GB" => GpuModel::NvidiaA100_80GB,
            "GPU_MODEL_NVIDIA_H100_80GB" => GpuModel::NvidiaH100_80GB,
            "GPU_MODEL_NVIDIA_TESLA_T4" => GpuModel::NvidiaTeslaT4,
            "GPU_MODEL_NVIDIA_A6000" => GpuModel::NvidiaA6000,
            "GPU_MODEL_NVIDIA_A10" => GpuModel::NvidiaA10,
            _ => GpuModel::Unknown,
        }
    }
}
