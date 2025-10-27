use std::sync::Arc;

use omniqueue::QueueError;
use opentelemetry::KeyValue;
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::{metrics, metrics::Increment};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueBackend {
    #[default]
    InMemory,
    AmazonSqs {
        queue_url: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QueueConfig {
    pub backend: QueueBackend,
}

pub struct Queue {
    producer: Arc<omniqueue::DynProducer>,
    metrics: metrics::queue::Metrics,
}

impl Queue {
    pub async fn new(config: QueueConfig) -> anyhow::Result<Self> {
        let producer = match &config.backend {
            QueueBackend::InMemory => {
                info!("using in-memory queue backend");

                // We have to build the pair because the in-memory backend
                // does not allow instantiating a single half
                let (producer, _) = omniqueue::backends::InMemoryBackend::builder()
                    .make_dynamic()
                    .build_pair()
                    .await?;

                producer
            }
            QueueBackend::AmazonSqs { queue_url } => {
                info!("using sqs queue config with url: {queue_url}");

                let sqs_config = omniqueue::backends::SqsConfig {
                    queue_dsn: queue_url.clone(),
                    override_endpoint: false,
                };

                omniqueue::backends::SqsBackend::builder(sqs_config)
                    .make_dynamic()
                    .build_producer()
                    .await?
            }
        };

        Ok(Self {
            producer: Arc::new(producer),
            metrics: metrics::queue::Metrics::default(),
        })
    }

    pub async fn send_json<P: serde::Serialize + Sync>(
        &self,
        payload: &P,
    ) -> omniqueue::Result<()> {
        let send_result = self.producer.send_serde_json(payload).await;

        match send_result {
            Ok(_) => {
                Increment::inc(&self.metrics.messages_sent, &[]);
                Ok(())
            }
            Err(queue_error) => {
                let attrs = &[KeyValue::new(
                    "queue.error_type",
                    queue_error_type(&queue_error),
                )];

                Increment::inc(&self.metrics.send_errors, attrs);

                Err(queue_error)
            }
        }
    }
}

fn queue_error_type(queue_error: &QueueError) -> String {
    match queue_error {
        QueueError::Generic(inner_error) => {
            error!("sqs error: {}", inner_error);

            "generic".to_string()
        }
        QueueError::Serde(inner_error) => {
            error!("sqs serialization error: {}", inner_error);

            "serde".to_string()
        }
        QueueError::CannotAckOrNackTwice => "cannot_ack_or_nack_twice".to_string(),
        QueueError::NoData => "no_data".to_string(),
        QueueError::Unsupported(reason) => {
            error!("sqs unsupported operation: {}", reason);

            "unsupported".to_string()
        }
        QueueError::PayloadTooLarge { actual, limit } => {
            error!("sqs payload too large: {} > {}", actual, limit);

            "payload_too_large".to_string()
        }
        QueueError::CannotCreateHalf => "cannot_create_half".to_string(),
    }
}
