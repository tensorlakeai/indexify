use std::sync::Arc;

use omniqueue::QueueError;
use opentelemetry::KeyValue;
use tracing::{error, info};

use crate::{
    config::{QueueBackend, QueueConfig},
    metrics::{self, Increment},
};

pub struct Queue {
    producer: Arc<omniqueue::DynProducer>,
    metrics: metrics::queue::Metrics,
}

impl Queue {
    pub async fn new(config: QueueConfig) -> anyhow::Result<Self> {
        let producer = match &config.backend {
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
            QueueBackend::Redis(cfg) => {
                info!("using redis queue config with dsn: {}", cfg.dsn);

                let redis_config = omniqueue::backends::RedisConfig {
                    dsn: cfg.dsn.clone(),
                    max_connections: cfg.max_connections,
                    reinsert_on_nack: cfg.reinsert_on_nack,
                    queue_key: cfg.queue_key.clone(),
                    delayed_queue_key: cfg.delayed_queue_key.clone(),
                    delayed_lock_key: cfg.delayed_lock_key.clone(),
                    consumer_group: cfg.consumer_group.clone(),
                    consumer_name: cfg.consumer_name.clone(),
                    payload_key: cfg.payload_key.clone(),
                    ack_deadline_ms: cfg.ack_deadline_ms,
                    dlq_config: None,
                    sentinel_config: None,
                };

                omniqueue::backends::RedisBackend::builder(redis_config)
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
