use anyhow::anyhow;
use omniqueue::QueueProducer;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
}

impl Queue {
    pub async fn new(config: QueueConfig) -> anyhow::Result<Self> {
        let producer = match &config.backend {
            QueueBackend::InMemory => {
                match omniqueue::backends::InMemoryBackend::builder()
                    .make_dynamic()
                    .build_producer()
                    .await
                {
                    Ok(p) => p,
                    Err(e) => {
                        // If building the producer fails, it might be because the in-memory
                        // backend hasn't been initialized yet. Initialize it now

                        tracing::error!("");
                        return anyhow!(e.to_string());
                    }
                }
            }
            QueueBackend::AmazonSqs { queue_url } => {
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
        })
    }

    pub async fn send_json<P: serde::Serialize + Sync>(&self, payload: &P) -> anyhow::Result<()> {
        self.producer
            .send_serde_json(payload)
            .await
            .map_err(Into::into)
    }
}
