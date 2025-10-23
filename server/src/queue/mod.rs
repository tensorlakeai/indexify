use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tracing::info;

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
        })
    }

    pub async fn send_json<P: serde::Serialize + Sync>(&self, payload: &P) -> anyhow::Result<()> {
        self.producer
            .send_serde_json(payload)
            .await
            .map_err(Into::into)
    }
}
