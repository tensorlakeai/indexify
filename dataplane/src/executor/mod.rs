use prometheus_client::{metrics::gauge::Gauge, registry::Registry};

pub mod executor;
pub mod executor_api;
pub mod monitoring;

#[derive(Debug)]
pub struct Metrics {
    pub(crate) healthy_gauge: Gauge,
}

impl Metrics {
    pub fn new(registry: &mut Registry) -> Self {
        let healthy_gauge = Gauge::default();
        registry.register(
            "healthy",
            "1 if the executor is healthy, 0 otherwise",
            healthy_gauge.clone(),
        );

        Self { healthy_gauge }
    }
}
