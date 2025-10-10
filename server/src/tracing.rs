use anyhow::Result;
use opentelemetry::global;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::{SdkTracerProvider, TracerProviderBuilder};
use tracing::error;
use tracing_subscriber::{Layer, layer::SubscriberExt};

use crate::config::ServerConfig;

pub fn get_env_filter() -> tracing_subscriber::EnvFilter {
    // RUST_LOG used to control logging level.
    tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::default()
            .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
    })
}

pub fn get_log_layer<S>(config: &ServerConfig) -> Box<dyn Layer<S> + Send + Sync>
where
    S: for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    S: tracing::Subscriber,
{
    if config.structured_logging() {
        return Box::new(
            json_subscriber::fmt::layer()
                .with_span_list(false)
                .flatten_event(true)
                .flatten_current_span_on_top_level(true),
        );
    }

    Box::new(tracing_subscriber::fmt::layer().compact())
}

pub fn setup_tracing(config: &ServerConfig) -> Result<Option<SdkTracerProvider>> {
    let env_filter_layer = get_env_filter();
    let log_layer = get_log_layer(config);
    let subscriber =
        tracing_subscriber::Registry::default().with(log_layer.with_filter(env_filter_layer));

    if !config.telemetry.enable_tracing {
        if let Err(e) = tracing::subscriber::set_global_default(subscriber) {
            error!("logger was already initiated, continuing: {:?}", e);
        }
        return Ok(None);
    }

    let mut span_exporter = SpanExporter::builder().with_tonic();
    if let Some(endpoint) = &config.telemetry.endpoint {
        span_exporter = span_exporter.with_endpoint(endpoint.clone());
    }
    let span_exporter = span_exporter.build()?;

    let tracer_provider = TracerProviderBuilder::default()
        .with_simple_exporter(span_exporter)
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    Ok(Some(tracer_provider))
}
