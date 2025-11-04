use anyhow::Result;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{SpanExporter as OtlpSpanExporter, WithExportConfig};
use opentelemetry_sdk::{Resource, trace::TracerProviderBuilder};
use opentelemetry_stdout::SpanExporter as StdoutSpanExporter;
use tracing::Metadata;
use tracing_subscriber::{
    Layer,
    layer::{self, Filter, SubscriberExt},
};

use crate::config::{ServerConfig, TracingExporter};

/// SlateDB internal task threads are very noisy and are mixed with our own
/// traces. This filter disables their instrumentation, which we don't use at
/// the moment.
struct SlateDBFilter;

impl<S> Filter<S> for SlateDBFilter {
    fn enabled(&self, metadata: &Metadata<'_>, _: &layer::Context<'_, S>) -> bool {
        !metadata.target().starts_with("slatedb::")
    }
}

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

pub fn setup_tracing(config: &ServerConfig) -> Result<()> {
    let mut tracer_provider = TracerProviderBuilder::default().with_resource(
        Resource::builder_empty()
            .with_service_name("indexify-server")
            .build(),
    );
    match &config.telemetry.tracing_exporter {
        Some(TracingExporter::Otlp) => {
            let mut otlp = OtlpSpanExporter::builder().with_tonic();
            if let Some(endpoint) = &config.telemetry.endpoint {
                otlp = otlp.with_endpoint(endpoint);
            }
            let exporter = otlp.build()?;
            tracer_provider = tracer_provider.with_batch_exporter(exporter)
        }
        Some(TracingExporter::Stdout) => {
            tracer_provider = tracer_provider.with_simple_exporter(StdoutSpanExporter::default());
        }
        _ => {}
    }

    let sdk_tracer = tracer_provider.build();

    let tracer = sdk_tracer.tracer("indexify-server");
    let tracing_span_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(SlateDBFilter);

    let env_filter_layer = get_env_filter();
    let log_layer = get_log_layer(config).with_filter(env_filter_layer);
    let subscriber = tracing_subscriber::Registry::default()
        .with(tracing_span_layer)
        .with(log_layer);

    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}
