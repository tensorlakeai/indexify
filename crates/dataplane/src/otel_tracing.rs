use std::io::IsTerminal;

use anyhow::Result;
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::{SpanExporter as OtlpSpanExporter, WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::{
    Resource,
    trace::{BatchConfigBuilder, BatchSpanProcessor, TracerProviderBuilder},
};
use opentelemetry_stdout::SpanExporter as StdoutSpanExporter;
use tracing::Metadata;
use tracing_subscriber::{
    Layer,
    filter::FilterExt,
    layer::{self, Filter, SubscriberExt},
};

use crate::config::{DataplaneConfig, TracingExporter};

/// Filter out noisy internal spans from third-party crates that we don't need
/// to export to our tracing backend.
struct NoisyModulesFilter;

impl<S> Filter<S> for NoisyModulesFilter {
    fn enabled(&self, metadata: &Metadata<'_>, _: &layer::Context<'_, S>) -> bool {
        let target = metadata.target();
        // Filter out noisy internal spans from third-party crates
        !target.starts_with("h2::") && !target.starts_with("tokio::")
    }
}

pub fn get_env_filter() -> tracing_subscriber::EnvFilter {
    // RUST_LOG used to control logging level.
    tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::default()
            .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
    })
}

pub fn get_log_layer<S>(config: &DataplaneConfig) -> Box<dyn Layer<S> + Send + Sync>
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

    Box::new(
        tracing_subscriber::fmt::layer()
            .with_ansi(std::io::stderr().is_terminal())
            .compact(),
    )
}

pub fn setup_tracing(config: &DataplaneConfig) -> Result<()> {
    let env_filter_layer = get_env_filter();

    let base = tracing_subscriber::Registry::default();

    let subscriber: Box<dyn tracing::Subscriber + Send + Sync> =
        if let Some(tracing_exporter) = &config.telemetry.tracing_exporter {
            let mut tracer_provider = TracerProviderBuilder::default().with_resource(
                Resource::builder_empty()
                    .with_service_name("indexify-dataplane")
                    .build(),
            );
            match tracing_exporter {
                TracingExporter::Otlp => {
                    // Use gzip compression to reduce payload size and avoid gRPC message limits
                    let mut otlp = OtlpSpanExporter::builder()
                        .with_tonic()
                        .with_compression(opentelemetry_otlp::Compression::Gzip);
                    if let Some(endpoint) = &config.telemetry.endpoint {
                        otlp = otlp.with_endpoint(endpoint);
                    }
                    let exporter = otlp.build()?;
                    // Use smaller batch size to avoid exceeding receiver's 4MB message limit
                    let batch_config = BatchConfigBuilder::default()
                        .with_max_export_batch_size(128) // Default is 512
                        .build();
                    let batch_processor =
                        BatchSpanProcessor::builder(exporter).with_batch_config(batch_config);
                    tracer_provider = tracer_provider.with_span_processor(batch_processor.build())
                }
                TracingExporter::Stdout => {
                    tracer_provider =
                        tracer_provider.with_simple_exporter(StdoutSpanExporter::default());
                }
            }

            let sdk_tracer = tracer_provider.build();

            let tracer = sdk_tracer.tracer("indexify-dataplane");
            let span_layer = tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_filter(NoisyModulesFilter.and(get_env_filter()));

            let log_layer = get_log_layer(config).with_filter(env_filter_layer.clone());
            Box::new(base.with(span_layer).with(log_layer))
        } else {
            let log_layer = get_log_layer(config).with_filter(env_filter_layer.clone());
            Box::new(base.with(log_layer))
        };

    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}
