use anyhow::Result;
use tracing_subscriber::{Layer, layer::SubscriberExt};

use crate::config::Config;

pub fn get_env_filter() -> tracing_subscriber::EnvFilter {
    // RUST_LOG used to control logging level.
    tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::default()
            .add_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
    })
}

pub fn get_log_layer<S>(config: &Config) -> Box<dyn Layer<S> + Send + Sync>
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

pub fn setup_tracing(config: &Config) -> Result<()> {
    let env_filter = get_env_filter();
    let log_layer = get_log_layer(config).with_filter(env_filter);

    let subscriber = tracing_subscriber::Registry::default().with(log_layer);

    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

