use axum_otel_metrics::{HttpMetricsLayer, HttpMetricsLayerBuilder};

pub(super) fn get_metrics_layer() -> HttpMetricsLayer {
    HttpMetricsLayerBuilder::new().build()
}
