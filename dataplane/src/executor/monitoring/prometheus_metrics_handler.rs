use std::{convert::Infallible, sync::Arc};

use http_body_util::combinators::BoxBody;
use hyper::{Response, body::Bytes};
use prometheus_client::{encoding::text::encode, registry::Registry};

use crate::executor::monitoring::{handler::Handler, server::full};

#[derive(Debug, Clone)]
pub struct PrometheusMetricsHandler {
    registry: Arc<Registry>,
}

impl PrometheusMetricsHandler {
    pub fn new(registry: Arc<Registry>) -> Self {
        PrometheusMetricsHandler { registry }
    }
}

impl Handler<hyper::body::Incoming> for PrometheusMetricsHandler {
    async fn handle(
        &self,
        _request: hyper::Request<hyper::body::Incoming>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        let mut buf = String::new();
        encode(&mut buf, &self.registry).expect("Failed to encode metrics.");
        let body = full(Bytes::from(buf));
        let response = Response::builder()
            .header(
                hyper::header::CONTENT_TYPE,
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )
            .status(200)
            .body(body)
            .expect("Failed to build response.");
        Ok(response)
    }
}
