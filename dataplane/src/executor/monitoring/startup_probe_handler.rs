use std::convert::Infallible;

use anyhow::Result;
use http_body_util::combinators::BoxBody;
use hyper::{Request, Response, body::Bytes};
use serde_json::json;

use crate::executor::monitoring::{handler::Handler, server::full};

#[derive(Debug, Clone, Copy)]
pub struct StartupProbeHandler {
    ready: bool,
}

impl StartupProbeHandler {
    pub fn new() -> Self {
        StartupProbeHandler { ready: false }
    }
    pub(crate) fn set_ready(&mut self, ready: bool) {
        self.ready = ready;
    }
}

impl Handler<hyper::body::Incoming> for StartupProbeHandler {
    #[inline]
    // why are we taking in request when we don't use it?
    async fn handle(
        &self,
        _request: Request<hyper::body::Incoming>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        if self.ready {
            let body = full(Bytes::from(json!({"status": "ok"}).to_string()));
            let response = Response::builder()
                .header("Content-Type", "application/json")
                .status(200)
                .body(body)
                .expect("Failed to build response");
            Ok(response)
        } else {
            let body = full(Bytes::from(json!({"status": "nok"}).to_string()));
            let response = Response::builder()
                .header("Content-Type", "application/json")
                .status(503)
                .body(body)
                .expect("Failed to build response");
            Ok(response)
        }
    }
}
