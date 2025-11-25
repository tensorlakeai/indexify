use std::convert::Infallible;

use http_body_util::combinators::BoxBody;
use hyper::{Response, body::Bytes};

use crate::executor::{
    executor_api::ExecutorStateReporter,
    monitoring::{handler::Handler, server::full},
};

#[derive(Debug, Clone)]
pub struct ReportedStateHandler {
    state_reporter: ExecutorStateReporter,
}

impl ReportedStateHandler {
    pub fn new(state_reporter: ExecutorStateReporter) -> Self {
        ReportedStateHandler { state_reporter }
    }
}

impl Handler<hyper::body::Incoming> for ReportedStateHandler {
    async fn handle(
        &self,
        _request: hyper::Request<hyper::body::Incoming>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        let request = self.state_reporter.last_state_report_request().await;
        match request {
            Some(request) => {
                let body = full(Bytes::from(format!("{request:?}")));
                let response = Response::builder()
                    .header(hyper::header::CONTENT_TYPE, "text/plain; charset=utf-8")
                    .status(200)
                    .body(body)
                    .expect("Failed to build response.");
                Ok(response)
            }
            None => {
                let body = full(Bytes::from("No state reported so far"));
                let response = Response::builder()
                    .header(hyper::header::CONTENT_TYPE, "text/plain; charset=utf-8")
                    .status(200)
                    .body(body)
                    .expect("Failed to build response.");
                Ok(response)
            }
        }
    }
}
