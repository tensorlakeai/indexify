use std::convert::Infallible;
use std::sync::Arc;

use http_body_util::combinators::BoxBody;
use hyper::{body::Bytes, Response};

use tokio::sync::Mutex;

use crate::executor::{
    executor_api::ExecutorStateReconciler,
    monitoring::{handler::Handler, server::full},
};

#[derive(Clone)]
pub struct DesiredStateHandler {
    state_reconciler: Arc<Mutex<ExecutorStateReconciler>>,
}

impl DesiredStateHandler {
    pub fn new(state_reconciler: Arc<Mutex<ExecutorStateReconciler>>) -> Self {
        Self { state_reconciler }
    }
}

impl Handler<hyper::body::Incoming> for DesiredStateHandler {
    async fn handle(
        &self,
        _request: hyper::Request<hyper::body::Incoming>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        let result = self.state_reconciler.lock().await.get_desired_state();
        match result {
            Some(req) => {
                let body = full(Bytes::from(format!("{req:?}")));
                let response = Response::builder()
                    .header(hyper::header::CONTENT_TYPE, "text/plain; charset=utf-8")
                    .status(200)
                    .body(body)
                    .expect("Failed to build response.");
                Ok(response)
            }
            None => Ok(Response::builder()
                .header(hyper::header::CONTENT_TYPE, "text/plain; charset=utf-8")
                .status(200)
                .body(full(Bytes::from(
                    "No desired state received from Server yet".to_string(),
                )))
                .expect("Failed to build response.")),
        }
    }
}
