use std::{convert::Infallible, sync::Arc};

use anyhow::Result;
use http_body_util::combinators::BoxBody;
use hyper::{Request, Response, body::Bytes};
use serde_json::json;
use tokio::sync::Mutex;

use crate::executor::monitoring::{
    handler::Handler,
    health_checker::{HealthChecker, generic_health_checker::GenericHealthChecker},
    server::full,
};

#[derive(Debug, Clone)]
pub struct HealthCheckHandler {
    health_checker: Arc<Mutex<GenericHealthChecker>>,
}

impl HealthCheckHandler {
    pub fn new(health_checker: Arc<Mutex<GenericHealthChecker>>) -> Self {
        Self { health_checker }
    }
}

impl Handler<hyper::body::Incoming> for HealthCheckHandler {
    async fn handle(
        &self,
        _request: Request<hyper::body::Incoming>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        let result = self.health_checker.lock().await.check();
        if result.is_success {
            let message = match result.status_message {
                Some(message) => message,
                None => "Successful".to_string(),
            };
            let body = full(Bytes::from(
                json!({
                  "status": "ok",
                  "message": message,
                  "checker": result.checker_name
                })
                .to_string(),
            ));
            let response = Response::builder()
                .header("Content-Type", "application/json")
                .status(200)
                .body(body)
                .expect("Failed to build response.");
            Ok(response)
        } else {
            let body = full(Bytes::from(
                json!({
                  "status": "nok",
                  "message": result.status_message.unwrap_or_default(),
                  "checker": result.checker_name
                })
                .to_string(),
            ));
            let response = Response::builder()
                .header("Content-Type", "application/json")
                .status(503)
                .body(body)
                .expect("Failed to build response.");
            Ok(response)
        }
    }
}
