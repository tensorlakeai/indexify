use std::convert::Infallible;

use http_body_util::{BodyExt, Empty, Full, combinators::BoxBody};
use hyper::{
    Method, Request, Response, StatusCode,
    body::{self, Bytes},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

use crate::executor::monitoring::{
    handler::Handler, health_check_handler::HealthCheckHandler,
    prometheus_metrics_handler::PrometheusMetricsHandler,
    startup_probe_handler::StartupProbeHandler,
};

#[derive(Clone)]
pub struct MonitoringServer {
    host: String,
    port: u16,
    startup_probe_handler: StartupProbeHandler,
    health_probe_handler: HealthCheckHandler,
    metrics_handler: PrometheusMetricsHandler,
}

impl MonitoringServer {
    pub fn new(
        host: String,
        port: u16,
        startup_probe_handler: StartupProbeHandler,
        health_probe_handler: HealthCheckHandler,
        metrics_handler: PrometheusMetricsHandler,
    ) -> Self {
        MonitoringServer {
            host,
            port,
            startup_probe_handler,
            health_probe_handler,
            metrics_handler,
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(format!("{}:{}", self.host, self.port)).await?;
        loop {
            let (stream, _) = listener.accept().await?;

            let io = TokioIo::new(stream);
            let server = self.clone();

            tokio::task::spawn(async move {
                if let Err(err) = http1::Builder::new()
                    .serve_connection(
                        io,
                        service_fn(|request: Request<body::Incoming>| async {
                            server.routes(request).await
                        }),
                    )
                    .await
                {
                    eprintln!("Error serving connection: {:?}", err);
                }
            });
        }
    }

    async fn routes(
        &self,
        request: Request<hyper::body::Incoming>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        match (request.method(), request.uri().path()) {
            (&Method::POST, "/monitoring/startup") => self.startup_probe_handler.handle(request),
            (&Method::POST, "/monitoring/health") => self.health_probe_handler.handle(request),
            (&Method::POST, "/monitoring/metrics") => self.metrics_handler.handle(request),

            _ => {
                let mut not_found = Response::new(empty());
                *not_found.status_mut() = StatusCode::NOT_FOUND;
                Ok(not_found)
            }
        }
    }
}

pub fn empty() -> BoxBody<Bytes, hyper::Error> {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}

pub fn full<T: Into<Bytes>>(chunk: T) -> BoxBody<Bytes, hyper::Error> {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}
