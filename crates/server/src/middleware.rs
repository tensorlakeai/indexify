use hyper::Request;
use tower_http::trace::MakeSpan;
use tracing::Span;

#[derive(Clone)]
pub struct InstanceRequestSpan {
    env: String,
    instance_id: String,
}

impl InstanceRequestSpan {
    pub fn new(env: &str, instance_id: &str) -> Self {
        Self {
            env: env.to_string(),
            instance_id: instance_id.to_string(),
        }
    }
}

impl<B> MakeSpan<B> for InstanceRequestSpan {
    fn make_span(&mut self, _: &Request<B>) -> Span {
        tracing::info_span!(
            "request",
            env = %self.env,
            instance_id = %self.instance_id,
        )
    }
}
