use std::convert::Infallible;

use anyhow::Result;
use http_body_util::combinators::BoxBody;
use hyper::{
    body::Bytes,
    http::{Request, Response},
};

pub trait Handler<T> {
    fn handle(
        &self,
        request: Request<T>,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible>;
}
