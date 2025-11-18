use hyper::http::{Request, Response};

pub trait Handler {
    fn handle(&self, request: Request) -> Result<Response, Error>;
}
