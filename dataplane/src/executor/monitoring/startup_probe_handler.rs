pub struct StartupProbeHandler {
    ready: bool,
}

impl StartupProbeHandler {
    pub(crate) fn new() -> Self {
        StartupProbeHandler { ready: false }
    }
}

impl Handler for StartupProbeHandler {
    #[inline]
    fn handle(&self, request: Request) -> Result<Response, Error> {
        Ok(Response::new(Body::empty()))
    }
}
