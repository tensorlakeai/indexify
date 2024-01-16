use std::{
    error::Error,
    time::{SystemTime, UNIX_EPOCH},
};

use tonic::Status;

pub fn timestamp_secs() -> u64 {
    let now = SystemTime::now();
    let duration = now.duration_since(UNIX_EPOCH).unwrap();
    duration.as_secs()
}

pub fn match_for_io_error(err_status: &Status) -> Option<&std::io::Error> {
    let mut err: &(dyn Error + 'static) = err_status;

    if let Some(err_source) = err_status.source() {
        err = err_source;
    }

    loop {
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            return Some(io_err);
        }

        // h2::Error do not expose std::io::Error with `source()`
        // https://github.com/hyperium/h2/pull/462
        if let Some(h2_err) = err.downcast_ref::<h2::Error>() {
            if let Some(io_err) = h2_err.get_io() {
                return Some(io_err);
            }
        }

        err = match err.source() {
            Some(err) => err,
            None => return None,
        };
    }
}
