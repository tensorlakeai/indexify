use std::time::{SystemTime, UNIX_EPOCH};

pub fn timestamp_msec() -> u128 {
    let now = SystemTime::now();
    let duration = now.duration_since(UNIX_EPOCH).unwrap();
    duration.as_millis()
}

pub fn timestamp_secs() -> u64 {
    let now = SystemTime::now();
    let duration = now.duration_since(UNIX_EPOCH).unwrap();
    duration.as_secs()
}
