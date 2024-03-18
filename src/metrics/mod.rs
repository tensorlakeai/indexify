use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use pin_project_lite::pin_project;

pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct TimedFuture<F, C>
    where
        F: Future,
        C: FnOnce(Duration),
    {
        #[pin]
        inner: F,
        start: Instant,
        callback: Option<C>, // This is an Option because the future might be polled even after completion
    }
}

impl<F, C> TimedFuture<F, C>
where
    F: Future,
    C: FnOnce(Duration),
{
    pub fn new(inner: F, callback: C) -> Self {
        Self {
            inner,
            callback: Some(callback),
            start: Instant::now(),
        }
    }
}

impl<F, C> Future for TimedFuture<F, C>
where
    F: Future,
    C: FnOnce(Duration),
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let poll_result = this.inner.poll(cx);

        if poll_result.is_ready() {
            let elapsed = this.start.elapsed();
            if let Some(callback) = this.callback.take() {
                callback(elapsed);
            }
        }

        poll_result
    }
}

pub fn create_timed_future<F, C>(future: F, callback: C) -> TimedFuture<F, C>
where
    F: Future,
    C: FnOnce(Duration),
{
    TimedFuture::new(future, callback)
}

pub struct CounterGuard<'a, F>
where
    F: Fn(&str, i64),
{
    node_addr: &'a str,
    func: F,
}

impl<'a, F> CounterGuard<'a, F>
where
    F: Fn(&str, i64),
{
    pub fn new(node_addr: &'a str, func: F) -> Self {
        func(node_addr, 1);
        Self { node_addr, func }
    }
}

impl<'a, F> Drop for CounterGuard<'a, F>
where
    F: Fn(&str, i64),
{
    fn drop(&mut self) {
        (self.func)(self.node_addr, -1);
    }
}

pub mod raft_metrics {
    pub mod network {
        use std::{
            collections::HashMap,
            sync::Mutex,
            time::{Duration, Instant},
        };

        use once_cell::sync::Lazy;

        struct RaftMetrics {
            fail_connect_to_peer: HashMap<String, u64>,
            sent_bytes: HashMap<String, u64>,
            recv_bytes: HashMap<String, u64>,
            sent_failures: HashMap<String, u64>,
            snapshot_send_success: HashMap<String, u64>,
            snapshot_send_failure: HashMap<String, u64>,
            snapshot_recv_success: HashMap<String, u64>,
            snapshot_recv_failure: HashMap<String, u64>,
            snapshot_send_inflights: HashMap<String, u64>,
            snapshot_recv_inflights: HashMap<String, u64>,
            snapshot_sent_seconds: HashMap<String, Vec<Duration>>,
            snapshot_recv_seconds: HashMap<String, Vec<Duration>>,
            snapshot_size: Vec<u64>,
            last_snapshot_creation_time: Option<Instant>,
        }

        impl RaftMetrics {
            fn new() -> Self {
                RaftMetrics {
                    fail_connect_to_peer: HashMap::new(),
                    sent_bytes: HashMap::new(),
                    recv_bytes: HashMap::new(),
                    sent_failures: HashMap::new(),
                    snapshot_send_success: HashMap::new(),
                    snapshot_send_failure: HashMap::new(),
                    snapshot_recv_success: HashMap::new(),
                    snapshot_recv_failure: HashMap::new(),
                    snapshot_send_inflights: HashMap::new(),
                    snapshot_recv_inflights: HashMap::new(),
                    snapshot_sent_seconds: HashMap::new(),
                    snapshot_recv_seconds: HashMap::new(),
                    snapshot_size: Vec::new(),
                    last_snapshot_creation_time: None,
                }
            }
        }

        static RAFT_METRICS: Lazy<Mutex<RaftMetrics>> =
            Lazy::new(|| Mutex::new(RaftMetrics::new()));

        pub fn incr_fail_connect_to_peer(node_addr: &str) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let count = metrics
                .fail_connect_to_peer
                .entry(node_addr.into())
                .or_insert(0);
            *count += 1;
        }

        pub fn incr_sent_bytes(node_addr: &str, bytes: u64) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let count = metrics.sent_bytes.entry(node_addr.into()).or_insert(0);
            *count += bytes;
        }

        pub fn incr_recv_bytes(node_addr: &str, bytes: u64) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let count = metrics.recv_bytes.entry(node_addr.into()).or_insert(0);
            *count += bytes;
        }

        pub fn incr_sent_failures(node_addr: &str) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let count = metrics.sent_failures.entry(node_addr.into()).or_insert(0);
            *count += 1;
        }

        pub fn incr_snapshot_send_success(node_addr: &str) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let count = metrics
                .snapshot_send_success
                .entry(node_addr.into())
                .or_insert(0);
            *count += 1;
        }

        pub fn incr_snapshot_send_failure(node_addr: &str) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let count = metrics
                .snapshot_send_failure
                .entry(node_addr.into())
                .or_insert(0);
            *count += 1;
        }

        pub fn incr_snapshot_recv_success(node_addr: &str) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let count = metrics
                .snapshot_recv_success
                .entry(node_addr.into())
                .or_insert(0);
            *count += 1;
        }

        pub fn incr_snapshot_recv_failure(node_addr: &str) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let count = metrics
                .snapshot_recv_failure
                .entry(node_addr.into())
                .or_insert(0);
            *count += 1;
        }

        pub fn incr_snapshot_send_inflight(node_addr: &str, increment_cnt: i64) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let count = metrics
                .snapshot_send_inflights
                .entry(node_addr.into())
                .or_insert(0);
            if increment_cnt < 0 {
                *count = count.saturating_sub((-increment_cnt) as u64);
            } else {
                *count = count.saturating_add(increment_cnt as u64);
            }
        }

        pub fn incr_snapshot_recv_inflight(node_addr: &str, increment_cnt: i64) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let count = metrics
                .snapshot_recv_inflights
                .entry(node_addr.into())
                .or_insert(0);
            if increment_cnt < 0 {
                *count = count.saturating_sub((-increment_cnt) as u64);
            } else {
                *count = count.saturating_add(increment_cnt as u64);
            }
        }

        pub fn incr_snapshot_sent_seconds(node_addr: &str, duration: Duration) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let durations = metrics
                .snapshot_sent_seconds
                .entry(node_addr.into())
                .or_default();
            durations.push(duration);
        }

        pub fn incr_snapshot_recv_seconds(node_addr: &str, duration: Duration) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let durations = metrics
                .snapshot_recv_seconds
                .entry(node_addr.into())
                .or_default();
            durations.push(duration);
        }

        pub fn add_snapshot_size(size: u64) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            metrics.snapshot_size.push(size);
        }

        pub fn set_last_snapshot_creation_time(time: Instant) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            metrics.last_snapshot_creation_time = Some(time);
        }
    }
}
