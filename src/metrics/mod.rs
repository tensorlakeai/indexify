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
            sync::RwLock,
            time::{Duration, Instant},
        };

        use once_cell::sync::Lazy;
        use serde::Serialize;

        #[derive(Serialize)]
        pub struct MetricsSnapshot {
            pub fail_connect_to_peer: HashMap<String, u64>,
            pub sent_bytes: HashMap<String, u64>,
            pub recv_bytes: HashMap<String, u64>,
            pub sent_failures: HashMap<String, u64>,
            pub snapshot_send_success: HashMap<String, u64>,
            pub snapshot_send_failure: HashMap<String, u64>,
            pub snapshot_recv_success: HashMap<String, u64>,
            pub snapshot_recv_failure: HashMap<String, u64>,
            pub snapshot_send_inflights: HashMap<String, u64>,
            pub snapshot_recv_inflights: HashMap<String, u64>,
            pub snapshot_sent_seconds: HashMap<String, Vec<Duration>>,
            pub snapshot_recv_seconds: HashMap<String, Vec<Duration>>,
            pub snapshot_size: Vec<u64>,
            pub last_snapshot_creation_time: Duration,
        }

        struct Metric<T: Default + Clone> {
            data: RwLock<T>,
        }

        impl<T: Default + Clone> Metric<T> {
            fn new() -> Self {
                Metric {
                    data: RwLock::new(T::default()),
                }
            }

            fn read(&self) -> T {
                self.data.read().unwrap().clone()
            }

            fn write<F>(&self, f: F)
            where
                F: FnOnce(&mut T),
            {
                let mut data = self.data.write().unwrap();
                f(&mut *data);
            }
        }

        struct RaftMetrics {
            fail_connect_to_peer: Metric<HashMap<String, u64>>,
            sent_bytes: Metric<HashMap<String, u64>>,
            recv_bytes: Metric<HashMap<String, u64>>,
            sent_failures: Metric<HashMap<String, u64>>,
            snapshot_send_success: Metric<HashMap<String, u64>>,
            snapshot_send_failure: Metric<HashMap<String, u64>>,
            snapshot_recv_success: Metric<HashMap<String, u64>>,
            snapshot_recv_failure: Metric<HashMap<String, u64>>,
            snapshot_send_inflights: Metric<HashMap<String, u64>>,
            snapshot_recv_inflights: Metric<HashMap<String, u64>>,
            snapshot_sent_seconds: Metric<HashMap<String, Vec<Duration>>>,
            snapshot_recv_seconds: Metric<HashMap<String, Vec<Duration>>>,
            snapshot_size: Metric<Vec<u64>>,
            last_snapshot_creation_time: Metric<Option<Instant>>,
        }

        impl RaftMetrics {
            fn new() -> Self {
                Self {
                    fail_connect_to_peer: Metric::new(),
                    sent_bytes: Metric::new(),
                    recv_bytes: Metric::new(),
                    sent_failures: Metric::new(),
                    snapshot_send_success: Metric::new(),
                    snapshot_send_failure: Metric::new(),
                    snapshot_recv_success: Metric::new(),
                    snapshot_recv_failure: Metric::new(),
                    snapshot_send_inflights: Metric::new(),
                    snapshot_recv_inflights: Metric::new(),
                    snapshot_sent_seconds: Metric::new(),
                    snapshot_recv_seconds: Metric::new(),
                    snapshot_size: Metric::new(),
                    last_snapshot_creation_time: Metric::new(),
                }
            }
        }

        //  TODO: Make the locks more granular and atomic
        static RAFT_METRICS: Lazy<RaftMetrics> = Lazy::new(RaftMetrics::new);

        pub fn incr_fail_connect_to_peer(node_addr: &str) {
            RAFT_METRICS.fail_connect_to_peer.write(|metric| {
                *metric.entry(node_addr.into()).or_insert(0) += 1;
            });
        }

        pub fn incr_sent_bytes(node_addr: &str, bytes: u64) {
            RAFT_METRICS.sent_bytes.write(|metric| {
                *metric.entry(node_addr.into()).or_insert(0) += bytes;
            });
        }

        pub fn incr_recv_bytes(node_addr: &str, bytes: u64) {
            RAFT_METRICS.recv_bytes.write(|metric| {
                *metric.entry(node_addr.into()).or_insert(0) += bytes;
            });
        }

        pub fn incr_sent_failures(node_addr: &str) {
            RAFT_METRICS.sent_failures.write(|metric| {
                *metric.entry(node_addr.into()).or_insert(0) += 1;
            });
        }

        pub fn incr_snapshot_send_success(node_addr: &str) {
            RAFT_METRICS.snapshot_send_success.write(|metric| {
                *metric.entry(node_addr.into()).or_insert(0) += 1;
            });
        }

        pub fn incr_snapshot_send_failure(node_addr: &str) {
            RAFT_METRICS.snapshot_send_failure.write(|metric| {
                *metric.entry(node_addr.into()).or_insert(0) += 1;
            });
        }

        pub fn incr_snapshot_recv_success(node_addr: &str) {
            RAFT_METRICS.snapshot_recv_success.write(|metric| {
                *metric.entry(node_addr.into()).or_insert(0) += 1;
            });
        }

        pub fn incr_snapshot_recv_failure(node_addr: &str) {
            RAFT_METRICS.snapshot_recv_failure.write(|metric| {
                *metric.entry(node_addr.into()).or_insert(0) += 1;
            });
        }

        pub fn incr_snapshot_send_inflight(node_addr: &str, increment_cnt: i64) {
            RAFT_METRICS.snapshot_send_inflights.write(|metric| {
                let count = metric.entry(node_addr.into()).or_insert(0);
                if increment_cnt < 0 {
                    *count = count.saturating_sub((-increment_cnt) as u64);
                } else {
                    *count = count.saturating_add(increment_cnt as u64);
                }
            });
        }

        pub fn incr_snapshot_recv_inflight(node_addr: &str, increment_cnt: i64) {
            RAFT_METRICS.snapshot_recv_inflights.write(|metric| {
                let count = metric.entry(node_addr.into()).or_insert(0);
                if increment_cnt < 0 {
                    *count = count.saturating_sub((-increment_cnt) as u64);
                } else {
                    *count = count.saturating_add(increment_cnt as u64);
                }
            });
        }

        pub fn incr_snapshot_sent_seconds(node_addr: &str, duration: Duration) {
            RAFT_METRICS.snapshot_sent_seconds.write(|metric| {
                let durations = metric.entry(node_addr.into()).or_default();
                durations.push(duration);
            });
        }

        pub fn incr_snapshot_recv_seconds(node_addr: &str, duration: Duration) {
            RAFT_METRICS.snapshot_recv_seconds.write(|metric| {
                let durations = metric.entry(node_addr.into()).or_default();
                durations.push(duration);
            });
        }

        pub fn add_snapshot_size(size: u64) {
            RAFT_METRICS.snapshot_size.write(|metric| {
                metric.push(size);
            });
        }

        pub fn set_last_snapshot_creation_time(time: Instant) {
            RAFT_METRICS.last_snapshot_creation_time.write(|metric| {
                *metric = Some(time);
            });
        }

        pub fn get_metrics_snapshot() -> MetricsSnapshot {
            MetricsSnapshot {
                fail_connect_to_peer: RAFT_METRICS.fail_connect_to_peer.read(),
                sent_bytes: RAFT_METRICS.sent_bytes.read(),
                recv_bytes: RAFT_METRICS.recv_bytes.read(),
                sent_failures: RAFT_METRICS.sent_failures.read(),
                snapshot_send_success: RAFT_METRICS.snapshot_send_success.read(),
                snapshot_send_failure: RAFT_METRICS.snapshot_send_failure.read(),
                snapshot_recv_success: RAFT_METRICS.snapshot_recv_success.read(),
                snapshot_recv_failure: RAFT_METRICS.snapshot_recv_failure.read(),
                snapshot_send_inflights: RAFT_METRICS.snapshot_send_inflights.read(),
                snapshot_recv_inflights: RAFT_METRICS.snapshot_recv_inflights.read(),
                snapshot_sent_seconds: RAFT_METRICS.snapshot_sent_seconds.read(),
                snapshot_recv_seconds: RAFT_METRICS.snapshot_recv_seconds.read(),
                snapshot_size: RAFT_METRICS.snapshot_size.read(),
                last_snapshot_creation_time: RAFT_METRICS
                    .last_snapshot_creation_time
                    .read()
                    .map(|t| t.elapsed())
                    .unwrap_or_default(),
            }
        }
    }
}
