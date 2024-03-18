pub struct CounterGuard<F>
where
    F: Fn(String, i64),
{
    node_addr: String,
    func: F,
}

impl<F> CounterGuard<F>
where
    F: Fn(String, i64),
{
    pub fn new(node_addr: String, func: F) -> Self {
        func(node_addr.clone(), 1);
        Self { node_addr, func }
    }
}

impl<F> Drop for CounterGuard<F>
where
    F: Fn(String, i64),
{
    fn drop(&mut self) {
        (self.func)(self.node_addr.clone(), -1);
    }
}

pub mod raft_metrics {
    pub mod network {
        use once_cell::sync::Lazy;
        use std::collections::HashMap;
        use std::sync::Mutex;

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
                }
            }
        }

        static RAFT_METRICS: Lazy<Mutex<RaftMetrics>> =
            Lazy::new(|| Mutex::new(RaftMetrics::new()));

        pub fn incr_fail_connect_to_peer(node_addr: String) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let count = metrics.fail_connect_to_peer.entry(node_addr).or_insert(0);
            *count += 1;
        }

        pub fn incr_sent_bytes(node_addr: String, bytes: u64) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let count = metrics.sent_bytes.entry(node_addr).or_insert(0);
            *count += bytes;
        }

        pub fn incr_recv_bytes(node_addr: String, bytes: u64) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let count = metrics.recv_bytes.entry(node_addr).or_insert(0);
            *count += bytes;
        }

        pub fn incr_sent_failures(node_addr: String) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let count = metrics.sent_failures.entry(node_addr).or_insert(0);
            *count += 1;
        }

        pub fn incr_snapshot_send_success(node_addr: String) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let count = metrics.snapshot_send_success.entry(node_addr).or_insert(0);
            *count += 1;
        }

        pub fn incr_snapshot_send_failure(node_addr: String) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let count = metrics.snapshot_send_failure.entry(node_addr).or_insert(0);
            *count += 1;
        }

        pub fn incr_snapshot_recv_success(node_addr: String) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let count = metrics.snapshot_recv_success.entry(node_addr).or_insert(0);
            *count += 1;
        }

        pub fn incr_snapshot_recv_failure(node_addr: String) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let count = metrics.snapshot_recv_failure.entry(node_addr).or_insert(0);
            *count += 1;
        }

        pub fn incr_snapshot_send_inflight(node_addr: String, increment_cnt: i64) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let count = metrics
                .snapshot_send_inflights
                .entry(node_addr)
                .or_insert(0);
            if increment_cnt < 0 {
                *count = count.saturating_sub((-increment_cnt) as u64);
            } else {
                *count = count.saturating_add(increment_cnt as u64);
            }
        }

        pub fn incr_snapshot_recv_inflight(node_addr: String, increment_cnt: i64) {
            let mut metrics = RAFT_METRICS.lock().unwrap();
            let count = metrics
                .snapshot_recv_inflights
                .entry(node_addr)
                .or_insert(0);
            if increment_cnt < 0 {
                *count = count.saturating_sub((-increment_cnt) as u64);
            } else {
                *count = count.saturating_add(increment_cnt as u64);
            }
        }
    }
}
