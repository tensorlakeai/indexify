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
        }

        impl RaftMetrics {
            fn new() -> Self {
                RaftMetrics {
                    fail_connect_to_peer: HashMap::new(),
                    sent_bytes: HashMap::new(),
                    recv_bytes: HashMap::new(),
                    sent_failures: HashMap::new(),
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
    }
}
