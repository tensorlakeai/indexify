use std::hash::Hash;

use prometheus_client::{
    encoding::{EncodeLabelSet, EncodeLabelValue},
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram, info::Info},
    registry::Registry,
};

#[derive(Debug)]
pub struct Metrics {
    pub(crate) healthy_gauge: Gauge,
    pub(crate) grpc_server_channel_creations: Counter,
    pub(crate) grpc_server_channel_creation_retries: Counter,
    pub(crate) grpc_server_channel_creation_latency: Histogram,
    pub(crate) executor_info: Info<Vec<(String, String)>>,
    pub(crate) executor_state: Family<ExecutorStateLabel, Counter>,
    pub(crate) executor_events_pushed: Counter,
    pub(crate) executor_event_push_errors: Counter,
    pub(crate) desired_state_streams: Counter,
    pub(crate) desired_state_stream_errors: Counter,
    pub(crate) state_reconciliations: Counter,
    pub(crate) state_reconciliation_errors: Counter,
    pub(crate) state_reconciliation_latency: Histogram,
    pub(crate) state_report_rpcs: Counter,
    pub(crate) state_report_rpc_errors: Counter,
    pub(crate) state_report_rpc_latency: Histogram,
}

impl Metrics {
    pub fn new(registry: &mut Registry, executor_info_vec: Vec<(String, String)>) -> Self {
        let healthy_gauge = Gauge::default();
        registry.register(
            "healthy",
            "1 if the executor is healthy, 0 otherwise",
            healthy_gauge.clone(),
        );
        let grpc_server_channel_creations = Counter::default();
        registry.register(
            "grpc_server_channel_creations",
            "Number of times a channel to gRPC Server was created",
            grpc_server_channel_creations.clone(),
        );
        let grpc_server_channel_creation_retries = Counter::default();
        registry.register(
            "grpc_server_channel_creation_retries",
            "Number of retries during a channel creation to gRPC Server",
            grpc_server_channel_creation_retries.clone(),
        );
        // TODO: peek at impl in ::new
        let grpc_server_channel_creation_latency =
            Histogram::new(latency_metric_for_fast_operation());
        registry.register(
            "grpc_server_channel_creation_latency_seconds",
            "Latency of gRPC server channel creation",
            grpc_server_channel_creation_latency.clone(),
        );
        let executor_info = Info::new(executor_info_vec);
        let executor_state = Family::<ExecutorStateLabel, Counter>::default();
        registry.register(
            "executor_state",
            "Current Executor state",
            executor_state.clone(),
        );
        let executor_events_pushed = Counter::default();
        registry.register(
            "executor_events_pushed",
            "Number of events pushed to collector",
            executor_events_pushed.clone(),
        );
        let executor_event_push_errors = Counter::default();
        registry.register(
            "executor_event_push_errors",
            "Number of errors while pushing events to collector",
            executor_event_push_errors.clone(),
        );
        let desired_state_streams = Counter::default();
        registry.register(
            "desired_state_streams",
            "Number of desired state streams created",
            desired_state_streams.clone(),
        );
        let desired_state_stream_errors = Counter::default();
        registry.register(
            "desired_state_stream_errors",
            "Number of desired state stream errors",
            desired_state_stream_errors.clone(),
        );
        let state_reconciliations = Counter::default();
        registry.register(
            "state_reconciliations",
            "Number of Executor state reconciliations",
            state_reconciliations.clone(),
        );
        let state_reconciliation_errors = Counter::default();
        registry.register(
            "state_reconciliation_errors",
            "Number of Executor state reconciliation errors after all retries",
            state_reconciliation_errors.clone(),
        );
        let state_reconciliation_latency = Histogram::new(latency_metric_for_fast_operation());
        registry.register(
            "state_reconciliation_latency_seconds",
            "Latency of Executor state reconciliation",
            state_reconciliation_latency.clone(),
        );
        let state_report_rpcs = Counter::default();
        registry.register(
            "state_report_rpcs",
            "Number of Executor state report RPCs to Server",
            state_report_rpcs.clone(),
        );
        let state_report_rpc_errors = Counter::default();
        registry.register(
            "state_report_rpc_errors",
            "Number of Executor state report RPC errors",
            state_report_rpc_errors.clone(),
        );
        let state_report_rpc_latency = Histogram::new(latency_metric_for_fast_operation());
        registry.register(
            "state_report_rpc_latency_seconds",
            "Latency of Executor state report RPC to Server",
            state_report_rpc_latency.clone(),
        );

        Self {
            healthy_gauge,
            grpc_server_channel_creations,
            grpc_server_channel_creation_retries,
            grpc_server_channel_creation_latency,
            executor_info,
            executor_state,
            executor_events_pushed,
            executor_event_push_errors,
            desired_state_streams,
            desired_state_stream_errors,
            state_reconciliations,
            state_reconciliation_errors,
            state_reconciliation_latency,
            state_report_rpcs,
            state_report_rpc_errors,
            state_report_rpc_latency,
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ExecutorStateLabel {
    states: ExecutorState,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
enum ExecutorState {
    Starting,
    Running,
    ShuttingDown,
}

fn latency_metric_for_fast_operation() -> impl IntoIterator<Item = f64> {
    []
}
