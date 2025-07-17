import prometheus_client

from ..monitoring.metrics import latency_metric_for_fast_operation

metric_state_report_rpcs = prometheus_client.Counter(
    "state_report_rpcs",
    "Number of Executor state report RPCs to Server",
)
metric_state_report_rpc_errors = prometheus_client.Counter(
    "state_report_rpc_errors",
    "Number of Executor state report RPC errors",
)
metric_state_report_rpc_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "state_report_rpc", "Executor state report rpc to Server"
    )
)
