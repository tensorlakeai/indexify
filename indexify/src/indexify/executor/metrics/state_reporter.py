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
metric_state_report_message_size_mb = prometheus_client.Histogram(
    "state_report_message_size_mb",
    "Size of state report messages sent from Executor to Server in MB",
    # We cap message size at less than 100 MB but adding more buckets in case we go over it by mistake.
    buckets=[
        0.01,
        0.1,
        0.5,
        1.0,
        5.0,
        10.0,
        20.0,
        30.0,
        40.0,
        50.0,
        60.0,
        70.0,
        80.0,
        90.0,
        100.0,
        150.0,
        200.0,
        1000.0,
        10000.0,
    ],
)
metric_state_report_messages_over_size_limit = prometheus_client.Counter(
    "state_report_messages_over_size_limit",
    "Number of state report messages sent from Executor to Server that exceeded size limit",
)
metric_state_report_message_fragmentations = prometheus_client.Counter(
    "state_report_message_fragmentations",
    "Number of times state report messages were fragmented due to size limits",
)
