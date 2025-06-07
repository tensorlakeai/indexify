import prometheus_client

from ..monitoring.metrics import latency_metric_for_fast_operation

metric_grpc_server_channel_creations = prometheus_client.Counter(
    "grpc_server_channel_creations",
    "Number of times a channel to gRPC Server was created",
)
metric_grpc_server_channel_creation_retries = prometheus_client.Counter(
    "grpc_server_channel_creation_retries",
    "Number of retries during a channel creation to gRPC Server",
)
metric_grpc_server_channel_creation_latency: prometheus_client.Histogram = (
    latency_metric_for_fast_operation(
        "grpc_server_channel_creation",
        "gRPC server channel creation",
    )
)
