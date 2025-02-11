import prometheus_client

# This file contains all metrics used by TaskFetcher.

metric_server_registration_errors = prometheus_client.Counter(
    "server_registration_errors",
    "Number of failed Executor registrations at the server",
)
