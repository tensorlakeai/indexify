# Accessing metrics

Indexify servers and coordinators export the metrics in Prometheus format for easy integration with existing services.

The Central Coordinator is runs on port 8960 while the server runs on port 8900 by default.

| Coordinator URL | Description                                       |
| --------------- | ------------------------------------------------- |
| /metrics        | cluster metrics for content upload and extraction |

| Server URL      | Description                                            |
| --------------- | ------------------------------------------------------ |
| /metrics        | http api metrics on this node                          |
| /metrics/ingest | metrics for content upload and extraction on this node |

The following metrics are specific to Indexify cluster operation:

- indexify_coordinator_executors_online
- indexify_coordinator_tasks_in_progress
- indexify_coordinator_content_uploads_total
- indexify_coordinator_content_bytes_uploaded_total
- indexify_coordinator_content_extracted_total
- indexify_coordinator_content_bytes_extracted_total
- indexify_coordinator_tasks_completed_total
- indexify_coordinator_tasks_errored_total

This is an example of prometheus configuration to collect metrics from server and coordinator hosts:

```yaml
scrape_configs:
  - job_name: "indexify-server-ingest"
    metrics_path: /metrics/ingest
    static_configs:
      - targets: ["server:8900"]

  - job_name: "indexify-server"
    metrics_path: /metrics
    static_configs:
      - targets: ["server:8900"]

  - job_name: "indexify-coordinator"
    metrics_path: /metrics
    static_configs:
      - targets: ["coordinator:8960"]
```
