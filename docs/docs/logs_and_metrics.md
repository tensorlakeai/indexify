# Logs and Metrics

Indexify uses [OpenTelemetry](https://opentelemetry.io/) to bundle logs and metrics.
All logs are exported using the unified [OTLP](https://opentelemetry.io/docs/specs/otel/protocol/). You can plug in any downstream data-collectors and visualizers

## Logs

Indexify exposes logs (traces) and metrics to manage the system better.
The service name is `indexify-service`.
You can modify the log-level using the RUST_LOG environment variable.
<!-- TODO: this should be set before Docker is run -->
These traces are automatically displayed in stdout.

### Visualizing logs in Jaeger

You can also use Jaeger to have a better view of the tracers, view flamegraphs to identify bottlenecks in your application.
For this, run Jaeger inside docker as such:

```sh
docker run --rm --name jaeger \
  -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 \
  -e COLLECTOR_OTLP_ENABLED=true \
  --network="host" \
  jaegertracing/all-in-one:1.49
```

As you make specific API calls to your indexify application (such as "localhost:8900/namespaces"), traces are populated, which you can then view spans and details in Jaeger.

![Traces](docs/docs/images/jaeger/traces.png)
![Detailed Spans & Logs](docs/docs/images/jaeger/traces.png)
![Statistics](docs/docs/images/jaeger/stats.png)
![Flamegraph](docs/docs/images/jaeger/flamegraph.png)

### Visualizin Metrics

The coordinator, executor and server individually expose metrics using [this package](https://github.com/ttys3/axum-otel-metrics).
To visualize these metrics, you can use any visualization library that can collect and parse OLTP data.

For example, we can use prometheus and grafana. We define the data-sources that prometheus will parse for the metrics in a `prometheus.yml` as such:

```yaml
global:
  scrape_interval:     5s # Set the scrape interval to every 15 seconds. Default is every 1 minute.
  evaluation_interval: 5s # Evaluate rules every 15 seconds. The default is every 1 minute.
  # scrape_timeout is set to the global default (10s).

scrape_configs:
  - job_name: 'indexify-server'
    static_configs:
      - targets: ['localhost:8900']
  - job_name: 'indexify-executor'
    static_configs:
      - targets: ['localhost:8901']
  - job_name: 'indexify-coordinator'
    static_configs:
      - targets: ['localhost:8902']
```

We can then use this config file to spin up the prometheus instance using the following docker command:

```sh
docker run --rm \
    --name prometheus \
    --network="host" \
    -v "./prometheus.yml:/etc/prometheus/prometheus.yml" \
    prom/prometheus
```

as well as grafana to visualize the metrics

```sh
docker run --rm \
  --name grafana \
  --network="host" \
  grafana/grafana
```

After setting up Prometheus as a data-source, we can then explore the data in real-time and build a dashboard.

![Grafana Explore Data](docs/docs/images/grafana/total_requests.png)
