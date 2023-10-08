# Developing Indexify

## Install Dependencies

### Rust Compiler

Install various rust related tools -

* Rust Compiler - <http://rustup.rs>
* Cargo tools - clippy, rustfmt is very helpful for formating and fixing warnings.

### Python Dependencies

Create a virtual env

```shell
python3.11 -m venv ve
source ve/bin/activate
```

Install the  extractors

```shell
pip install .
```

If you are working on the Python SDK then install the SDK as well

```shell
(cd sdk-py && pip install -e .)
```

### MAC OS

The following workaround is needed until PyO3 can detect virtualenvs in OSX and some Ubuntu versions

```shell
 export PYTHONPATH=${PYTHONPATH}:${PWD}/ve/lib/python3.11/site-packages
```

Install coreutils

```shell
brew install coreutils
```

## Running Tests

We currently depend on the Qdrant VectorDB and Postgres to test Indexify.

### Start Development Dependencies

```shell
make local-dev
```

### Run Tests

Run the unit and integration tests

```shell
cargo test -- --test-threads 1
```

## Running the service locally

### Build the Binary

Build the server in development mode

```shell
cargo build
```

### Create a development database

```shell
make local-dev
```

### Start the server

Once the binary is built start it with a default config -

```shell
./target/debug/indexify start-server -d -c local_config.yaml
```

## Visual Studio DevContainer

Visual Studio Code Devcontainers have been setup as well. Opening the codebase in VS Studio Code should prompt opening the project in a container. Once the container is up, test that the application can be compiled and run -

1. `make local-dev`
2. Install the Python Dependencies as described above.
3. Compile and Run the application as described above.

### OpenTelemetry

You can visualize all logs using Jaeger All-In-One which parses all logs from the OTLP

```
docker network create jaeger-prom-network
```

```
docker run --rm --name jaeger \
  -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 \
  -e METRICS_STORAGE_TYPE=prometheus \
  --network=jaeger-prom-network \
  -p 6831:6831/udp \
  -p 6832:6832/udp \
  -p 5778:5778 \
  -p 16686:16686 \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 14250:14250 \
  -p 14268:14268 \
  -p 14269:14269 \
  -p 9411:9411 \
  jaegertracing/all-in-one:1.49 \
  --log-level=debug \
  --prometheus.server-url=http://prometheus:9090
```

docker run --rm --name jaeger \
  -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 \
  -e METRICS_STORAGE_TYPE=prometheus \
  --network="host" \
  jaegertracing/all-in-one:1.49
```
 <!-- \
  --log-level=debug -->

<!-- --prometheus.query.support-spanmetrics-connector=true \ -->
<!-- -- -e JAEGER_DISABLED=true \ -->

To monitor metrics, we need to temporarily save them in a time-series database

```

docker run --rm \
    --name prometheus \
    -p 9090:9090 \
    --network=jaeger-prom-network \
    -v "./prometheus.yaml:/etc/prometheus/prometheus.yml" \
    prom/prometheus

```

```

docker run --rm \
  --name grafana \
  --network=jaeger-prom-network \
  -p 3000:3000 \
  grafana/grafana

```

```

docker run --rm \
  --name grafana \
  --network="host" \
  grafana/grafana

```

### Docker-compose

If you're within the dev container, you can call the docker-compose-v1 from within /usr/bin/
