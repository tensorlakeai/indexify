# Local Testing with FoundationDB

This directory contains the configuration and scripts needed to run Indexify against a local FoundationDB instance.

## Files

- `compose.yaml` — Docker Compose services (FDB 7.4.5 + Jaeger for tracing)
- `fdb.cluster` — FDB cluster file pointing to the local Docker container (`docker:docker@127.0.0.1:4500`)
- `start-fdb-cluster.sh` — Entrypoint script mounted into the FDB container; configures a single-node memory database on boot
- `testing_config.yaml` — Server config that points Indexify at the local FDB cluster

## Starting FDB

```bash
cd crates/server/testing
docker compose up -d foundationdb
```

The container will initialize a single-node memory cluster automatically. It is ready when `docker compose logs foundationdb` shows `Configuring new single memory FDB database`.

To also start Jaeger for local tracing (optional):

```bash
docker compose up -d
```

Jaeger UI is available at http://localhost:16686.

## Running the server against FDB

From the repo root:

```bash
./indexify-server --config crates/server/testing/testing_config.yaml
```

## Running tests against FDB

Set `INDEXIFY_TESTING_DRIVER_FDB_CLUSTER` to point at the cluster file before running tests:

```bash
INDEXIFY_TESTING_DRIVER_FDB_CLUSTER=crates/server/testing/fdb.cluster cargo test
```

When this env var is set, the test harness uses FDB instead of spinning up a temporary RocksDB instance. When it is unset, tests use RocksDB and require no external services.

## Stopping FDB

```bash
cd crates/server/testing
docker compose down
```

Add `-v` to also remove the persistent data volume:

```bash
docker compose down -v
```
