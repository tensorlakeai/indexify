# Service Configuration

Indexify is configured by a YAML configuration file. The easiest way to start is by generating it with the CLI or by downloading a sample configuration file, and then tweaking it to fit your needs.

## Generate with CLI

```shell
indexify init-config --config-path /tmp/indexify.yaml
```

## Configuration Reference

### Network Configuration

```yaml
listen_if: 0.0.0.0
api_port: 8900
coordinator_port: 8950
raft_port: 8970
coordinator_addr: 0.0.0.0:8950
```

* **listen_if:** The interface on which the servers listens on. Typically you would want to listen on all interfaces.
* **api_port:** The port in which the application facing API server is exposed. This is the HTTP port on which applications upload data, create extraction policies and retreived extracted data from indexes.
* **coordinator_port:** Port on which the coordinator is exposed. This is available as a separate configuration becasue in the dev mode, we expose both the api server and the coordinator server in the same process.
* **raft_port:** Port on which internal messages across coordinator nodes are transmitted. This is only needed if Indexify is either started as a coordinator or in dev mode.

### Blob Storage Configuration
```yaml
blob_storage:
  backend: disk
  disk:
     path: /tmp/indexify-blob-storage
```
```yaml
blob_storage:
  backend: s3
  s3:
    bucket: indexifydata
    region: us-east-1
```
### Vector Index Storage
* **index_store:** (Default: LancDb): Name of the vector be, possible values: `LancdDb`, `Qdrant`, `PgVector`

#### Qdrant Config
`addr`: Address of the Qdrant http endpoint

#### Pg Vector Config
`addr`: Address of Postgres

#### LanceDb Config
`path`: Path of the database

```yaml
index_config:
  index_store: Qdrant
  qdrant_config:
    addr: "http://127.0.0.1:6334"
```
```yaml
index_config:
  index_store: PgVector
  pg_vector_config:
    addr: postgres://postgres:postgres@localhost/indexify
    m: 16
    efconstruction: 64
```

### Caching
```yaml
cache:
  backend: none
```
```yaml
cache:
  backend: memory
  memory:
    max_size: 1000000
```
```yaml
cache:
  backend: redis
  redis:
    addr: redis://localhost:6379
```

### API Server SSL

```yaml
tls:
  api: false
  ca_file: .dev-tls/ca.crt        # Path to the CA certificate
  cert_file: .dev-tls/server.crt  # Path to the server certificate
  key_file: .dev-tls/server.key   # Path to the server private key
```
