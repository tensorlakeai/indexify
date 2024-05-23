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
coordinator_http_port: 8960
raft_port: 8970
coordinator_addr: 0.0.0.0:8950
```

* **listen_if:** The interface on which the servers listens on. Typically you would want to listen on all interfaces.
* **api_port:** The port in which the application facing API server is exposed. This is the HTTP port on which applications upload data, create extraction policies and retrieved extracted data from indexes.
* **coordinator_port:** Port on which the coordinator is exposed. This is available as a separate configuration becasue in the dev mode, we expose both the api server and the coordinator server in the same process.
* **coordinator_http_port** Port to access coordinator metrics
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
```yaml
index_config:
  index_store: Qdrant
  qdrant_config:
    addr: "http://127.0.0.1:6334"
```
#### Pg Vector Config
`addr`: Address of Postgres

```yaml
index_config:
  index_store: PgVector
  pg_vector_config:
    addr: postgres://postgres:postgres@localhost/indexify
    m: 16
    efconstruction: 64
```

#### LanceDb Config
`path`: Path of the database

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

### API Server TLS

To set up mTLS for the indexify server, you first need to create a root certificate along with a client certificate and key pair along with a server certificate and key pair. The commands below will generate the certificates and keys and store them in a folder called `.dev-tls`.

```
local-dev-tls-insecure: ## Generate local development TLS certificates (insecure)
	@mkdir -p .dev-tls && \
	openssl req -x509 -newkey rsa:4096 -keyout .dev-tls/ca.key -out .dev-tls/ca.crt -days 365 -nodes -subj "/C=US/ST=TestState/L=TestLocale/O=IndexifyOSS/CN=localhost" && \
	openssl req -new -newkey rsa:4096 -keyout .dev-tls/server.key -out .dev-tls/server.csr -nodes -config ./client_cert_config && \
	openssl x509 -req -in .dev-tls/server.csr -CA .dev-tls/ca.crt -CAkey .dev-tls/ca.key -CAcreateserial -out .dev-tls/server.crt -days 365 -extensions v3_ca -extfile ./client_cert_config && \
	openssl req -new -nodes -out .dev-tls/client.csr -newkey rsa:2048 -keyout .dev-tls/client.key -config ./client_cert_config && \
	openssl x509 -req -in .dev-tls/client.csr -CA .dev-tls/ca.crt -CAkey .dev-tls/ca.key -CAcreateserial -out .dev-tls/client.crt -days 365 -extfile ./client_cert_config -extensions v3_ca
```

Once you have the certificates and keys generated, add the config below to your server config and provide the paths to where you have stored the root certificate and the server certificate and key pair.

```yaml
tls:
  api: true
  ca_file: .dev-tls/ca.crt        # Path to the CA certificate
  cert_file: .dev-tls/server.crt  # Path to the server certificate
  key_file: .dev-tls/server.key   # Path to the server private key
```
### HA configuration 

To setup mulitple coordinator nodes for high availability configuration, start with a single node, called a seed node. Create a separate configuration file for each additional coordinator instance. Each node should have a unique node_id field in configuration file. seed_node field should be set to ip address and port of the original coordinator node. 

Seed node:

```yaml
raft_port: 8970
node_id: 0
seed_node: localhost:8970
```

New node (replace 10.0.0.10 with actual seed node IP address, 8970 should match configured raft_port of the seed node):
```yaml
node_id: 1
seed_node: 10.0.0.10:8970
```

