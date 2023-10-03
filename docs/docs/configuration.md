# Service Configuration

Indexify is configured by a YAML configuration file. The easiest way to start is by generating it with the CLI or by downloading a sample configuration file, and then tweaking it to fit your needs.

### Generate with CLI
```shell
indexify server init-config /tmp/indexify.yaml
```

### Sample Configuration
```yaml
listen_addr: 0.0.0.0:8900

available_models:
- model: all-minilm-l12-v2
  device: cpu
  default: true
- model: text-embedding-ada-002
  device: remote

openai:
  api_key: xxxxx

db_url: postgres://postgres:postgres@172.20.0.5/indexify

coordinator_addr: "http://172.20.0.8950"

index_config:
  index_store: Qdrant
  qdrant_config:
    addr: "http://172.20.0.8:6334"
```

### Configuration Reference

* `listen_addr` -  Address on which the service binds to for api requests.

* `available_models` - List of available embedding models. Model attributes - 
    * `model` - Name of the model
    * `device` - Device on which the model runs on. Possible values `cpu` or `gpu`.
    * `default` - The default embedding model to use to create a default vector index view for every data repository.

* `openai` - Open AI related attributes - 
    * `api_key` - API Key to access OpenAI. The environment variable `OPENAI_API_KEY` can be also used to set the openai api key.

* `db_url` - The URL of the postgres database to store metadata related to documents.

* `coordinator_addr`: The address of the co-ordinator HTTP API that executors connect for getting work for extractors

* `index_config` - List of vector Index related configurations.
    * `index_store` - Name of the index store to use.
    * `qdrant_config` - Qdrant Vector store config.
        * `addr` - Address of the qdrant server.