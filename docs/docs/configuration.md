# Service Configuration

Indexify is configured by a YAML configuration file. The easiest way to start is by generating it with the CLI or by downloading a sample configuration file, and then tweaking it to fit your needs.

### Generate with CLI
```
indexify server init-config /tmp/indexify.yaml
```

### Sample Configuration
```
listen_addr: 0.0.0.0:8900

available_models:
- model: all-minilm-l12-v2
  device: cpu
- model: text-embedding-ada-002
  device: remote

openai:
  api_key: xxxxx

index_config:
  index_store: Qdrant
  db_url: sqlite://indexify.db
  qdrant_config:
    addr: "http://172.20.0.8:6334"
```

### Configuration Reference

* `listen_addr` -  Address on which the service binds to for api requests.

* `available_models` - List of available embedding models. Model attributes - 
    * `model` - Name of the model
    * `device` - Device on which the model runs on. Possible values `cpu` or `gpu`.

* `openai` - Open AI related attributes - 
    * `api_key` - API Key to access OpenAI. The environment variable `OPENAI_API_KEY` can be also used to set the openai api key.

* `index_config` - Vector Index related configurations.
    * `index_store` - Name of the index store to use.
    * `db_url` - The URL of the database to store metadata related to documents. Possible values are connection strings for sqlite, postgres and mysql.
    * `qdrant_config` - Qdrant Vector store config.
        * `addr` - Address of the qdrant server.