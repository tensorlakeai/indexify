# Indexify

Indexify is a knowledge and memory retrieval service for Large Language Models. It faciliates in-context learning of LLMs by providing relevant context in a prompt or expsing relevant memory to AI agents.
It also faciliates efficient execution of fine tuned/pre-trained embedding models and expose them over APIs. Several state of the art retreival algorithms are implemented to provide a batteries-included retrieval experience.

Currently for production use-case, the embedding generation APIs are stable, while the other features are coming along.

## Why Use Indexify
1. Create indexes from documents and search them to retrieve relevant context for Large Language Model based inference.
2. Efficient execution of pre-trained and fine-tuned embedding models in a standalone service.
3. APIs to retreive context from indexes or generate embeddings from any application runtime.

## Getting Started

### Start the Service with a Vector Database
```
docker compose up indexify
```
*If you wish to use OpenAI for Embedding, please set the OPENAPI_API_KEY environment variable, or pass it through the configuration file(see below).*

### Create Index
```
curl -v -X POST http://localhost:8900/index/create   -H "Content-Type: application/json" -d '{"name": "myindex", "embedding_model": "all-minilm-l12-v2","metric": "Dot", "text_splitter": "new_line"}'
```

### Add Texts
```
curl -v -X POST http://localhost:8900/index/add   -H "Content-Type: application/json" -d '{"index": "myindex", "texts": [{"text": "Indexify is amazing!", "metadata":{"key": "k1"}}]}'
```

### Retreive
```
curl -v -X GET http://localhost:8900/index/search   -H "Content-Type: application/json" -d '{"index": "myindex", "query": "good", "k": 1}'
```

### Query Embeddings 
```
 curl -v -X GET http://localhost:8900/embeddings/generate   -H "Content-Type: application/json" -d '{"inputs": ["lol", "world"], "model": "all-minilm-l12-v2"}'
```

### Custom Configuration
Creating a custom configuration is easy by tweaking the default configuration -
```
docker run -v "$(pwd)":/indexify/config/ diptanu/indexify init-config ./config/custom_config.yaml
```
This will create the default configuration in the current directory in `custom_config.yaml`.
Make changes to it and mount it on the container and use it.
```
docker run -v "$(pwd)/custom_config.yaml:/indexify/config/custom_config.yaml" diptanu/indexify start -c ./config/custom_config.yaml
```

## API Reference

### List Embedding Models
Provides a list of embedding models available and their dimensions.

```
/embeddings/models
```
### Generate Embeddings
Generate embeddings for a collection of strings

```
/embeddings/generate
```

## List of Embedding Models
* OpenAI
   * text-embedding-ada02 (`text-embedding-ada-002`)
* Sentence Transformers
   * All-MiniLM-L12-V2 (`all-minilm-l12-v2`)
   * All-MiniLM-L6-V2 (`all-minilm-l6-v2`)
   * T5-Base (`t5-base`)

*More models are on the way. Contributions are welcome!* 

## List of Vector Databases
* Qdrant

*More integrations on the way, and contributions welcome!*

## Server Configuration Reference
Configure the behavior of the server and models through a YAML configuration scheme.
1. `listen_addr` - The adrress and port on which the server is listening.
2. `available_models` - A list of models the server is serving.
    *  `model` -  Name of the model. Default Models: `openai(text-embedding-ada002)`, `all-minilm-l12-v2`.
    *  `device` - Device on which the model is running. Default: `cpu`
3. `openai` - OpenAI configuration options.
    * `api_key` - The api key to use with openai. This is not set by default. We use OPENAI_API_KEY by default but use this when it's set.

### Default Configuration
```
# Address on which the server listens
listen_addr: 0.0.0.0:8900

# List of available models via the api. The name corresponds to a model
# that the service knows how to load, and the device is where the model
# is executed.
available_models:
- model: all-mpnet-base-v2
  device: cpu
- model: text-embedding-ada-002
  device: remote

# OpenAI key. Either set it here or set via the OPENAI_API_KEY
# environment variable
openai:
  api_key: xxxx
```

## Operational Metrics
Indexify exposes operational metrics of the server on a prometheous endpoint at `/metrics`

## Building indexify
```
make build-container
```

## Coming Soon
* Text splitting and chunking strategies for generating embeddings for longer sentences.
* Suppport for hardware acceleration, and using ONNX runtime for faster execution on CPUs.
* Retrieval strategies for dense embeddings, and a plugin mechanism to add new strategies.
* Resource Usage of Embedding Models.
* Asynchronous Embedding Generation for large corpuses of documents.
* Real Time Index updates

## Contact 
Join the Discord Server - https://discord.gg/mrXrq3DmV8 <br />
Email - diptanuc@gmail.com
