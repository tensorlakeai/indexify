# Indexify

Indexify provides APIs to generate embeddings from SOTA models and manage and query indexes on vector databases.

## Why Use Indexify
1. Efficient execution of embedding models outside of applications in a standalone service.
2. REST APIs to access all the models and indexes from any application runtime.
3. Access to SOTA models and all the logic around chunking texts are implemented on the service.
4. Does not require distribution of embedding models with applications, reduces size of application bundle.
5. Support for hardware accelerators to run models faster.

## APIs

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
Example: Generate embeddings from t5-base
```
 curl -v -X GET http://localhost:8900/embeddings/generate   -H "Content-Type: application/json" -d '{"inputs": ["lol", "world"], "model": "t5-base"}'
```

## List of Embedding Models
* OpenAI
   * text-embedding-ada02
* Sentence Transformers
   * All-MiniLM-L12-V2
   * All-MiniLM-L6-V2
   * T5-Base

*More models are on the way. Contributions are welcome!* 


## Server Commands
### Generate Configuration
```
indexify init-config /path/to/config.yaml
```

### Start the Server
```
indexify start /path/to/config.yaml
```

### Server Configuration
Configure the behavior of the server and models through a YAML configuration scheme.
1. `listen_addr` - The adrress and port on which the server is listening.
2. `available_models` - A list of models the server is serving.
    *  `model` -  Name of the model. Default Models: `openai(text-embedding-ada002)`, `all-minilm-l12-v2`.
    *  `device` - Device on which the model is running. Default: `cpu`
3. `openai` - OpenAI configuration options.
    * `api_key` - The api key to use with openai. This is not set by default. We use OPENAI_API_KEY by default but use this when it's set.

## Operational Metrics
Indexify exposes operational metrics of the server on a prometheous endpoint at `/metrics`


### Docker Distribution of Indexify

## Start the server
The docker distribution of Indexify makes it easy to run the service on any cloud or on-prem hardware.

### Default Configuration

### Custom configuration

## Building indexify


## Coming Soon
* Text splitting and chunking strategies for generating embeddings for longer sentences.
* Retrieval strategies for dense embeddings, and a plugin mechanism to add new strategies.
* Resource Usage of Embedding Models.
* Asynchronous Embedding Generation for large corpuses of documents.
* Real Time Index updates