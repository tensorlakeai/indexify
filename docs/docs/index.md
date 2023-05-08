# Welcome to Indexify

Indexify is a knowledge and memory retrieval service for Large Language Models. It faciliates in-context learning of LLMs by providing relevant context in a prompt or expsing relevant memory to AI agents.

The service facilitates efficient execution of fine tuned/pre-trained embedding models and expose them over APIs. Several state of the art retreival algorithms are implemented to provide a batteries-included retrieval experience.

## Why use Indexify
* Flexibility: An API based embedding serving and index querying approach allows easy integrations without needing native libraries for every language.
* Reduced Footprint: Models and inference runtime like PyTorch are large, Indexify alleviates the need to package them with applications.
* Scalability: Indexify provides hardware optimized versions of the models whenever possible.
* State of the Art Embedding Models - As new embedding models are developed we will add support for them in the service, without applications needing to be updated or re-packaged.
* Integration with Langchain, Deepset and NextJS - Integration via indexify python and TypeScript libraries.

## Getting Started 
1. Download the Indexify Container 
2. Update the server configuration to turn on more models than the defaults.
3. Consume Embedding and Index APIs from applications.

## Available Embedding Models 
1. all-MiniLM-L12-v2
2. sentence-t5-base
3. SimCSE (coming soon)
4. OpenAI

## Available Vector Datastores
1. Qdrant
2. Pinecone(Coming Soon)
3. Milvus(Coming Soon)
4. Ephemeral Storage(Coming Soon).


## HTTP APIs

### List Embedding Models Available
```
GET /embedding/list-models
```

### Generate Embeddings
```
GET /embedding/create
```
##### Request Body
*model* string
ID of the model to use. You can use the List models API to see all of your available models.

*inputs* array of text
Array of texts for which embeddings have to be generated

### Query Index By Text
```
GET /index?text=<text>&algorithm=<algorithm>&limit=<top-k>
```

### Generate Embeddings and Write to Index
```
POST /index/write
```

