# Welcome to Indexify

Indexify is a knowledge and memory retrieval service for Large Language Models. It faciliates in-context learning of LLMs by providing relevant context in a prompt or expsing relevant memory to AI agents.

The service facilitates efficient execution of fine tuned/pre-trained embedding models and expose them over APIs. Several state of the art retreival algorithms are implemented to provide a batteries-included retrieval experience.

## Why use Indexify
* **Knowledge Base for LLMs:** Provide LLM based applications and agents with knowledge and context from documents and structured data to improve accuracy of inference queries.
* **Improved Embedding Generation:** Out of the box support for state of the art embedding models and improve them further by fine tuning them on the target knowledge base which will be used for queries.
* **Memory Engine for Co-Pilot agents:** Provide LLM agents and co-pilot temporal context by storage and retreival of relevant memory for personalization and improved accuracy during a co-pilot session.
* **Real Time Data:** Indexify will keep indexes updated if the source of the data is known, such as a S3 bucket or a database, so LLM models can answer queries that require real time information about the world.

## Road Map 
* Asynchronous index updates for large corpuses of documents.
* Real time index updates from documents stored in databases and object stores.
* Retrieval strategies for dense embeddings, and a plugin mechanism to add new strategies.
* Suppport for hardware acceleration, and using ONNX runtime for faster execution on CPUs.
* Resource Usage of Embedding Models.
* Support for more vector stores - PineCone, Milvus, etc. (contributions welcome).

## Next Steps
Read the [Getting Started](getting_started.md) to learn how to use Indexify.

## Available Embedding Models 
1. all-MiniLM-L12-v2/all-MiniLM-L6-v2
2. sentence-t5-base
3. SimCSE (coming soon)
4. OpenAI

## Available Vector Datastores
1. Qdrant