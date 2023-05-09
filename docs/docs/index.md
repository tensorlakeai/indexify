# Welcome to Indexify

Indexify is a knowledge and memory retrieval service for Large Language Models. It faciliates in-context learning of LLMs by providing relevant context in a prompt or expsing relevant memory to AI agents.

The service facilitates efficient execution of fine tuned/pre-trained embedding models and expose them over APIs. Several state of the art retreival algorithms are implemented to provide a batteries-included retrieval experience.

## Why use Indexify
* Flexibility: An API based embedding serving and index querying approach allows easy integrations without needing native libraries for every language.
* Reduced Application Footprint: Embedding Models and their inference runtime like PyTorch to run them are large, Indexify alleviates the need to package them with applications.
* Pre-trained or Fine-Tuned Embedding Models - Out of the box support for some of the best pre-trained models or serve fine-tuned embedding models.

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