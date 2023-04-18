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

### Generate Embeddings

## List of Embedding Models
1. OpenAI
2. SBERT Family Models

## Operational Metrics
Indexify exposes operational metrics of the server on a prometheous endpoint at `/metrics`


## Coming Soon
* Support for Vector Databases to manage and query indexes
* ACLs for managing indexes
* Resource Usage of Embedding Models