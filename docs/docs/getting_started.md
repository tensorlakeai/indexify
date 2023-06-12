# Getting Started

Indexify is very easy to get started with Docker Compose.

### Clone the Repository
```
git clone https://github.com/diptanu/indexify.git
```

### Start the Service using Docker Compose
```
docker compose up indexify
```

This starts the following services -

* Indexify Server which provides the HTTP APIs to the retrieval services.
* Qdrant for storing vectors.

#### Create an Index

```
curl -v -X POST http://localhost:8900/index/create   -H "Content-Type: application/json" -d '{"name": "myindex", "embedding_model": "all-minilm-l12-v2","distance": "dot", "text_splitter": "new_line"}'
```

#### Add some Texts
```
curl -v -X POST http://localhost:8900/index/add   -H "Content-Type: application/json" -d '{"index": "myindex", "documents": [{"text": "Indexify is amazing!", "metadata":{"key": "k1"}}]}'
```

#### Query the Index
```
curl -v -X GET http://localhost:8900/index/search   -H "Content-Type: application/json" -d '{"index": "myindex", "query": "good", "k": 1}'
```

#### Generate Embeddings
```
 curl -v -X GET http://localhost:8900/embeddings/generate   -H "Content-Type: application/json" -d '{"inputs": ["lol", "world"], "model": "all-minilm-l12-v2"}'
 ```

