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
This starts the Indexify server at port `8900` and additionally starts a postgres server for storing metadata and qdrant for storing embeddings.

That's it! Let's explore some basic document storage and retrieval APIs

Indexify stars with a default data repository, we can start adding texts to it straight away.

#### Add some Texts
=== "curl"

    ```
    curl -v -X POST http://localhost:8900/repository/add_text
    -H "Content-Type: application/json"
    -d '{
            "documents": [
            {"text": "Indexify is amazing!", 
            "metadata":{"key": "k1"}
            }
        ]}'
    ```

The default data repository is configured to have an extractor which populates an index for searching content.

#### Query the Index
=== "curl"
    ```
    curl -v -X GET http://localhost:8900/index/search
    -H "Content-Type: application/json"
    -d '{
            "index": "default/default",
            "query": "good", 
            "k": 1
        }'
    ```

#### Start Using Memory
1. Create Memory Session
=== "curl"
    ```
    ```

2. Add Memory Events
=== "curl"
    ```
    ```

3. Retrieve All Memory Events
=== "curl"
    ```
    ```

4. Retrieve using search
=== "curl"
    ```
    ```

