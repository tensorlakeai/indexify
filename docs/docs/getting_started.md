# Getting Started

Indexify is very easy to get started with Docker Compose.

### Clone the Repository
```shell
git clone https://github.com/diptanu/indexify.git
```

### Start the Service using Docker Compose
```shell
docker compose up indexify
```
This starts the Indexify server at port `8900` and additionally starts a Postgres server for storing metadata and Qdrant for storing embeddings.

That's it! Let's explore some primary document storage and retrieval APIs.

### Install the client libraries (Optional)
Indexify comes with Python and Typescript clients. They use the HTTP APIs exposed by Indexify under the hood, and provide a convenient way of interacting with the server.
=== "python"

    ```shell
    pip install indexify
    ```
=== "typescript"

    ```shell
    npm install getindexify
    ```

### Data repository

Data Repositories are logical buckets that store content. Indexify starts with a default data repository. We can start adding documents to it straight away.

#### Add some documents

=== "curl"

    ```bash
    curl -v -X POST http://localhost:8900/repository/add_texts \
    -H "Content-Type: application/json" \
    -d '{
            "documents": [ 
            {"text": "Indexify is amazing!", 
            "metadata":{"topic": "llm"} 
            },
            {"text": "Indexify is a retrieval service for LLM agents!", "metadata": {"topic": "ai"}}, 
            {"text": "Kevin Durant is the best basketball player in the world.", "metadata": {"topic": "nba"}}
        ]}' 
    ```
=== "python"

    ```python
    from indexify import Repository

    repo = Repository()
    repo.add_documents([
        {"text": "Indexify is amazing!", "metadata": {"topic": "llm"}},
        {"text": "Indexify is a retrieval service for LLM agents!", "metadata": {"topic": "ai"}},
        {"text": "Kevin Durant is the best basketball player in the world.", "metadata": {"topic": "nba"}}
    ])
    ```

### Using extractors

Extractors are used to extract information from the documents in our repository. The extracted information can be structured (entities, keywords, etc.) or unstructured (embeddings) in nature, and is stored in an index for retrieval. 

#### Get available extractors

=== "curl"

    ```bash
    curl -X GET http://localhost:8900/extractors \
    -H "Content-Type: application/json" 
    ```
=== "python"

    ```python
    from indexify import IndexifyClient

    client = IndexifyClient()
    print(client.extractors)
    ```

#### Bind some extractors to the repository

To start extracting information from the documents, we need to bind some extractors to the repository. Let's bind a named entity extractor so that we can retrieve some data in the form of key/value pairs, and an embedding extractor so that we can run semantic search over the raw text.

Every extractor we bind results in a corresponding index being created in Indexify to store the extracted information for fast retrieval. So we must also provide an index name for each extractor.

=== "curl"

    ```bash
    curl -X POST http://localhost:8900/repository/default/extractor_bindings
    -H "Content-Type: application/json"
    -d '{
            "name": "EntityExtractor",
            "index_name": "entityindex",
            "filter": {
                "content_type": "text"
            }
        }'

    curl -X POST http://localhost:8900/repository/default/extractor_bindings
    -H "Content-Type: application/json"
    -d '{
            "name": "MiniLML6",
            "index_name": "embeddingindex",
            "filter": {
                "content_type": "text"
            }
        }'
    ```
=== "python"

    ```python
    repo.add_extractor("EntityExtractor", index_name="entityindex", filter={"content_type": "text"})
    repo.add_extractor("MiniLML6", index_name="embeddingindex", filter={"content_type": "text"})

    print(repo.extractors)
    ```


#### Query the Indexes

Now we can query the index created by the named entity extractor. The index will have json documents containing the key/value pairs extracted from the text.

=== "curl"
    ```bash
    curl -X GET http://localhost:8900/repository/default/indexes/entityindex/attributes
    ```
=== "python"

    ```python
    attributes = repo.query_attribute("entityindex")
    print(attributes)
    ```

Next let's query the index created by the embedding extractor. The index will allow us to do semantic search over the text.

=== "curl"
    ```bash
    curl -v -X POST http://localhost:8900/repository/default/search \
    -H "Content-Type: application/json" \
    -d '{
            "index": "embeddingindex",
            "query": "good", 
            "k": 1
        }'
    ```
=== "python"

    ```python
    search_results = repo.search_index("embeddingindex", "Indexify", 10)
    print(search_results)
    ```


#### Start Using Long Term Memory
Long Term Memory in Indexify indicates there may be some causal relationships in data ingested into the system. And to serve such use cases where the order of messages in extraction or generation is important, Indexify provides a `Event` abstraction. Events contain a message and a timestamp in addition to any other opaque metadata that you might want to use for filtering events while creating indexes and retrieving from them.
- Create Memory Session
Memory is usually stored for interactions of an agent with a user or in a given context. Related messages are grouped in Indexify as a `Session`, so first create a session!

- Add Memory Events
=== "curl"
    ```
    curl -X POST http://localhost:8900/events \
    -H "Content-Type: application/json" \
    -d '{
            "events": [
                {
                "text": "Indexify is amazing!",
                "metadata": {"role": "human"}
                },
                {
                "text": "How are you planning on using Indexify?!",
                "metadata": {"role": "ai"}
                }
        ]}'
    ```


- Retrieve All Memory Events
You can retrieve all the previously stored messages in Indexify for a given session.
=== "curl"
    ```
    curl -X GET http://localhost:8900/events/get \
    -H "Content-Type: application/json" \
    -d '{
            "repository": "default"
        }'
    ```