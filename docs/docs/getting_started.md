# Getting Started

Indexify is very easy to get started with Docker Compose.

### Clone the Repository
```shell
git clone https://github.com/diptanu/indexify.git
```

### Mac OS/Windows Instructions
Please allocate at least 6 GB of RAM to Docker Desktop on Mac or Windows to try it out. This all inclusive version of Indexify runs the extraction executors which loads a embedding and a NER model in memory.

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
    curl -v -X POST http://localhost:8900/repositories/default/add_texts \
    -H "Content-Type: application/json" \
    -d '{"documents": [ 
            {"text": "Indexify is amazing!"},
            {"text": "Indexify is a retrieval service for LLM agents!"}, 
            {"text": "Kevin Durant is the best basketball player in the world."}
        ]}' 
    ```
=== "python"

    ```python
    from indexify import Repository

    repo = Repository()
    repo.add_documents([
        {"text": "Indexify is amazing!"},
        {"text": "Indexify is a retrieval service for LLM agents!"},
        {"text": "Kevin Durant is the best basketball player in the world."}
    ])
    ```

### Using extractors

Extractors are used to extract information from the documents in our repository. The extracted information can be structured (entities, keywords, etc.) or unstructured (embeddings) in nature, and is stored in an index for retrieval. 

#### Get available extractors

=== "curl"

    ```bash
    curl -X GET http://localhost:8900/extractors
    ```

    Response:

    ```json
    {"extractors": [
        {
            "name": "EntityExtractor",
            "description": "EntityExtractor",
            "extractor_type": {
                "embedding": {
                    "schema": "{\"entity\": \"string\", \"value\": \"string\", \"score\": \"float\"}"
                }
            }
        },
        {
            "name": "MiniLML6",
            "description": "MiniLML6 Embeddings",
            "extractor_type": {
                "embedding": {
                    "dim": 384,
                    "distance": "cosine"
                }
            }
        }
    ]}
    ```

=== "python"

    ```python
    from indexify import IndexifyClient

    client = IndexifyClient()
    print(client.extractors)
    ```

    Output:

    ```
    [Extractor(name=EntityExtractor, description=EntityExtractor),
     Extractor(name=MiniLML6, description=MiniLML6 Embeddings)]
    ```

#### Bind some extractors to the repository

To start extracting information from the documents, we need to bind some extractors to the repository. Let's bind a named entity extractor so that we can retrieve some data in the form of key/value pairs, and an embedding extractor so that we can run semantic search over the raw text.

Every extractor we bind results in a corresponding index being created in Indexify to store the extracted information for fast retrieval. So we must also provide an index name for each extractor.

=== "curl"

    ```bash
    curl -v -X POST http://localhost:8900/repositories/default/extractor_bindings \
    -H "Content-Type: application/json" \
    -d '{
            "extractor_name": "EntityExtractor",
            "index_name": "entities"
        }'

    curl -v -X POST http://localhost:8900/repositories/default/extractor_bindings \
    -H "Content-Type: application/json" \
    -d '{
            "extractor_name": "MiniLML6",
            "index_name": "embeddings"
        }'
    ```
=== "python"

    ```python
    repo.bind_extractor("EntityExtractor", index_name="entities")
    repo.bind_extractor("MiniLML6", index_name="embeddings")

    print(repo.extractor_bindings)
    ```

    Output:

    ```
    [ExtractorBinding(extractor_name=MiniLML6, index_name=embeddings), 
     ExtractorBinding(extractor_name=EntityExtractor, index_name=entities)]
    ```

We now have two indexes - one for entity data extracted by the EntityExtractor and one for embeddings extracted by MiniLML6.


#### Query the entity index

Now we can query the index created by the named entity extractor. The index will have json documents containing the key/value pairs extracted from the text.

=== "curl"

    ```bash
    curl -v -X GET http://localhost:8900/repositories/default/attributes\?index=entities
    ```

    Response:

    ```json
    {"attributes": [
        {
            "id": "6cb7908f79888e9b",
            "content_id": "a64c6ddc9683f031",
            "attributes": {
                "entity": "Organization",
                "score": "0.99146568775177",
                "value": "LLM"
            },
            "extractor_name": "EntityExtractor"
        },
        {
            "id": "5b414b846bad4ba3",
            "content_id": "b11de1146ff5f9ec",
            "attributes": {
                "entity": "Person",
                "score": "0.9999637603759766",
                "value": "Kevin Durant"
            },
            "extractor_name": "EntityExtractor"
        },
        {
            "id": "fc682f2bb202cb2d",
            "content_id": "5e55fda07f7be40b",
            "attributes": {
                "entity": "Media",
                "score": "0.7528226971626282",
                "value": "Indexify"
            },
            "extractor_name": "EntityExtractor"
        }
    ]}
    ```

=== "python"

    ```python
    attributes = repo.query_attribute("entities")
    print('Attributes:', *attributes, sep='\n')
    ```

    Output:

    ```
    Attributes: 
    {'id': '6cb7908f79888e9b', 'content_id': 'a64c6ddc9683f031', 'attributes': {'entity': 'Organization', 'score': '0.99146568775177', 'value': 'LLM'}, 'extractor_name': 'EntityExtractor'}
    {'id': '5b414b846bad4ba3', 'content_id': 'b11de1146ff5f9ec', 'attributes': {'entity': 'Person', 'score': '0.9999637603759766', 'value': 'Kevin Durant'}, 'extractor_name': 'EntityExtractor'}
    {'id': 'fc682f2bb202cb2d', 'content_id': '5e55fda07f7be40b', 'attributes': {'entity': 'Media', 'score': '0.7528226971626282', 'value': 'Indexify'}, 'extractor_name': 'EntityExtractor'}
    ```

#### Query the embedding index

Next let's query the index created by the embedding extractor. The index will allow us to do semantic search over the text.

Let's look for documents related to "sports":

=== "curl"

    ```bash
    curl -v -X POST http://localhost:8900/repositories/default/search \
    -H "Content-Type: application/json" \
    -d '{
            "index": "embeddings",
            "query": "sports", 
            "k": 3
        }'
    ```

    Here are the results:

    ```json
    {"results": [
        {
            "text": "Kevin Durant is the best basketball player in the world.",
            "confidence_score": 0.22862448,
            "metadata": {}
        },
        {
            "text": "Indexify is a retrieval service for LLM agents!",
            "confidence_score": -0.012608046,
            "metadata": {}
        },
        {
            "text": "Indexify is amazing!",
            "confidence_score": -0.04807447,
            "metadata": {}
        }
    ]}
    ```

=== "python"

    ```python
    search_results = repo.search_index("embeddings", "sports", 3)
    print('Search results:', *search_results, sep='\n')
    ```
    
    Here are the results:

    ```
    Search results: 
    {'text': 'Kevin Durant is the best basketball player in the world.', 'confidence_score': 0.22862448, 'metadata': {}}
    {'text': 'Indexify is a retrieval service for LLM agents!', 'confidence_score': -0.012608046, 'metadata': {}}
    {'text': 'Indexify is amazing!', 'confidence_score': -0.04807447, 'metadata': {}}
    ```

### Automatic extraction and indexing

Indexify automatically watches your data repository and runs your extractors whenever new documents are added. Let's go through an example. 

#### Add a new document to the repository

=== "curl"

    ```bash
    curl -v -X POST http://localhost:8900/repositories/default/add_texts \
    -H "Content-Type: application/json" \
    -d '{"documents": [ 
            {"text": "Steph Curry is also an amazing player!"}
        ]}' 
    ```
=== "python"

    ```python
    repo.add_documents([
        {"text": "Steph Curry is also an amazing player!"}
    ])
    ```

#### Query the embedding index

Now let's rerun our query for documents related to "sports":

=== "curl"

    ```bash
    curl -v -X POST http://localhost:8900/repositories/default/search \
    -H "Content-Type: application/json" \
    -d '{
            "index": "embeddings",
            "query": "sports", 
            "k": 3
        }'
    ```

    Here's the new response:

    ```json
    {"results": [
        {
            "text": "Kevin Durant is the best basketball player in the world.",
            "confidence_score": 0.22862448,
            "metadata": {}
        },
        {
            "text": "Steph Curry is also an amazing player!",
            "confidence_score": 0.17857659,
            "metadata": {}
        },
        {
            "text": "Indexify is a retrieval service for LLM agents!",
            "confidence_score": -0.012608046,
            "metadata": {}
        }
    ]}
    ```

=== "python"

    ```python
    search_results = repo.search_index("embeddings", "sports", 3)
    print('Updated search results:', *search_results, sep='\n')
    ```

    Here are the new search results:

    ```
    Updated search results: 
    {'text': 'Kevin Durant is the best basketball player in the world.', 'confidence_score': 0.22862448, 'metadata': {}}
    {'text': 'Steph Curry is also an amazing player!', 'confidence_score': 0.17857659, 'metadata': {}}
    {'text': 'Indexify is a retrieval service for LLM agents!', 'confidence_score': -0.012608046, 'metadata': {}}
    ```

We can see the new document we added about Steph Curry is now included in the search results. Indexify automatically ran our extractors when we added the new document and updated the relevant indexes.

### Specify filters for extractor bindings

Sometimes you might want to restrict the content from a data repository that's extracted and added to an index. For example, you might only want to process the documents that are downloaded from a specific URL. Indexify provides an easy way to do this using filters.

=== "curl"

    ```bash
    curl -v -X POST http://localhost:8900/repositories/default/add_texts \
    -H "Content-Type: application/json" \
    -d '{"documents": [ 
            {"text": "The Cayuga was launched in 2245.", 
             "metadata": 
                {"url": "https://memory-alpha.fandom.com/wiki/USS_Cayuga"}
            }
        ]}' 
    ```
=== "python"

    ```python
    repo.add_documents([
        {"text": "The Cayuga was launched in 2245.", 
         "metadata": 
            {"url": "https://memory-alpha.fandom.com/wiki/USS_Cayuga"}
        }
    ])
    ```

Now you can add extractor bindings with filters which match the URL and index content only from those documents.

=== "curl"

    ```bash
    curl -v -X POST http://localhost:8900/repositories/default/extractor_bindings \
    -H "Content-Type: application/json" \
    -d '{
            "extractor_name": "MiniLML6",
            "index_name": "star_trek_embeddings",
            "filters": [
                {
                    "eq": {
                        "url": "https://memory-alpha.fandom.com/wiki/USS_Cayuga"
                    }
                }
            ]
        }'
    ```
=== "python"

    ```python
    repo.bind_extractor("MiniLML6", index_name="star_trek_embeddings",
                        include=dict(url="https://memory-alpha.fandom.com/wiki/USS_Cayuga"))

    print(repo.extractor_bindings)
    ```



#### Start Using Long Term Memory
Long Term Memory in Indexify indicates there may be some causal relationships in data ingested into the system. And to serve such use cases where the order of messages in extraction or generation is important, Indexify provides a `Event` abstraction. Events contain a message and a timestamp in addition to any other opaque metadata that you might want to use for filtering events while creating indexes and retrieving from them.

- Create Memory Session
Memory is usually stored for interactions of an agent with a user or in a given context. Related messages are grouped in Indexify as a `Session`, so first create a session!

- Add Memory Events
=== "curl"
    ```bash
    curl -v -X GET http://localhost:8900/repositories/default/events \
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
    ```bash
    curl -v -X GET http://localhost:8900/repositories/default/events
    ```
