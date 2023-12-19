# Getting Started

Indexify is very easy to get started with Docker Compose.

### Clone the Repository
```shell
git clone https://github.com/diptanu/indexify.git
```

### Start the Service using Docker Compose
```shell
docker compose up
```
This starts the Indexify server at port `8900` and additionally starts a Postgres server for storing metadata and storing embedding. We also start a basic embedding extractor which can chunk text and extract embedding from the chunks. Later, we will add another extractor to the mix and demonstrate how easy it is to add new capabilities to Indexify by adding a new extractor.

That's it! Let's explore some structured extraction capabilities on documents and retrieval APIs.

### Install the client libraries (Optional)
Indexify comes with a Python client. It uses the HTTP APIs of Indexify under the hood, and provide a convenient way of interacting with the server.
=== "python"

    ```python
    pip install indexify
    ```

### Initialize the Python library
=== "python"

    ```python
    from indexify import IndexifyClient

    client = IndexifyClient()
    ```

### Data repository

Data Repositories are logical buckets that store content. Indexify starts with a default data repository. We can start adding documents to it straight away.

#### Add some documents

=== "curl"

    ```shell
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
    repo = client.get_repository("default")
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

    ```shell
    curl -X GET http://localhost:8900/extractors
    ```

=== "python"

    ```python
    from indexify import IndexifyClient

    client = IndexifyClient()
    extractors = client.extractors
    ```
#### Bind some extractors to the repository

To start extracting information from the documents, we need to bind some extractors to the repository. Let's bind a named entity extractor so that we can retrieve some data in the form of key/value pairs, and an embedding extractor so that we can run semantic search over the raw text.

Every extractor we bind results in a corresponding index being created in Indexify to store the extracted information for fast retrieval. So we must also provide an index name for each extractor.

=== "curl"

    ```shell
    curl -v -X POST http://localhost:8900/repositories/default/extractor_bindings \
    -H "Content-Type: application/json" \
    -d '{
            "extractor": "diptanu/minilm-l6-extractor",
            "name": "minil6",
        }'
    ```
=== "python"

    ```python
    repo.bind_extractor("diptanu/minilm-l6-extractor", "minil6")

    bindings = repo.extractor_bindings()
    ```

We now have an index with embedding extracted by MiniLML6.

#### Query the embedding index

Next let's query the index created by the embedding extractor. The index will allow us to do semantic search over the text.

Let's look for documents related to "sports":

=== "curl"

    ```shell
    curl -v -X POST http://localhost:8900/repositories/default/search \
    -H "Content-Type: application/json" \
    -d '{
            "index": "minil6-embedding",
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

    ```shell
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

    ```shell
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

    ```shell
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

    ```shell
    curl -v -X POST http://localhost:8900/repositories/default/extractor_bindings \
    -H "Content-Type: application/json" \
    -d '{
            "extractor": "diptanu/minilm-l6-extractor",
            "name": "star_trek",
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
    filter = FilterBuilder().include("url", "https://memory-alpha.fandom.com/wiki/USS_Cayuga").build()
    repo.bind_extractor("diptanu/minilm-l6-extractor", "star_trek", filter=filter)

    print(repo.extractor_bindings)
    ```
