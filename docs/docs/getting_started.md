# Getting Started

Indexify is very easy to get started with Docker Compose.

### Clone the Repository
```shell
git clone https://github.com/tensorlakeai/indexify.git
```

### Start the Service using Docker Compose
```shell
docker compose up
```
This starts the Indexify server at port `8900` and additionally starts a Postgres server for storing metadata and storing embedding. We also start a basic embedding extractor which can chunk text and extract embedding from the chunks. Later, we will add another extractor to the mix and demonstrate how easy it is to add new capabilities to Indexify by adding a new extractor.

### Downloading binaries directly

If you prefer to download the indexify binaries and use the binary directly, you can find them on our GitHub releases [here](https://github.com/tensorlakeai/indexify/releases/latest).

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

### Namespaces

Namespaces are logical buckets that store content. Indexify starts with a default namespace. We can start adding documents to it straight away.

#### Add some documents

=== "python"

    ```python
    client.add_documents([
        "Indexify is amazing!",
        "Indexify is a retrieval service for LLM agents!",
        "Kevin Durant is the best basketball player in the world."
    ])
    ```
=== "curl"

    ```shell
    curl -v -X POST http://localhost:8900/namespaces/default/add_texts \
    -H "Content-Type: application/json" \
    -d '{"documents": [ 
            {"text": "Indexify is amazing!"},
            {"text": "Indexify is a retrieval service for LLM agents!"}, 
            {"text": "Kevin Durant is the best basketball player in the world."}
        ]}'
    ```

#### You can also upload files to Indexify
=== "python"

    ```python
    client.upload_file("kd.txt")
    ```
    
=== "curl"

    ```
    curl -v http://localhost:8900/namespaces/default/upload_file \
    -F "files=@kd.txt"
    ```

#### Look at the metadata of the content which has been ingested 
Sometimes you might want to read all the metadata of the content for use in another application or debugging.

=== "python"

    ```
    client.get_content()
    ```

=== "curl"

    ```
    curl -v http://localhost:8900/namespaces/default/content
    ```

Content can be filtered:

=== "python"

    ```
    client.get_content(parent_id="some_parent_id", labels_eq="key1:value1,key2:value2")
    ```

=== "curl"

    ```
    curl -v http://localhost:8900/namespaces/default/content?source=some_source&parent_id=some_parent_id&labels_eq=key1:value1,key2:value2
    ```
=== "api spec"

    ```
    source: string
    parent_id: string
    labels_eq: <key>:<value>,<key>:<value>...
    ```

### Using extractors

Extractors are used to extract information from the documents. The extracted information can be structured (entities, keywords, etc.) or unstructured (embeddings) in nature, and is stored in an index for retrieval. 

#### Get available extractors

=== "python"

    ```python
    extractors = client.extractors()
    ```

=== "curl"

    ```shell
    curl -X GET http://localhost:8900/extractors
    ```

#### Bind some extractors to the namespace

To start extracting information from the documents, we need to bind some extractors to the namespace. Let's bind a named entity extractor so that we can retrieve some data in the form of key/value pairs, and an embedding extractor so that we can run semantic search over the raw text.

Every extractor we bind results in a corresponding index being created in Indexify to store the extracted information for fast retrieval. So we must also provide an index name for each extractor.

=== "python"

    ```python
    client.bind_extractor("tensorlake/minilm-l6", "minil6")

    bindings = client.extractor_bindings
    ```

=== "curl"

    ```shell
    curl -v -X POST http://localhost:8900/namespaces/default/extractor_bindings \
    -H "Content-Type: application/json" \
    -d '{
            "extractor": "tensorlake/minilm-l6",
            "name": "minil6"
        }'
    ```

We now have an index with embedding extracted by MiniLML6.

#### Query the embedding index

Next let's query the index created by the embedding extractor. The index will allow us to do semantic search over the text.

Let's look for documents related to "sports":

=== "python"

    ```python
    search_results = client.search_index("minil6.embedding", "sports", 3)
    print('Search results:', *search_results, sep='\n')
    ```
    
    Here are the results:

    ```
    [
        {
            'content_id': '8a4e86c1ed871aa5', 
            'text': 'Kevin Durant is the best basketball player in the world.', 
            'confidence_score': 0.22862443, 
            'labels': {}
        },
        {
            'content_id': 'cca837cf4d0654aa', 
            'text': 'Indexify is a retrieval service for LLM agents!',
            'confidence_score': -0.012608088, 
            'labels': {}
        },
        {
            'content_id': 'ad2540d8cf3fb9b7', 
            'text': 'Indexify is amazing!', 
            'confidence_score': -0.048074536, 
            'labels': {}
        }
    ]
    ```
=== "curl"

    ```shell
    curl -v -X POST http://localhost:8900/namespaces/default/search \
    -H "Content-Type: application/json" \
    -d '{
            "index": "minil6.embedding",
            "query": "sports", 
            "k": 3
        }'
    ```

    Here are the results:

    ```json
    {
        "results": [
            {
                "content_id": "8a4e86c1ed871aa5",
                "text": "Kevin Durant is the best basketball player in the world.",
                "confidence_score": 0.22862443,
                "labels": {}
            },
            {
                "content_id": "cca837cf4d0654aa",
                "text": "Indexify is a retrieval service for LLM agents!",
                "confidence_score": -0.012608088,
                "labels": {}
            },
            {
                "content_id": "ad2540d8cf3fb9b7",
                "text": "Indexify is amazing!",
                "confidence_score": -0.048074536,
                "labels": {}
            }
        ]
    }

    ```


### Automatic extraction and indexing

Indexify automatically watches your namespace and runs your extractors whenever new documents are added. Let's go through an example. 

#### Add a new document to the namespace

=== "python"

    ```python
    client.add_documents("Steph Curry is also an amazing player!")
    ```

=== "curl"

    ```shell
    curl -v -X POST http://localhost:8900/namespaces/default/add_texts \
    -H "Content-Type: application/json" \
    -d '{"documents": [ 
            {"text": "Steph Curry is also an amazing player!"}
        ]}' 
    ```

#### Query the embedding index

Now let's rerun our query for documents related to "sports":

=== "python"

    ```python
    search_results = client.search_index("minil6.embedding", "sports", 3)
    print('Updated search results:', *search_results, sep='\n')
    ```

    Here are the new search results:

    ```
    Updated search results: 
    [
        {
            'content_id': '8a4e86c1ed871aa5', 
            'text': 'Kevin Durant is the best basketball player in the world.',
            'confidence_score': 0.22862443, 
            'labels': {}
        }, 
        {
            'content_id': 'fcb76d63e3324d9c', 
            'text': 'Steph Curry is also an amazing player!', 
            'confidence_score': 0.17857653, 
            'labels': {}
        }, 
        {
            'content_id': 'cca837cf4d0654aa', 
            'text': 'Indexify is a retrieval service for LLM agents!',
            'confidence_score': -0.012608088, 
            'labels': {}
        }
    ]
    ```

=== "curl"

    ```shell
    curl -v -X POST http://localhost:8900/namespaces/default/search \
    -H "Content-Type: application/json" \
    -d '{
            "index": "minil6.embedding",
            "query": "sports", 
            "k": 3
        }'
    ```

    Here's the new response:

    ```json
    {
        "results": [
            {
                "content_id": "8a4e86c1ed871aa5",
                "text": "Kevin Durant is the best basketball player in the world.",
                "confidence_score": 0.22862443,
                "labels": {}
            },
            {
                "content_id": "fcb76d63e3324d9c",
                "text": "Steph Curry is also an amazing player!",
                "confidence_score": 0.17857653,
                "labels": {}
            },
            {
                "content_id": "cca837cf4d0654aa",
                "text": "Indexify is a retrieval service for LLM agents!",
                "confidence_score": -0.012608088,
                "labels": {}
            }
        ]
    }

    ```


We can see the new document we added about Steph Curry is now included in the search results. Indexify automatically ran our extractors when we added the new document and updated the relevant indexes. Extractors will only match content with the same mime type as the extractor. For example, the embedding extractor will only match text documents.

### Specify filters for extractor bindings

Sometimes you might want to restrict the content that's extracted and added to an index. For example, you might only want to process the documents that are downloaded from a specific URL. Indexify provides an easy way to do this using filters.

=== "python"

    ```python
    from indexify.client import Document
    client.add_documents([
        Document(
            text="The Cayuga was launched in 2245.", 
            labels={"source": "https://memory-alpha.fandom.com/wiki/USS_Cayuga"}
        )
    ])
    ```

=== "curl"

    ```shell
    curl -v -X POST http://localhost:8900/namespaces/default/add_texts \
    -H "Content-Type: application/json" \
    -d '{"documents": [ 
            {"text": "The Cayuga was launched in 2245.", 
             "labels": 
                {"source": "https://memory-alpha.fandom.com/wiki/USS_Cayuga"}
            }
        ]}' 
    ```
    
Now you can add extractor bindings with filters which match the URL and index content only from those documents.

=== "python"

    ```python
    client.bind_extractor("tensorlake/minilm-l6", "star_trek", filters={
        "source": "https://memory-alpha.fandom.com/wiki/USS_Cayuga"
    })

    print(client.extractor_bindings)
    ```

=== "curl"

    ```shell
    curl -v -X POST http://localhost:8900/namespaces/default/extractor_bindings \
    -H "Content-Type: application/json" \
    -d '{
            "extractor": "tensorlake/minilm-l6",
            "name": "star_trek",
            "filters": {
                "source": "https://memory-alpha.fandom.com/wiki/USS_Cayuga"
            }
        }'
    ```
    
