# Data Repository API

Data Repository APIs allow users to create a new repository where documents and memory of interactive LLM agents can be stored for retrieval. One or many extractors can be added to the repositories to extract information from data, such as creating indexes by embedding documents for semantic search. 


## Create or Update a Data Repository
A data repository can be created and updated using the `sync` API call. Extractors are started, stopped or refreshed when the repository definition are updated. In some cases indexes might be recreated if the extractor definitions change such as changing the distance from `cosine` to `dot` or changing the embedding model.

=== "curl"
    ```
    curl -X POST http://localhost:8900/repository/sync \
    -H 'Content-Type: application/json' \
    -d '{
            "name":"default",
            "extractors": [{
                "name": "top-k-index",
                "extractor_type":{
                    "embedding":{
                        "model": "dpr",
                        "distance": "cosine",
                        "text_splitter": "new_line"
                        }
                    }
                }],
            "metadata": {"my key": 1}
        }'
    ```

## List Repositories
=== "curl"
    ``` console
    curl -X GET http://localhost:8900/repository/list
    ```

#### Output
``` json
{
    "repositories":[{
        "name":"default",
        "extractors":[
            {
                "name": "default",
                "extractor_type" :{
                    "embedding": {
                        "model": "all-minilm-l12-v2",
                        "distance": "cosine",
                        "text_splitter": "none"
                        }
                },
                "content_type": "Text"}],
                "metadata":{}
            }
        ]
    }
```

## Adding Extractors to a Repository
Adding an extractor is the primary means to create new indexes for a repository. This can be achieved by adding a new extractor to the repository spec, and syncing it back with Indexify. There is also a convenience api to add extractors. The API `/repository/add_extractor` adds an extractor to an existing data repository.

=== "curl"
    ``` console
    curl -X POST http://localhost:8900/repository/add_extractor \
    -H "Content-Type: application/json" \
    -d '{
        "extractor": {
            "name": "dpr-index",
            "extractor_binding": {
                  "name": "default_embedder",
                  "index_name": "myindex",
                  "filter": {
                    "content_type": "text"
                  }
            }
        }
    }'
    ```
