# Retrieval APIs

Retrieval APIs allow querying vector indexes and other datastores, derived from the content added in data repositories. A default extractor to embed and create indexes are automatically created by indexify for every data repository, but you can add a new extractor using any embedding model available on the platform and create as many indexes you need.

## Index Query

=== "curl"
      ```
      curl -X GET http://localhost:8900/index/search \
      -H "Content-Type: application/json" \
      -d '{
            "index": "default/default",
            "query": "good",
            "k": 1
      }'
      ```

#### Output 
``` json
{
      "results":[{
            "text":"Indexify is amazing!",
            "metadata":{
                  "key":"k1"
                  }
            }
      ]}
```
### Request Body
* `index` - Name of the index to search on.
* `query` - Query string.
* `k` - top k responses.


## Adding Extractors to a Repository
Adding an extractor is the primary means to create new indexes for a repository. This can be achieved by adding a new extractor to the repository spec, and syncing it back with Indexify. The API `/repository/add_extractor` adds an extractor to an existing data repository.

=== "curl"
    ``` console
    curl -X POST http://localhost:8900/repository/add_extractor \
    -H "Content-Type: application/json" \
    -d '{
        "extractor": {
            "name": "dpr-index",
            "content_type": "text",
            "extractor_type": {
                  "embedding" :{
                        "model": "dpr",
                        "distance": "cosine",
                        "text_splitter": "none"
                        }
                  }
            }
    }'
    ```
