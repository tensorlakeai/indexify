# Retrieval APIs

Retrieval APIs allow querying vector indexes and other datastores, derived from the content added in data repositories. A default extractor to embed and create indexes are automatically created by indexify for every data repository, but you can add a new extractor using any embedding model available on the platform and create as many indexes you need.

## Index Query

=== "curl"
      ```
      curl -X GET http://localhost:8900/index/search \
      -H "Content-Type: application/json" \
      -d '{
            "repository": "default",
            "index": "default_index",
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