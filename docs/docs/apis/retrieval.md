# Retrieval APIs

Retrieval APIs allow querying the indexes, derived from the content added in data repositories. Currently there are two types of indexes supported:
- Vector Indexes for Semantic Search 
- Content Attribute Indexes

## Vector Indexes

Vector Indexes are created by running embedding models on content. They allow doing semantic search on the indexes. The search results contain the chunks of text which matched the query and their corresponding scores.

The following example searches the repository `default` for the index `embeddingindex` for the query `good` and returns the top `k` results.

=== "curl"
      ```
      curl -v -X POST http://localhost:8900/repositories/default/search \
      -H "Content-Type: application/json" \
      -d '{
            "index": "embeddingindex",
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

## Attribute Indexes
Attribute Indexes are created by extractors powered by AI Models which produced structured data. The output of such extractors are JSON documents and stored in a document store. 

The schema of such indexes are defined by the extractors. The retrieval API for attribute indexes allows querying all the attributes in the index or the ones of a specific content id. 

In the future we will add support for searching these indexes as well using sparse vectors, or add them to knowledge graphs.

The following example queries the repository `default` for the index `entityindex` and returns all the attributes in the index.

=== "curl"
      ```
      curl -v -X GET http://localhost:8900/repositories/default/attributes?index=entityindex
      ```

The following example queries the repository `default` for the index `entityindex` and returns the attributes for the content id `foo`.