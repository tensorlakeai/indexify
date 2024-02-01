# Retrieval APIs

Retrieval APIs allow querying the indexes, derived from the content added in data repositories. Currently there are two types of indexes supported:

- Vector Indexes for Semantic Search 
- Content Attribute Indexes

## Vector Indexes

Vector Indexes are created by running embedding models on content. They allow doing semantic search on the indexes. The search results contain the chunks of text which matched the query and their corresponding scores.

The following example searches the repository `default` for the index `embeddings` for the query `good` and returns the top `k` results.

=== "python"

      ```python
      repository.search_index("minilml6.embedding","good", 3)
      ```

=== "curl"

      ```shell
      curl -v -X POST http://localhost:8900/repositories/default/search \
      -H "Content-Type: application/json" \
      -d '{
            "index": "minilml6.embedding",
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

## Metadata Indexes
Metadata Indexes are created by extractors powered by AI Models which produced structured data. The output of such extractors are JSON documents and stored in a document store. 

The schema of such indexes are defined by the extractors. The retrieval API for metadata indexes allows querying all the metadata in the index or the ones of a specific content id. 

In the future we will add support for searching these indexes as well using sparse vectors, or add them to knowledge graphs.

The following example queries the repository `default` for the index `entities` and returns all the metadata in the index.

=== "python"

      ```python
      repo.query_metadata(index_name="entities")
      ```

=== "curl"

      ```shell
      curl -v -X GET http://localhost:8900/repositories/default/metadata\?index=entities
      ```

The following example queries the repository `default` for the index `entities` and returns the metadata for the content id `foo`.

=== "python"

      ```python
      repo.query_metadata(index_name="entities", content_id="foo")
      ```

=== "curl"
      ``` shell
      curl -v -X GET http://localhost:8900/repositories/default/metadata\?index=entities&content_id=foo
      ```