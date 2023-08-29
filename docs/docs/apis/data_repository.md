# Data Repository API

Data Repository APIs allow users to create a new repository where documents and memory of interactive LLM agents can be stored for retrieval. One or many extractor bindings can be added to the repositories to extract information from data, such as creating indexes by embedding documents for semantic search. 


## Create or Update a Data Repository
A data repository can be created by specifying a unique name, and any additional metadata or extractor bindings.

=== "curl"
    ``` bash
    curl -X POST http://localhost:8900/repositories \
    -H 'Content-Type: application/json' \
    -d '
        {
          "name": "default",
          "extractor_bindings": [
            {
              "extractor_name": "MiniLML6",
              "index_name": "search_index"
            }
          ],
          "metadata": {"sensitive": true}
        }
    '
    ```

## List Repositories
=== "curl"
    ``` console
    curl -X GET http://localhost:8900/repositories
    ```

#### Output
``` json
{
  "repositories": [
    {
      "name": "default",
      "extractor_bindings": [
        {
          "extractor_name": "MiniLML6",
          "index_name": "search_index",
          "filters": [],
          "input_params": {}
        }
      ],
      "metadata": {
        "sensitive": true
      }
    }
  ]
}
```

## Adding Extractor Bindings to a Repository
Adding an extractor binding creating indexes from content in a repository by running extractors on them. Once a binding is added, any new content being ingested is automatically extracted and indexes are updated. Additionally, filters can be added to specifically restrict the content being extracted and added to the index.

For ex, the example below binds the extractor `MiniLML6` to all the content in the repository `default` which has metadata `url` as `https://www.example.com`. Anytime any text is added to the repository with metadata that matches the content they are indexed.

=== "curl"
    ``` bash
    curl -v -X POST http://localhost:8900/repositories/default/extractor_bindings \
    -H "Content-Type: application/json" \
    -d '{
            "repository": "default",
            "extractor_name": "MiniLML6",
            "index_name": "myindex",
            "filters": [
                {
                    "eq": {
                        "url": "https://example.com/"
                    }
                }
            ]
        }'
    ```
