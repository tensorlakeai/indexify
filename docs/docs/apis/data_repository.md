# Content Ingestion 

Data Repositories store content either uploaded by applications or from extractors that chunk or transform content.

!!! note

    A default data repository, named `default` is created when Indexify is started.

## Create or Update a Data Repository
A data repository can be created by specifying a unique name, and any additional metadata or extractor bindings.

=== "curl"
    ``` shell
    curl -X POST http://localhost:8900/repositories \
    -H 'Content-Type: application/json' \
    -d '
        {
          "name": "research",
          "extractor_bindings": [
            {
              "extractor": "MiniLML6",
              "name": "minilm61"
            }
          ],
          "metadata": {"sensitive": true}
        }
    '
    ```

## List Repositories
=== "curl"
    ``` shell
    curl -X GET http://localhost:8900/repositories
    ```
??? abstract "output"

    ``` json
    {
      "repositories": [
        {
          "name": "research",
          "extractor_bindings": [
            {
              "extractor": "diptanu/minilm-l6-extractor",
              "name": "minilm61",
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

## Extractor Bindings 
Extractor Bindings are rules to instruct Indexify to run a particular extractor on content in a repository. Bindings are evaluated when new content is added and extractors are run automatically on new or existing content. Bindings keep indexes updated as new content is ingested.
Additionally, filters can be added to specifically restrict the content being extracted and added to the index.

For ex, the example below binds the extractor `MiniLML6` to all the content in the repository `default` which has metadata `url` as `https://www.example.com`. Anytime any text is added to the repository with metadata that matches the content they are indexed.

=== "curl"
    ``` shell
    curl -v -X POST http://localhost:8900/repositories/default/extractor_bindings \
    -H "Content-Type: application/json" \
    -d '{
            "repository": "default",
            "extractor": "MiniLML6",
            "name": "minilml6-embedding",
            "filters": [
                {
                    "eq": {
                        "url": "https://example.com/"
                    }
                }
            ]
        }'
    ```
