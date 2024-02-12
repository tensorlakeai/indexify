# Content Ingestion 

Namespaces store content either uploaded by applications or from extractors that chunk or transform content.

!!! note

    A default namespace, named `default` is created when Indexify is started.

## Create or Update a Namespace
A namespace can be created by specifying a unique name, and any additional labels or extractor bindings.

=== "python"

    ```python
    from indexify import IndexifyClient, ExtractorBinding

    minilm_binding = ExtractorBinding(
        extractor="tensorlake/minilm-l6",
        name="minilm-l6",
        content_source="source",
        filters={},
        input_params={},
    )
    
    IndexifyClient.create_namespace(
        name="research",
        extractor_bindings=[minilm_binding],
        labels={"sensitive": "true"},
    )
    ```

=== "curl"

    ``` shell
    curl -X POST http://localhost:8900/namespaces \
    -H 'Content-Type: application/json' \
    -d '
        {
          "name": "research",
          "extractor_bindings": [
            {
              "extractor": "tensorlake/minilm-l6",
              "name": "minilm-l6"
            }
          ],
          "labels": {"sensitive": "true"}
        }
    '
    ```

## List Namespaces
=== "python"

    ```python
    IndexifyClient.namespaces()
    ```

=== "curl"

    ``` shell
    curl -X GET http://localhost:8900/namespaces
    ```
??? abstract "output"

    ``` json
    {
      "namespaces": [
        {
          "name": "research",
          "extractor_bindings": [
            {
              "extractor": "diptanu/minilm-l6-extractor",
              "name": "minilml6",
              "filters": [],
              "input_params": {}
            }
          ],
          "labels": {
            "sensitive": "true"
          }
        }
      ]
    }
    ```

## Extractor Bindings 
Extractor Bindings are rules to instruct Indexify to run a particular extractor on content in a namespace. Bindings are evaluated when new content is added and extractors are run automatically on new or existing content. Bindings keep indexes updated as new content is ingested.
Additionally, filters can be added to specifically restrict the content being extracted and added to the index.

For ex, the example below binds the extractor `MiniLML6` to all the content in the namespace `default` which has labels `url` as `https://www.example.com`. Anytime any text is added to the namespace with labels that matches the content they are indexed.

=== "python"
    ```python
    client.bind_extractor(
        extractor="tensorlake/minilm-l6",
        name="minil6",
        filters={"url": "https://www.example.com"},
    )
    ```

=== "curl"
    ```shell
    curl -v -X POST http://localhost:8900/namespaces/default/extractor_bindings \
    -H "Content-Type: application/json" \
    -d '{
            "extractor": "tensorlake/minilm-l6",
            "name": "minil6",
            "filters": {"url": "https://www.example.com"}
        }'
    ```