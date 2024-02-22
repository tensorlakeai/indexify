# Content Ingestion 

## Upload File

## Uplaod Raw Text

## Namespaces

Namespaces store content either uploaded by applications or from extractors that chunk or transform content.

!!! note

    A default namespace, named `default` is created when Indexify is started.

## Create or Update a Namespace
A namespace can be created by specifying a unique name, and any additional labels or extraction policies.

=== "python"

    ```python
    from indexify import IndexifyClient, ExtractionPolicy

    minilm_policy = ExtractionPolicy(
        extractor="tensorlake/minilm-l6",
        name="minilm-l6",
        content_source="source",
        input_params={},
    )
    
    IndexifyClient.create_namespace(
        name="research",
        extraction_policies=[minilm_policy],
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
          "extraction_policies": [
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
          "extraction_policies": [
            {
              "extractor": "diptanu/minilm-l6-extractor",
              "name": "minilml6",
              "filters_eq": {},
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

## Extraction Policies 
Extraction Policies are rules to instruct Indexify to run a particular extractor on content in a namespace. These policies are evaluated when new content is added and extractors are run automatically on new or existing content. Extraction Policies keep indexes updated as new content is ingested.
Additionally, filters can be added to specifically restrict the content being extracted and added to the index.

For ex, the example below adds the policy `MiniLML6` to all the content in the namespace `default` which has labels `source` as `google`. Anytime any text is added to the namespace with labels that matches the content they are indexed.

=== "python"
    ```python
    client.add_extraction_policy(
        extractor="tensorlake/minilm-l6",
        name="minil6",
        labels_eq="source:google",
    )
    ```

=== "curl"
    ```shell
    curl -v -X POST http://localhost:8900/namespaces/default/extraction_policies \
    -H "Content-Type: application/json" \
    -d '{
            "extractor": "tensorlake/minilm-l6",
            "name": "minil6",
            "filters_eq": "source:google"
        }'
    ```