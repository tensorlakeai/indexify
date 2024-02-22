# Content Ingestion 

We provide APIs to upload raw text and any files from which you might want to extract information and build indexes.

Import the language specific clients
=== "Python"

    ```python
    from indexify import IndexifyClient
    ```
=== "TypeScript"

    ```typescript
    import { IndexifyClient } from "getindexify";
    ```

## Upload File

=== "Python"

    ```python
    client = IndexifyClient()
    content = client.upload_file(path="/path/to/file")
    ```

=== "TypeScript"
  
    ```typescript
    const client = await IndexifyClient.createClient();
    await client.uploadFile(`files/test.txt`);
    ```

## Upload Raw Text
=== "Python"

    ```python
    client.add_documents([
      "Indexify is amazing!",
      "Indexify is a retrieval service for LLM agents!",
      "Kevin Durant is the best basketball player in the world."
    ])
    ```
=== "TypeScript"

    ```typescript
    await client.addDocuments([
      "Indexify is amazing!",
      "Indexify is a retrieval service for LLM agents!",
      "Kevin Durant is the best basketball player in the world."
    ]);
    ```

## Namespaces

Namespaces are used to isolate content uploaded by applications or from extractors that chunk or transform content.

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
    )
    
    IndexifyClient.create_namespace(
        name="research",
        extraction_policies=[minilm_policy],
        labels={"sensitive": "true"},
    )
    ```

=== "TypeScript"

    ```typescript
    import { IndexifyClient, IExtractionPolicy } from "getindexify";

    const minilmPolicy:IExtractionPolicy = {
      extractor: "tensorlake/minilm-l6",
      name: "minilm-l6"
    };

    IndexifyClient.createNamespace("research", [minilmPolicy], {"sensitive":"true"});
    ```

=== "curl"

    ```shell
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
    namespaces = IndexifyClient.namespaces()
    ```

=== "TypeScript"

    ```typescript
    const namespaces = await IndexifyClient.namespaces();
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
