# Content Ingestion 

We provide APIs to upload raw text and any files from which you might want to extract information and build indexes.

Import the language specific clients
=== "Python"

    ```python
    from indexify import IndexifyClient
    extraction_graph_spec = """
    name: 'myextractiongraph'
    extraction_policies:
      - extractor: 'tensorlake/minilm-l6'
        name: 'minilml6'
    """
    extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
    client.create_extraction_graph(extraction_graph)  
    ```
=== "TypeScript"

    ```typescript
    import { IndexifyClient, ExtractionGraph } from "getindexify";

    const client = await IndexifyClient.createClient();
    const graph = ExtractionGraph.fromYaml(`
    name: 'myextractiongraph'
    extraction_policies:
    - extractor: 'minilm-l6'
      name: 'minilml6'
    `);
    await client.createExtractionGraph(graph);
    ```

## Upload File

=== "Python"

    ```python
    content = client.upload_file(extraction_graphs="myextractiongraph",path="/path/to/file")
    ```

=== "TypeScript"
  
    ```typescript
    await client.uploadFile("myextractiongraph", `files/test.txt`);
    ```

## Upload Raw Text
=== "Python"

    ```python
    client.add_documents("myextractiongraph", [
      "Indexify is amazing!",
      "Indexify is a retrieval service for LLM agents!",
      "Kevin Durant is the best basketball player in the world."
    ])
    ```
=== "TypeScript"

    ```typescript
    await client.addDocuments("myextractiongraph", [
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

    IndexifyClient.create_namespace(
        name="research",
        extraction_graphs=[],
        labels={"sensitive": "true"},
    )
    ```

=== "TypeScript"

    ```typescript
    import { IndexifyClient, IExtractionPolicy } from "getindexify";

    IndexifyClient.createNamespace({
      name:"research", 
      extractionGraphs:[], 
      labels:{"sensitive":"true"}});
    ```

=== "curl"

    ```shell
    curl -X POST http://localhost:8900/namespaces \
    -H 'Content-Type: application/json' \
    -d '
        {
          "name": "research",
          "extraction_graphs": [],
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
      "namespaces" : [
          {
            "extraction_graphs" : [
                {
                  "extraction_policies" : [
                      {
                        "content_source" : "",
                        "extractor" : "tensorlake/minilm-l6",
                        "filters_eq" : {},
                        "graph_name" : "sportsknowledgebase",
                        "id" : "f4ac72a165927ada",
                        "input_params" : null,
                        "name" : "minilml6"
                      }
                  ],
                  "id" : "default",
                  "name" : "sportsknowledgebase",
                  "namespace" : "default"
                }
            ],
            "name" : "default"
          },
      ]
    }
    ```
