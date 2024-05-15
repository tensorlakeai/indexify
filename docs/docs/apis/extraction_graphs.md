# Extraction Graphs 
Extraction Graphs are rules to instruct Indexify to run a particular extractor on content. Graphs are evaluated when new content is added, and the corresponding extractors are automatically run.
Additionally, filters can be added to specifically restrict the content being extracted and added to the index.

For ex, the example below adds graph with a policy `minilml6` to all the content in the namespace `default` which has labels `source` as `google`. Anytime any text is added to the namespace with labels that matches the content they are indexed.

=== "python"

    ```python
    extraction_graph_spec = """
    name: 'myextractiongraph'
    extraction_policies:
      - extractor: 'tensorlake/minilm-l6'
        name: 'minilml6'
        labels_eq: source:google
    """
    extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
    client.create_extraction_graph(extraction_graph)  
    ```
=== "TypeScript"

    ```typescript
    const client = await IndexifyClient.createClient();
    const graph = ExtractionGraph.fromYaml(`
    name: 'myextractiongraph'
    extraction_policies:
      - extractor: 'tensorlake/minilm-l6'
        name: 'minilml6'
        labels_eq: source:google
    `);
    await client.createExtractionGraph(graph);
    ```
=== "curl"

    ```shell
    curl -v -X POST http://localhost:8900/namespaces/default/extraction_graphs \
    -H "Content-Type: application/json" \
    -d '
    {
        "name": "myextractiongraph",
        "extraction_policies": [
            {
              "extractor": "tensorlake/minilm-l6",
              "name": "minil6",
              "filters_eq": "source:google"
            }
        ]
    }'
    ```

## Chained Extraction Graph Policies
Within your Extraction Graph you can chain policies together to enable transformation and extraction of content by multiple extractors. 
For example, you can create a policy that triggers a PDF extractor to extract text, images and tables, and then another policy to trigger an extractor which produces embedding and populates indexes to search through text extracted by the upstream extractor.
Specify a `content_source` in a policy for creating such chains.

For ex -
=== "Python"

    ```python
    extraction_graph_spec = """
    name: 'myextractiongraph'
    extraction_policies:
      - extractor: 'tensorlake/wikipedia'
        name: 'wikipedia'
      - extractor: 'tensorlake/minilm-l6'
        name: 'minilml6'
        content_source: 'wikipedia'
    """
    extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
    client.create_extraction_graph(extraction_graph)  
    ```
=== "TypeScript"

    ```typescript
    const graph = ExtractionGraph.fromYaml(`
    name: 'myextractiongraph'
    extraction_policies:
      - extractor: 'tensorlake/wikipedia'
        name: 'wikipedia'
      - extractor: 'tensorlake/minilm-l6'
        name: 'minilml6'
        content_source: 'wikipedia'
    `);
    await client.createExtractionGraph(graph);
    ```