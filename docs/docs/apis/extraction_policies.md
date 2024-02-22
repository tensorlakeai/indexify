# Extraction Policies 
Extraction Policies are rules to instruct Indexify to run a particular extractor on content. Policies are evaluated when new content is added, and the corresponding extractors are automatically run.
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
=== "TypeScript"

    ```typescript
    await client.addExtractionPolicy({
        extractor: "tensorlake/wikipedia",
        name: "wikipedia",
        content_source: "ingestion",
        input_params: {},
    });
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

## Chained Policies
Extraction Policies can be chained to enable transformation and extraction of content by multiple extractors. 
For example, you can create a policy that triggers a PDF extractor to extract text, images and tables, and then another policy to trigger an extractor which produces embedding and populates indexes to search through text extracted by the upstream extractor.
Specify a `content_source` in a policy for creating such chains.

For ex -
=== "Python"

    ```python
    client.add_extraction_policy(
        extractor="tensorlake/wikipedia",
        name="wikipedia",
        content_source="ingestion"
    )
    client.add_extraction_policy(
        extractor="tensorlake/minilm-l6",
        name="minilml6",
        content_source="wikipedia"
    )
    ```
=== "TypeScript"

    ```typescript
    await client.addExtractionPolicy({
        extractor: "tensorlake/wikipedia",
        name: "wikipedia",
        content_source: "ingestion",
    });
    await client.addExtractionPolicy({
        extractor: "tensorlake/minilm-l6",
        name: "minilml6",
        content_source: "wikipedia",
    });
    ```