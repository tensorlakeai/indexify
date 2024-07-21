# Effortless Ingestion and Extraction for Unstructured Data at Any Scale

![Indexify High Level](images/Indexify_KAT.gif)

Indexify is an open-source data framework. It is designed for creating ingestion and extraction pipelines for unstructured data. The framework uses a declarative approach to perform structured extraction with any AI model or to transform ingested data.

Indexify pipelines operate in real-time. They process data immediately after ingestion. This makes them ideal for interactive applications and low-latency scenarios.

## Multi-Stage Ingestion and Extraction Workflows

Extraction Graphs are at the center of Indexify. They are pipelines which processes data and writes the output to databases for retrieval. Extraction policies are linked using the `content_source` attribute.

**AI Native**: Use models from OpenAI, HuggingFace and Ollama in the pipeline.

**Extensible**: Extend Indexify by plugging in Python modules in the pipeline.

**Scalable**: Handles ingestion and processing data at scale powered by a low latency and distributed scheduler.

**APIs**: Pipelines are exposed as HTTP APIs, making them accessible from applications written in TypeScript, Java, Python, Go, etc.

```yaml title="graph.yaml"
name: 'pdf-ingestion-pipeline'
extraction_policies:
- extractor: 'tensorlake/marker'
  name: 'pdf_to_markdown'
- extractor: 'tensorlake/ner'
  name: 'entity_extractor'
  content_source: 'pdf_to_markdown'
- extractor: 'tensorlake/minilm-l6'
  name: 'embedding'
  content_source: 'pdf_to_markdown'
```

=== "HTTP"
    ```bash
    curl -X 'POST' \
    'http://localhost:8900/namespace/default/extraction_graphs' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
        "description": "PDF To Text Extraction Pipeline",
        "extraction_policies": [
          {
            "extractor": "tensorlake/marker",
            "name": "pdf_to_markdown"
          },
          {
            "extractor": "tensorlake/ner",
            "name": "entity_extractor",
            "content_source": "pdf_to_markdown"
          },
          {
            "extractor": "tensorlake/minilm-l6",
            "name": "embedding",
            "content_source": "pdf_to_markdown"
          }
        ],
        "name": "pdf-ingestion-pipeline"
    }'
    ```

=== "Python"
    ```python
    from indexify import IndexifyClient, ExtractionGraph
    graph = ExtractionGraph.from_yaml_file("graph.yaml")
    client = IndexifyClient()
    client.create_extraction_graph(graph)
    ```

=== "Typescript"
    ```typescript
    ```

### Continuous Data Ingestion  

=== "HTTP"
    ```bash
    curl -X 'POST' \
    'http://localhost:8900/namespaces/default/extraction_graphs/pdf-ingestion-pipeline/extract' \
    -H 'accept: */*' \
    -H 'Content-Type: multipart/form-data' \
    -F 'file=@file.pdf;type=application/pdf' \
    -F 'labels={"source":"arxiv"}'
    ```

=== "Python"
    ```python
    client = IndexifyClient()

    files = ["file1.pdf", .... "file100.pdf"]
    for file in files:
      client.upload_file("pdf-ingestion-pipeline", file)
    ```

=== "Typescript"
    ```Typescript
    ```

### Retrieve Extracted Data

Retrieve extracted named entities 

=== "HTTP"

    ```bash
    curl -X 'GET' \
    'http://localhost:8900/namespaces/:namespace/extraction_graphs/pdf-ingestion-pipeline/content/585ebafdbc9dbbbe/extraction_policies/entity_extractor' \
    ```

=== "Python"

    ```python
    content_ids = [content.id for content in client.list_content("pdf-ingestion-pipeline")]

    markdown = client.get_extracted_content(content_ids[0], "pdf-ingestion-pipeline", "pdf_to_markdown")
    named_entities = client.get_extracted_content(content_ids[0], "pdf-ingestion-pipeline", "entity_extractor")
    ```

=== "TypeScript"
    
    ```typescript
    ```

Search vector indexes populated by embeddings 

=== "HTTP"

    ```bash
    curl -X 'POST' \
    'http://localhost:8900/namespaces/default/indexes/pdf-ingestion-pipeline.embedding.embedding/search' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
        "filters": [],
        "include_content": true,
        "k": 3,
        "query": "wework"
    }'
    ```

=== "Python"

    ```python
    results = client.search("pdf-ingestion-pipeline.embedding.embedding","Who won the 2017 NBA finals?", k=3)
    ```

=== "Typescript"

    ```typescript
    ```

To understand usage of indexify for various use cases we recommend the following starting points.

## Indexify Learning Path 

| Guide | What You'll Build | Key Concepts Covered |
|-------|-------------------|----------------------|
| [Getting Started - Basic](https://docs.getindexify.ai/getting_started/) | A Wikipedia information retrieval system | - Indexify Server setup<br>- Extraction Graphs<br>- Basic Extractors<br>- Data Ingestion<br>- Vector Search |
| [Getting Started - Intermediate](https://docs.getindexify.ai/getting_started_intermediate/) | A tax document processing and Q&A system | - PDF processing<br>- Custom Extractors<br>- Structured Data Extraction<br>- Advanced Querying |
| [Multi-Modal RAG on PDF](https://docs.getindexify.ai/example_code/pdf/indexing_and_rag) | A video content analysis and retrieval system | - Multi-modal data processing<br>- Video frame extraction<br>- Speech-to-text conversion<br>- Cross-modal retrieval |


## Why Use Indexify?

Indexify is for continuous ingestion and extraction from unstructured data in a streaming fashion. If you are building LLM Application that need structured data from any unstructured data sources, Indexify is the tool for you.

While there lies a divide in tools for prototyping and for deploying to production, Indexify runs locally on laptops for rapid prototyping, but you can also deploy a scaled up version in production for high availability and reliability.

You should use Indexify if you care about -

* Processing Multi-Modal Data such as PDFs, Videos, Images, and Audio
* High Availability and Fault Tolerance
* Local Experience for rapid prototyping
* Automatically updating indexes whenever upstream data sources are updated
* Incremental Extraction and Selective Deletion
* Compatibility with Various LLM Frameworks (Langchain, DSPy, etc.)
* Integration with any database, blob store or vector store.
* Owning your data infrastructure and being able to deploy on any cloud.
