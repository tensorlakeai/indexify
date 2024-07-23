# Key Concepts 

The core idea is to build ingestion pipelines with stages that transforms unstructured data, and perform structured extraction or create embeddings using AI models.

![Extraction Graph Concept Image](images/extraction_graph_key_concept.png)
 
## Extraction Graphs
These are multi-step workflows created by chaining multiple extractors together. They allow you to manage and orchestrate complex data processing pipelines by,

  - Applying a sequence of extractors on ingested content
  - Tracking lineage of transformed content and extracted features
  - Enabling deletion of all transformed content and features when sources are deleted

## Extractor

Extractors are functions that take data from upstream sources and produce three types of output. At the moment, Extractors are implemented as Python classes that can,

  - Transform data: For example, converting a PDF to plain text, or audio to text.
  - Create Embeddings: Vector representations of the data, useful for semantic search.
  - Extract Structured data: Extracted metadata or features in a structured format.

## Content
Extractors consume `Content` which contains raw bytes of unstructured data, and they produce a list of Content and features from them.

## Namespaces

Indexify uses namespaces as logical abstractions for storing related content. This feature allows for effective data partitioning based on security requirements or organizational boundaries, making it easier to manage large-scale data operations.

## How does Indexify Fit into LLM Applications?

Indexify sits between data sources and your application. It will keep ingesting new data, run pipelines and keep your databases updated. LLM applications can query the databases whenever they need to. A typical workflow we see - 

  1. Uploading unstructured data (documents, videos, images, audio) to pipelines
  2. Indexify pipelines automatically extracts information and updates vector indexes and structured stores
  3. Retrieving information via semantic search on vector indexes and SQL queries on structured data tables