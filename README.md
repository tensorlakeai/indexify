# Indexify - Extraction and Retrieval from Videos, PDF and Audio for Interactive AI Applications

![Tests](https://github.com/tensorlakeai/indexify/actions/workflows/test.yaml/badge.svg?branch=main)
[![Discord](https://dcbadge.vercel.app/api/server/VXkY7zVmTD?style=flat&compact=true)](https://discord.gg/VXkY7zVmTD)


![Indexify High Level](docs/docs/images/Indexify_KAT.gif)

> **LLM applications backed by Indexify will never answer outdated information.**

Indexify is an open-source engine for buidling fast data pipelines for unstructured data(video, audio, images and documents) using re-usable extractors for embedding, transformation and feature extraction. LLM Applications can query transformed content friendly to LLMs by semantic search and SQL queries. 

Indexify keeps vectordbs, structured databases(postgres) updated by automatically invoking the pipelines as new data is ingested into the system from external data sources.  

## Why use Indexify

* Makes Unstructured Data **Queryable** with **SQL** and **Semantic Search**
* **Real Time** Extraction Engine to keep indexes **automatically** updated as new data is ingested.
* Create **Extraction Graph** to describe **data transformation** and extraction of **embedding** and **structured extraction**.
* **Incremental Extraction** and **Selective Deletion** when content is deleted or updated.
* **Extractor SDK** allows adding new extraction capabilities, and many readily available extractors for **PDF**, **Image** and **Video** indexing and extraction.
* Works with **any LLM Framework** including **Langchain**, **DSPy**, etc.
* Runs on your laptop during **prototyping** and also scales to **1000s of machines** on the cloud.
* Works with many **Blob Stores**, **Vector Stores** and **Structured Databases**
* We have even **Open Sourced Automation** to deploy to Kubernetes in production.


## Detailed Getting Started

To get started follow our [documentation](https://docs.getindexify.ai/getting_started/).

## Quick Start

#### Download and start Indexify 
```bash title="Terminal 1"
curl https://getindexify.ai | sh
./indexify server -d
```

#### Install the Indexify Extractor and Client SDKs
```bash title="Terminal 2"
virtualenv ve
source ve/bin/activate
pip install indexify indexify-extractor-sdk
```

#### Download some extractors
```bash title="Terminal 2"
indexify-extractor download tensorlake/minilm-l6
indexify-extractor download tensorlake/pdf-extractor
indexify-extractor download tensorlake/yolo-extractor
indexify-extractor download tensorlake/chunk-extractor
indexify-extractor download tensorlake/summarization
indexify-extractor download tensorlake/asrdiarization
indexify-extractor join-server
```

### Basic RAG
This example shows how to implement RAG on text
#### Create an Extraction Graph
```python
from indexify import IndexifyClient, ExtractionGraph
client = IndexifyClient()

extraction_graph_spec = """
name: 'sportsknowledgebase'
extraction_policies:
   - extractor: 'tensorlake/minilm-l6'
     name: 'minilml6'
"""
extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph) 
print("indexes", client.indexes())
```
#### Add Texts
```python
client.add_documents("sportsknowledgebase", ["Adam Silver is the NBA Commissioner", "Roger Goodell is the NFL commisioner"])
```

#### Retrieve
```python
context = client.search_index(name="sportsknowledgebase.minilml6.embedding", query="NBA commissioner", top_k=1)
```


### Podcast Summarization and Embedding
This example shows how to transcribe audio, and create a pipeline that embeds the transcription 
More details about Audio Use Cases - https://docs.getindexify.ai/usecases/audio_extraction/

#### Create an Extraction Graph
```python
from indexify import IndexifyClient, ExtractionGraph
client = IndexifyClient()

extraction_graph_spec = """
name: 'audiosummary'
extraction_policies:
   - extractor: 'tensorlake/asrdiarization'
     name: 'asrextractor'
   - extractor: 'tensorlake/summarization'
     name: 'summarizer'
     input_params:
        max_length: int = 400
        min_length: int = 300
        chunk_method: str = 'recursive'
     content_source: 'asrextractor'
   - extractor: 'tensorlake/minilm-l6'
     name: 'minilml6'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
```

#### Upload an Audio
```python
with open("sample.mp3", 'wb') as file:
  file.write((requests.get("https://extractor-files.diptanu-6d5.workers.dev/sample-000009.mp3")).content)
content_id = client.upload_file("audiosummary", "sample.mp3")
```

> Adding Texts and Files can be a time consuming process and by default we allow asynchronous ingestion for parallel operations. However the following codes might fail until the extraction has been completed. To make it a blocking call, use `client.wait_for_extraction(content_id)` after getting the content_id from above.

#### Retrieve Summary
```python
client.get_extracted_content(content_id)
```

#### Search Transcription Index
```python
context = client.search_index(name="audiosummary.minilml6.embedding", query="President of America", top_k=1)
```

### Object Detection on Images
This example shows how to create a pipeline that performs object detection on images using the Yolo extractor.
More details about Image understanding and retrieval - https://docs.getindexify.ai/usecases/image_retrieval/

#### Create an Extraction Graph
```python
from indexify import IndexifyClient, ExtractionGraph
client = IndexifyClient()

extraction_graph_spec = """
name: 'imageknowledgebase'
extraction_policies:
   - extractor: 'tensorlake/yolo-extractor'
     name: 'object_detection'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
```

#### Upload Images
```python
with open("sample.jpg", 'wb') as file:
  file.write((requests.get("https://extractor-files.diptanu-6d5.workers.dev/people-standing.jpg")).content)
content_id = client.upload_file("imageknowledgebase", "sample.jpg")
```

#### Retrieve Features of an Image
```python
client.get_extracted_content(content_id)
```

#### Query using SQL
```python
result = client.sql_query("select * from ingestion where object_name='skateboard';")
```

###  PDF Extraction and Retrieval
This example shows how to create a pipeline that extracts from PDF documents.
More information here - https://docs.getindexify.ai/usecases/pdf_extraction/

#### Create an Extraction Graph
```python
from indexify import IndexifyClient, ExtractionGraph
client = IndexifyClient()

extraction_graph_spec = """
name: 'pdfqa'
extraction_policies:
   - extractor: 'tensorlake/pdf-extractor'
     name: 'docextractor'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
```

#### Upload a Document
```python
with open("sample.pdf", 'wb') as file:
  file.write((requests.get("https://extractor-files.diptanu-6d5.workers.dev/scientific-paper-example.pdf")).content)
content_id = client.upload_file("pdfqa", "sample.pdf")
```

#### Get Text, Image and Tables
```python
client.get_extracted_content(content_id)
```

### LLM Framework Integration 
Indexify can work with any LLM framework, or with your applications directly. We have an example of a Langchain application [here](https://getindexify.ai/integrations/langchain/python_langchain/) and DSPy [here](https://docs.getindexify.ai/integrations/dspy/python_dspy/).

### Try out other extractors
We have a ton of other extractors, you can list them and try them out - 
```bash
indexify-extractor list
```

### Custom Extractors
Any extraction or transformation algorithm can be expressed as an Indexify Extractor. We provide an SDK to write your own. Please follow [the docs here for instructions](https://docs.getindexify.ai/apis/develop_extractors/). 

### Structured Data

Extractors which produce structured data from content, such as bounding boxes and object type, or line items of invoices are stored in
structured store. You can query extracted structured data using Indexify's SQL interface.

We have an example [here](https://getindexify.ai/usecases/image_retrieval/)

## Contributions
Please open an issue to discuss new features, or join our Discord group. Contributions are welcome, there are a bunch of open tasks we could use help with! 

If you want to contribute on the Rust codebase, please read the [developer readme](docs/docs/develop.md).
