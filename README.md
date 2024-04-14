# Indexify - Extraction and Retreival from Videos, PDF and Audio for Interactive AI Applications

![Tests](https://github.com/tensorlakeai/indexify/actions/workflows/test.yaml/badge.svg?branch=main)
[![Discord](https://dcbadge.vercel.app/api/server/VXkY7zVmTD?style=flat&compact=true)](https://discord.gg/VXkY7zVmTD)


![Indexify High Level](docs/docs/images/Indexify_Home_Diagram.gif)

> **LLM applications backed by Indexify will never answer outdated information.**

Indexify is an open-source engine for buidling fast data pipelines for unstructured data(video, audio, images and documents) using re-usable extractors for embedding, transformation and feature extraction. LLM Applications can query transformed content friendly to LLMs by semantic search and SQL queries. 

Indexify keeps vectordbs, structured databases(postgres) updated by automatically invoking the pipelines as new data is ingested into the system from external data sources. 

## Why use Indexify

* Makes Unstructured Data **Queryable** with **SQL** and **Semantic Search**
* **Real Time** Extraction Engine to keep indexes **automatically** updated as new data is ingested.
* Create **Extraction Graph** to describe **data transformation** and extraction of **embedding** and **structured extraction**.
* **Incremental Extraction** and **Selective Deletion** when content is deleted or updated.
* **Extractor SDK** allows adding new extraction capabilities, and many readily avaialble extractors for **PDF**, **Image** and **Video** indexing and extraction.
* Works with **any LLM Framework** including **Langchain**, **DSPy**, etc.
* Runs on your laptop during **prototyping** and also scales to **1000s of machines** on the cloud.
* Works with many **Blob Stores**, **Vector Stores** and **Structured Databases**
* We have even **Open Sourced Automation** to deploy to Kubernetes in production.


## Detailed Getting Started

To get started follow our [documentation](https://getindexify.ai/getting_started/).

## Quick Start

#### Download Indexify 
```bash
curl https://tensorlake.ai | sh
```

#### Start the server
```bash
./indexify server -d
```

#### Install the Indexify Extractor and Client SDKs
```bash
pip install indexify indexify-extractors
```

#### Start an embedding extractor 
```bash
indexify-extractor download hub://embedding/minilm-l6
indexify-extractor join-server minilm-l6.minilm_l6:MiniLML6Extractor
```

#### Upload some texts 
```python
from indexify import IndexifyClient
client = IndexifyClient()
client.add_extraction_policy(extractor="tensorlake/minilm-l6", name="minilml6")
client.indexes()
client.add_documents(["Adam Silver is the NBA Commissioner", "Roger Goodell is the NFL commisioner"])
```

#### Search the Index
```python
client.search_index(name="minilm6.embedding", query="NBA commissioner", top_k=1)
```

#### Use Extracted Data in Applications
You can now use the extracted data in your application. As data is ingested by Indexify, your indexes are going to be automatically
updated by Indexify. We have an example of a Langchain application [here](https://getindexify.ai/integrations/langchain/python_langchain/)

#### Try out Video, Audio or PDF Extractors 
We have extractors for [Video](https://getindexify.ai/usecases/video_rag/), [Audio](https://getindexify.ai/usecases/audio_extraction/) and [PDF](https://getindexify.ai/usecases/pdf_extraction/) as well, you can list all the available extractors 

```bash
indexify-extractor list
```
#### Structured Data

Extractors which produce structured data from content, such as bounding boxes and object type, or line items of invoices are stored in
structured store. You can query extracted structured data using Indexify's SQL interface.

We have an example [here](https://getindexify.ai/usecases/image_retrieval/)

## Contributions
Please open an issue to discuss new features, or join our Discord group. Contributions are welcome, there are a bunch of open tasks we could use help with! 

If you want to contribute on the Rust codebase, please read the [developer readme](docs/docs/develop.md).
