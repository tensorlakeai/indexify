# Reliable Extraction and Retrieval for AI Applications at Any Scale

![Indexify High Level](images/Indexify_KAT.gif)

Indexify is data framework for building LLM Applications that derives knowledge from enterprise or consumer data to make decisions or answer questions. It shines in use-cases where ground truth sources of infromation changes over time. Most data sources in the real world are unstructured and orginate from PDFs, clicks on websites, emails, text messages or videos. Indexify focusses on bringing information from unstructured data sources to LLMs continously for production applications.

It provides the following primitives - 
1. **Real Time Data Pipelines** - For defining data transformation and extraction graphs that execute whenever new data is ingested.
2. **Extractor SDK** - SDK to extract information models, or other video, document, audio models.
3. **Storage** - Extracted data is automatically stored in vector stores, structured data stores and blob stores.
4. **Retreival API** - Exposes data from vector stores and structured stores using Semantic Search APIs and SQL. 


## Why Use Indexify 
Building a product with LLMs often involves -

1. Ingesting new data, extracting structured infromation, or embedding and writing them to storage.
2. Retreiving extracted structured data and embedding so that products don't respond to user queries with stale data.

The challenges of building such applications are -  

1. Extraction of structured data using LLMs, CV or other models are compute intensive and can hurt the performance of your LLM applications. 
2. Multi-Stage extraction process such as breaking down a document or other media files into chunks, and extract embedding and structured information require building a replicated state machine for fault tolerance, and can be hard to get right.

Indexify solves the complexity of building a durable and fast orchestation and ingestion system for running complex extraction and data transformation workflows for LLM Applications. Indexify assures that content meant for extraction are not dropped in production because of faults in compute and storage infrastructure.

## Start Using Indexify

Dive into [Getting Started](getting_started.md) to learn how to use Indexify.

If you would like to learn some common usecases - 
1. Learn how to build production grade [RAG Applications](usecases/rag.md)
2. Extract [PDF](usecases/pdf_extraction.md), [Videos](usecases/video_rag.md) and [Audio](usecases/audio_extraction.md) to extract embedding and [structured data](usecases/image_retrieval.md).

## Features

* Makes Unstructured Data **Queryable** with **SQL** and **Semantic Search**
* **Real Time** Extraction Engine to keep indexes **automatically** updated as new data is ingested.
* Create **Extraction Graph** to create multi-step workflows for **data transformation**, **embedding** and **structured extraction**.
* **Incremental Extraction** and **Selective Deletion** when content is deleted or updated.
* **Extractor SDK** allows adding new extraction capabilities, and many readily avaialble extractors for **PDF**, **Image** and **Video** indexing and extraction.
* **Multi-Tenant** from the ground up, **Namespaces** to isolate sensistive data.
* Works with **any LLM Framework** including **Langchain**, **DSPy**, etc.
* Runs on your laptop during **prototyping** and also scales to **1000s of machines** on the cloud.
* Works with many **Blob Stores**, **Vector Stores** and **Structured Databases**
* We have even **Open Sourced Automation** to deploy to Kubernetes in production.

