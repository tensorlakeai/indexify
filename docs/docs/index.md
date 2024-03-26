# Multi-Modal Extraction Engine for Large Language Models Applications

Indexify is a reactive structured extraction and embedding engine for un-structured data such as PDFs, raw text, audio and video. You can use Indexify to index and serve data to RAG applications, or consume structured data from any unstructured data into any kind of applications in real time.

## Why Use Indexify 
Building a product with LLMs often involve ingesting new data, extracting structured infromation, or embedding and writing them to storage and retreiving them from applications so that they don't respond to user queries on stale data.

Moving compute heavy extraction improves performance of your LLM applications. Making extraction processes which requries a series of steps such as breaking down a document or media into chunks, extracting embeddig, requires a persistent state machine which is hard to get right.

Indexify solves the complexity of building a durable, reliable and fast distributed orchestation and ingestion system for running complex extraction and data transformation workflow for production applications. Indexify will assure that content meant for extraction are not dropped on the floor in production. 

## Features

* Makes Unstructured Data **Queryable** with **SQL** and **Semantic Search**
* **Real Time** Extraction Engine to keep indexes **automatically** updated as new data is ingested.
* Create **Extraction Graph** to describe **data transformation** and extraction of **embedding** and **structured extraction**.
* **Incremental Extraction** and **Selective Deletion** when content is deleted or updated.
* **Extractor SDK** allows adding new extraction capabilities, and many readily avaialble extractors for **PDF**, **Image** and **Video** indexing and extraction.
* **Multi-Tenant** from the ground up, **Namespaces** to isolate sensistive data.
* Works with **any LLM Framework** including **Langchain**, **DSPy**, etc.
* Runs on your laptop during **prototyping** and also scales to **1000s of machines** on the cloud.
* Works with many **Blob Stores**, **Vector Stores** and **Structured Databases**
* We have even **Open Sourced Automation** to deploy to Kubernetes in production.

## Start Using Indexify

Dive into [Getting Started](getting_started.md) to learn how to use Indexify.

## Difference Between Indexify and LlamaIndex

Indexify is the distributed data framework and compute engine. Your extraction and data processing workflows will run asynchronously and reliably in Indexify. LlamaIndex is an application framework for querying data from vector stores and for response synthesis with LLMs. It doesn't include a fault tolerant and reliable distributed orchestration engine in the open source library. LlamaIndex and Indexify are complementary, you can use LlamaIndex's query engine and other components such as data loaders to ingest content for transformation and extraction using Indexify. 

## Different Between Indexify and Spark
Spark is a map-reduce system, works well with tabular data and with compute functions written in Java. Indexify is faster than Spark as it doesn't rely on an external scheduler like Kubernetes for Mesos for task scheduling. Indexify also remembers the extracted content form data so it can go and update them when the source changes.