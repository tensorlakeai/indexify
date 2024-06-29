# Effortless Ingestion and Extraction for AI Applications at Any Scale

![Indexify High Level](images/Indexify_KAT.gif)

Indexify is a data framework for building ingestion and extraction pipelines for unstructured data. The ingestion pipelines are defined using declarative configuration, and every stage of the pipeline can perform structured extraction using any AI model, or transform the ingested data. The pipelines start working immediately when data is ingested into Indexify, thus making them ideal for interactive applications and low-latency use-cases.

You should use Indexify if - 

1. You are working with non-trivial amount of data, >1000s of documents, audio files, videos or images. 
2. The data volume grows over time, and LLMs need access to updated data as quickly as possible
3. You care about reliability and availability of your ingestion pipelines. 
4. User Experience of your application degrades if your LLM application is reading stale data when data sources are updated.

## Differences with LLM Frameworks
Most common LLM data frameworks for unstructured data are primarily optimized for prototyping applications locally. A typical MVP data processing pipeline is written as follows -
```python
data = load_data(source)
embedding = generate_embedding(data) 
structured_data = structured_extraction_function(data)
db.save(embedding)
db.save(structured_data)
```
All of these lines in the above code snippet can fail. If your application relies on your data framework being reliable and not losing data, you will inevitably lose data in production with LLM data frameworks designed for prototyping MVPs.

Indexify is designed to be distributed on many machines to allow scale-outs, fault-tolerant to hardware or software crashes, predictable latencies and throughput, and observable to help troubleshoot. The pipeline state, which tracks which steps of a pipeline is currently running is replicated across multiple machines which allows recovering from hardware failures, software crashes, losing even clusters of machines.

## Local Experience
Indexify runs locally without **any** dependencies, making it easy to build applications and test and iterate locally. It does so without sacrificing the properties that make data systems shine in production environments. Applications built with Indexify can run on laptops and can run unchanged in production. Indexify can auto-scale, is distributed, fault-tolerant, and is fully observable with predictable latencies and throughput. 

## Start Using Indexify

Dive into [Getting Started](getting_started.md) to learn how to use Indexify.

If you would like to learn some common use-cases - 

1. Learn how to build production grade [RAG Applications](usecases/rag.md)
2. Extract [PDF](usecases/pdf_extraction.md), [Videos](usecases/video_rag.md) and [Audio](usecases/audio_extraction.md) to extract embedding and [structured data](usecases/image_retrieval.md).

## Features

* Makes Unstructured Data **Queryable** with **SQL** and **Semantic Search**
* **Real Time** Extraction Engine to keep indexes **automatically** updated as new data is ingested.
* Create **Extraction Graph** to create multi-step workflows for **data transformation**, **embedding** and **structured extraction**.
* **Incremental Extraction** and **Selective Deletion** when content is deleted or updated.
* **Extractor SDK** allows adding new extraction capabilities, and many readily available extractors for **PDF**, **Image** and **Video** indexing and extraction.
* **Multi-Tenant** from the ground up, **Namespaces** to isolate sensitive data.
* Works with **any LLM Framework** including **Langchain**, **DSPy**, etc.
* Runs on your laptop during **prototyping** and also scales to **1000s of machines** on the cloud.
* Works with many **Blob Stores**, **Vector Stores** and **Structured Databases**
* We have even **Open Sourced Automation** to deploy to Kubernetes in production.

