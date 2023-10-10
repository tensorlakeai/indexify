# Welcome to Indexify

Indexify is a reactive content extraction and retrieval engine for Generative AI Applications.

Applications that use LLMs on real-world data to plan actions autonomously or to answer queries require the indexes to be updated as the data is updated.

Indexes are constantly updated by applying extractors on data ingested into the service. Indexify has a data-parallel and hardware accelerator-aware extraction engine that allows indexing large amounts of data in real-time.

Extractors are modules that apply AI models to data and produce embeddings or structured information, such as named entities in a document or objects of interest and their location in images. Developers can build new extractors for their use cases, such as creating indexes from healthcare records or indexes from a code repository for searching code and documentation.

Storage is pluggable so that we can quickly support any Vector Storage service or a document/NoSQL store for structured data.

## Why use Indexify

* **Knowledge Base for LLMs:** Real-time retrieval of knowledge and context from private documents and structured data to improve the accuracy of LLM models.
* **Distributed Extraction Engine For Scale:** Distributed extraction to scale indexing large amounts of data without sacrificing retrieval performance.
* **Custom Extractors:** You can extend Indexify by writing a custom extractor for your use cases to extract specific information from data.
* **Pluggable Storage:** Easily add support for new storage backends.

## Start Using Indexify

Dive into [Getting Started](getting_started.md) to learn how to use Indexify.
