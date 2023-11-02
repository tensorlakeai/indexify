# Indexify

![Tests](https://github.com/diptanu/indexify/actions/workflows/test.yaml/badge.svg?branch=main)

Indexify is a reactive content extraction and retrieval engine for Generative AI Applications.

Applications that use LLMs on real-world data to plan actions autonomously or to answer queries require the indexes to be updated as the data is updated.

Indexes are constantly updated by applying extractors on data ingested into the service. Indexify has a data-parallel and hardware accelerator-aware extraction engine that allows indexing large amounts of data in real-time.

Extractors are modules that apply AI models to data and produce embeddings or structured information, such as named entities in a document or objects of interest and their location in images. Developers can build new extractors for their use cases, such as creating indexes from healthcare records or indexes from a code repository for searching code and documentation.

## Why use Indexify

* **Knowledge Base for LLMs:** Real-time retrieval of knowledge and context from private documents and structured data to improve the accuracy of LLM models.
* **Distributed Extraction Engine For Scale:** Distributed extraction to scale indexing large amounts of data without sacrificing retrieval performance.
* **Custom Extractors:** You can extend Indexify by writing a custom extractor for your use cases to extract specific information from data.
* **Pluggable Storage:** Easily add support for new storage backends.

## Getting Started

To get started follow our [documentation](https://getindexify.ai/getting_started/).

## Documentation

Our comprehensive documentation is available - https://getindexify.ai

## Contributions
Please open an issue to discuss new features, or join our Discord group. Contributions are welcome, there are a bunch of open tasks we could use help with! 

If you want to contribute on the Rust codebase, please read the [developer readme](docs/docs/develop.md).

## Contact 
Join the Discord Server - https://discord.gg/mrXrq3DmV8 <br />
