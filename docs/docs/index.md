# Welcome to Indexify

Indexify is a Retrieval and Long Term Memory Service for Generative AI Applications.

Indexes are always kept up to date by running extraction models such as embedding, NER, etc. on new documents that are uploaded to the service. Indexify has a built in distributed extraction scheduler that allows indexing large amount of data for production use cases.

Applications can store long term memory and query them in real time to personalize co-pilot or chat based applications.

Indexes are created by Extractors which work on any kind of data and produce extracted data such as embeddings(in case of embedding extractors) or a JSON document containing some specific attributes of the content(NER, Segment Anything or most other machine learning models). Developers can extend indexify by building and deploying custom extractors like building indexes from objects in images. 

The value of Indexify lies in being able to ingest any kind of content, apply extractors on them and letting indexify store the extracted data in indexes that can be queried in real time by applications. Indexes are always kept up to date as new data comes in, and over time as new extractors are added to the platform your data platform will have more capabilities! 

## Why use Indexify
* **Knowledge Base for LLMs:** Real time retrieval of knowledge and context from private documents and structured data to improve accuracy of LLM models.
* **Memory Engine for Co-Pilot agents:** Store and retrieve long-term memory of agents in real-time, providing enhanced personalization and improved user experiences for co-pilot and chat based applications.
* **Distributed Extraction Engine For Scale:** Distributed extraction to scale indexing large amount of data without sacrificing retrieval performance.
* **Custom Extractors:** You can extend Indexify by writing a custom extractor for your use cases to extract specific information from data.

## Start Using Indexify
Dive into [Getting Started](getting_started.md) to learn how to use Indexify.
