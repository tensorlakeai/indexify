# Key Concepts

A typical workflow using Indexify involves adding content to a data repository, wait for views such as vector indexes to be updated as extractors run on the content. Once the views are populated, you could retreive information from the views, via semantic search or read named entities, or see how content is clustered.

![High Level Concept](images/indexify_high_level.svg)

## Data Repositories
Data Repositories are logical abstractions for storing related documents, and storing long term memory of LLM applications which require access to knowledge encapsulated in the documents. Repositories allow parititioning data based on query patterns, and we will add support for access control per repository in the future. By default, we create a `default` data repository to make it easy to get started.

## Document

A document represents information as corpus of text, along with corresponding metadata. Document text can be as large as the user wants.
Documents are chunked, embedded, and indexed automatically. Documents ingested in the system are stored permanently, and features like embedding, named entity, etc, can be extracted from them at any point.

## Memory

Memory APIs provides long term storage and retrieval of interactions with LLMs and users. Memory retrieval APIs provide access to all previously stored messages or most relevant messages based on a query. Memory events are stored in a `Session` in a data repository. 

## Extractors and Views

Extractors are process that extracts information from documents and conversational memory and populates *Views* indexes and other databases to be made available for retrieval processes. Extractors run asynchronously which makes it easy to scale the service and extract information from millions of documents. 

We create a default extractor which creates a vector index view to do semantic searches over all documents in a repository.

Examples of views are vector indexes populated by embedding extractors or an entity store populated by a NER model. Views are per data repository, and any extractor attached to a data repository can write to any view. 

Even if you only need vector indexes for retrieval, having the ability to create views from existing content allows testing different embedding models and retrieval parameters or update them over time in production, and create new views.

## Vector Index

A vector index allows for quick retrieval of relevant information, given a user query using semantic search. Indexes are automatically updated by extractors when content or memory is added to a data repository, and are made available for retrieval.
Indexify currently supports [HNSW (Hierarchical Navigable Small World Graph)](https://arxiv.org/abs/1603.09320) based vector indexes.

