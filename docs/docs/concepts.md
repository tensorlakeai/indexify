# Key Concepts

A typical workflow using Indexify involves adding content to a data repository, wait for views such as vector indexes to be updated as extractors run on the content. Once the views are populated, you could retrieve information from the views, via semantic search or read named entities, or see how content is clustered.

![High Level Concept](images/indexify_high_level.svg)

## Data Repositories
Data Repositories are logical abstractions for storing related documents, and storing long term memory of LLM applications which require access to knowledge encapsulated in the documents. Repositories allow partitioning data based on query patterns, and we will add support for access control per repository in the future. By default, we create a `default` data repository to make it easy to get started.

## Document

A document represents information as corpus of text, along with corresponding metadata. Document text can be as large as the user wants.
Documents are chunked, embedded, and indexed automatically. Documents ingested in the system are stored permanently, and Indexify can extract features like embedding, named entity, etc, and store them in various forms of indexes at any point of time in the future.

## Events 

Events are messages which are causal in nature, such as a chat message history between a user and an LLM. When information from events are extracted, like generating summary from them, the extractors are presented the events in the order they were created.

## Extractors and Indexes

Extractors are process that extracts information from documents and event and populates indexes to be made available for retrieval processes. Extractors run asynchronously and in a distributed manner which makes it easy to scale the service and extract information from millions of documents.  

Examples of indexes are vector indexes populated by embedding extractors or an entity store populated by a NER model. Indexes are per data repository, and any extractor bound to a data repository can write to any Index.


### Indexes

#### Vector Index

A vector index allows for quick retrieval of relevant information, given a user query using semantic search. Indexes are automatically updated by extractors when content or memory is added to a data repository, and are made available for retrieval.
Indexify currently supports [HNSW (Hierarchical Navigable Small World Graph)](https://arxiv.org/abs/1603.09320) based vector indexes.

#### Attribute Index

Attribute Index are created from information extracted by models which can be used for filtering and faceting. For example, listing all documents which have a particular named entity like physical addresses of businesses, or all documents which have a particular topic. 

Attribute Indexes are stored as JSON documents under the hood with the key/value pairs as properties of the document.