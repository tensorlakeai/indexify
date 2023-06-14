# Indexify APIs

Indexify has REST APIs and a python library for adding documents and long term memory for retrieval.

Indexes are automatically updated when content or memory is added, and are made available for retrieval. More information on the Retrieval APIs can be found [here](retrieval.md).

Memory APIs provides long term storage and retrieval of interactions with LLMs and users. Memory retrieval APIs provide access to all previously stored messages or most relevant messages based on a query. More information on Memory APIs can be found [here](memory.md).

## Key Concepts

### Data Repositories
Data Repositories are logical abstractions for storing related documents, and storing long term memory of LLM applications which require access to knowledge encapsulated in the documents. By default, we crate a `default` data repository to make it easy to get started.

### Extractors
Extractors are process that extracts information from documents and memory and populates indexes and other databases to be made available for retrieval processes. 
Extractors enables embedding documents and populating indexes in vector databases, or extracting named entities from memory. 
We create a default extractor to enable indexing all documents in a data repository.

### Index
An index is a data structure that allows for quick retrieval of relevant information, given a user query.
Indexify currently supports [HNSW (Hierarchical Navigable Small World Graph)](https://arxiv.org/abs/1603.09320) based vector indexes.

### Document 

A document represents information as corpus of text, along with corresponding metadata. Document text can be as large as the user wants.

Documents are chunked, embedded, and indexed automatically.

### Memory

Memory module allows for storage, retrieval of, and search of temporal information data (ie: chatbot conversation history). Memory events are stored in a `Session` in a data repository. 
