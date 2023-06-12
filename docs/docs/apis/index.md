# Indexify APIs

Indexify provides REST HTTP APIs, that can be interacted with from any programming languages, as well as a python library.

Retrieval APIs are centered around managing and populating indexes, and querying them using various algorithms. More information on the Retrieval APIs can be found [here](retrieval.md).

Memory APIs are centered around retrieving memory from conversation history to use in context. Indexify supports retrieving all conversation history for a memory session, as well as query-based semantic search. More information on Memory APIs can be found [here](memory.md).

## Key Concepts

### Index
An index is a data structure that allows for quick retrieval of relevant information, given a user query.

Indexify currently supports [HNSW (Hierarchical Navigable Small World Graph)](https://arxiv.org/abs/1603.09320) based vector indexes.

### Document

A document represents information as corpuses of text, along with corresponding metadata. Document text can be as large as the user wants.

Documents are chunked, embedded, and indexed; semantic search can be performed on the embedded vectors.

### Memory Session

A memory session allows for storage, retrieval of, and search of temporal information data (ie: chatbot conversation history). 

By default LLMs are stateless; memory sessions can be utilized for applications such as chatbots to retain context from prior messages in a conversation.


## Python Library

### Getting started
``
pip install indexify
```

More information coming soon.
