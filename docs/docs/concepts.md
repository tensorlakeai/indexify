# Key Concepts

A typical workflow using Indexify involves adding content to a data repository, wait for views such as vector indexes to be updated as extractors run on the content. Once the views are populated, you could retrieve information from the views, via semantic search or read named entities, or see how content is clustered.

![High Level Concept](images/indexify_high_level_abstract.png)

## Extractors
Extractors extract features such as embedding or any other structured data from content to be made available for retrieval processes. Some examples of extractors are - PDF Extractors that extract embedding from texts, finds named entities, or summarize content. Extractors can also produce more content, that can be consumed by other extractors. Some examples are - audio segmentation extractors which chunks audio based on presence of human speech. Extractors run asynchronously and in a distributed manner which makes it easy to scale the service and extract information from millions of documents.  

## Indexes
Indexes are the sinks where Indexify stores extracted features and content from extractors. Indexify uses vector storage to store embedding and structured data such as JSON are stored in Postgres and other document stores. Indexify presents a unified interface and logical data model to abstract away details of accessing the underlying storage systems. Indexes are automatically updated when content is added to a data repository by applying extractors on them, and are made available for retrieval.

## Data Repositories
Data Repositories are logical abstractions for storing related documents. Repositories allow partitioning data based on query patterns, and we will add support for access control per repository in the future. By default, we create a `default` data repository to make it easy to get started.

## Content 
Content represents information as corpus of text, vidoe, audio or other unstructured data along with corresponding metadata. Content is stored in blob and K/V stores. 
Documents are chunked, embedded, and indexed automatically. Documents ingested in the system are stored permanently, and Indexify can extract features like embedding, named entity, etc, and store them in various forms of indexes at any point of time in the future.

## Retrieval APIs

Retrieval APIs are used to retrieve information from indexes. Indexify supports two types of retrieval APIs - semantic search and faceted search.

#### Vector Index

A vector index allows for quick retrieval of relevant information, given a user query using semantic search. 
Indexify currently supports [HNSW (Hierarchical Navigable Small World Graph)](https://arxiv.org/abs/1603.09320) based vector indexes.

#### Attribute Index

Attribute Index are created from information extracted by models which can be used for filtering and faceting. For example, listing all documents which have a particular named entity like physical addresses of businesses, or all documents which have a particular topic. 
Attribute Indexes are stored as JSON documents under the hood.