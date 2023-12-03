# Key Concepts

A typical workflow using Indexify involves adding content to a data repository, wait for views such as vector indexes to be updated as extractors run on the content. Once the views are populated, you could retrieve information from the views, via semantic search or read named entities, or see how content is clustered.

![High Level Concept](images/indexify_high_level_abstract.png)

## Extractors
Extractors extract features such as embedding or any other structured data from content to be made available for retrieval processes. Extractors can also produce more content, that can be consumed by other extractors. 

Some examples of extractors are -
* PDF Extractors that extract embedding from texts, finds named entities, or summarize content.
* Audio segmentation extractors which chunks audio based on presence of human speech. 

## Indexes
Indexify stores extracted features in Indexes. Indexify uses vector storage to store embedding and structured data such as JSON are stored in Postgres and other document stores. Indexes are automatically updated when content is added to a data repository by applying extractors on them, and are made available for retrieval.

## Data Repositories
Data Repositories are logical abstractions for storing related content. Repositories allow partitioning data based on security and organizational boundaries.

## Content 
Content are ny kind of unstructured data such as text, video or audio along with corresponding metadata. Content is stored in blob and K/V stores. 
Content are chunked, embedded, and indexed automatically by extractors. Content are either added by external systems or from extractors which chunk or transform unstructured data into intermediate forms, such as text chunks of large PDF/HTML docs, small speech segments of a podcast.

## Retrieval APIs

Retrieval APIs are used to retrieve information from indexes. Indexify supports two types of retrieval APIs - semantic search and faceted search.

#### Vector Index

A vector index allows for quick retrieval of relevant information, given a user query using semantic search. 
Indexify currently supports [HNSW (Hierarchical Navigable Small World Graph)](https://arxiv.org/abs/1603.09320) based vector indexes.

#### Metadata Index

Metadata Index are created from metadata extracted as JSON documents from content. For example, output of a NER extractor can be searched for chunks of PDFs that has the name of a person. We support full text search on metadata indexes and also json path based queries.