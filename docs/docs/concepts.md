# Key Concepts

A typical workflow using Indexify involves uploading unstructured data(documents, videos, images, audio), wait for vector indexes or structured stores to be updated as extractors are applied on the content. As indexes are updated continuously, you could retrieve updated information from them, via semantic search on vector indexes and SQL queries from structured data tables.

![Block Diagram](images/key_concepts_block_diagram.png)

### Extractor
A python class that can -

1. Transform unstructured data into intermediate forms. For example, a PDF document transformed into text, images, structured data if it contains tabular data.
2. Extract features like embedding or metadata(JSON) that can be retrieved by LLM applications.

###### Transformation
Extractors can transform a source by simply returning a list of `Content`. Examples - Returning an audio file from a video. Returning chunks of text from a PDF. Returning audio segments based on presence of speech in an audio file.

```
Extractor(Content) -> List[Content]
```

![Content Transformation](images/key_concepts_transform.png)

###### Structured Data Extraction
An extractor can enrich an ingested content by associating structured data to the content. For example, adding bounding boxes of detected objects and their labels to an image. Structured Data is made queryable using SQL queries.

```
Extractor(Content) -> List[Feature(Type=Metadata)]
```

![Feature Extraction](images/key_concepts_feature_extraction.png)

###### Embedding Extraction
An extractor can return embeddings for any ingested content. Indexify automatically creates Indexes from extracted embeddings. 
```
Extractor(Content) -> List[Feature(Type=Embedding)]
```

![Embedding Extraction](images/key_concepts_embeddings.png)

###### Transformation, Embedding and Metadata Extraction Combined
An extractor can transform, and extract embedding/metadata at the same time. Just return a list of new Content, along with the features of the transformed content, and a list of features at the same time. Indexify assumes that the features returned without any content belong to the ingested content and the list of new content are transformed content.
```
Extractor(Content) -> List[Feature... Content ...]
```

### Namespaces
Namespaces are logical abstractions for storing related content. Namespaces allow partitioning data based on security and organizational boundaries.

### Content
Unstructured data(documents, video, images) is represented as Content. 

### Extraction Graphs
Extraction Graphs apply a sequence of extractors on ingested content in a streaming manner. Individual steps in an Extraction Graph is known as Extraction Policy. 
Indexify tracks lineage of transformed content and extracted features from source. This allows deleting all the transformed content and features when the sources are deleted.

![Extraction Policy](images/key_concepts_extraction_policy.png)

### Vector Index and Retrieval APIs
Vector Indexes are automatically created from extractors that returns embeddings. You can use any of the supported vector databases(Qdrant, Elastic Search, Open Search, PostgreSQL and LanceDB). They can be looked up using semantic/KNN search. 

### Structured Data Tables
Metadata extracted from content is exposed using SQL Queries. Every Extraction Graph exposes a virtual SQL table, and any metadata added to content can be queried by SQL Queries on the virtual table.

Example - If you create a policy named `object_detector` which runs the YOLO object detector against all images ingested, you can query all the images which has a ball like this -

```
select * from object_detector where object_name='ball'
```