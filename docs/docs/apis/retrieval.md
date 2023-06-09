# Retrieval APIs

Retrieval APIs are centered around managing and populating indexes, and querying them using various algorithms.


## Index Creation

```
POST /index/create 
```
### Request Body
* `name` - Name of the index

* `embedding_model`- Name of the embedding model to use.

* `metric` - Distance Metric to use for similarity search on the Index. Possible values - `dot`, `cosine` and `euclidean`.

* `text_splitter` - Text Splitting algorithm to use to chunk long text into shorter text. Possible values - `none`, `new_line`, `{"html": {"num_elements": 1}}`


* `hash_on` - List of attributes in the metadata of documents to hash on for uniqueness of content. If the list is empty, we will hash on the document content such that duplicates are not inserted in the index.

### Example 
```
curl -X POST http://localhost:8900/index/create   -H "Content-Type: application/json" -d '{"name": "myindex", "embedding_model": "all-minilm-l12-v2","metric": "dot", "text_splitter": "new_line"}'
```

## Adding to the Indexes

```
POST /index/add
```

### Request Body
* `index` - Index in which the text belongs to.
* `documents` - List of document objects. Structure of document objects - 
    * `text` - Text of the document
    * `metadata` - Key/Value pair of metadata associated with the text. 

### Example
```
curl -X POST http://localhost:8900/index/add   -H "Content-Type: application/json" -d '{"index": "myindex", "documents": [{"text": "Indexify is amazing!", "metadata":{"key": "k1"}}]}'
```

## Index Query
```
GET /index/search
```
### Request Body
* `index` - Name of the index to search on.
* `query` - Query string.
* `k` - top k responses.

### Example 
```
curl -X GET http://localhost:8900/index/search   -H "Content-Type: application/json" -d '{"index": "myindex", "query": "good", "k": 1}'
```