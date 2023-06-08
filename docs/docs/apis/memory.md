# Indexify APIs

Indexify provides HTTP APIs, that can be interacted with from any programming languages. We will have official libraries for Python and Typescript in the future.

## Memory APIs

Memory APIs are centered around retrieving memory from conversation history to use in context. Indexify supports retrieving all conversation history for a memory session, as well as query-based semantic search.


### Create Memory Session
```
POST /memory/create
```
#### Request Body
* `index_args`
    * `name` - Name of the index
    * `embedding_model` - Name of the embedding model to use.
    * `metric` - Distance Metric to use for similarity search on the Index. Possible values - `dot`, `cosine` and `euclidean`.
    * `text_splitter` - Text Splitting algorithm to use to chunk long text into shorter text. Possible values - `none`, `new_line`, `{"html": {"num_elements": 1}}`.
    * `hash_on` - List of attributes in the metadata of documents to hash on for uniqueness of content. If the list is empty, we will hash on the document content such that duplicates are not inserted in the index.

#### Example 
```
curl -X POST http://localhost:8900/memory/create   -H "Content-Type: application/json" -d '{"index_args": {"name": "myindex", "embedding_model": "all-minilm-l12-v2", "metric": "dot", "text_splitter": "new_line"}}'
```

### Add Memory
```
POST /memory/add
```
#### Request Body
* `session_id` - UUID corresponding to memory session.
* `messages` -
    * `role` - Role (typically `Human` or `AI`).
    * `text` - Conversation text.
    * `metadata` - Key/Value pair of metadata associated with the text. 

#### Example 
```
curl -X POST http://localhost:8900/memory/add   -H "Content-Type: application/json" -d '{"session_id": "77569cf7-8f4c-4f4b-bcdb-aa54355eee13", "messages": [{"role": "Human", "text": "Indexify is amazing!", "metadata": {}}]}'
```

### Retrieve Memory
```
GET /memory/get
```
#### Request Body
* `session_id` - UUID corresponding to memory session.

#### Example 
```
curl -X GET http://localhost:8900/memory/get   -H "Content-Type: application/json" -d '{"session_id": "77569cf7-8f4c-4f4b-bcdb-aa54355eee13"}'
```

### Search Memory
```
GET /memory/search
```
#### Request Body
* `session_id` - UUID corresponding to memory session.
* `query` - Query string.
* `k` - Top k responses.

#### Example 
```
curl -X GET http://localhost:8900/memory/search   -H "Content-Type: application/json" -d '{"session_id": "77569cf7-8f4c-4f4b-bcdb-aa54355eee13", "query": "good", "k": 1}'
```