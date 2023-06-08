# Indexify APIs

Indexify provides HTTP APIs, that can be interacted with from any programming languages. We will have official libraries for Python and Typescript in the future.

## Embedding APIs

Embedding models can be directly accessed through the APIs, and can be used with custom/third party retrieval systems. For ex, retrieval systems built with Langchain can use embedding models from Indexify.

### List Models
```
GET /embeddings/models
```


### Generate Embeddings
```
GET /embeddings/generate
```
#### Request Body
* `inputs` - List of strings to generate embeddings for.
* `model` - Name of the model to use for generating embeddings.

#### Example
```
 curl -X GET http://localhost:8900/embeddings/generate   -H "Content-Type: application/json" -d '{"inputs": ["lol", "world"], "model": "all-minilm-l12-v2"}'
 ```
