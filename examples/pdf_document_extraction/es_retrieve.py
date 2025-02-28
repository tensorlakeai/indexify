from elasticsearch import Elasticsearch
import random

#### Connect from host machine, using the port exposed by Docker
es = Elasticsearch(["http://localhost:9200"])
print(es.cluster.health())

#### List all indices
indices = es.indices.get(index='*')
for index_name in indices:
    print(index_name)

#### With details
indices_info = es.cat.indices(format='json')
for index in indices_info:
    print(f"{index['index']} doc count: {index['docs.count']}")

#### Print fields in index.
# Indexes from this example are `image_embeddings` and `text_embeddings`.

INDEX_NAME = "text_embeddings"

mapping = es.indices.get_mapping(index=INDEX_NAME)
index_name = list(mapping.body.keys())[0]
fields = mapping.body[index_name]['mappings']['properties'].keys()

print("Fields in index:")
for field in fields:
    print(field)

#### Sanity test.
# A random text embedding vector to test the pipeline. In production you would have to call the same model as the workflow to compute the embedding.
# Text embedding as per the workflow has a size of 768. Change the seed or the uniform generator for different results.

#random.seed(42)
random.seed(4)
query_vec = [random.uniform(0.8, 1.) for _ in range(768)]
#query_vec = [random.uniform(0.5, .6) for _ in range(768)]

# Query the documents with knn. Change k and num_candidates (k <= num_candidates)
QUERY_FIELD_TEXT_INDEX = "embedding"
response = es.search(
    index=INDEX_NAME,
    body={
        "knn": {
            "field": QUERY_FIELD_TEXT_INDEX,
            "query_vector": query_vec,
            "k": 2,
            "num_candidates": 3
        }
    }
)

#### Print document structure
print("\n KNN Documents:")
for hit in response['hits']['hits']:
    print(f"Document ID: {hit['_id']}, Score: {hit['_score']}")
    print(hit['_source']['chunk'])