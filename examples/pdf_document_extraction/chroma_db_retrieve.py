from chromadb  import HttpClient, QueryResult
client = HttpClient(host="localhost", port=8000)
from sentence_transformers import SentenceTransformer

# Query VectorDB for similar text
text_collection = client.get_collection("text_embeddings")
query_embeddings = SentenceTransformer('Alibaba-NLP/gte-base-en-v1.5', trust_remote_code=True).encode(["transformers"])
result: QueryResult = text_collection.query(query_embeddings, n_results=5, )
documents = result["documents"][0]
distances = result["distances"][0]
for i, document in enumerate(documents):
    print(f"document {document}, score: {distances[i]}")
