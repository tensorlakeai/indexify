from indexify import IndexifyClient
from llama_cpp import Llama

client = IndexifyClient()
llm = Llama.from_pretrained(repo_id='NousResearch/Hermes-2-Theta-Llama-3-8B-GGUF', filename='*Q8_0.gguf',  verbose=False, n_ctx=2048)

# Get entities
ingested_content_list = client.list_content("wiki_extraction_pipeline")
content_id = ingested_content_list[0].id
entities = client.get_extracted_content(
    content_id, 
    "wiki_extraction_pipeline", 
    "entity-extractor")

# Get chunks
chunks = client.get_extracted_content(
    content_id, 
    "wiki_extraction_pipeline", 
    "chunker")

def query_database(question: str, index: str, top_k=3):
    retrieved_results = client.search_index(name=index, query=question, top_k=top_k)
    context = "\n-".join([item["text"] for item in retrieved_results])
    
    response = llm.create_chat_completion(
        messages=[
            {"role": "system", "content": "You are a helpful assistant that answers questions based on the given context."},
            {"role": "user", "content": f"Answer the following question based on the context provided:\nQuestion: {question}\nContext: {context}"}
        ]
    )
    
    return response["choices"][0]["message"]["content"]

if __name__ == "__main__":
    index_name = "wiki_extraction_pipeline.wikiembedding.embedding"
    indexes = client.indexes()
    print(f"Vector indexes present: {indexes}, querying index: {index_name}")
    print(
        query_database(
            "What accomplishments did Kevin durant achieve during his career?",
            "wiki_extraction_pipeline.wikiembedding.embedding",
            4,
        )
    )

