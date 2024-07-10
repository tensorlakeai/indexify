from indexify import IndexifyClient

client = IndexifyClient()

ingested_content_list = client.list_content("wiki_extraction_pipeline") 
content_id = ingested_content_list[0].id
entities = client.get_extracted_content(
    content_id, 
    "wiki_extraction_pipeline", 
    "entity-extractor")


chunks = client.get_extracted_content(
    content_id, 
    "wiki_extraction_pipeline", 
    "chunker") #(1)!

from openai import OpenAI

client_openai = OpenAI()

def query_database(question: str, index: str, top_k=3):
    retrieved_results = client.search_index(name=index, query=question, top_k=top_k) 
    context = "\n-".join([item["text"] for item in retrieved_results])
    response = client_openai.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": f"Answer the question, based on the context.\n question: {question} \n context: {context}",
            },
        ],
        model="gpt-3.5-turbo",
    )
    return response.choices[0].message.content


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