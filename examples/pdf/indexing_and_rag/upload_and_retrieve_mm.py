from indexify import IndexifyClient
from indexify.data_loaders import UrlLoader
import requests
from openai import OpenAI
import base64

client = IndexifyClient()
client_openai = OpenAI()

def get_page_number(content_id: str) -> int:
    content_metadata = client.get_content_metadata(content_id)
    page_number = content_metadata["extracted_metadata"]["metadata"]['page_num']
    return page_number

def get_context(question: str, index: str, top_k=3):
    results = client.search_index(name=index, query=question, top_k=top_k)
    context = ""
    for result in results:
        parent_id = result['content_metadata']['parent_id']
        page_number = get_page_number(parent_id)
        context = context + f"content id: {result['content_id']} \n\n page number: {page_number} \n\n passage: {result['text']}\n"
    return context

def create_prompt(question, context):
    return f"""Answer the question, based on the context.
    Mention the content ids and page numbers as citation at the end of the response, format -
    Citations: 
    Content ID: <> Page Number <>.

    question: {question}
    context: {context}"""

def answer_question(question):
    text_context = get_context(question, "rag_pipeline_mm.chunks_to_embeddings.embedding")
    image_context = client.search_index(name="rag_pipeline_mm.image_to_embeddings.embedding", query=question, top_k=1)
    image_id = image_context[0]['content_metadata']['id']
    image_url = f"http://localhost:8900/namespaces/default/content/{image_id}/download"
    prompt = create_prompt(question, text_context)

    image_data = requests.get(image_url).content
    base64_image = base64.b64encode(image_data).decode('utf-8')

    chat_completion = client_openai.chat.completions.create(
        messages=[
            {
            "role": "user",
            "content": [
                {
                "type": "text",
                "text": prompt
                },
                {
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/jpeg;base64,{base64_image}"
                }
                }
            ]
            }
        ],
        model="gpt-4o-mini",
    )
    return chat_completion.choices[0].message.content

if __name__ == "__main__":
    # Uncomment the lines if you want to upload more than 1 pdf
    pdf_urls = [
        "https://proceedings.neurips.cc/paper_files/paper/2017/file/3f5ee243547dee91fbd053c1c4a845aa-Paper.pdf",
    #    "https://arxiv.org/pdf/1810.04805.pdf",
    #    "https://arxiv.org/pdf/2304.08485"
    ]

    data_loader = UrlLoader(pdf_urls)
    content_ids = client.ingest_from_loader(data_loader, "rag_pipeline_mm")

    print(f"Uploaded {len(content_ids)} documents")
    client.wait_for_extraction(content_ids)

    # Ask questions
    questions = [
        "What does the architecture diagram show?",
        "Explain the attention mechanism in transformers.",
        "What are the key contributions of BERT?",
    ]
    for question in questions:
        answer = answer_question(question)
        print(f"\nQuestion: {question}")
        print(f"Answer: {answer}")
        print("-" * 50)
