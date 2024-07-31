import os
from indexify import IndexifyClient
import requests
import base64
from openai import OpenAI

client = IndexifyClient()
client_openai = OpenAI(api_key="YOUR_OPENAI_API_KEY")

def download_pdf(url, save_path):
    response = requests.get(url)
    with open(save_path, 'wb') as f:
        f.write(response.content)
    print(f"PDF downloaded and saved to {save_path}")

def process_pdf(pdf_path):
    content_id = client.upload_file("rag_pipeline", pdf_path)
    client.wait_for_extraction(content_id)

def get_context(question: str, index: str, top_k=3):
    results = client.search_index(name=index, query=question, top_k=top_k)
    context = ""
    for result in results:
        context = context + f"content id: {result['content_id']} \n\n passage: {result['text']}\n"
    return context

def create_prompt(question, context):
    return f"Answer the question, based on the context.\n question: {question} \n context: {context}"

def answer_question(question):
    text_context = get_context(question, "rag_pipeline.chunks_to_embeddings.embedding")
    image_context = client.search_index(name="rag_pipeline.image_to_embeddings.embedding", query=question, top_k=1)
    image_id = image_context[0]['content_metadata']['id']
    image_url = f"localhost:8900/namespaces/default/content/{image_id}/download"
    prompt = create_prompt(question, text_context)
    
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
                    "url": f"{image_url}"
                }
                }
            ]
            }
        ],
        model="gpt-4o-mini",
    )
    return chat_completion.choices[0].message.content

def process_pdf_url(url, index):
    pdf_path = f"reference_document_{index}.pdf"
    try:
        download_pdf(url, pdf_path)
        process_pdf(pdf_path)
        print(f"Successfully processed: {url}")
    except Exception as exc:
        print(f"Error processing {url}: {exc}")

# Example usage
if __name__ == "__main__":
    pdf_urls = [
        "https://proceedings.neurips.cc/paper_files/paper/2017/file/3f5ee243547dee91fbd053c1c4a845aa-Paper.pdf",
        "https://arxiv.org/pdf/1810.04805.pdf"
    ]
    
    # Download and process PDFs sequentially
    for i, url in enumerate(pdf_urls):
        process_pdf_url(url, i)

    # Ask questions
    questions = [
        "What does the architecture diagram show?",
        "Explain the attention mechanism in transformers.",
        "What are the key contributions of BERT?"
    ]

    for question in questions:
        answer = answer_question(question)
        print(f"\nQuestion: {question}")
        print(f"Answer: {answer}")
        print("-" * 50)