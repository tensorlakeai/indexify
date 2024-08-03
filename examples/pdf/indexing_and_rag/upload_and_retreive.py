import os
from indexify import IndexifyClient
import requests
from openai import OpenAI

client = IndexifyClient()

client_openai = OpenAI()

def download_pdf(url, save_path):
    response = requests.get(url)
    with open(save_path, 'wb') as f:
        f.write(response.content)
    print(f"PDF downloaded and saved to {save_path}")

def process_pdf(pdf_path):
    content_id = client.upload_file("rag_pipeline", pdf_path)
    client.wait_for_extraction(content_id)

def get_page_number(content_id: str) -> int:
    content_metadata = client.get_content_metadata(content_id)
    page_number = content_metadata["extracted_metadata"]["metadata"]['page_num']
    return page_number

def get_context(question: str, index: str, top_k=3):
    results = client.search_index(name=index, query=question, top_k=top_k)
    context = ""
    for result in results:
        # Search result returns the chunk id. Chunks are derived from extracted pages, which are 
        # the 'parent', so we grab the parent id and get the content metadata of the page. The page numbers
        # are stored in the extracted metadata of the pages.
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
    context = get_context(question, "rag_pipeline.chunk_embedding.embedding")
    prompt = create_prompt(question, context)
    
    chat_completion = client_openai.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": prompt,
            }
        ],
        model="gpt-3.5-turbo",
    )
    return chat_completion.choices[0].message.content

# Example usage
if __name__ == "__main__":
    pdf_url = "https://proceedings.neurips.cc/paper_files/paper/2017/file/3f5ee243547dee91fbd053c1c4a845aa-Paper.pdf"
    pdf_path = "reference_document.pdf"
    
    # Download the PDF
    download_pdf(pdf_url, pdf_path)

    process_pdf(pdf_path)
    
    question = "What was the hardware the model was trained on and how long it was trained?"
    answer = answer_question(question)
    print(f"Question: {question}")
    print(f"Answer: {answer}")
