import os
from indexify import IndexifyClient
from openai import OpenAI

client = IndexifyClient()
client_openai = OpenAI(api_key="YOUR_OPENAI_API_KEY")

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
    context = get_context(question, "pdfqa.pdfembedding.embedding")
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
    pdf_path = "your_document.pdf"
    process_pdf(pdf_path)
    
    question = "Who is the greatest player of all time and what is his record?"
    answer = answer_question(question)
    print(f"Question: {question}")
    print(f"Answer: {answer}")
