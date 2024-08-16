from indexify import IndexifyClient
from indexify.data_loaders import UrlLoader
from openai import OpenAI

client = IndexifyClient()

client_openai = OpenAI()

def get_page_number(content_id: str) -> int:
    content_metadata = client.get_content_metadata(content_id)
    page_number = content_metadata["extracted_metadata"]["metadata"]['page_num']
    return page_number

def get_context(question: str, index: str, top_k=5):
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
    return f"""Answer the question based only on the following context, which can include text and tables.
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

if __name__ == "__main__":
    # Uncomment the lines if you want to upload more than 1 pdf
    pdf_urls = [
        "http://arxiv.org/pdf/2304.08485"
        "https://arxiv.org/pdf/2304.08485.pdf",
    #    "https://arxiv.org/pdf/0910.2029.pdf",
    #    "https://arxiv.org/pdf/2402.01968.pdf",
    #    "https://arxiv.org/pdf/2401.13138.pdf",
    #    "https://arxiv.org/pdf/2402.03578.pdf",
    #    "https://arxiv.org/pdf/2309.07864.pdf",
    #    "https://arxiv.org/pdf/2401.03568.pdf",
    #    "https://arxiv.org/pdf/2312.10256.pdf",
    #    "https://arxiv.org/pdf/2312.01058.pdf",
    #    "https://arxiv.org/pdf/2402.01680.pdf",
    #    "https://arxiv.org/pdf/2403.07017.pdf"
    ]

    data_loader = UrlLoader(pdf_urls)
    content_ids = client.ingest_from_loader(data_loader, "rag_pipeline")

    print(f"Uploaded {len(content_ids)} documents")
    client.wait_for_extraction(content_ids)
    
    question = "What is the performance of LLaVa across across multiple image domains / subjects?"
    answer = answer_question(question)
    print(f"Question: {question}")
    print(f"Answer: {answer}")
