import requests
import os
os.environ["OPENAI_API_KEY"] = "YOUR_OPENAI_API_KEY"

from langchain_openai import ChatOpenAI
llm = ChatOpenAI(model="gpt-4o-mini")

from indexify import IndexifyClient
client = IndexifyClient()

def download_pdf(url, save_path):
    response = requests.get(url)
    with open(save_path, 'wb') as f:
        f.write(response.content)
    print(f"PDF downloaded and saved to {save_path}")

def process_pdf(pdf_path):
    content_id = client.upload_file("rag_pipeline", pdf_path)
    client.wait_for_extraction(content_id)

pdf_url = "https://proceedings.neurips.cc/paper_files/paper/2017/file/3f5ee243547dee91fbd053c1c4a845aa-Paper.pdf"
pdf_path = "reference_document.pdf"

download_pdf(pdf_url, pdf_path)

process_pdf(pdf_path)

from indexify_langchain import IndexifyRetriever
params = {"name": "rag_pipeline.chunks_to_embeddings.embedding", "top_k": 3}
retriever = IndexifyRetriever(client=client, params=params)

from langchain.chains import create_retrieval_chain
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain_core.prompts import ChatPromptTemplate

system_prompt = (
    "You are an assistant for question-answering tasks. "
    "Use the following pieces of retrieved context to answer "
    "the question. If you don't know the answer, say that you "
    "don't know. Use three sentences maximum and keep the "
    "answer concise."
    "\n\n"
    "{context}"
)

prompt = ChatPromptTemplate.from_messages(
    [
        ("system", system_prompt),
        ("human", "{input}"),
    ]
)

question_answer_chain = create_stuff_documents_chain(llm, prompt)
rag_chain = create_retrieval_chain(retriever, question_answer_chain)

results = rag_chain.invoke({"input": "What was the hardware the model was trained on and how long it was trained?"})

print(results)