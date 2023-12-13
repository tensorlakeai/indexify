from langchain.document_loaders import TextLoader
from langchain.text_splitter import CharacterTextSplitter
import requests
import dotenv

# specify your OPENAI_API_KEY in .env file
dotenv.load_dotenv()


# load state of the union text into langchain TextLoader
url = "https://raw.githubusercontent.com/langchain-ai/langchain/master/docs/docs/modules/state_of_the_union.txt"
res = requests.get(url)

with open("state_of_the_union.txt", "w") as f:
    f.write(res.text)

loader = TextLoader("./state_of_the_union.txt")
documents = loader.load()


# chunk text
text_splitter = CharacterTextSplitter(chunk_size=500, chunk_overlap=50)
chunks = text_splitter.split_documents(documents)


# initialize indexify client
from indexify.client import IndexifyClient
from indexify.repository import Document, FilterBuilder

client = IndexifyClient()
repository = client.get_repository("default")


# Bind openai embedding extractor
filter = FilterBuilder().include("source", "./state_of_the_union.txt").build()
repository.bind_extractor(
    "lucas/openai-embedding-ada-002-extractor",
    {"embedding": "state_of_the_union"},
    filter=filter,
)


# Add Documents to repository
import time

docs = [Document(doc.page_content, doc.metadata) for doc in chunks]
repository.add_documents(docs)


# Setup indexify langchain retriever
from retriever import IndexifyRetriever

params = {"repository_name": "default", "index_name": "state_of_the_union", "top_k": 9}
retriever = IndexifyRetriever(client=client, params=params)


# Setup Chat Prompt Template
from langchain.prompts import ChatPromptTemplate

template = """You are an assistant for question-answering tasks. 
Use the following pieces of retrieved context to answer the question. 
If you don't know the answer, just say that you don't know. 
Use three sentences maximum and keep the answer concise.
Question: {question} 
Context: {context} 
Answer:
"""
prompt = ChatPromptTemplate.from_template(template)


# Ask llm question with retriever context
from langchain.chat_models import ChatOpenAI
from langchain.schema.runnable import RunnablePassthrough
from langchain.schema.output_parser import StrOutputParser

llm = ChatOpenAI(model_name="gpt-3.5-turbo", temperature=0)

rag_chain = (
    {"context": retriever, "question": RunnablePassthrough()}
    | prompt
    | llm
    | StrOutputParser()
)


query = "What did the president say about the PRO Act"
print(rag_chain.invoke(query))
