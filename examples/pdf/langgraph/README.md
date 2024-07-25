# Adaptive RAG using LangGraph and Indexify

Adaptive RAG is a strategy for RAG that unites (1) [query analysis](https://blog.langchain.dev/query-construction/) with (2) [active / self-corrective RAG](https://blog.langchain.dev/agentic-rag-with-langgraph/).

In the [paper](https://arxiv.org/abs/2403.14403), they report query analysis to route across:

* No Retrieval
* Single-shot RAG
* Iterative RAG

In this cookbook, we'll explore how to create a PDF chunking pipeline using Indexify, the tensorlake/marker for PDF text extraction, and the tensorlake/chunk-extractor with RecursiveCharacterTextSplitter and integrate it with LangGraph. By the end of this document, you should have a pipeline capable of routing between:

* Web search: for questions related to recent events
* Self-corrective RAG: for questions related to our index

## Prerequisites

Before we begin, ensure you have the following:

- Create a virtual env with Python 3.9 or later
  ```shell
  python3.9 -m venv ve
  source ve/bin/activate
  ```
- `pip` (Python package manager)
- Basic familiarity with Python and command-line interfaces

## Setup

### Install Indexify

First, let's install Indexify using the official installation script & start the server:

```bash
curl https://getindexify.ai | sh
./indexify server -d
```
This starts a long-running server that exposes ingestion and retrieval APIs to applications.

### Install Required Extractors

Next, we'll install the necessary extractors in a new terminal:

```bash
pip install -U indexify-extractor-sdk langchain_community tiktoken langchain-openai langchain-cohere langchainhub chromadb langchain langgraph  tavily-python
indexify-extractor download tensorlake/marker
indexify-extractor download tensorlake/chunk-extractor
```

Once the extractors are downloaded, you can start them:
```bash
indexify-extractor join-server
```

## Creating the Extraction Graph

The extraction [graph](graph.py) defines the flow of data through our chunking pipeline. We'll create a graph that first extracts text from PDFs, then chunks that text using the RecursiveCharacterTextSplitter.

```python
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'pdfqa'
extraction_policies:
  - extractor: 'tensorlake/marker'
    name: 'pdf_to_text'
  - extractor: 'tensorlake/chunk-extractor'
    name: 'text_to_chunk'
    input_params:
      text_splitter: 'recursive'
      chunk_size: 500
      overlap: 0
    content_source: 'pdf_to_text'
  - extractor: 'tensorlake/minilm-l6'
    name: 'chunk_to_embedding'
    content_source: 'text_to_chunk'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
```

Run this script to set up the pipeline:
```bash
python graph.py
```

## Ingesting and Retrieving

The code for [ingestion](ingestion.py) let's us take a question as input, runs it through the graph workflow, and returns the final answer. Various data models and prompts are defined for different components of the system (routing, grading, etc.).

```python
import os
from typing import List, Literal
from typing_extensions import TypedDict
from pprint import pprint

import requests
from indexify import IndexifyClient
from langchain import hub
from langchain.schema import Document
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.pydantic_v1 import BaseModel, Field
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI
from langchain_community.tools.tavily_search import TavilySearchResults
from langgraph.graph import END, StateGraph, START

# Initialize IndexifyClient
client = IndexifyClient()

# Docs to index
urls = [
    "https://pub-5dc4d0c0254749378ccbcfffa4bd2a1e.r2.dev/LLM%20Powered%20Autonomous%20Agents%20_%20Lil'Log.pdf",
    "https://pub-5dc4d0c0254749378ccbcfffa4bd2a1e.r2.dev/Prompt%20Engineering%20_%20Lil'Log.pdf",
    "https://pub-5dc4d0c0254749378ccbcfffa4bd2a1e.r2.dev/Adversarial%20Attacks%20on%20LLMs%20_%20Lil'Log.pdf",
]

def index_documents():
    for url in urls:
        response = requests.get(url)
        with open("files.pdf", 'wb') as file:
            file.write(response.content)
        client.upload_file("pdfqa", "files.pdf")
    os.remove("files.pdf")

def get_context(question: str, index: str, top_k=3):
    results = client.search_index(name=index, query=question, top_k=3)
    return results

# Data models
class RouteQuery(BaseModel):
    datasource: Literal["vectorstore", "web_search"] = Field(
        ...,
        description="Given a user question choose to route it to web search or a vectorstore.",
    )

class GradeDocuments(BaseModel):
    binary_score: str = Field(
        description="Documents are relevant to the question, 'yes' or 'no'"
    )

class GradeHallucinations(BaseModel):
    binary_score: str = Field(
        description="Answer is grounded in the facts, 'yes' or 'no'"
    )

class GradeAnswer(BaseModel):
    binary_score: str = Field(
        description="Answer addresses the question, 'yes' or 'no'"
    )

class GraphState(TypedDict):
    question: str
    generation: str
    documents: List[str]

# Initialize LLM
llm = ChatOpenAI(model="gpt-3.5-turbo-0125", temperature=0)

# Router
structured_llm_router = llm.with_structured_output(RouteQuery)
route_prompt = ChatPromptTemplate.from_messages([
    ("system", "You are an expert at routing a user question to a vectorstore or web search. The vectorstore contains documents related to agents, prompt engineering, and adversarial attacks. Use the vectorstore for questions on these topics. Otherwise, use web-search."),
    ("human", "{question}"),
])
question_router = route_prompt | structured_llm_router

# Retrieval Grader
structured_llm_grader = llm.with_structured_output(GradeDocuments)
grade_prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a grader assessing relevance of a retrieved document to a user question. If the document contains keyword(s) or semantic meaning related to the user question, grade it as relevant. It does not need to be a stringent test. The goal is to filter out erroneous retrievals. Give a binary score 'yes' or 'no' score to indicate whether the document is relevant to the question."),
    ("human", "Retrieved document: \n\n {document} \n\n User question: {question}"),
])
retrieval_grader = grade_prompt | structured_llm_grader

# Generate
rag_prompt = hub.pull("rlm/rag-prompt")
rag_chain = rag_prompt | llm | StrOutputParser()

# Hallucination Grader
structured_llm_hallucination_grader = llm.with_structured_output(GradeHallucinations)
hallucination_prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a grader assessing whether an LLM generation is grounded in / supported by a set of retrieved facts. Give a binary score 'yes' or 'no'. 'Yes' means that the answer is grounded in / supported by the set of facts."),
    ("human", "Set of facts: \n\n {documents} \n\n LLM generation: {generation}"),
])
hallucination_grader = hallucination_prompt | structured_llm_hallucination_grader

# Answer Grader
structured_llm_answer_grader = llm.with_structured_output(GradeAnswer)
answer_prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a grader assessing whether an answer addresses / resolves a question. Give a binary score 'yes' or 'no'. Yes' means that the answer resolves the question."),
    ("human", "User question: \n\n {question} \n\n LLM generation: {generation}"),
])
answer_grader = answer_prompt | structured_llm_answer_grader

# Question Re-writer
re_write_prompt = ChatPromptTemplate.from_messages([
    ("system", "You a question re-writer that converts an input question to a better version that is optimized for vectorstore retrieval. Look at the input and try to reason about the underlying semantic intent / meaning."),
    ("human", "Here is the initial question: \n\n {question} \n Formulate an improved question."),
])
question_rewriter = re_write_prompt | llm | StrOutputParser()

# Web Search
web_search_tool = TavilySearchResults(k=3)

# Graph functions
def retrieve(state):
    print("---RETRIEVE---")
    question = state["question"]
    documents = get_context(question, "pdfqa.chunk_to_embedding.embedding")
    return {"documents": documents, "question": question}

def generate(state):
    print("---GENERATE---")
    question = state["question"]
    documents = state["documents"]
    generation = rag_chain.invoke({"context": documents, "question": question})
    return {"documents": documents, "question": question, "generation": generation}

def grade_documents(state):
    print("---CHECK DOCUMENT RELEVANCE TO QUESTION---")
    question = state["question"]
    documents = state["documents"]
    filtered_docs = []
    for d in documents:
        score = retrieval_grader.invoke({"question": question, "document": d['text']})
        grade = score.binary_score
        if grade == "yes":
            print("---GRADE: DOCUMENT RELEVANT---")
            filtered_docs.append(d)
        else:
            print("---GRADE: DOCUMENT NOT RELEVANT---")
    return {"documents": filtered_docs, "question": question}

def transform_query(state):
    print("---TRANSFORM QUERY---")
    question = state["question"]
    documents = state["documents"]
    better_question = question_rewriter.invoke({"question": question})
    return {"documents": documents, "question": better_question}

def web_search(state):
    print("---WEB SEARCH---")
    question = state["question"]
    docs = web_search_tool.invoke({"query": question})
    web_results = "\n".join([d["content"] for d in docs])
    web_results = [{"text": web_results}]
    return {"documents": web_results, "question": question}

def route_question(state):
    print("---ROUTE QUESTION---")
    question = state["question"]
    source = question_router.invoke({"question": question})
    if source.datasource == "web_search":
        print("---ROUTE QUESTION TO WEB SEARCH---")
        return "web_search"
    elif source.datasource == "vectorstore":
        print("---ROUTE QUESTION TO RAG---")
        return "vectorstore"

def decide_to_generate(state):
    print("---ASSESS GRADED DOCUMENTS---")
    filtered_documents = state["documents"]
    if not filtered_documents:
        print("---DECISION: ALL DOCUMENTS ARE NOT RELEVANT TO QUESTION, TRANSFORM QUERY---")
        return "transform_query"
    else:
        print("---DECISION: GENERATE---")
        return "generate"

def grade_generation_v_documents_and_question(state):
    print("---CHECK HALLUCINATIONS---")
    question = state["question"]
    documents = state["documents"]
    generation = state["generation"]
    score = hallucination_grader.invoke({"documents": documents, "generation": generation})
    grade = score.binary_score
    if grade == "yes":
        print("---DECISION: GENERATION IS GROUNDED IN DOCUMENTS---")
        print("---GRADE GENERATION vs QUESTION---")
        score = answer_grader.invoke({"question": question, "generation": generation})
        grade = score.binary_score
        if grade == "yes":
            print("---DECISION: GENERATION ADDRESSES QUESTION---")
            return "useful"
        else:
            print("---DECISION: GENERATION DOES NOT ADDRESS QUESTION---")
            return "not useful"
    else:
        print("---DECISION: GENERATION IS NOT GROUNDED IN DOCUMENTS, RE-TRY---")
        return "not supported"

# Build graph
workflow = StateGraph(GraphState)
workflow.add_node("web_search", web_search)
workflow.add_node("retrieve", retrieve)
workflow.add_node("grade_documents", grade_documents)
workflow.add_node("generate", generate)
workflow.add_node("transform_query", transform_query)

workflow.add_conditional_edges(
    START,
    route_question,
    {
        "web_search": "web_search",
        "vectorstore": "retrieve",
    },
)
workflow.add_edge("web_search", "generate")
workflow.add_edge("retrieve", "grade_documents")
workflow.add_conditional_edges(
    "grade_documents",
    decide_to_generate,
    {
        "transform_query": "transform_query",
        "generate": "generate",
    },
)
workflow.add_edge("transform_query", "retrieve")
workflow.add_conditional_edges(
    "generate",
    grade_generation_v_documents_and_question,
    {
        "not supported": "generate",
        "useful": END,
        "not useful": "transform_query",
    },
)

# Compile graph
app = workflow.compile()

def answer_question(question: str) -> str:
    inputs = {"question": question}
    for output in app.stream(inputs):
        for key, value in output.items():
            pprint(f"Node '{key}':")
    return value["generation"]

if __name__ == "__main__":
    # Index documents (only need to run this once)
    index_documents()

    # Example usage
    question = input("Enter your question: ")
    answer = answer_question(question)
    print("\nFinal Answer:")
    print(answer)
```

### Running the RAG System

Sample Page to extract markdown from:
<img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/langgraph/Adversarial.jpg" width="200"/><img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/langgraph/LLM.jpg" width="200"/><img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/langgraph/Prompt.jpg" width="200"/>

You can run the Python script to process a PDF and answer questions:
```bash
python ingest.py
```

<img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/langgraph/image1.png" width="600"/>
<img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/langgraph/image2.png" width="600"/>

## Conclusion

This example demonstrates the power and flexibility of using Indexify and LangGraph for advanced Adaptive RAG on PDF documents:

1. **Scalability**: Indexify server can be deployed on a cloud and process numerous PDFs uploaded into it. If any step in the pipeline fails, it automatically retries on another machine.
2. **Flexibility**: You can easily swap out components or adjust parameters to suit your specific needs.
3. **Integration**: The chunked output can be easily integrated into downstream tasks such as text analysis, indexing, or further processing.

## Next Steps

- Learn more about Indexify on our docs - https://docs.getindexify.ai
- Explore how to use these chunks for tasks like semantic search or document question-answering.