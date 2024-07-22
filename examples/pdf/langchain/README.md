# LangChain Integration with Indexify for PDF QA

This README demonstrates how to integrate Indexify with LangChain for a PDF Question-Answering (QA) system. This approach replaces the official LangChain tutorial (https://python.langchain.com/v0.2/docs/tutorials/pdf_qa/) by using Indexify for data extraction, management, and retrieval.

## Setup

### 1. Create a Virtual Environment

```bash
python3.9 -m venv ve
source ve/bin/activate
```

### 2. Install Indexify

Install Indexify using the official installation script and start the server:

```bash
curl https://getindexify.ai | sh
./indexify server -d
```

### 3. Install Required Extractors

Install and start the necessary extractors:

```bash
pip install indexify-extractor-sdk
indexify-extractor download tensorlake/pdfextractor
indexify-extractor download tensorlake/chunk-extractor
indexify-extractor download tensorlake/minilm-l6
indexify-extractor join-server
```

### 4. Install Required Python Packages

```bash
pip install indexify indexify-langchain langchain langchain-openai
```

## Implementation

### 1. Set Up the Extraction Graph

Create a file named `setup_extraction_graph.py`:

```python
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'rag_pipeline'
extraction_policies:
  - extractor: 'tensorlake/pdfextractor'
    name: 'pdf_to_text'
  - extractor: 'tensorlake/chunk-extractor'
    name: 'text_to_chunks'
    input_params:
      text_splitter: 'recursive'
      chunk_size: 1000
      overlap: 200
    content_source: 'pdf_to_text'
  - extractor: 'tensorlake/minilm-l6'
    name: 'chunks_to_embeddings'
    content_source: 'text_to_chunks'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
```

Run this script to set up the extraction pipeline:

```bash
python setup_extraction_graph.py
```

### 2. Implement the PDF QA System

Create a file named `upload_and_retrieve.py`:

```python
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
```

Replace `"YOUR_OPENAI_API_KEY"` with your actual OpenAI API key.

## Running the PDF QA System

Execute the Python script to process a PDF and answer questions:

```bash
python upload_and_retrieve.py
```
<img src="https://docs.getindexify.ai/example_code/pdf/langchain/carbon.png" width="600"/>

This script will download a sample PDF, process it using the Indexify pipeline, and then answer a question about the content.

## Customization and Advanced Usage

1. Adjust chunking parameters in the extraction graph to optimize for your specific use case.
2. Experiment with different embedding models by changing `tensorlake/minilm-l6` to other compatible models.
3. Modify the `top_k` parameter in the `IndexifyRetriever` to retrieve more or fewer chunks.
4. Try different LLM models by changing the `model` parameter in the `ChatOpenAI` initialization.
5. Customize the system prompt to better suit your specific QA needs.

## Next Steps

- Explore more advanced LangChain features and combine them with Indexify's capabilities.
- Experiment with multi-modal capabilities by incorporating image processing from PDFs.

For more information on Indexify, visit our documentation at https://docs.getindexify.ai.