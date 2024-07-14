# Retrieval-Augmented Generation (RAG) with Indexify

In this cookbook, we'll explore how to create a Retrieval-Augmented Generation (RAG) system using Indexify. We'll use tensorlake/pdfextractor for PDF text extraction, tensorlake/chunk-extractor for text chunking, and tensorlake/minilm-l6 for generating embeddings. Finally, we'll use OpenAI's GPT-3.5-turbo for generating answers based on the retrieved context.

## Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Setup](#setup)
   - [Install Indexify](#install-indexify)
   - [Install Required Extractors](#install-required-extractors)
4. [Creating the Extraction Graph](#creating-the-extraction-graph)
5. [Implementing the RAG Pipeline](#implementing-the-rag-pipeline)
6. [Running the RAG System](#running-the-rag-system)
7. [Customization and Advanced Usage](#customization-and-advanced-usage)
8. [Conclusion](#conclusion)

## Introduction

The RAG pipeline will be composed of several steps:
1. PDF to Text extraction using the `tensorlake/pdfextractor`.
2. Text chunking using the `tensorlake/chunk-extractor`.
3. Embedding generation using `tensorlake/minilm-l6`.
4. Retrieval of relevant chunks based on a question.
5. Answer generation using OpenAI's GPT-3.5-turbo.

## Prerequisites

Before we begin, ensure you have the following:

- Create a virtual env with Python 3.9 or later
  ```shell
  python3.9 -m venv ve
  source ve/bin/activate
  ```
- `pip` (Python package manager)
- An OpenAI API key
- Basic familiarity with Python and command-line interfaces

## Setup

### Install Indexify

First, let's install Indexify using the official installation script & start the server:

```bash
curl https://getindexify.ai | sh
./indexify server -d
```

### Install Required Extractors

Next, install the necessary extractors:

```bash
pip install indexify-extractor-sdk
indexify-extractor download tensorlake/pdfextractor
indexify-extractor download tensorlake/chunk-extractor
indexify-extractor download tensorlake/minilm-l6
```

Start the extractors:
```bash
indexify-extractor join-server
```

## Creating the Extraction Graph

Create a new Python file called `rag_extraction_graph.py` and add the following code:

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

Run this script to set up the pipeline:
```bash
python rag_extraction_graph.py
```

## Ingestion and RAG from the Pipeline

Create a file `upload_and_retreive.py`:

```python
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
```

Replace `"YOUR_OPENAI_API_KEY"` with your actual OpenAI API key.

## Running the RAG System

You can run the Python script to process a PDF and answer questions:
```bash
python upload_and_retreive.py
```
<img src="https://docs.getindexify.ai/example_code/pdf/indexing_and_rag/carbon.png" width="600"/>

## Customization and Advanced Usage

You can customize the RAG system in several ways:

1. Adjust chunking parameters in the extraction graph:
   ```yaml
   input_params:
     text_splitter: 'recursive'
     chunk_size: 500
     overlap: 50
   ```

2. Use a different embedding model by changing `tensorlake/minilm-l6` to another compatible model.

3. Modify the `get_context` function to retrieve more or fewer chunks:
   ```python
   def get_context(question: str, index: str, top_k=5):
       # ... (rest of the function remains the same)
   ```

4. Experiment with different OpenAI models or adjust the prompt structure in the `create_prompt` function.

## Conclusion

This RAG system demonstrates the power of combining Indexify with large language models:

1. **Scalability**: Indexify can process and index large numbers of PDFs efficiently.
2. **Flexibility**: You can easily swap out components or adjust parameters to suit your specific needs.
3. **Integration**: The system seamlessly integrates PDF processing, embedding generation, and text generation.

## Next Steps

- Learn more about Indexify on our docs - https://docs.getindexify.ai
- Explore ways to evaluate and improve the quality of retrieved contexts and generated answers.
- Consider implementing a user interface for easier interaction with your RAG system.
