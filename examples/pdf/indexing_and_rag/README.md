# Retrieval-Augmented Generation (RAG) on PDFs with Indexify

We show how to create a Retrieval-Augmented Generation (RAG) system using Indexify. We'll cover two approaches: a text-based RAG system and a multi-modal RAG system.

1. A basic RAG leveraging a pipeline that extracts text, chunks, and indexes vector embeddings in LanceDB.
2. A multi-modal RAG leveraging a pipeline that extracts tables, images and text from PDFs. Text and Tables are indexed using text embedding models and images are indexed using CLIP. 

## Prerequisites

Before we begin, ensure you have the following:

- Create a virtual env with Python 3.9 or later
  ```shell
  python3.9 -m venv ve
  source ve/bin/activate
  ```
- `pip` (Python package manager)
- An OpenAI API key

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
indexify-extractor download tensorlake/clip-extractor
```

Start the extractors:
```bash
indexify-extractor join-server
```

## Part 1: Text-based RAG

### Creating the Extraction Graph

Define an extraction graph in a file [`graph.yaml`](graph.yaml) - 
```yaml
name: 'rag_pipeline'
extraction_policies:
  - extractor: 'tensorlake/pdfextractor'
    name: 'text_extractor'
    input_params:
      output_types: ["text", "table"]
      output_format: "text"
  - extractor: 'tensorlake/chunk-extractor'
    name: 'text_chunker'
    input_params:
      text_splitter: 'recursive'
      chunk_size: 2000
      overlap: 200
    content_source: 'text_extractor'
  - extractor: 'tensorlake/minilm-l6'
    name: 'chunk_embedding'
    content_source: 'text_chunker'
```

Create a new Python file called [`setup_graph.py`](setup_graph.py) and add the following code:

```python
import os
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()
script_dir = os.path.dirname(os.path.abspath(__file__))
yaml_file_path = os.path.join(script_dir, "graph.yaml")

extraction_graph = ExtractionGraph.from_yaml_file(yaml_file_path)
client.create_extraction_graph(extraction_graph)
```

Run this script to set up the pipeline:
```bash
python setup_graph.py
```

### Implementing the RAG Pipeline

Create a file [`upload_and_retrieve.py`](upload_and_retrieve.py):

```python
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
        #"https://arxiv.org/pdf/2304.08485.pdf",
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

```

Setup the OPENAI API KEY in your terminal before running the script.

### Running the RAG System

Out of 11 PDF files, reference from the PDF file from which answer should be generated:

<img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/indexing_and_rag/paper.png" width="600"/>

You can run the Python script to process a PDF and answer questions:
```bash
python upload_and_retrieve.py
```
<img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/indexing_and_rag/carbon.png" width="600"/>

The answer above incorporates information from tables in addition to text. Tables get parsed as structured JSON, for example:

<img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/indexing_and_rag/tt_output.png" width="600"/>

Here is another table extraction [example](https://github.com/tensorlakeai/indexify/tree/main/examples/pdf/table_extraction).

## Part 2: Multi-Modal RAG with GPT-4o mini

### Creating the Multi-Modal Extraction Graph

Define the extraction graph in a file [`graph_mm.yaml`](graph_mm.yaml) -

```yaml
name: 'rag_pipeline'
extraction_policies:
  - extractor: 'tensorlake/pdfextractor'
    name: 'pdf_to_text'
    input_params:
      output_format: 'text'
  - extractor: 'tensorlake/pdfextractor'
    name: 'pdf_to_image'
    input_params:
      output_types: ["image"]
  - extractor: 'tensorlake/chunk-extractor'
    name: 'text_to_chunks'
    input_params:
      text_splitter: 'recursive'
      chunk_size: 4000
      overlap: 1000
    content_source: 'pdf_to_text'
  - extractor: 'tensorlake/minilm-l6'
    name: 'chunks_to_embeddings'
    content_source: 'text_to_chunks'
  - extractor: 'tensorlake/clip-extractor'
    name: 'image_to_embeddings'
    content_source: 'pdf_to_image'
```

Create a new Python file called [`setup_graph_mm.py`](setup_graph_mm.py). It's similar to the one we created above, we will simply replace the graph file name to `graph_mm.yaml`

Run this script to set up the multi-modal pipeline:
```bash
python setup_graph_mm.py
```

### Implementing the Multi-Modal RAG Pipeline

Create a file [`upload_and_retrieve_mm.py`](upload_and_retrieve_mm.py), which is mostly the same as the previous example, with slight changes to include the images

```python
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
```

### Running the Multi-Modal RAG System

Reference from PDF file from which answer should be generated:

<img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/indexing_and_rag/image.png" width="600"/>

You can run the Python script to process a PDF, including both text and images, and answer questions:
```bash
python upload_and_retrieve_mm.py
```
<img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/indexing_and_rag/mm_carbon.png" width="600"/>

## Customization and Advanced Usage

You can customize both RAG systems in several ways:

1. Adjust chunking parameters in the extraction graph:
   ```yaml
   input_params:
     text_splitter: 'recursive'
     chunk_size: 500
     overlap: 50
   ```

2. Use different embedding models by changing `tensorlake/minilm-l6` or `tensorlake/clip-extractor` to other compatible models.

3. Modify the `get_context` function to retrieve more or fewer chunks:
   ```python
   def get_context(question: str, index: str, top_k=5):
       # ... (rest of the function remains the same)
   ```

4. Experiment with different OpenAI models or adjust the prompt structure in the `create_prompt` function.

5. For the multi-modal RAG, you can adjust the number of images retrieved or how they are incorporated into the prompt.
