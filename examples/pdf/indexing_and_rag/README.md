# Retrieval-Augmented Generation (RAG) on PDFs with Indexify

We show how to create a Retrieval-Augmented Generation (RAG) system using Indexify. We'll cover two approaches: a text-based RAG system and a multi-modal RAG system.

1. A text-based RAG system using `tensorlake/pdfextractor`, `tensorlake/chunk-extractor`, and `tensorlake/minilm-l6`.
2. A multi-modal RAG system that includes image processing using `tensorlake/clip-extractor` and GPT-4o mini for answer generation.

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
indexify-extractor download tensorlake/clip-extractor
```

Start the extractors:
```bash
indexify-extractor join-server
```

## Part 1: Text-based RAG

### Creating the Extraction Graph

Define an extraction graph in a file `graph.yaml` - 
```yaml
name: 'rag_pipeline'
extraction_policies:
  - extractor: 'tensorlake/pdfextractor'
    name: 'text_extractor'
    input_params:
      output_format: 'text'
  - extractor: 'tensorlake/text-chunker'
    name: 'text_chunker
    input_params:
      text_splitter: 'recursive'
      chunk_size: 1000
      overlap: 200
    content_source: 'text_extractor'
  - extractor: 'tensorlake/minilm-l6'
    name: 'chunk_embedding'
    content_source: 'text_chunker'
```

Create a new Python file called `setup_graph.py` and add the following code:

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

Create a file `upload_and_retrieve.py`:

```python
import os
from indexify import IndexifyClient
import requests
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
```

Replace `"YOUR_OPENAI_API_KEY"` with your actual OpenAI API key.

### Running the RAG System

Reference from PDF file from which answer should be generated:

<img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/indexing_and_rag/paper.png" width="600"/>

You can run the Python script to process a PDF and answer questions:
```bash
python upload_and_retrieve.py
```
<img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/indexing_and_rag/carbon.png" width="600"/>

## Part 2: Multi-Modal RAG with GPT-4o mini

### Creating the Multi-Modal Extraction Graph

Define the extraction graph in a file `graph_mm.yaml` -
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
      chunk_size: 1000
      overlap: 200
    content_source: 'pdf_to_text'
  - extractor: 'tensorlake/minilm-l6'
    name: 'chunks_to_embeddings'
    content_source: 'text_to_chunks'
  - extractor: 'tensorlake/clip-extractor'
    name: 'image_to_embeddings'
    content_source: 'pdf_to_image'
```

Create a new Python file called `setup_graph_mm.py` and add the following code:

```python
import os
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

script_dir = os.path.dirname(os.path.abspath(__file__))
yaml_file_path = os.path.join(script_dir, "graph_mm.yaml")

extraction_graph = ExtractionGraph.from_yaml_file(yaml_file_path)
client.create_extraction_graph(extraction_graph)
```

Run this script to set up the multi-modal pipeline:
```bash
python setup_graph_mm.py
```

### Implementing the Multi-Modal RAG Pipeline

Create a file `upload_and_retrieve_mm.py`:

```python
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

def get_page_number(content_id: str) -> int:
    pass

def create_prompt(question, context):
    return f"Answer the question, based on the context.\n question: {question} \n context: {context}"

# Function to encode the image
def encode_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def answer_question(question):
    text_context = get_context(question, "rag_pipeline.chunk_embeddings.embedding")
    image_context = client.search_index(name="rag_pipeline.image_embeddings.embedding", query=question, top_k=1)
    image_path = image_context[0]['content_metadata']['storage_url']
    image_path = image_path.replace('file://', '')
    base64_image = encode_image(image_path)
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
                    "url": f"data:image/jpeg;base64,{base64_image}"
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
```

Replace `"YOUR_OPENAI_API_KEY"` with your actual OpenAI API key.

### Running the Multi-Modal RAG System

Reference from PDF file from which answer should be generated:

<img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/indexing_and_rag/image.png" width="600"/>

You can run the Python script to process a PDF, including both text and images, and answer questions:
```bash
python mm_upload_and_retrieve.py
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

## Conclusion

These RAG systems demonstrate the power of combining Indexify with large language models:

1. **Scalability**: Indexify can process and index large numbers of PDFs efficiently, including both text and images.
2. **Flexibility**: You can easily swap out components or adjust parameters to suit your specific needs.
3. **Integration**: The systems seamlessly integrate PDF processing, embedding generation, and text generation.
4. **Multi-Modal Capabilities**: The second system shows how to incorporate both text and image data for more comprehensive question answering.

## Next Steps

- Learn more about Indexify on our docs - https://docs.getindexify.ai
- Explore ways to evaluate and improve the quality of retrieved contexts and generated answers.
- Consider implementing a user interface for easier interaction with your RAG systems.
- Experiment with different multi-modal models and ways of combining text and image data for more sophisticated question answering.
