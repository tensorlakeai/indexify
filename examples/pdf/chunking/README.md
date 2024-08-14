# PDF Chunking with Indexify and RecursiveCharacterTextSplitter

Pipeline to extract and chunk text from a PDF. The pipeline uses - 

1. `tensorlake/marker` for PDF text extraction
2. `tensorlake/chunk-extractor` chunks markdown from the previous step with Langchain's RecursiveCharacterTextSplitter.

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
pip install indexify-extractor-sdk
indexify-extractor download tensorlake/marker
indexify-extractor download tensorlake/chunk-extractor
```

Once the extractors are downloaded, you can start them:
```bash
indexify-extractor join-server
```

## Creating the Extraction Graph

The extraction graph defines the flow of data through our chunking pipeline. We'll create a graph that first extracts text from PDFs, then chunks that text using the RecursiveCharacterTextSplitter.

Create a new Python file called `setup_graph.py` and add the following code:

```python
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'pdf_chunker'
extraction_policies:
  - extractor: 'tensorlake/marker'
    name: 'pdf_to_text'
  - extractor: 'tensorlake/chunk-extractor'
    name: 'text_to_chunks'
    input_params:
      text_splitter: 'recursive'
      chunk_size: 1000
      overlap: 200
    content_source: 'pdf_to_text'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
```

You can run this script to set up the pipeline:
```bash
python setup_graph.py
```

## Ingestion and Retrieval from the Pipeline

Now that we have our extraction graph set up, we can upload files and make the pipeline generate chunks. Create a file `upload_and_retrieve.py`:

```python
import os
import requests
from indexify import IndexifyClient

def download_pdf(url, save_path):
    response = requests.get(url)
    with open(save_path, 'wb') as f:
        f.write(response.content)
    print(f"PDF downloaded and saved to {save_path}")

def retreive_chunks(pdf_path):
    client = IndexifyClient()
    
    # Upload the PDF file
    content_id = client.upload_file("pdf_chunker", pdf_path)
    
    # Wait for the extraction to complete
    client.wait_for_extraction(content_id)
    
    # Retrieve the chunked content
    chunks = client.get_extracted_content(
        ingested_content_id=content_id,
        graph_name="pdf_chunker",
        policy_name="text_to_chunks"
    )
    
    return [chunk['content'].decode('utf-8') for chunk in chunks]

# Example usage
if __name__ == "__main__":
    pdf_url = "https://arxiv.org/pdf/2310.06825.pdf"
    pdf_path = "reference_document.pdf"
    
    # Download the PDF
    download_pdf(pdf_url, pdf_path)
    
    # Chunk the PDF
    chunks = retreive_chunks(pdf_path)
    print(f"Number of chunks generated: {len(chunks)}")
    print("\nLast chunk:")
    print(chunks[0][:500] + "...")  # Print first 500 characters of the first chunk
```

## Running the Chunking Process

You can run the Python script to process a PDF and generate chunks:
```bash
python upload_and_retrieve.py
```
   Sample Page to extract chunk from:

   <img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/chunking/screenshot.png" width="600"/>

   Sample Chunk extracted from page:
   
   <img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/chunking/carbon.png" width="600"/>

## Customization and Advanced Usage

You can customize the chunking process by modifying the `input_params` in the extraction graph. For example:

- To change the chunk size and overlap:
  ```yaml
  input_params:
    text_splitter: 'recursive'
    chunk_size: 500
    overlap: 50
  ```

- To use a different text splitter:
  ```yaml
  input_params:
    text_splitter: 'char'
    chunk_size: 1000
    overlap: 200
  ```

You can also experiment with different parameters to find the best balance between chunk size and coherence for your specific use case.

## Conclusion

This example demonstrates the power and flexibility of using Indexify for PDF chunking:

1. **Scalability**: Indexify server can be deployed on a cloud and process numerous PDFs uploaded into it. If any step in the pipeline fails, it automatically retries on another machine.
2. **Flexibility**: You can easily swap out components or adjust parameters to suit your specific needs.
3. **Integration**: The chunked output can be easily integrated into downstream tasks such as text analysis, indexing, or further processing.

## Next Steps

- Learn more about Indexify on our docs - https://docs.getindexify.ai
- Explore how to use these chunks for tasks like semantic search or document question-answering.