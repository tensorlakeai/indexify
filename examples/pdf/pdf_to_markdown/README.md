# PDF Text Extraction with Indexify and Marker

We show how to create a pipeline capable of extracting text content from PDF documents It uses the `tensorlake/marker` extractor to convert PDF documents into markdown.

## Prerequisites

Before starting, ensure you have:

- A virtual environment with Python 3.9 or later
  ```shell
  python3.9 -m venv ve
  source ve/bin/activate
  ```
- `pip` (Python package manager)
- Basic familiarity with Python and command-line interfaces

## Setup

### Install Indexify

First, install Indexify using the official installation script & start the server:

```bash
curl https://getindexify.ai | sh
./indexify server -d
```

This starts a long-running server that exposes ingestion and retrieval APIs to applications.

### Install Required Extractor

Next, install the necessary extractor in a new terminal and start it:

```bash
pip install indexify-extractor-sdk
indexify-extractor download tensorlake/marker
indexify-extractor join-server
```

## Creating the Extraction Graph

The extraction graph defines the flow of data through our text extraction pipeline. We'll create a graph that extracts text from PDFs using the tensorlake/marker extractor.

Create a new Python file called `setup_graph.py` and add the following code:

```python
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'pdf_text_extractor'
extraction_policies:
  - extractor: 'tensorlake/marker'
    name: 'pdf_to_text'
    input_params:
      batch_multiplier: 2
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
```

Run this script to set up the pipeline:
```bash
python setup_graph.py
```

## Implementing the Text Extraction Pipeline

Now that we have our extraction graph set up, we can upload files and make the pipeline extract text. Create a file `upload_and_retrieve.py`:

```python
import requests
from indexify import IndexifyClient

def download_pdf(url, save_path):
    response = requests.get(url)
    with open(save_path, 'wb') as f:
        f.write(response.content)
    print(f"PDF downloaded and saved to {save_path}")

def extract_text(pdf_path):
    client = IndexifyClient()
    
    # Upload the PDF file
    content_id = client.upload_file("pdf_text_extractor", pdf_path)
    
    # Wait for the extraction to complete
    client.wait_for_extraction(content_id)
    
    # Retrieve the extracted text content
    extracted_text = client.get_extracted_content(
        ingested_content_id=content_id,
        graph_name="pdf_text_extractor",
        policy_name="pdf_to_text"
    )
    
    return extracted_text[0]['content'].decode('utf-8')

# Example usage
if __name__ == "__main__":
    pdf_url = "https://arxiv.org/pdf/2310.06825.pdf"
    pdf_path = "reference_document.pdf"
    
    # Download the PDF
    download_pdf(pdf_url, pdf_path)
    
    # Extract text from the PDF
    extracted_text = extract_text(pdf_path)
    print(f"Extracted text (first 500 characters):")
    print(extracted_text[:500] + "...")
```

## Running the Text Extraction Process

You can run the Python script to process a PDF and extract its text:
```bash
python upload_and_retrieve.py
```
   Sample Page to extract markdown from:

   <img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/pdf_to_markdown/screenshot.png" width="600"/>

   Sample Markdown extracted from page:
   
   <img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/pdf_to_markdown/carbon.png" width="600"/>

## Customization and Advanced Usage

You can customize the text extraction process by modifying the `input_params` in the extraction graph. For example:

- To extract text from only the first 5 pages:
  ```yaml
  input_params:
    max_pages: 5
  ```

- To start extraction from page 3:
  ```yaml
  input_params:
    start_page: 3
  ```

- To adjust the batch processing:
  ```yaml
  input_params:
    batch_multiplier: 4
  ```

Experiment with these parameters to optimize the extraction process for your specific use case.

## Conclusion

This example demonstrates the efficiency of using Indexify for PDF text extraction:

1. **Scalability**: Indexify server can be deployed on a cloud and process numerous PDFs uploaded into it. If any step in the pipeline fails, it automatically retries on another machine.
2. **Flexibility**: You can easily adjust parameters to suit your specific needs.
3. **Integration**: The extracted text can be easily integrated into downstream tasks such as text analysis, indexing, or further processing.

## Next Steps

- Learn more about Indexify on our docs - https://docs.getindexify.ai
- Explore how to use the extracted text for tasks like semantic search or document question-answering.