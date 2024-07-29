# Invoice Extraction with Indexify and Donut

This guide demonstrates how to create a pipeline for extracting information from invoice PDFs using Indexify and the Donut model. The pipeline uses the `tensorlake/donutcord` extractor to convert PDF invoices into structured text.

**Input**

<img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/invoice/screenshot.jpg" width="600"/>

**Output**

<img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/invoice/carbon.png" width="600"/>

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
indexify-extractor download tensorlake/donutcord
indexify-extractor join-server
```

## Creating the Extraction Graph

The extraction graph defines the flow of data through our invoice extraction pipeline. We'll create a graph that extracts information from invoice PDFs using the tensorlake/donutcord extractor.

Create a new Python file called `invoice_extraction_graph.py` and add the following code:

```python
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'invoice_extractor'
extraction_policies:
  - extractor: 'tensorlake/donutcord'
    name: 'invoice_to_text'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
```

Run this script to set up the pipeline:
```bash
python invoice_extraction_graph.py
```

## Implementing the Invoice Extraction Pipeline

Now that we have our extraction graph set up, we can upload invoice files and make the pipeline extract information. Create a file `extract_invoice.py`:

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
    content_id = client.upload_file("invoice_extractor", pdf_path)
    
    # Wait for the extraction to complete
    client.wait_for_extraction(content_id)
    
    # Retrieve the extracted text content
    extracted_text = client.get_extracted_content(
        ingested_content_id=content_id,
        graph_name="invoice_extractor",
        policy_name="invoice_to_text"
    )
    
    return extracted_text

# Example usage
if __name__ == "__main__":
    pdf_url = "https://extractor-files.diptanu-6d5.workers.dev/invoice-example.pdf"
    pdf_path = "reference_document.pdf"
    
    # Download the PDF
    download_pdf(pdf_url, pdf_path)
    
    # Extract text from the PDF
    extracted_text = extract_text(pdf_path)
    print(extracted_text)
```

## Running the Invoice Extraction Process

You can run the Python script to process an invoice PDF and extract its information:
```bash
python extract_invoice.py
```

## Next Steps

- Explore how to use the extracted invoice information for tasks like automated data entry or financial analysis.
- Learn more about Indexify on our docs - https://docs.getindexify.ai
- Investigate other document processing tasks that could benefit from this pipeline approach.
