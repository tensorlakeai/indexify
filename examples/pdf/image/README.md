# PDF Image Extraction with Indexify

This example shows how to extract images frm a pdf using Indexify. If you haven't already please read the [Getting Started](https://docs.getindexify.ai/getting_started/) guide to get acquinted with usage of Indexify,

Pipeline to extract images from PDF. The pipeline uses - 
1. tensorlake/pdfextractor to extract images from PDFs.
We provide a script that downloads a PDF, uploads it to Indexify, and retrieves the extracted images.

| Sample Page |
|:-----------:|
| <img src="https://raw.githubusercontent.com/tensorlakeai/indexify/docsupdate/examples/pdf/image/2310.06825v1_page-0004.jpg" width="600"/> |

Source: [https://arxiv.org/pdf/2310.06825.pdf](https://arxiv.org/pdf/2310.06825.pdf)

## Prerequisites
Before we begin, ensure you have the following:
- Create a virtual env with Python 3.9 or later
  ```shell
  python3.9 -m venv ve
  source ve/bin/activate
  ```
- Indexify installed and running
- Required Python packages: `indexify`, `requests`

## Setup
1. Install Indexify:
   ```bash
   curl https://getindexify.ai | sh
   ./indexify server -d
   ```

2. Install the required extractor:
   ```bash
   pip install indexify-extractor-sdk
   indexify-extractor download tensorlake/pdfextractor
   indexify-extractor join-server
   ```

## File Descriptions
1. `setup_graph.py`: This script sets up the extraction graph for converting PDFs to images.
2. `upload_and_retrieve.py`: This script downloads a PDF, uploads it to Indexify, and retrieves the extracted images.

## Usage
1. First, run the [setup_graph.py](https://github.com/tensorlakeai/indexify/blob/main/examples/pdf/image/setup_graph.py) script to set up the extraction graph:

```python
from indexify import IndexifyClient, ExtractionGraph
client = IndexifyClient()
extraction_graph_spec = """
name: 'image_extractor'
extraction_policies:
  - extractor: 'tensorlake/pdfextractor'
    name: 'pdf_to_image'
    input_params:
      output_types: ["image"]
"""
extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
```

This script creates an ExtractionGraph and defines the extraction policy for PDF to image conversion.

   ```bash
   python setup_graph.py
   ```



2. Then, run the [upload_and_retrieve.py](https://github.com/tensorlakeai/indexify/blob/main/examples/pdf/image/upload_and_retrieve.py) script to process a PDF and extract images:

```python
import os
import requests
from indexify import IndexifyClient

def download_pdf(url, save_path):
    response = requests.get(url)
    with open(save_path, 'wb') as f:
        f.write(response.content)
    print(f"PDF downloaded and saved to {save_path}")

def get_images(pdf_path):
    client = IndexifyClient()

    # Upload the PDF file
    content_id = client.upload_file("image_extractor", pdf_path)

    # Wait for the extraction to complete
    client.wait_for_extraction(content_id)

    # Retrieve the images content
    images = client.get_extracted_content(
        ingested_content_id=content_id,
        graph_name="image_extractor",
        policy_name="pdf_to_image"
    )

    return images

# Example usage
if __name__ == "__main__":
    pdf_url = "https://arxiv.org/pdf/2310.06825.pdf"
    pdf_path = "reference_document.pdf"

    # Download the PDF
    download_pdf(pdf_url, pdf_path)

    # Get images from the PDF
    images = get_images(pdf_path)
    for image in images:
        content_id = image["id"]
        with open(f"{content_id}.png", 'wb') as f:
            print("writing image ", image["id"])
            f.write(image["content"])
```

This script downloads a PDF, uploads it to Indexify, extracts images, and saves them locally.

   ```bash
   python upload_and_retrieve.py
   ```

   Sample Page to extract image from:
   <img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/image/2310.06825v1_page-0004.jpg" width="600"/>
   Sample Image extracted from page:
   <img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/pdf/image/5561f24377d1c264.png" width="600"/>

| Extracted Image |
|:---------------:|
| <img src="https://docs.getindexify.ai/example_code/pdf/image/5561f24377d1c264.png" width="600"/> |

This script will:
- Download a sample PDF from arXiv
- Upload the PDF to Indexify
- Extract images from the PDF
- Print the extracted image content

## Customization
You can customize the image extraction process by modifying the extraction_graph_spec in `setup_graph.py`. For example, you could add additional extraction steps or change the output format.

In `upload_and_retrieve.py`, you can modify the `pdf_url` variable to process different PDF documents.

## Conclusion
This project demonstrates the power and flexibility of Indexify for extracting images from PDF documents. While this example focuses on image extraction, Indexify can be used for various other document processing tasks, making it a versatile tool for handling large volumes of documents efficiently.

For more information on Indexify and its capabilities, visit the [official documentation](https://docs.getindexify.ai).
