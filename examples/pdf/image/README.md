# PDF Image Extraction with Indexify

Pipeline to extract images from PDF. The pipeline uses - 

1. `tensorlake/pdfextractor` to extract images from PDFs.

We provide a script that downloads a PDF, uploads it to Indexify, and retrieves the extracted images.

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

3. Install the required extractor:
   ```bash
   pip install indexify-extractor-sdk
   indexify-extractor download tensorlake/pdfextractor
   indexify-extractor join-server
   ```

## File Descriptions

1. `setup_graph.py`: This script sets up the extraction graph for converting PDFs to images.

2. `upload_and_retreive.py`: This script downloads a PDF, uploads it to Indexify, and retrieves the extracted images.

## Usage

1. First, run the [`setup_graph.py`](https://github.com/tensorlakeai/indexify/blob/main/examples/pdf/image/setup_graph.py) script to set up the extraction graph:
   ```bash
   python setup_graph.py
   ```

2. Then, run the [`upload_and_retrieve.py`](https://github.com/tensorlakeai/indexify/blob/main/examples/pdf/image/upload_and_retrieve.py) script to process a PDF and extract images:
   ```bash
   python upload_and_retrieve.py
   ```
   Sample Page to extract image from:
   <img src="https://docs.getindexify.ai/example_code/pdf/image/2310.06825v1_page-0004.jpg" width="600"/>
   Sample Image extracted from page:
   <img src="https://docs.getindexify.ai/example_code/pdf/image/5561f24377d1c264.png" width="600"/>

   This script will:
   - Download a sample PDF from arXiv
   - Upload the PDF to Indexify
   - Extract images from the PDF
   - Print the extracted image content

## Customization

You can customize the image extraction process by modifying the `extraction_graph_spec` in `setup_graph.py`. For example, you could add additional extraction steps or change the output format.

In `upload_and_retrieve.py`, you can modify the `pdf_url` variable to process different PDF documents.

## Conclusion

This project demonstrates the power and flexibility of Indexify for extracting images from PDF documents. While this example focuses on image extraction, Indexify can be used for various other document processing tasks, making it a versatile tool for handling large volumes of documents efficiently.

For more information on Indexify and its capabilities, visit the [official documentation](https://docs.getindexify.ai).
