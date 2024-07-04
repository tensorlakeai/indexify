# PDF Image Extraction with Indexify

This project demonstrates how to extract images from PDF documents using Indexify. It includes two main components: setting up an extraction graph for image extraction and a script to process PDFs and retrieve the extracted images.

## Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Setup](#setup)
4. [File Descriptions](#file-descriptions)
5. [Usage](#usage)
6. [Customization](#customization)
7. [Conclusion](#conclusion)

## Introduction

This project showcases the use of Indexify to create a pipeline for extracting images from PDF documents. It consists of two main parts:
- An extraction graph that defines the process of converting PDFs to images.
- A script that downloads a PDF, uploads it to Indexify, and retrieves the extracted images.

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

1. `image_pipeline.py`: This script sets up the extraction graph for converting PDFs to images.

2. `upload_and_retreive.py`: This script downloads a PDF, uploads it to Indexify, and retrieves the extracted images.

## Usage

1. First, run the [`setup.py`](setup.py) script to set up the extraction graph:
   ```bash
   python setup.py
   ```

2. Then, run the [`upload_and_retrieve.py`](upload_and_retrieve.py) script to process a PDF and extract images:
   ```bash
   python upload_and_retrieve.py
   ```

   This script will:
   - Download a sample PDF from arXiv
   - Upload the PDF to Indexify
   - Extract images from the PDF
   - Print the extracted image content

## Customization

You can customize the image extraction process by modifying the `extraction_graph_spec` in `image_pipeline.py`. For example, you could add additional extraction steps or change the output format.

In `upload_and_retrieve.py`, you can modify the `pdf_url` variable to process different PDF documents.

## Conclusion

This project demonstrates the power and flexibility of Indexify for extracting images from PDF documents. While this example focuses on image extraction, Indexify can be used for various other document processing tasks, making it a versatile tool for handling large volumes of documents efficiently.

For more information on Indexify and its capabilities, visit the [official documentation](https://docs.getindexify.ai).
