# Table Extraction from PDFs

This project demonstrates how to extract tables from PDF documents using Indexify. It includes two main components: setting up an extraction graph for table extraction and a script to process PDFs and retrieve the extracted tables.

## Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Setup](#setup)
4. [File Descriptions](#file-descriptions)
5. [Usage](#usage)
6. [Customization](#customization)
7. [Conclusion](#conclusion)

## Introduction

This project showcases the use of Indexify to create a pipeline for extracting tables from PDF documents. It consists of two main parts:
- An extraction graph that defines the process of converting PDFs to tables.
- A script that downloads a PDF, uploads it to Indexify, and retrieves the extracted tables.

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

1. `table_pipeline.py`: This script sets up the extraction graph for converting PDFs to tables.

2. `upload_and_retreive.py`: This script downloads a PDF, uploads it to Indexify, and retrieves the extracted tables.

## Usage

1. First, run the [`table_pipeline.py`](table_pipeline.py) script to set up the extraction graph:
   ```bash
   python table_pipeline.py
   ```

2. Then, run the [`upload_and_retreive.py`](upload_and_retreive.py) script to process a PDF and extract tables:
   ```bash
   python upload_and_retreive.py
   ```
   <img src="https://docs.getindexify.ai/example_code/pdf/table_extraction/carbon.png" width="600"/>

   This script will:
   - Download a sample PDF from arXiv
   - Upload the PDF to Indexify
   - Extract tables from the PDF
   - Print the extracted table content

## Customization

You can customize the table extraction process by modifying the `extraction_graph_spec` in `table_pipeline.py`. For example, you could add additional extraction steps or change the output format.

In `upload_and_retreive.py`, you can modify the `pdf_url` variable to process different PDF documents.

## Conclusion

This project demonstrates the power and flexibility of Indexify for extracting tables from PDF documents. While this example focuses on table extraction, Indexify can be used for various other document processing tasks, making it a versatile tool for handling large volumes of documents efficiently.

For more information on Indexify and its capabilities, visit the [official documentation](https://docs.getindexify.ai).