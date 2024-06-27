# PDF Translation with Indexify and OpenAI GPT-3.5-turbo

This cookbook demonstrates how to build a robust PDF translation pipeline from English to French using Indexify and OpenAI's GPT-3.5-turbo model. You will learn how to efficiently translate PDF documents for various applications such as localization, multilingual content creation, and cross-language information retrieval.

## Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Setup](#setup)
   - [Install Indexify](#install-indexify)
   - [Install Required Extractors](#install-required-extractors)
4. [Creating the Extraction Graph](#creating-the-extraction-graph)
5. [Implementing the PDF Translation Pipeline](#implementing-the-pdf-translation-pipeline)
6. [Running the PDF Translation](#running-the-pdf-translation)
7. [Customization and Advanced Usage](#customization-and-advanced-usage)
8. [Conclusion](#conclusion)

## Introduction

PDF translation involves extracting text from PDF files and then translating that text from one language to another. By applying this technique, we can automatically generate translated versions of PDF documents, making information more accessible across language barriers.

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

First, let's install Indexify using the official installation script:

```bash
curl https://getindexify.ai | sh
```

Start the Indexify server:
```bash
./indexify server -d
```
This starts a long running server that exposes ingestion and retrieval APIs to applications.

### Install Required Extractors

Next, we'll install the necessary extractors in a new terminal:

```bash
pip install indexify-extractor-sdk
indexify-extractor download tensorlake/pdfextractor
indexify-extractor download tensorlake/openai
```

Once the extractors are downloaded, you can start them:
```bash
indexify-extractor join-server
```

## Creating the Extraction Graph

The extraction graph defines the flow of data through our PDF translation pipeline. We'll create a graph that first extracts text from PDFs, then sends that text to GPT-3.5-turbo for translation to French.

Create a new Python file called `pdf_translation_pipeline.py` and add the following code:

```python
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'pdf_translator_en_to_fr'
extraction_policies:
  - extractor: 'tensorlake/pdfextractor'
    name: 'pdf_to_text'
  - extractor: 'tensorlake/openai'
    name: 'text_to_french'
    input_params:
      model_name: 'gpt-3.5-turbo'
      key: 'YOUR_OPENAI_API_KEY'
      system_prompt: 'You are a professional translator. Translate the following English text to French. Maintain the original formatting and structure as much as possible.'
    content_source: 'pdf_to_text'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
```

Replace `'YOUR_OPENAI_API_KEY'` with your actual OpenAI API key.

You can run this script to set up the pipeline:
```bash
python pdf_translation_pipeline.py
```

## Implementing the PDF Translation Pipeline

Now that we have our extraction graph set up, we can upload files and retrieve the translated content:

Create a file `upload_and_retreive.py`:

```python
import json
import os
import requests
from indexify import IndexifyClient

def download_pdf(url, save_path):
    response = requests.get(url)
    with open(save_path, 'wb') as f:
        f.write(response.content)
    print(f"PDF downloaded and saved to {save_path}")


def translate_pdf(pdf_path):
    client = IndexifyClient()
    
    # Upload the PDF file
    content_id = client.upload_file("pdf_translator_en_to_fr", pdf_path)
    
    # Wait for the extraction to complete
    client.wait_for_extraction(content_id)
    
    # Retrieve the translated content
    translated_content = client.get_extracted_content(
        content_id=content_id,
        graph_name="pdf_translator_translator_en_to_fr",
        policy_name="text_to_french"
    )
    
    # Decode the translated content
    translated_text = translated_content[0]['content'].decode('utf-8')
    return translated_text

# Example usage
if __name__ == "__main__":
    pdf_url = "https://arxiv.org/pdf/2310.06825.pdf"
    pdf_path = "document_to_translate.pdf"

    # Download the PDF
    download_pdf(pdf_url, pdf_path)
    translated_text = translate_pdf(pdf_path)
    
    print("Translated Text (first 500 characters):")
    print(translated_text[:500])
```

You can run the Python script to translate a PDF:
```bash
python upload_and_retreive.py
```

## Customization and Advanced Usage

You can customize the translation process by modifying the `system_prompt` in the extraction graph. For example:

- To focus on a specific domain or style:
  ```yaml
  system_prompt: 'You are a professional translator specializing in scientific literature. Translate the following English text to French, maintaining academic tone and terminology.'
  ```

- To handle multiple languages:
  ```yaml
  system_prompt: 'You are a multilingual translator. Translate the following text from English to [TARGET_LANGUAGE]. Maintain the original formatting and structure as much as possible.'
  ```

You can also experiment with different OpenAI models by changing the `model_name` parameter to find the best balance between speed, accuracy, and cost for your specific use case.

## Conclusion

This PDF translation pipeline demonstrates the power and flexibility of Indexify:

1. **Scalability**: Indexify can handle large volumes of PDFs, processing them efficiently and with automatic retries if any step fails.
2. **Flexibility**: You can easily swap out the PDF extractor or translation model to suit your specific needs.
3. **Integration**: This pipeline can be seamlessly integrated into larger workflows or applications for automated document translation.

## Next Steps

- Explore more Indexify features in the [official documentation](https://docs.getindexify.ai)
- Experiment with different language pairs and domain-specific translations
- Integrate this translation pipeline into your own applications or workflows