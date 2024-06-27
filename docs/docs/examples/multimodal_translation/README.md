# PDF Translation with Indexify and GPT-4o

This project demonstrates how to build a robust PDF translation pipeline from English to French using Indexify and OpenAI's GPT-4o model. You will learn how to efficiently translate PDF documents for various applications such as multilingual document processing, content localization, and cross-language information retrieval.

## Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Setup](#setup)
   - [Install Indexify](#install-indexify)
   - [Install Required Extractor](#install-required-extractor)
4. [Creating the Extraction Graph](#creating-the-extraction-graph)
5. [Implementing the PDF Translation Pipeline](#implementing-the-pdf-translation-pipeline)
6. [Running the PDF Translation](#running-the-pdf-translation)
7. [Customization and Advanced Usage](#customization-and-advanced-usage)
8. [Conclusion](#conclusion)

## Introduction

PDF translation involves converting the textual content of a PDF document from one language to another while maintaining the original formatting and structure as much as possible. By leveraging the capabilities of GPT-4o, we can achieve high-quality translations directly from PDF files, streamlining the translation process.

## Prerequisites

Before we begin, ensure you have the following:

- Python 3.9 or later
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
This starts a long-running server that exposes ingestion and retrieval APIs to applications.

### Install Required Extractor

Next, we'll install the necessary extractor:

```bash
pip install indexify-extractor-sdk
indexify-extractor download tensorlake/openai
```

Once the extractor is downloaded, you can start it:
```bash
indexify-extractor join-server
```

## Creating the Extraction Graph

The extraction graph defines the flow of data through our translation pipeline. We'll create a graph that sends the PDF directly to GPT-4o for translation.

Create a new Python file called `pdf_translation_pipeline.py` and add the following code:

```python
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'pdf_translator'
extraction_policies:
  - extractor: 'tensorlake/openai'
    name: 'pdf_to_french'
    input_params:
      model_name: 'gpt-4o'
      key: 'YOUR_OPENAI_API_KEY'
      system_prompt: 'Translate the content of the following PDF from English to French. Maintain the original formatting and structure as much as possible. Provide the translation in plain text format.'
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

Now that we have our extraction graph set up, we can upload files and retrieve the translations. Create a file `upload_and_retreive.py` with the following content:

```python
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
    content_id = client.upload_file("pdf_translator", pdf_path)
    
    # Wait for the translation to complete
    client.wait_for_extraction(content_id)
    
    # Retrieve the translated content
    translated_content = client.get_extracted_content(
        content_id=content_id,
        graph_name="pdf_translator",
        policy_name="pdf_to_french"
    )
    
    # Decode the translated content
    translation = translated_content[0]['content'].decode('utf-8')
    return translation

# Example usage
if __name__ == "__main__":
    pdf_url = "https://arxiv.org/pdf/2310.06825.pdf"
    pdf_path = "reference_document.pdf"

    # Download the PDF
    download_pdf(pdf_url, pdf_path)
    translated_text = translate_pdf(pdf_path)
    
    print("Translated Content (first 500 characters):")
    print(translated_text[:500])
```

## Running the PDF Translation

You can run the Python script to translate a PDF:
```bash
python upload_and_retreive.py
```

## Customization and Advanced Usage

You can customize the translation process by modifying the `system_prompt` in the extraction graph. For example:

- To translate to a different language:
  ```yaml
  system_prompt: 'Translate the content of the following PDF from English to Spanish. Maintain the original formatting and structure as much as possible. Provide the translation in plain text format.'
  ```

- To focus on specific sections or types of content:
  ```yaml
  system_prompt: 'Translate only the main body text of the following PDF from English to French, ignoring headers, footers, and references. Maintain the original paragraph structure. Provide the translation in plain text format.'
  ```

## Conclusion

This project demonstrates the power of using Indexify with GPT-4o for PDF translation. Some key advantages include:

1. **Simplified Pipeline**: GPT-4o can directly process PDFs, eliminating the need for a separate PDF text extraction step.
2. **High-Quality Translation**: Leveraging GPT-4o ensures high-quality translations that maintain context and nuance.
3. **Scalability**: Indexify allows for processing of multiple PDFs efficiently, making it suitable for large-scale translation tasks.

## Next Steps

- Explore more about Indexify in the official documentation: https://docs.getindexify.ai
- Experiment with different language pairs or customized translation instructions by modifying the system prompt.