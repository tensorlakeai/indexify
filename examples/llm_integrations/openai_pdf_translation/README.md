# PDF Translation with Indexify and OpenAI

This demonstrates two robust approaches to building a PDF translation pipeline from English to French using Indexify. We'll explore using both OpenAI's GPT-4o and GPT-3.5-turbo models for translation. You will learn how to efficiently translate PDF documents for various applications such as multilingual document processing, content localization, and cross-language information retrieval.

## Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Setup](#setup)
   - [Install Indexify](#install-indexify)
   - [Install Required Extractors](#install-required-extractors)
4. [Approach 1: Direct PDF Translation with GPT-4o](#approach-1-direct-pdf-translation-with-gpt-4o)
   - [Creating the Extraction Graph](#creating-the-extraction-graph-gpt-4o)
   - [Implementing the PDF Translation Pipeline](#implementing-the-pdf-translation-pipeline-gpt-4o)
5. [Approach 2: Two-Step PDF Translation with GPT-3.5-turbo](#approach-2-two-step-pdf-translation-with-gpt-35-turbo)
   - [Creating the Extraction Graph](#creating-the-extraction-graph-gpt-35-turbo)
   - [Implementing the PDF Translation Pipeline](#implementing-the-pdf-translation-pipeline-gpt-35-turbo)
6. [Running the PDF Translation](#running-the-pdf-translation)
7. [Customization and Advanced Usage](#customization-and-advanced-usage)
8. [Comparison of Approaches](#comparison-of-approaches)
9. [Conclusion](#conclusion)
10. [Next Steps](#next-steps)

## Introduction

PDF translation involves converting the textual content of a PDF document from one language to another while maintaining the original formatting and structure as much as possible. This README presents two approaches to achieve this using Indexify:

1. Direct PDF translation using GPT-4o
2. Two-step translation using a PDF extractor and GPT-3.5-turbo

Both methods have their advantages, and the choice between them depends on your specific requirements.

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
This starts a long-running server that exposes ingestion and retrieval APIs to applications.

### Install Required Extractors

Next, we'll install the necessary extractors:

```bash
pip install indexify-extractor-sdk
indexify-extractor download tensorlake/openai
indexify-extractor download tensorlake/pdfextractor
```

Once the extractors are downloaded, you can start them:
```bash
indexify-extractor join-server
```

## Approach 1: Direct PDF Translation with GPT-4o

This approach leverages GPT-4o's ability to directly process PDFs, eliminating the need for a separate PDF text extraction step.

### Creating the Extraction Graph (GPT-4o)

Create a new Python file called `setup_graph_gpt4o.py` and add the following code:

```python
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'pdf_translator_gpt4o'
extraction_policies:
  - extractor: 'tensorlake/openai'
    name: 'pdf_to_french'
    input_params:
      model: 'gpt-4o'
      api_key: 'YOUR_OPENAI_API_KEY'
      system_prompt: 'Translate the content of the following PDF from English to French. Maintain the original formatting and structure as much as possible. Provide the translation in plain text format.'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
```

Replace `'YOUR_OPENAI_API_KEY'` with your actual OpenAI API key.

### Implementing the PDF Translation Pipeline (GPT-4o)

Create a file `upload_and_retrieve_gpt4o.py` with the following content:

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
    content_id = client.upload_file("pdf_translator_gpt4o", pdf_path)
    
    # Wait for the translation to complete
    client.wait_for_extraction(content_id)
    
    # Retrieve the translated content
    translated_content = client.get_extracted_content(
        ingested_content_id=content_id,
        graph_name="pdf_translator_gpt4o",
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
<img src="https://raw.githubusercontent.com/tensorlakeai/indexify/main/examples/llm_integrations/openai_pdf_translation/carbon.png" width="600"/>

## Approach 2: Two-Step PDF Translation with GPT-3.5-turbo

This approach first extracts text from the PDF, then sends that text to GPT-3.5-turbo for translation.

### Creating the Extraction Graph (GPT-3.5-turbo)

Create a new Python file called `setup_graph_gpt35.py` and add the following code:

```python
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'pdf_translator_gpt35'
extraction_policies:
  - extractor: 'tensorlake/pdfextractor'
    name: 'pdf_to_text'
  - extractor: 'tensorlake/openai'
    name: 'text_to_french'
    input_params:
      model: 'gpt-3.5-turbo'
      api_key: 'YOUR_OPENAI_API_KEY'
      system_prompt: 'You are a professional translator. Translate the following English text to French. Maintain the original formatting and structure as much as possible.'
    content_source: 'pdf_to_text'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
```

Replace `'YOUR_OPENAI_API_KEY'` with your actual OpenAI API key.

### Implementing the PDF Translation Pipeline (GPT-3.5-turbo)

Create a file `upload_and_retrieve_gpt35.py` with the following content:

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
    content_id = client.upload_file("pdf_translator_gpt35", pdf_path)
    
    # Wait for the extraction to complete
    client.wait_for_extraction(content_id)
    
    # Retrieve the translated content
    translated_content = client.get_extracted_content(
        ingested_content_id=content_id,
        graph_name="pdf_translator_gpt35",
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

## Running the PDF Translation

You can run either Python script to translate a PDF:

```bash
python setup_graph_gpt4o.py
# or
python setup_graph_gpt35.py
```

```bash
python upload_and_retrieve_gpt4o.py
# or
python upload_and_retrieve_gpt35.py
```

## Customization and Advanced Usage

You can customize the translation process for both approaches by modifying the `system_prompt` in the respective extraction graphs. For example:

- To translate to a different language:
  ```yaml
  system_prompt: 'Translate the content of the following PDF from English to Spanish. Maintain the original formatting and structure as much as possible. Provide the translation in plain text format.'
  ```

- To focus on specific sections or types of content:
  ```yaml
  system_prompt: 'Translate only the main body text of the following PDF from English to French, ignoring headers, footers, and references. Maintain the original paragraph structure. Provide the translation in plain text format.'
  ```

- To handle multiple languages:
  ```yaml
  system_prompt: 'You are a multilingual translator. Translate the following text from English to [TARGET_LANGUAGE]. Maintain the original formatting and structure as much as possible.'
  ```

## Comparison of Approaches

1. **GPT-4o Approach**:
   - Pros: Simplified pipeline, potentially better handling of PDF structure
   - Cons: May be more expensive, potentially slower for large documents

2. **GPT-3.5-turbo Approach**:
   - Pros: More cost-effective, potentially faster for large documents
   - Cons: Two-step process, may lose some PDF structural information

Choose the approach that best fits your specific requirements, considering factors such as cost, speed, and accuracy.

## Conclusion

This project demonstrates the power of using Indexify with different GPT models for PDF translation. Some key advantages include:

1. **Flexibility**: Choose between direct PDF processing or a two-step approach based on your needs.
2. **High-Quality Translation**: Leveraging advanced language models ensures high-quality translations that maintain context and nuance.
3. **Scalability**: Indexify allows for processing of multiple PDFs efficiently, making it suitable for large-scale translation tasks.

## Next Steps

- Explore more about Indexify in the official documentation: https://docs.getindexify.ai
- Experiment with different language pairs or customized translation instructions by modifying the system prompt.
- Compare the results of both approaches to determine which works best for your specific use case.
- Integrate this translation pipeline into larger workflows or applications for automated document translation.