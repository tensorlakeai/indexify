# PDF Summarization with Indexify and Mistral

In this cookbook, we'll explore how to create a powerful PDF summarization pipeline using Indexify and Mistral's large language models. This solution combines the strengths of both platforms to provide efficient, accurate, and customizable summarization of PDF documents.

## Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Setup](#setup)
   - [Install Indexify](#install-indexify)
   - [Install Required Extractors](#install-required-extractors)
4. [Creating the Extraction Graph](#creating-the-extraction-graph)
5. [Implementing the Summarization Pipeline](#implementing-the-summarization-pipeline)
6. [Running the Summarization](#running-the-summarization)
7. [Customization and Advanced Usage](#customization-and-advanced-usage)
8. [Conclusion](#conclusion)

## Introduction

PDF summarization is a crucial task in many industries, from academic research to business intelligence. By leveraging Indexify's real-time structured data extraction capabilities and Mistral's advanced language models, we can create a robust and flexible summarization pipeline that can handle various types of PDF documents.

## Prerequisites

Before we begin, ensure you have the following:

- Python 3.7 or later
- `pip` (Python package manager)
- A Mistral API key
- Basic familiarity with Python and command-line interfaces

## Setup

### Install Indexify

First, let's install Indexify using the official installation script:

```bash
curl https://getindexify.ai | sh
```

### Install Required Extractors

Next, we'll install the necessary extractors:

```bash
pip install indexify-extractor-sdk
indexify-extractor download tensorlake/pdfextractor
indexify-extractor download tensorlake/mistral
```

## Creating the Extraction Graph

The extraction graph defines the flow of data through our summarization pipeline. We'll create a graph that first extracts text from PDFs, then sends that text to Mistral for summarization.

Create a new Python file called `pdf_summarization.py` and add the following code:

```python
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'pdf_summarizer'
extraction_policies:
  - extractor: 'tensorlake/pdfextractor'
    name: 'pdf_to_text'
  - extractor: 'tensorlake/mistral'
    name: 'text_to_summary'
    input_params:
      model_name: 'mistral-large-latest'
      key: 'YOUR_MISTRAL_API_KEY'
      system_prompt: 'Summarize the following text in a concise manner, highlighting the key points:'
    content_source: 'pdf_to_text'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
```

Replace `'YOUR_MISTRAL_API_KEY'` with your actual Mistral API key.

## Implementing the Summarization Pipeline

Now that we have our extraction graph set up, let's implement the summarization pipeline:

```python
import os
from indexify import IndexifyClient

def summarize_pdf(pdf_path):
    client = IndexifyClient()
    
    # Upload the PDF file
    content_id = client.upload_file("pdf_summarizer", pdf_path)
    
    # Wait for the extraction to complete
    client.wait_for_extraction(content_id)
    
    # Retrieve the summarized content
    summary = client.get_extracted_content(
        content_id=content_id,
        graph_name="pdf_summarizer",
        policy_name="text_to_summary"
    )
    
    return summary.data.decode('utf-8')

# Example usage
if __name__ == "__main__":
    pdf_path = "/path/to/your/document.pdf"
    summary = summarize_pdf(pdf_path)
    print(summary)
```

## Running the Summarization

To run the summarization pipeline:

1. Start the Indexify server:
   ```bash
   ./indexify server -d
   ```

2. In a new terminal, start the extractors:
   ```bash
   indexify-extractor join-server
   ```

3. Run your Python script:
   ```bash
   python pdf_summarization.py
   ```

## Customization and Advanced Usage

You can customize the summarization process by modifying the `system_prompt` in the extraction graph. For example:

- To generate bullet-point summaries:
  ```yaml
  system_prompt: 'Summarize the following text as a list of bullet points:'
  ```

- To focus on specific aspects of the document:
  ```yaml
  system_prompt: 'Summarize the main arguments and supporting evidence from the following text:'
  ```

You can also experiment with different Mistral models by changing the `model_name` parameter.

## Conclusion

In this cookbook, we've created a powerful PDF summarization pipeline using Indexify and Mistral. This solution offers several advantages:

1. **Flexibility**: Easily customize the summarization approach by modifying the system prompt.
2. **Scalability**: Handle multiple PDFs efficiently using Indexify's extraction graph.
3. **Quality**: Leverage Mistral's advanced language models for high-quality summaries.

By combining these technologies, you can create a robust document summarization system tailored to your specific needs.
