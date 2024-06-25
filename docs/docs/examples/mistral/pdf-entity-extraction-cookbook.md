# PDF Entity Extraction with Indexify and Mistral

This cookbook demonstrates how to build a robust entity extraction pipeline for PDF documents using Indexify and Mistral's large language models. By combining these powerful tools, we can efficiently extract named entities from PDF files, providing valuable insights for various applications such as information retrieval, content analysis, and data mining.

## Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Setup](#setup)
   - [Install Indexify](#install-indexify)
   - [Install Required Extractors](#install-required-extractors)
4. [Creating the Extraction Graph](#creating-the-extraction-graph)
5. [Implementing the Entity Extraction Pipeline](#implementing-the-entity-extraction-pipeline)
6. [Running the Entity Extraction](#running-the-entity-extraction)
7. [Customization and Advanced Usage](#customization-and-advanced-usage)
8. [Conclusion](#conclusion)

## Introduction

Entity extraction, also known as named entity recognition (NER), is a crucial task in natural language processing. It involves identifying and classifying named entities in text into predefined categories such as persons, organizations, locations, dates, and more. By applying this technique to PDF documents, we can automatically extract structured information from unstructured text, making it easier to analyze and utilize the content of these documents.

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

The extraction graph defines the flow of data through our entity extraction pipeline. We'll create a graph that first extracts text from PDFs, then sends that text to Mistral for entity extraction.

Create a new Python file called `pdf_entity_extraction.py` and add the following code:

```python
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'pdf_entity_extractor'
extraction_policies:
  - extractor: 'tensorlake/pdfextractor'
    name: 'pdf_to_text'
  - extractor: 'tensorlake/mistral'
    name: 'text_to_entities'
    input_params:
      model_name: 'mistral-large-latest'
      key: 'YOUR_MISTRAL_API_KEY'
      system_prompt: 'Extract and categorize all named entities from the following text. Provide the results in a JSON format with categories: persons, organizations, locations, dates, and miscellaneous.'
    content_source: 'pdf_to_text'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
```

Replace `'YOUR_MISTRAL_API_KEY'` with your actual Mistral API key.

## Implementing the Entity Extraction Pipeline

Now that we have our extraction graph set up, let's implement the entity extraction pipeline:

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

def extract_entities_from_pdf(pdf_path):
    client = IndexifyClient()
    
    # Upload the PDF file
    content_id = client.upload_file("pdf_entity_extractor", pdf_path)
    
    # Wait for the extraction to complete
    client.wait_for_extraction(content_id)
    
    # Retrieve the extracted entities
    entities_content = client.get_extracted_content(
        content_id=content_id,
        graph_name="pdf_entity_extractor",
        policy_name="text_to_entities"
    )
    
    # Parse the JSON response
    entities = json.loads(entities_content.data.decode('utf-8'))
    return entities

# Example usage
if __name__ == "__main__":
    pdf_url = "https://arxiv.org/pdf/2310.06825.pdf"
    pdf_path = "reference_document.pdf"
    
    # Download the PDF
    download_pdf(pdf_url, pdf_path)
    
    # Extract entities
    extracted_entities = extract_entities_from_pdf(pdf_path)
    
    print("Extracted Entities:")
    for category, entities in extracted_entities.items():
        print(f"\n{category.capitalize()}:")
        for entity in entities:
            print(f"- {entity}")
```

## Running the Entity Extraction

To run the entity extraction pipeline:

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
   python pdf_entity_extraction.py
   ```

## Customization and Advanced Usage

You can customize the entity extraction process by modifying the `system_prompt` in the extraction graph. For example:

- To focus on specific entity types:
  ```yaml
  system_prompt: 'Extract only person names and organizations from the following text. Provide the results in a JSON format with categories: persons and organizations.'
  ```

- To include entity relationships:
  ```yaml
  system_prompt: 'Extract named entities and their relationships from the following text. Provide the results in a JSON format with categories: entities (including type and name) and relationships (including type and involved entities).'
  ```

You can also experiment with different Mistral models by changing the `model_name` parameter to find the best balance between speed and accuracy for your specific use case.

## Conclusion

In this cookbook, we've created a powerful PDF entity extraction pipeline using Indexify and Mistral. This solution offers several advantages:

1. **Flexibility**: Easily customize the entity extraction approach by modifying the system prompt.
2. **Scalability**: Handle multiple PDFs efficiently using Indexify's extraction graph.
3. **Quality**: Leverage Mistral's advanced language models for accurate entity recognition.
4. **Structured Output**: Obtain entities in a structured JSON format, ready for further processing or analysis.

By combining these technologies, you can create a robust entity extraction system that can be integrated into various applications, such as:

- Content categorization and tagging
- Information retrieval systems
- Trend analysis in large document collections
- Automated metadata generation for document management systems

This approach provides a solid foundation for extracting valuable insights from your PDF documents, enabling more efficient data processing and analysis workflows.
