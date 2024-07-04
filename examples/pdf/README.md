# PDF Extraction Pipelines

Indexify is capable of ingesting and processing various types of unstructured data. This repository contains cookbooks demonstrating how to build pipelines for:

- [Image Extraction](image)
- [Table Extraction](table)
- [Translation (Uses OpenAI)](openai_pdf_translation)
- [Summarization (Uses Mistral)](mistral/pdf-summarization)
- [Entity Extraction (Uses Mistral)](mistral/pdf-entity-extraction)
- [Chunk Extraction (Uses LangChain)](chunking)
- [Indexing and RAG (Uses OpenAI)](rag)
- [Structured Extraction guided by Schema (Uses OpenAI)](schema)

Each example is organized as:
1. Extraction Graph/Pipeline Description
2. Pipeline Setup Script
3. Upload files and retrieve artifacts

## Available Extractors

Indexify provides several extractors out of the box. You can list all available extractors by running:

```
indexify-extractor list
```

## Indexing

The examples use various indexing methods. You can swap these out with any VectorDB that Indexify supports.

## Customization

These examples use specific models and configurations. Feel free to modify them to suit your needs or contribute new extractors for additional APIs.

For more information, visit the [Indexify documentation](https://docs.getindexify.ai).
