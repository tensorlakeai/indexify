# Knowledge Graph RAG Pipeline with Indexify

This project demonstrates how to build a Knowledge Graph Retrieval-Augmented Generation (RAG) pipeline using Indexify. The pipeline extracts entities and relationships from text, builds a knowledge graph, stores it in Neo4j, and generates embeddings for the text.

## Features

- Entity extraction using spaCy NER
- Relationship extraction based on sentence co-occurrence
- Knowledge graph construction
- Storage of the knowledge graph in Neo4j
- Text embedding generation using Sentence Transformers

## Prerequisites

- Python 3.9+
- Neo4j database
- Indexify library

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/tensorlakeai/indexify
   cd indexify/examples/knowledge_graph_generation
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   python -m spacy download en_core_web_sm
   ```

3. Set up environment variables for Neo4j connection:
   ```
   export NEO4J_URI=bolt://localhost:7687
   export NEO4J_USER=neo4j
   export NEO4J_PASSWORD=your_password
   ```

## Usage

1. Ensure your Neo4j database is running and accessible.

2. Run the main script:
   ```
   python kg_rag_pipeline.py
   ```

3. The script will process a sample document about Albert Einstein, create a knowledge graph, store it in Neo4j, and generate embeddings.

## How it Works

1. **Entity Extraction**: Uses spaCy to identify named entities in the input text.
2. **Relationship Extraction**: Creates simple "RELATED_TO" relationships between entities in the same sentence.
3. **Knowledge Graph Construction**: Builds a graph structure from the extracted entities and relationships.
4. **Neo4j Storage**: Stores the knowledge graph in a Neo4j database for later querying and analysis.
5. **Embedding Generation**: Creates embeddings of the input text using Sentence Transformers.

## Customization

- Modify the `sample_doc` in the `main()` function to process different texts.
- Adjust the relationship extraction logic in `extract_relationships()` for more sophisticated relationship identification.
- Change the embedding model in `generate_embeddings()` to use different pre-trained models.