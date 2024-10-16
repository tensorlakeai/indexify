# Knowledge Graph RAG and Question Answering with Indexify

This project demonstrates how to build a Knowledge Graph Retrieval-Augmented Generation (RAG) pipeline and a Question Answering system using Indexify.

## Features

- Entity and relationship extraction using spaCy NER
- Knowledge graph construction and storage in Neo4j
- Text embedding generation using Sentence Transformers
- Natural language question to Cypher query conversion using Google's Gemini AI
- Question answering based on the knowledge graph

## Prerequisites

- Python 3.9+
- Google Cloud account with Gemini API access
- Docker and Docker Compose (for containerized setup)

## Installation and Usage

### Option 1: Local Installation - In Process

1. Clone this repository:
   ```
   git clone https://github.com/tensorlakeai/indexify
   cd indexify/examples/knowledge_graph
   ```

2. Create a virtual environment and activate it:
   ```
   python -m venv venv
   source venv/bin/activate
   ```

3. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Install and start a Neo4j database locally.

5. Set up environment variables:
   ```
   export NEO4J_URI=bolt://localhost:7687
   export NEO4J_USER=neo4j
   export NEO4J_PASSWORD=your_password
   export GOOGLE_API_KEY=your_google_api_key
   ```

6. Run the main script:
   ```
   python workflow.py --mode in-process-run
   ```

### Option 2: Using Docker Compose - deployed graph

1. Clone this repository:
   ```
   git clone https://github.com/tensorlakeai/indexify
   cd indexify/examples/knowledge_graph
   ```

2. Create a virtual environment and activate it:
   ```
   python -m venv venv
   source venv/bin/activate
   ```

3. Install indexify:
   ```
   pip install indexify
   ```

4. Ensure Docker and Docker Compose are installed on your system.

5. Create a `.env` file in the project directory and add your Google API key:
   ```
   GOOGLE_API_KEY=your_google_api_key_here
   ```

4. Build the images for the functions:
   ```
   indexify-cli build-image workflow.py NLPFunction
   indexify-cli build-image workflow.py generate_embeddings
   indexify-cli build-image workflow.py build_knowledge_graph
   indexify-cli build-image workflow.py store_in_neo4j
   indexify-cli build-image workflow.py generate_answer
   docker-compose up --build
   ```

5. Run the main script:
   ```
   python workflow.py --mode remote-deploy
   python workflow.py --mode remote-run
   ```

## How it Works

1. **Knowledge Graph Creation:**
   - Entity Extraction: Uses spaCy to identify named entities in the input text.
   - Relationship Extraction: Creates simple relationships between entities extracted from the text.
   - Knowledge Graph Construction: Builds a graph structure from the extracted entities and relationships.
   - Neo4j Storage: Stores the knowledge graph in a Neo4j database for later querying and analysis.
   - Embedding Generation: Creates embeddings of the input text using Sentence Transformers.

2. **Question Answering:**
   - Question to Cypher: Converts a natural language question to a Cypher query using Google's Gemini AI.
   - Query Execution: Executes the Cypher query on the Neo4j database.
   - Answer Generation: Uses Gemini AI to generate a natural language answer based on the query results.

## Indexify Graph Structure

The project uses two Indexify graphs:

1. Knowledge Graph RAG Pipeline:
   ```
   extract_entities_and_text -> extract_relationships -> build_knowledge_graph -> store_in_neo4j
                                                                               -> generate_embeddings
   ```

2. Question Answering Pipeline:
   ```
   question_to_cypher -> execute_cypher_query -> generate_answer
   ```

## Customization

- Modify the `sample_doc` in the `main()` function of `kg_rag_qa_pipeline.py` to process different texts.
- Adjust the relationship extraction logic in `extract_relationships()` for more sophisticated relationship identification.
- Change the embedding model in `generate_embeddings()` to use different pre-trained models.
- Fine-tune the prompts in `question_to_cypher()` and `generate_answer()` functions for better results.
