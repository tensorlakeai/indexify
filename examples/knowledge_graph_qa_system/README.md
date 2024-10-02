# Knowledge Graph Question Answering System with Indexify

This project demonstrates how to build a Question Answering (QA) system over a Knowledge Graph using Indexify and Neo4j. The system takes a natural language question, converts it to a Cypher query, executes the query on a Neo4j database, and generates a natural language answer based on the query results.

## Features

- Natural language question to Cypher query conversion
- Execution of Cypher queries on a Neo4j database
- Fallback query mechanism for handling errors
- Natural language answer generation based on query results
- Utilizes Indexify for workflow orchestration
- Integrates with Google's Gemini AI for language tasks

## Prerequisites

- Python 3.9+
- Neo4j database instance
- Google Cloud account with Gemini API access
- Indexify library

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/tensorlakeai/indexify
   cd indexify/examples/knowledge_graph_qa_system
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Set up environment variables:
   ```
   export GOOGLE_API_KEY=your_google_api_key
   export NEO4J_URI=your_neo4j_uri
   export NEO4J_USER=your_neo4j_username
   export NEO4J_PASSWORD=your_neo4j_password
   ```

## Usage

1. Ensure your Neo4j database is populated with scientist data using the following schema:
   ```
   (Scientist {name, field, known_for})
   ```

2. Run the main script:
   ```
   python knowledge_graph_qa.py
   ```

3. The script will execute a sample question. To use your own questions, modify the `sample_question` in the main execution block.

## How it Works

1. **Question to Cypher Conversion**: The user's question is converted to a Cypher query using the Gemini AI model.
2. **Query Execution**: The Cypher query is executed on the Neo4j database.
3. **Fallback Mechanism**: If the query fails, a fallback query is executed to retrieve relevant information.
4. **Answer Generation**: The query results are formatted and passed to the Gemini AI model to generate a natural language answer.
5. **Result Delivery**: The answer is returned to the user.

## Customization

- Modify the Neo4j schema in the `question_to_cypher` function to match your database structure.
- Adjust the fallback query in the `execute_fallback_query` function to suit your needs.
- Fine-tune the prompts in `question_to_cypher` and `generate_answer` functions for better results.