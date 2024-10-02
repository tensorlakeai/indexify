import logging
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from indexify import create_client
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import indexify_function
from indexify.functions_sdk.image import Image
from neo4j import GraphDatabase
import google.generativeai as genai
import os
import re

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configure Gemini API
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
if not GOOGLE_API_KEY:
    raise ValueError("GOOGLE_API_KEY environment variable is not set")
genai.configure(api_key=GOOGLE_API_KEY)

# Data Models
class Question(BaseModel):
    text: str = Field(..., description="The user's question")

class CypherQuery(BaseModel):
    query: str = Field(..., description="The Cypher query to be executed")

class QueryResult(BaseModel):
    result: List[Dict[str, Any]] = Field(default_factory=list, description="The result of the Cypher query")

class Answer(BaseModel):
    text: str = Field(..., description="The generated answer to the user's question")

class CypherQueryAndQuestion(BaseModel):
    cypher_query: CypherQuery
    question: Question

class QuestionAndResult(BaseModel):
    question: Question
    query_result: QueryResult

# Indexify function definitions
base_image = "python:3.9-slim"

gemini_image = (
    Image()
    .name("gemini-image")
    .base_image(base_image)
    .run("pip install google-generativeai")
)

neo4j_image = (
    Image()
    .name("neo4j-image")
    .base_image(base_image)
    .run("pip install neo4j")
)

@indexify_function(image=gemini_image)
def question_to_cypher(question: Question) -> CypherQueryAndQuestion:
    """Convert a natural language question to a Cypher query."""
    try:
        model = genai.GenerativeModel("gemini-pro")
        prompt = f"""
        Convert the following question to a Cypher query for a Neo4j database with the following schema:
        (Scientist {{name, field, known_for}})
        
        Question: {question.text}
        
        Provide only the Cypher query without any additional text or code formatting.
        """
        response = model.generate_content(prompt)
        cypher_query = response.text.strip()
        
        # Remove code block formatting if present
        cypher_query = re.sub(r'^```\w*\n|```$', '', cypher_query, flags=re.MULTILINE).strip()
        
        logging.info(f"Generated Cypher query: {cypher_query}")
        return CypherQueryAndQuestion(cypher_query=CypherQuery(query=cypher_query), question=question)
    except Exception as e:
        logging.error(f"Error in question_to_cypher: {str(e)}")
        raise

@indexify_function(image=neo4j_image)
def execute_cypher_query(data: CypherQueryAndQuestion) -> QuestionAndResult:
    """Execute the Cypher query on the Neo4j database."""
    cypher_query, question = data.cypher_query, data.question
    uri = os.getenv('NEO4J_URI', "bolt://localhost:7687")
    user = os.getenv('NEO4J_USER', "neo4j")
    password = os.getenv('NEO4J_PASSWORD', "indexify")
    
    logging.info(f"Executing Cypher query: {cypher_query.query}")
    try:
        with GraphDatabase.driver(uri, auth=(user, password)) as driver:
            with driver.session() as session:
                result = session.run(cypher_query.query)
                records = [dict(record) for record in result]
        logging.info(f"Query executed successfully. Number of results: {len(records)}")
    except Exception as e:
        logging.error(f"Error executing Cypher query: {str(e)}")
        records = execute_fallback_query(question.text, uri, user, password)
    
    return QuestionAndResult(question=question, query_result=QueryResult(result=records))

def execute_fallback_query(question: str, uri: str, user: str, password: str) -> List[Dict[str, Any]]:
    """Execute a fallback query when the main query fails."""
    fallback_query = """
    MATCH (s:Scientist)
    WHERE toLower(s.name) CONTAINS toLower($keyword)
       OR toLower(s.field) CONTAINS toLower($keyword)
       OR toLower(s.known_for) CONTAINS toLower($keyword)
    RETURN s
    LIMIT 5
    """
    keyword = question.lower().replace("who", "").replace("what", "").strip()
    try:
        with GraphDatabase.driver(uri, auth=(user, password)) as driver:
            with driver.session() as session:
                result = session.run(fallback_query, keyword=keyword)
                records = [dict(record) for record in result]
        logging.info(f"Fallback query executed successfully. Number of results: {len(records)}")
        return records
    except Exception as e:
        logging.error(f"Error executing fallback query: {str(e)}")
        return []

@indexify_function(image=gemini_image)
def generate_answer(data: QuestionAndResult) -> Answer:
    """Generate a natural language answer based on the query results."""
    query_result, question = data.query_result, data.question

    if not query_result.result:
        return Answer(text="I'm sorry, but I couldn't find any information related to your question in the database.")

    model = genai.GenerativeModel("gemini-pro")
    
    formatted_results = format_query_results(query_result.result)

    prompt = f"""
    Question: {question.text}
    Database results: {formatted_results}
    
    Please provide a concise answer to the question based on the database results.
    If the results don't directly answer the question, provide the most relevant information available.
    """
    response = model.generate_content(prompt)
    return Answer(text=response.text.strip())

def format_query_results(results: List[Dict[str, Any]]) -> List[str]:
    """Format the query results for better readability."""
    formatted_results = []
    for record in results:
        if 's' in record and isinstance(record['s'], dict):
            scientist = record['s']
            formatted_results.append(f"Name: {scientist.get('name', 'Unknown')}, Field: {scientist.get('field', 'Unknown')}, Known for: {scientist.get('known_for', 'Unknown')}")
        else:
            formatted_results.append(str(record))
    return formatted_results

# Graph definition
def create_qa_graph():
    g = Graph("knowledge_graph_qa", start_node=question_to_cypher)
    g.add_edge(question_to_cypher, execute_cypher_query)
    g.add_edge(execute_cypher_query, generate_answer)
    return g

# Main execution
if __name__ == "__main__":
    try:
        # Create and register the graph
        graph = create_qa_graph()
        client = create_client(in_process=True)
        client.register_compute_graph(graph)
        
        # Sample question
        sample_question = Question(text="Who developed the theory of relativity?")
        
        # Invoke the graph
        logging.info(f"Invoking the graph with question: {sample_question.text}")
        invocation_id = client.invoke_graph_with_object(
            graph.name,
            block_until_done=True,
            question=sample_question
        )
        
        # Retrieve results
        answer_result = client.graph_outputs(graph.name, invocation_id, "generate_answer")
        if answer_result:
            answer = answer_result[0]
            logging.info(f"Generated Answer: {answer.text}")
        else:
            logging.warning("No answer generated")

        logging.info("Workflow completed successfully!")
    except Exception as e:
        logging.error(f"An error occurred during execution: {str(e)}")