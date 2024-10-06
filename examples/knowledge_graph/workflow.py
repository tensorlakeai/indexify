import logging
import os
from typing import List, Dict, Tuple, Any
from pydantic import BaseModel, Field
from indexify import RemoteGraph
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import indexify_function, IndexifyFunction
from indexify.functions_sdk.image import Image
from neo4j import GraphDatabase
import json
import google.generativeai as genai
import re
import spacy

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configure Gemini API
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
if not GOOGLE_API_KEY:
    raise ValueError("GOOGLE_API_KEY environment variable is not set")
genai.configure(api_key=GOOGLE_API_KEY)

# Data Models
class Entity(BaseModel):
    id: str = Field(..., description="Unique identifier of the entity")
    type: str = Field(..., description="Type of the entity")
    name: str = Field(..., description="Name of the entity")

class Relationship(BaseModel):
    source: str = Field(..., description="Source entity ID")
    target: str = Field(..., description="Target entity ID")
    type: str = Field(..., description="Type of the relationship")

class KnowledgeGraph(BaseModel):
    entities: List[Entity] = Field(default_factory=list, description="List of entities in the graph")
    relationships: List[Relationship] = Field(default_factory=list, description="List of relationships in the graph")

class Document(BaseModel):
    content: str = Field(..., description="Content of the document")
    metadata: Dict[str, str] = Field(default_factory=dict, description="Metadata of the document")

class TextChunk(BaseModel):
    text: str = Field(..., description="Text content of the chunk")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Metadata of the chunk, including embeddings")

class KnowledgeGraphOutput(BaseModel):
    knowledge_graph: KnowledgeGraph
    document: Document

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

# Indexify image definitions
base_image = "python:3.9-slim"

nlp_image = (
    Image()
    .name("nlp-image")
    .base_image(base_image)
    .run("pip install spacy")
    .run("python -m spacy download en_core_web_sm")
)

embedding_image = (
    Image()
    .name("embedding-image")
    .base_image(base_image)
    .run("pip install sentence-transformers")
)

neo4j_image = (
    Image()
    .name("neo4j-image")
    .base_image(base_image)
    .run("pip install neo4j")
)

gemini_image = (
    Image()
    .name("gemini-image")
    .base_image(base_image)
    .run("pip install google-generativeai")
)

class NLPFunction(IndexifyFunction):
    name = "nlp-function"
    image = nlp_image

    def __init__(self):
        super().__init__()
        self._nlp = None

    def get_nlp(self):
        if self._nlp is None:
            self._nlp = spacy.load("en_core_web_sm")
        return self._nlp

class ExtractEntitiesAndText(NLPFunction):
    name = "extract-entities-and-text"

    def run(self, doc: Document) -> Tuple[List[Entity], str, Document]:
        try:
            nlp = self.get_nlp()
            text = nlp(doc.content)
            entities = []
            for ent in text.ents:
                entity_type = ent.label_
                if entity_type == "PERSON":
                    entity_type = "Scientist"
                elif entity_type in ["GPE", "NORP"]:
                    entity_type = "Location"
                elif entity_type in ["ORG", "PRODUCT", "EVENT", "WORK_OF_ART"]:
                    entity_type = "Concept"
                
                entity_id = f"{entity_type}_{ent.text.replace(' ', '_')}"
                entities.append(Entity(
                    id=entity_id,
                    type=entity_type,
                    name=ent.text
                ))
            logging.info(f"Extracted {len(entities)} entities")
            return entities, doc.content, doc
        except Exception as e:
            logging.error(f"Error in extract_entities_and_text: {str(e)}")
            raise

class ExtractRelationships(NLPFunction):
    name = "extract-relationships"

    def run(self, data: Tuple[List[Entity], str, Document]) -> Tuple[List[Entity], List[Relationship], Document]:
        try:
            entities, content, doc = data
            nlp = self.get_nlp()
            relationships = []
            
            spacy_doc = nlp(content)
            
            scientist = next((e for e in entities if e.type == "Scientist"), None)
            
            if scientist:
                scientist_span = next((ent for ent in spacy_doc.ents if ent.text == scientist.name), None)
                
                if scientist_span:
                    for entity in entities:
                        if entity != scientist:
                            entity_span = next((ent for ent in spacy_doc.ents if ent.text == entity.name), None)
                            
                            if entity_span:
                                if entity.type == "Location":
                                    rel_type = "BORN_IN"
                                elif "theory of relativity" in entity.name.lower():
                                    rel_type = "DEVELOPED"
                                elif "mass energy equivalence" in entity.name.lower():
                                    rel_type = "FAMOUS_FOR"
                                else:
                                    rel_type = "ASSOCIATED_WITH"
                                
                                relationships.append(Relationship(
                                    source=scientist.id,
                                    target=entity.id,
                                    type=rel_type
                                ))
            
            logging.info(f"Extracted {len(relationships)} relationships")
            return entities, relationships, doc
        except Exception as e:
            logging.error(f"Error in extract_relationships: {str(e)}")
            raise

@indexify_function()
def build_knowledge_graph(data: Tuple[List[Entity], List[Relationship], Document]) -> KnowledgeGraphOutput:
    try:
        entities, relationships, doc = data
        kg = KnowledgeGraph(entities=entities, relationships=relationships)
        logging.info(f"Built Knowledge Graph with {len(kg.entities)} entities and {len(kg.relationships)} relationships")
        return KnowledgeGraphOutput(knowledge_graph=kg, document=doc)
    except Exception as e:
        logging.error(f"Error in build_knowledge_graph: {str(e)}")
        raise

@indexify_function(image=neo4j_image)
def store_in_neo4j(data: KnowledgeGraphOutput) -> bool:
    try:
        kg = data.knowledge_graph
        uri = os.getenv('NEO4J_URI', "bolt://localhost:7687")
        user = os.getenv('NEO4J_USER', "neo4j")
        password = os.getenv('NEO4J_PASSWORD', "indexify")
        
        with GraphDatabase.driver(uri, auth=(user, password)) as driver:
            with driver.session() as session:
                for entity in kg.entities:
                    session.run(
                        "MERGE (e:" + entity.type + " {id: $id}) SET e.name = $name",
                        id=entity.id, name=entity.name
                    )
                for rel in kg.relationships:
                    session.run(
                        "MATCH (a {id: $source}), (b {id: $target}) "
                        "MERGE (a)-[r:" + rel.type + "]->(b)",
                        source=rel.source, target=rel.target
                    )
        logging.info(f"Stored {len(kg.entities)} entities and {len(kg.relationships)} relationships in Neo4j")
        return True
    except Exception as e:
        logging.error(f"Error in store_in_neo4j: {str(e)}")
        raise

@indexify_function(image=embedding_image)
def generate_embeddings(data: KnowledgeGraphOutput) -> TextChunk:
    try:
        doc = data.document
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer('all-MiniLM-L6-v2')
        embedding = model.encode(doc.content)
        chunk = TextChunk(
            text=doc.content, 
            metadata={
                "embedding": json.dumps(embedding.tolist()), 
                **doc.metadata
            }
        )
        logging.info(f"Generated embedding of length {len(embedding)}")
        return chunk
    except Exception as e:
        logging.error(f"Error in generate_embeddings: {str(e)}")
        raise

@indexify_function(image=gemini_image)
def question_to_cypher(question: Question) -> CypherQueryAndQuestion:
    try:
        model = genai.GenerativeModel("gemini-pro")
        prompt = f"""
        Convert the following question to a Cypher query for a Neo4j database with the following schema:
        (Scientist {{id, name}})
        (Location {{id, name}})
        (Concept {{id, name}})
        [BORN_IN], [DEVELOPED], [FAMOUS_FOR], [ASSOCIATED_WITH]
        
        Question: {question.text}
        
        Provide only the Cypher query without any additional text or code formatting.
        Use 'Albert Einstein' as the full name when querying for Einstein.
        Remember that all entities have a 'name' property, not just an 'id' property.
        """
        response = model.generate_content(prompt)
        cypher_query = response.text.strip()
        
        cypher_query = re.sub(r'^```\w*\n|```$', '', cypher_query, flags=re.MULTILINE).strip()
        
        logging.info(f"Generated Cypher query: {cypher_query}")
        return CypherQueryAndQuestion(cypher_query=CypherQuery(query=cypher_query), question=question)
    except Exception as e:
        logging.error(f"Error in question_to_cypher: {str(e)}")
        raise

@indexify_function(image=neo4j_image)
def execute_cypher_query(data: CypherQueryAndQuestion) -> QuestionAndResult:
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
        records = []
    
    return QuestionAndResult(question=question, query_result=QueryResult(result=records))

@indexify_function(image=gemini_image)
def generate_answer(data: QuestionAndResult) -> Answer:
    query_result, question = data.query_result, data.question

    if not query_result.result:
        return Answer(text="I'm sorry, but I couldn't find any information related to your question in the database.")

    model = genai.GenerativeModel("gemini-pro")
    
    formatted_results = [str(record) for record in query_result.result]

    prompt = f"""
    Question: {question.text}
    Database results: {formatted_results}
    
    Please provide a concise answer to the question based on the database results.
    If the results don't directly answer the question, provide the most relevant information available.
    """
    response = model.generate_content(prompt)
    return Answer(text=response.text.strip())

# Graph definitions
def create_kg_rag_graph():
    g = Graph("knowledge_graph_rag", start_node=ExtractEntitiesAndText)
    g.add_edge(ExtractEntitiesAndText, ExtractRelationships)
    g.add_edge(ExtractRelationships, build_knowledge_graph)
    g.add_edge(build_knowledge_graph, store_in_neo4j)
    g.add_edge(build_knowledge_graph, generate_embeddings)
    return g

def create_qa_graph():
    g = Graph("knowledge_graph_qa", start_node=question_to_cypher)
    g.add_edge(question_to_cypher, execute_cypher_query)
    g.add_edge(execute_cypher_query, generate_answer)
    return g

def process_document(doc: Document):
    graph = create_kg_rag_graph()
    #remote_graph = RemoteGraph.deploy(graph)  # Uncomment to deploy remotely
    
    logging.info("Invoking the KG RAG graph")
    invocation_id = graph.run(
        block_until_done=True,
        doc=doc
    )
    
    process_kg_results(graph, invocation_id)

def process_kg_results(graph: Graph, invocation_id: str):
    kg_result = graph.output(invocation_id, "build_knowledge_graph")
    if kg_result:
        logging.info("Knowledge Graph created:")
        logging.info(f"Entities: {len(kg_result[0].knowledge_graph.entities)}")
        for entity in kg_result[0].knowledge_graph.entities:
            logging.info(f"  - ID: {entity.id}, Type: {entity.type}, Name: {entity.name}")
        logging.info(f"Relationships: {len(kg_result[0].knowledge_graph.relationships)}")
        for rel in kg_result[0].knowledge_graph.relationships:
            logging.info(f"  - {rel.source} -> {rel.target} [{rel.type}]")
    else:
        logging.warning("No Knowledge Graph result")

    neo4j_result = graph.output(invocation_id, "store_in_neo4j")
    if neo4j_result:
        logging.info(f"Stored in Neo4j: {neo4j_result[0]}")
    else:
        logging.warning("No Neo4j storage result")

    embeddings_result = graph.output(invocation_id, "generate_embeddings")
    if embeddings_result:
        embedding = json.loads(embeddings_result[0].metadata['embedding'])
        logging.info(f"Embeddings generated. First 5 values: {embedding[:5]}")
    else:
        logging.warning("No embeddings result")

def answer_question(question: Question):
    graph = create_qa_graph()
    # remote_graph = RemoteGraph.deploy(graph)  # Uncomment to deploy remotely
    
    logging.info(f"Invoking the QA graph with question: {question.text}")
    invocation_id = graph.run(
        block_until_done=True,
        question=question
    )
    
    answer_result = graph.output(invocation_id, "generate_answer")
    if answer_result:
        answer = answer_result[0]
        logging.info(f"Generated Answer: {answer.text}")
        return answer.text
    else:
        logging.warning("No answer generated")
        return "Sorry, I couldn't generate an answer to your question."

def main():
    try:
        sample_doc = Document(
            content="Albert Einstein was a theoretical physicist born in Germany who developed the Theory of Relativity. "
                    "He is best known for the Mass Energy Equivalence Formula.",
            metadata={"source": "wikipedia"}
        )
        process_document(sample_doc)
        
        questions = [
            Question(text="Where was Albert Einstein born?"),
            Question(text="What scientific theory did Einstein develop?"),
            Question(text="What is Einstein's most famous formula?")
        ]
        
        for question in questions:
            answer = answer_question(question)
            print(f"\nQuestion: {question.text}")
            print(f"Answer: {answer}")
        
        logging.info("Workflow completed successfully!")
    except Exception as e:
        logging.error(f"An error occurred during execution: {str(e)}")

if __name__ == "__main__":
    main()
