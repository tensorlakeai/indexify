import logging
import os
from typing import List, Dict, Tuple, Any
from pydantic import BaseModel, Field
from indexify import create_client
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import indexify_function
from indexify.functions_sdk.image import Image
from neo4j import GraphDatabase
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Data Models
class Entity(BaseModel):
    id: str = Field(..., description="Unique identifier of the entity")
    type: str = Field(..., description="Type of the entity")
    properties: Dict[str, str] = Field(default_factory=dict, description="Additional properties of the entity")

    def __hash__(self):
        return hash(self.id)

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

# Indexify function definitions
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

@indexify_function(image=nlp_image)
def extract_entities_and_text(doc: Document) -> Tuple[List[Entity], str, Document]:
    """Extract entities from the document using spaCy NER."""
    try:
        import spacy
        nlp = spacy.load("en_core_web_sm")
        text = nlp(doc.content)
        entities = [
            Entity(
                id=ent.text,
                type=ent.label_,
                properties={"text": ent.text, "start": str(ent.start_char), "end": str(ent.end_char)}
            ) for ent in text.ents
        ]
        logging.info(f"Extracted {len(entities)} entities")
        return entities, doc.content, doc
    except Exception as e:
        logging.error(f"Error in extract_entities_and_text: {str(e)}")
        raise

@indexify_function(image=nlp_image)
def extract_relationships(data: Tuple[List[Entity], str, Document]) -> Tuple[List[Relationship], Document]:
    """Extract relationships between entities in the same sentence."""
    try:
        entities, text, doc = data
        import spacy
        nlp = spacy.load("en_core_web_sm")
        doc_nlp = nlp(text)
        relationships = [
            Relationship(source=entity.id, target=other_entity.id, type="RELATED_TO")
            for entity in entities
            for other_entity in entities
            if entity != other_entity and any(entity.id in sent.text and other_entity.id in sent.text for sent in doc_nlp.sents)
        ]
        logging.info(f"Extracted {len(relationships)} relationships")
        return relationships, doc
    except Exception as e:
        logging.error(f"Error in extract_relationships: {str(e)}")
        raise

@indexify_function()
def build_knowledge_graph(data: Tuple[List[Relationship], Document]) -> KnowledgeGraphOutput:
    """Build a knowledge graph from extracted entities and relationships."""
    try:
        relationships, doc = data
        entities = set()
        for rel in relationships:
            entities.add(Entity(id=rel.source, type="Entity", properties={}))
            entities.add(Entity(id=rel.target, type="Entity", properties={}))
        kg = KnowledgeGraph(entities=list(entities), relationships=relationships)
        logging.info(f"Built Knowledge Graph with {len(kg.entities)} entities and {len(kg.relationships)} relationships")
        return KnowledgeGraphOutput(knowledge_graph=kg, document=doc)
    except Exception as e:
        logging.error(f"Error in build_knowledge_graph: {str(e)}")
        raise

@indexify_function(image=neo4j_image)
def store_in_neo4j(data: KnowledgeGraphOutput) -> bool:
    """Store the knowledge graph in Neo4j database."""
    try:
        kg = data.knowledge_graph
        uri = os.getenv('NEO4J_URI', "bolt://localhost:7687")
        user = os.getenv('NEO4J_USER', "neo4j")
        password = os.getenv('NEO4J_PASSWORD', "indexify")
        
        with GraphDatabase.driver(uri, auth=(user, password)) as driver:
            with driver.session() as session:
                for entity in kg.entities:
                    session.run(
                        "MERGE (e:Entity {id: $id}) SET e.type = $type, e += $properties",
                        id=entity.id, type=entity.type, properties=entity.properties
                    )
                for rel in kg.relationships:
                    session.run(
                        "MATCH (a:Entity {id: $source}), (b:Entity {id: $target}) "
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
    """Generate embeddings for the document using Sentence Transformers."""
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

def create_kg_rag_graph():
    """Create the Indexify graph for the Knowledge Graph RAG pipeline."""
    g = Graph("knowledge_graph_rag", start_node=extract_entities_and_text)
    g.add_edge(extract_entities_and_text, extract_relationships)
    g.add_edge(extract_relationships, build_knowledge_graph)
    g.add_edge(build_knowledge_graph, store_in_neo4j)
    g.add_edge(build_knowledge_graph, generate_embeddings)
    return g

def main():
    try:
        graph = create_kg_rag_graph()
        client = create_client(in_process=True)
        client.register_compute_graph(graph)
        
        sample_doc = Document(
            content="Albert Einstein was a German-born theoretical physicist who developed the theory of relativity. He is best known for his mass-energy equivalence formula E = mc^2.",
            metadata={"source": "wikipedia"}
        )
        
        logging.info("Invoking the graph")
        invocation_id = client.invoke_graph_with_object(
            graph.name,
            block_until_done=True,
            doc=sample_doc
        )
        
        process_results(client, graph.name, invocation_id)
        
        logging.info("Workflow completed successfully!")
    except Exception as e:
        logging.error(f"An error occurred during execution: {str(e)}")

def process_results(client, graph_name, invocation_id):
    """Process and log the results of the graph execution."""
    kg_result = client.graph_outputs(graph_name, invocation_id, "build_knowledge_graph")
    if kg_result:
        logging.info("Knowledge Graph created:")
        logging.info(f"Entities: {len(kg_result[0].knowledge_graph.entities)}")
        logging.info(f"Relationships: {len(kg_result[0].knowledge_graph.relationships)}")
    else:
        logging.warning("No Knowledge Graph result")

    neo4j_result = client.graph_outputs(graph_name, invocation_id, "store_in_neo4j")
    if neo4j_result:
        logging.info(f"Stored in Neo4j: {neo4j_result[0]}")
    else:
        logging.warning("No Neo4j storage result")

    embeddings_result = client.graph_outputs(graph_name, invocation_id, "generate_embeddings")
    if embeddings_result:
        embedding = json.loads(embeddings_result[0].metadata['embedding'])
        logging.info(f"Embeddings generated. First 5 values: {embedding[:5]}")
    else:
        logging.warning("No embeddings result")

if __name__ == "__main__":
    main()