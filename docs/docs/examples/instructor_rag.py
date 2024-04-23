from typing import List
from pydantic import BaseModel, Field
from indexify import IndexifyClient

client = IndexifyClient()

class Extraction(BaseModel):
    topic: str
    summary: str
    hypothetical_questions: List[str] = Field(
        default_factory=list,
        description="Hypothetical questions that this document could answer",
    )
    keywords: List[str] = Field(
        default_factory=list, description="Keywords that this document is about"
    )

text_chunk = """
## Simple RAG

****What is it?****

The simplest implementation of RAG embeds a user query and do a single embedding search in a vector database, like a vector store of Wikipedia articles. However, this approach often falls short when dealing with complex queries and diverse data sources.

**What are the limitations?**

- **Query-Document Mismatch:** It assumes that the query and document embeddings will align in the vector space, which is often not the case.
    - Query: "Tell me about climate change effects on marine life."
    - Issue: The model might retrieve documents related to general climate change or marine life, missing the specific intersection of both topics.
- **Monolithic Search Backend:** It relies on a single search method and backend, reducing flexibility and the ability to handle multiple data sources.
    - Query: "Latest research in quantum computing."
    - Issue: The model might only search in a general science database, missing out on specialized quantum computing resources.
- **Text Search Limitations:** The model is restricted to simple text queries without the nuances of advanced search features.
    - Query: "what problems did we fix last week"
    - Issue: cannot be answered by a simple text search since documents that contain problem, last week are going to be present at every week.
- **Limited Planning Ability:** It fails to consider additional contextual information that could refine the search results.
    - Query: "Tips for first-time Europe travelers."
    - Issue: The model might provide general travel advice, ignoring the specific context of first-time travelers or European destinations.
"""



def create_extraction_policy():
    import cloudpickle
    import base64
    extraction_model = base64.b64encode(cloudpickle.dumps(Extraction))
    extraction_model = extraction_model.decode('utf-8')
    input_params={'schema_bytes': extraction_model, 'system_message': 'Your role is to extract chunks from the following and create a set of topics.'}
    client.add_extraction_policy(extractor='tensorlake/instructor', name="extract_chunks", input_params=input_params)
    print("Extraction policy created successfully")


if __name__ == "__main__":
    create_extraction_policy()
    content_id = client.add_documents(text_chunk)
    result = client.get_structured_data(content_id)
    print(result)