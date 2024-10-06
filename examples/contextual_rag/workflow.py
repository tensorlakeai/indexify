from typing import List, Union

import anthropic
import lancedb
from lancedb.pydantic import LanceModel, Vector
from sentence_transformers import SentenceTransformer
from pydantic import BaseModel

from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.image import Image
from indexify.functions_sdk.indexify_functions import indexify_function, IndexifyFunction

contextual_retrieval_prompt = """
Here is the chunk we want to situate within the whole document
<chunk>
{chunk_content}
</chunk>

Please give a short succinct context to situate this chunk within the overall document for the purposes of improving search retrieval of the chunk.
Answer only with the succinct context and nothing else.
"""


class ChunkContext(BaseModel):
    chunks: List[str]
    chunk_contexts: List[str]

@indexify_function()
def generate_chunk_contexts(doc: str) -> ChunkContext:
    chunks = '.'.split(doc)

    output = ChunkContext()

    client = anthropic.Anthropic()

    for chunk in chunks:
        response = client.beta.prompt_caching.messages.create(
            model="claude-3-5-sonnet-20240620",
            max_tokens=1024,
            system=[
                {
                    "type": "text",
                    "text": "You are an AI assistant tasked with analyzing documents and chunks of text from the document.",
                },
                {
                    "type": "text",
                    "text": "<document> {doc_content} </document>".format(doc_content=doc),
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[
                {
                    "role": "user",
                    "content": contextual_retrieval_prompt.format(chunk_content=chunk)
                }
            ],
        )

        chunk_context = response.content

        output.chunks.append(chunk)
        output.chunk_context.append(chunk_context)

    return output


text_embedding_image = Image().name("tensorlake/pdf-blueprint-st").run("pip install sentence-transformers")

class TextChunk(BaseModel):
    embedding: Vector(384)
    chunk: str
    chunk_with_context: str

class TextEmbeddingExtractor(IndexifyFunction):
    name = "text-embedding-extractor"
    description = "Extractor class that captures an embedding model"
    system_dependencies = []
    input_mime_types = ["text"]
    image = text_embedding_image

    def __init__(self):
        super().__init__()
        self.model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

    def run(self, input: ChunkContext) -> TextChunk:
        output = TextChunk
        for chunk, context in zip(input.chunks, input.chunk_contexts):
            chunk_with_context = chunk + '\n' + context
            embeddings = self.model.encode(chunk_with_context)

            output.embedding = embeddings.tolist()
            output.chunk = chunk
            output.context = context

        return output

lance_db_image = Image().name("tensorlake/pdf-blueprint-lancdb").run("pip install lancedb")


class TextEmbeddingTable(LanceModel):
    vector: Vector(384)
    chunk: str
    chunk_with_context: str

class LanceDBWriter(IndexifyFunction):
    name = "lancedb_writer_context_rag"
    image = lance_db_image

    def __init__(self):
        super().__init__()
        self._client = lancedb.connect("vectordb.lance")
        self._text_table = self._client.create_table(
            "text_embeddings", schema=TextEmbeddingTable, exist_ok=True
        )

    def run(self, input: TextChunk) -> bool:
        self._text_table.add(
            [
                TextEmbeddingTable(
                    vector=input.embedding,
                    chunk=input.chunk,
                    chunk_with_context=input.chunk_with_context,
                )
            ]
        )

        return True


if __name__ == '__main__':
    graph: Graph = Graph("test", start_node=generate_chunk_contexts)

    graph.add_edge(generate_chunk_contexts, TextEmbeddingExtractor)
    graph.add_edge(TextEmbeddingExtractor, LanceDBWriter)

