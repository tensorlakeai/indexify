import os
from typing import List

import anthropic
import lancedb
from lancedb.pydantic import LanceModel
from sentence_transformers import SentenceTransformer
from pydantic import BaseModel

from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.image import Image
from indexify.functions_sdk.indexify_functions import indexify_function, IndexifyFunction

# TODO User set this
os.environ["ANTHROPIC_API_KEY"] = ""

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

    chunks_list = []
    chunks_context_list = []

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

        chunk_context = response.content # [TextBlock[text, type]]

        chunks_list.append(chunk)
        chunks_context_list.append(chunk_context[0].text)

    output = ChunkContext(
        chunks=chunks_list,
        chunk_contexts=chunks_context_list,
    )

    return output


text_embedding_image = Image().name("tensorlake/pdf-blueprint-st").run("pip install sentence-transformers")

class TextChunk(BaseModel):
    embeddings: List[List[float]]
    chunk: List[str]
    chunk_with_context: List[str]

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
        embeddings = []
        chunks = []
        chunk_with_contexts = []

        for chunk, context in zip(input.chunks, input.chunk_contexts):
            embedding = self.model.encode(context)

            embeddings.append(embedding.tolist())
            chunks.append(chunk)
            chunk_with_contexts.append(context)

        return TextChunk(
            embeddings=embeddings,
            chunk=chunks,
            chunk_with_context=chunk_with_contexts,
        )


lance_db_image = Image().name("tensorlake/pdf-blueprint-lancdb").run("pip install lancedb")


class TextEmbeddingTable(LanceModel):
    vector: List[float]
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
        for embedding, chunk, context in zip(input.embeddings, input.chunk, input.chunk_with_context):
            self._text_table.add(
                [
                    TextEmbeddingTable(
                        vector=embedding,
                        chunk=chunk,
                        chunk_with_context=context,
                    )
                ]
            )

        return True


if __name__ == '__main__':
    g: Graph = Graph("test", start_node=generate_chunk_contexts)

    g.add_edge(generate_chunk_contexts, TextEmbeddingExtractor)
    g.add_edge(TextEmbeddingExtractor, LanceDBWriter)

    # TODO User replace this with the document you are dealing with.
    doc = """
    this is a test.
    it might be a test.
    who knows if it is a test."""

    g.run(block_until_done=True, doc=doc)