import json
import os
from typing import List

import anthropic
import lancedb
from lancedb.pydantic import LanceModel, Vector
from rich import print
from rich.console import Console
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
    # Let's do a simple chunking strategy.
    paras = doc.split('\n')
    # Filter empty paras
    doc = [i for i in paras if len(i) > 0]
    # Just take the first 10 for this example
    chunks = doc[:30]

    chunks_list = []
    chunks_context_list = []

    client = anthropic.Anthropic()

    for i, chunk in enumerate(chunks):
        print(f"Processing chunk {i} with size {len(chunk)}")
        response = client.beta.prompt_caching.messages.create(
            # model="claude-3-5-sonnet-20240620",
            model="claude-3-haiku-20240307",
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
    context_embeddings: List[List[float]]
    chunk_embeddings: List[List[float]]

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
        context_embeddings = []
        chunk_embeddings = []

        chunks = []
        chunk_with_contexts = []

        for chunk, context in zip(input.chunks, input.chunk_contexts):
            context_embedding = self.model.encode(context)
            chunk_embedding = self.model.encode(chunk)

            context_embeddings.append(context_embedding.tolist())
            chunk_embeddings.append(chunk_embedding.tolist())

            chunks.append(chunk)
            chunk_with_contexts.append(context)

        return TextChunk(
            context_embeddings=context_embeddings,
            chunk_embeddings=chunk_embeddings,
            chunk=chunks,
            chunk_with_context=chunk_with_contexts,
        )


lance_db_image = Image().name("tensorlake/pdf-blueprint-lancdb").run("pip install lancedb")


class ContextualChunkEmbeddingTable(LanceModel):
    # vector: List[float]
    vector: Vector(384)
    chunk: str
    chunk_with_context: str


class ChunkEmbeddingTable(LanceModel):
    vector: Vector(384)
    chunk: str


class LanceDBWriter(IndexifyFunction):
    name = "lancedb_writer_context_rag"
    image = lance_db_image

    def __init__(self):
        super().__init__()
        self._client = lancedb.connect("vectordb.lance")
        self._contextual_chunk_table = self._client.create_table(
            "contextual-chunk-embeddings", schema=ContextualChunkEmbeddingTable, exist_ok=True
        )

        self._chunk_table = self._client.create_table(
            "chunk-embeddings", schema=ChunkEmbeddingTable, exist_ok=True
        )

    def run(self, input: TextChunk) -> bool:
        for context_embedding, chunk_embedding, chunk, context in (
                zip(input.context_embeddings, input.chunk_embeddings, input.chunk, input.chunk_with_context)
        ):
            self._contextual_chunk_table.add(
                [
                    ContextualChunkEmbeddingTable(vector=context_embedding, chunk=chunk, chunk_with_context=context,)
                ]
            )

            self._chunk_table.add(
                [
                    ChunkEmbeddingTable(vector=chunk_embedding, chunk=chunk,)
                ]
            )

        return True


def rag_call(payload):
    client = anthropic.Anthropic()
    response = client.beta.prompt_caching.messages.create(
        model="claude-3-haiku-20240307",
        max_tokens=1024,
        system=[
            {
                "type": "text",
                "text": "You are an AI assistant tasked with analyzing documents and answering questions about them.",
            },
        ],
        messages=[
            { "role": "user", "content": payload, }
        ],
    )

    return response.content


if __name__ == '__main__':
    # g: Graph = Graph("test", start_node=generate_chunk_contexts)
    #
    # g.add_edge(generate_chunk_contexts, TextEmbeddingExtractor)
    # g.add_edge(TextEmbeddingExtractor, LanceDBWriter)
    #
    # # TODO User replace this with the document you are dealing with.
    # # replace with GET, replce with github raw
    # with open('pg-essay.txt') as f:
    #     doc = f.read()
    #
    # g.run(block_until_done=True, doc=doc)


    # ----
    # Now that we have the indexes written to let's query the data, to verify.
    # ----
    console = Console()

    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


    l_client = lancedb.connect("vectordb.lance")

    question = "what did paul do when he was a kid"
    console.print("Asking Question -- " + f"`[bold red]{question}[/]`\n")
    question_embd = model.encode(question)

    console.print('----')

    console.print("\nRAG Output", style="bold red")

    chunks = l_client.open_table("chunk-embeddings").search(question_embd).limit(5).to_list()
    regular_prompt = f"""
Given the following chunks and chunk_ids can you answer this question `{question}`? Please
keep the answer concise and to the point. Only use the chunk data to answer the question. Please also
return the chunk_id that was used to answer the question.
    {chunks}
"""

    for ii, i in enumerate(chunks):
        print(f'chunk_id : {ii} \n')
        print(i['chunk'])
        print('\n')

    console.print(rag_call(regular_prompt), style="bold red")

    console.print('----')

    chunks = l_client.open_table("contextual-chunk-embeddings").search(question_embd).limit(5).to_list()
    _chunks = json.dumps([{'chunk': i['chunk'], 'context': i['chunk_with_context']} for i in chunks])

    contextual_prompt = f"""
Given the following contextual chunks and chunk_ids can you answer this question `{question}`? Please
keep the answer concise and to the point. Only use the chunk data to answer the question. Please also
return the chunk_id that was used to answer the question.
    {_chunks}
"""
    # print(chunks[0])
    for ii, i in enumerate(chunks):
        print(f'chunk_id : {ii} \n')
        print(i['chunk'])
        print('            -   ')
        print(i['chunk_with_context'])
        print('\n')

    console.print(rag_call(contextual_prompt), style="bold red")
