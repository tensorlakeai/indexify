import json
import os
import tempfile
from typing import List

import httpx
import lancedb
import openai
from lancedb.pydantic import LanceModel, Vector
from langchain_text_splitters import RecursiveCharacterTextSplitter
from rich import print
from rich.console import Console
from sentence_transformers import SentenceTransformer
from pydantic import BaseModel

from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.image import Image
from indexify.functions_sdk.indexify_functions import indexify_function, IndexifyFunction

# TODO User set this
contextual_retrieval_prompt = """
Here is the chunk we want to situate within the whole document
<chunk>
{chunk_content}
</chunk>

Please give a short succinct context to situate this chunk within the overall document for the purposes of improving search retrieval of the chunk.
Answer only with the succinct context and nothing else.
"""


image = (
    Image().name("tensorlake/contextual-rag")
    .run("pip install indexify")
    .run("pip install sentence-transformers")
    .run("pip install lancedb")
    .run("pip install openai")
    .run("pip install langchain")
)


class ChunkContext(BaseModel):
    chunks: List[str]
    chunk_contexts: List[str]

@indexify_function(image=image)
def generate_chunk_contexts(doc: str) -> ChunkContext:
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=750,
        chunk_overlap=75,
        length_function=len,
        is_separator_regex=False,
    )

    chunks = text_splitter.split_text(doc)

    chunks_list = []
    chunks_context_list = []

    client = openai.OpenAI()

    for i, chunk in enumerate(chunks):
        print(f"Processing chunk {i} of {len(chunks)} with size {len(chunk)}")
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a helpful assistant. Answer precisely."},
                {"role": "system", "content": f"Answer using the contents of this document <document> {doc} </document>"},
                {"role": "user", "content": contextual_retrieval_prompt.format(chunk_content=chunk)}
            ]
        )

        print(f'oai prompt read from cache {response.usage}')

        chunks_list.append(chunk)
        chunks_context_list.append(response.choices[0].message.content)

    output = ChunkContext(
        chunks=chunks_list,
        chunk_contexts=chunks_context_list,
    )

    return output


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
    image = image

    def __init__(self):
        super().__init__()
        self.model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

    def run(self, input: ChunkContext) -> TextChunk:
        context_embeddings = []
        chunk_embeddings = []

        chunks = []
        chunk_with_contexts = []

        for chunk, context in zip(input.chunks, input.chunk_contexts):
            context_embedding = self.model.encode(chunk + '-\n' + context)
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


class ContextualChunkEmbeddingTable(LanceModel):
    vector: Vector(384)
    chunk: str
    chunk_with_context: str


class ChunkEmbeddingTable(LanceModel):
    vector: Vector(384)
    chunk: str


class LanceDBWriter(IndexifyFunction):
    name = "lancedb_writer_context_rag"
    image = image

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
    client = openai.OpenAI()
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are an AI assistant tasked with analyzing documents and answering questions about them."},
            {"role": "user", "content": payload}
        ]
    )

    return response.choices[0].message.content, response.usage


if __name__ == '__main__':
    g: Graph = Graph("test", start_node=generate_chunk_contexts)

    g.add_edge(generate_chunk_contexts, TextEmbeddingExtractor)
    g.add_edge(TextEmbeddingExtractor, LanceDBWriter)

    # TODO User replace this with the document you are dealing with.
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file_path = os.path.join(temp_dir, 'nvda.txt')
        resp = httpx.get('https://gist.githubusercontent.com/stangirala/ce5ce8b12075542e366ea4c0429e7b69/raw')
        with open(temp_file_path, 'w') as f:
            f.write(resp.content.decode())

        with open(temp_file_path) as f:
                doc = f.read()

    g.run(block_until_done=True, doc=doc)


    # ----
    # Now that we have the indexes written to let's query the data, to verify.
    # ----
    console = Console()

    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

    l_client = lancedb.connect("vectordb.lance")

    question = "Where will Nvidia see the most growth in the near-term?"
    console.print("Asking Question -- " + f"`[bold red]{question}[/]`\n")
    question_embd = model.encode(question)

    console.print('----')

    console.print("\nRAG Output", style="bold red")

    chunks = l_client.open_table("chunk-embeddings").search(question_embd).limit(5).to_list()
    d = []
    for chunk_id, i in enumerate(chunks):
        d.append('chunk_id: ' + str(chunk_id))
        d.append('chunk: ' + i['chunk'])
    p = '\n'.join(d)

    regular_prompt = f"""
    You are presented with information from a recent SEC10K document. The document has been chunked, and we are providing the chunks and the chunk id. The information is presented in the form:

        chunk_id: <chunk_id>
        chunk: <Text>
        chunk_id: <chunk_id>
        chunk: <Text>

    Answer the question, by grounding your responses to only the information provided in the chunks. Cite the chunk ids at the end of the response in the format -
        Answer: <Answer>
        Citation: <Chunk ID>

    Question: {question}

        {p}
"""

    # TODO User uncomment this to debug the output
    # for chunk_id, i in enumerate(chunks):
    #     print(f'chunk_id : {chunk_id} \n')
    #     print(i['chunk'])
    #     print('\n')

    console.print(rag_call(regular_prompt), style="bold red")

    console.print('----')
    console.print('----')

    console.print("\nContextual RAG Output", style="bold red")

    chunks = l_client.open_table("contextual-chunk-embeddings").search(question_embd).limit(5).to_list()
    d = []
    for chunk_id, i in enumerate(chunks):
        d.append('chunk_id: ' + str(chunk_id))
        d.append('chunk: ' + i['chunk'])
        d.append('chunk_context: ' + i['chunk_with_context'])
    p = '\n'.join(d)

    contextual_prompt = f"""
    You are presented with information from a recent SEC10K document. The document has been chunked, and we are providing the chunks, context of the chunk and the chunk id. The information is presented in the form:
        chunk_id: <chunk_id>
        chunk: <Text>
        chunk_context: <Text>
        chunk_id: <chunk_id>
        chunk: <Text>
        chunk_context: <Text>

    Answer the questions, by grounding your responses to only the information provided in the chunks. Cite the chunk ids at the end of the response in the format -
        Answer: <Answer>
        Citation: <Chunk ID>

    Question: {question}

        {p}

"""
    # print(chunks[0])
    # print(chunks[0])
    # TODO User uncomment this to debug the output
    # for chunk_id, i in enumerate(chunks):
    #     print(f'chunk_id : {chunk_id} \n')
    #     print(i['chunk'])
    #     print('            -   ')
    #     print(i['chunk_with_context'])
    #     print('\n')

    console.print(rag_call(contextual_prompt), style="bold red")
