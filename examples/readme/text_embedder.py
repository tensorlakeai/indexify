from pydantic import BaseModel
from tensorlake import tensorlake_function, Graph, Image, TensorlakeCompute
from typing import List, Union


class Text(BaseModel):
    text: str


class TextChunk(BaseModel):
    chunk: str
    page_number: int


class ChunkEmbedding(BaseModel):
    text: str
    embedding: List[float]


embedding_image = (
    Image()
    .name("text_embedding_image")
    .run("pip install langchain")
    .run("pip install sentence_transformer")
    .run("pip install langchain-text-splitters")
    .run("pip install chromadb")
    .run("pip install uuid")
)


# Chunk the text for embedding and retrieval
@tensorlake_function(input_encoder="json", image=embedding_image)
def chunk_text(input: dict) -> List[TextChunk]:
    text = Text.model_validate(input)
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=20,
        length_function=len,
        is_separator_regex=False,
    )
    texts = text_splitter.create_documents([text.text])
    return [
        TextChunk(chunk=chunk.page_content, page_number=i)
        for i, chunk in enumerate(texts)
    ]


# Embed a single chunk.
# Note: (Automatic Map) Indexify automatically parallelize functions when they consume an element
# from functions that produces a List
class Embedder(TensorlakeCompute):
    name = "embedder"
    image = embedding_image

    def __init__(self):
        from sentence_transformers import SentenceTransformer

        self._model = SentenceTransformer("all-MiniLM-L6-v2")

    def run(self, chunk: TextChunk) -> ChunkEmbedding:
        embeddings = self._model.encode(chunk.chunk)
        return ChunkEmbedding(text=chunk.chunk, embedding=embeddings)


class EmbeddingWriter(TensorlakeCompute):
    name = "embedding_writer"
    image = embedding_image

    def __init__(self):
        import chromadb

        self._chroma = chromadb.PersistentClient("./chromadb_tensorlake")
        self._collection = collection = self._chroma.create_collection(
            name="my_collection", get_or_create=True
        )

    def run(self, embedding: ChunkEmbedding) -> None:
        import uuid

        self._collection.upsert(
            ids=[str(uuid.uuid4())],
            embeddings=[embedding.embedding],
            documents=[embedding.text],
        )


# Constructs a compute graph connecting the three functions defined above into a workflow that generates
# runs them as a pipeline
graph = Graph(
    name="text_embedder",
    start_node=chunk_text,
    description="Splits, embeds and indexes text",
)
graph.add_edge(chunk_text, Embedder)
graph.add_edge(Embedder, EmbeddingWriter)


if __name__ == "__main__":
    invocation_id = graph.run(input={"text": "This is a test text"})
    print(f"Invocation ID: {invocation_id}")
    embedding = graph.output(invocation_id, "embedder")
    print(embedding)
