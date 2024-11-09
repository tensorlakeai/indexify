from indexify import RemoteGraph, Graph, Image
from indexify.functions_sdk.indexify_functions import (
    IndexifyFunction,
)
from pydantic import BaseModel
from typing import List
from sentence_transformers import SentenceTransformer


tf_image = (
    Image()
    .base_image("pytorch/pytorch:2.4.1-cuda11.8-cudnn9-runtime")
    .name("tensorlake/common-torch-deps-indexify-executor")
    .run("pip install transformers")
    .run("pip install sentence_transformers")
    .run("pip install langchain")
)

class Embedding(BaseModel):
    embedding: List[List[float]]

class Sentences(BaseModel):
    sentences: List[str]

class EmbeddingFunction(IndexifyFunction):
    name = "sentence_embedder"
    image = tf_image 

    def __init__(self):
        super().__init__()
        self.model = SentenceTransformer("all-MiniLM-L6-v2")

    def run(self, sentences: Sentences) -> Embedding:
        #embeddings = self.model.encode(sentences.sentences)
        #embeddings = [embedding.tolist() for embedding in embeddings]
        return Embedding(embedding=[[1.0, 2.0], [3.0, 4.0]])


if __name__ == "__main__":
    import sys
    g = Graph(name="basic_embedding", start_node=EmbeddingFunction)
    g = RemoteGraph.deploy(g, additional_modules=[sys.modules[__name__]])
    sentences = Sentences(sentences=["hello world", "how are you"])
    invocation_id = g.run(block_until_done=True, img=sentences)
    output = g.output(invocation_id, "sentence_embedder")
    print(output)
