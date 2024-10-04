from typing import Union

from indexify import Image
from indexify.functions_sdk.indexify_functions import IndexifyFunction
from common_objects import ImageWithEmbedding, TextChunk

image = Image().name("pdf-blueprint-lancdb").run("pip install lancedb")


class LanceDBWriter(IndexifyFunction):
    name = "lancedb_writer"
    image = image

    def __init__(self):
        super().__init__()
        import lancedb
        from lancedb.pydantic import LanceModel, Vector
        class ImageEmbeddingTable(LanceModel):
            vector: Vector(512)
            page_number: int
            figure_number: int

        class TextEmbeddingTable(LanceModel):
            vector: Vector(384)
            text: str
            page_number: int

        self._client = lancedb.connect("vectordb.lance")
        self._text_table = self._client.create_table(
            "text_embeddings", schema=TextEmbeddingTable, exist_ok=True
        )
        self._clip_table = self._client.create_table(
            "image_embeddings", schema=ImageEmbeddingTable, exist_ok=True
        )
        self._ImageEmbeddingTable = ImageEmbeddingTable
        self._TextEmbeddingTable = TextEmbeddingTable

    def run(self, input: Union[ImageWithEmbedding, TextChunk]) -> bool:
        if type(input) == ImageWithEmbedding:
            self._clip_table.add(
                [
                    self._ImageEmbeddingTable(
                        vector=input.embedding,
                        page_number=input.page_number,
                        figure_number=0,
                    )
                ]
            )
        elif type(input) == TextChunk:
            self._text_table.add(
                [
                    self._TextEmbeddingTable(
                        vector=input.embeddings,
                        text=input.chunk,
                        page_number=input.page_number,
                    )
                ]
            )
        return True
