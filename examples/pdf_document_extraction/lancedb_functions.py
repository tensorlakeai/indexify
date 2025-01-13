from typing import Union

from tensorlake.functions_sdk.functions import TensorlakeCompute
from common_objects import ImageWithEmbedding, TextChunk
import lancedb
from lancedb.pydantic import LanceModel, Vector
from images import lance_image


class ImageEmbeddingTable(LanceModel):
    vector: Vector(512)
    image_bytes: bytes
    page_number: int

class TextEmbeddingTable(LanceModel):
    vector: Vector(384)
    text: str
    page_number: int

class LanceDBWriter(TensorlakeCompute):
    name = "lancedb_writer"
    image = lance_image

    def __init__(self):
        super().__init__()
        self._client = lancedb.connect("vectordb.lance")
        self._text_table = self._client.create_table(
            "text_embeddings", schema=TextEmbeddingTable, exist_ok=True
        )
        self._clip_table = self._client.create_table(
            "image_embeddings", schema=ImageEmbeddingTable, exist_ok=True
        )

    def run(self, input: Union[ImageWithEmbedding, TextChunk]) -> bool:
        if type(input) == ImageWithEmbedding:
            self._clip_table.add(
                [
                    ImageEmbeddingTable(
                        vector=input.embedding,
                        image_bytes=input.image_bytes,
                        page_number=input.page_number,
                    )
                ]
            )
        elif type(input) == TextChunk:
            self._text_table.add(
                [
                    TextEmbeddingTable(
                        vector=input.embeddings,
                        text=input.chunk,
                        page_number=input.page_number,
                    )
                ]
            )
        return True
