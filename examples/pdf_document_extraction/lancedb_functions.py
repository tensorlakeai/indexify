from typing import Union

from indexify.functions_sdk.indexify_functions import IndexifyFunction
from lancedb.pydantic import LanceModel, Vector

from common_objects import ImageWithEmbedding, TextChunk


class ImageEmbeddingTable(LanceModel):
    vector: Vector(512)
    page_number: int
    figure_number: int


class TextEmbeddingTable(LanceModel):
    vector: Vector(384)
    text: str
    page_number: int


class LanceDBWriter(IndexifyFunction):
    name = "lancedb_writer"

    def __init__(self):
        super().__init__()
        import lancedb
        import pyarrow as pa

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
                        page_number=input.page_number,
                        figure_number=0,
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
