from tensorlake.functions_sdk.functions import TensorlakeCompute
from typing import Union
from common_objects import ImageWithEmbedding, TextChunk
from images import chroma_image

class ChromaDBWriter(TensorlakeCompute):
    name = "chroma_db_writer"
    image = chroma_image

    def __init__(self):
        import chromadb
        super().__init__()
        self._client = chromadb.HttpClient(host="chromadb", port=8000)
        self._text_collection = self._client.create_collection(name="text_embeddings", metadata={"hnsw:space": "cosine"}, get_or_create=True)
        self._image_collection = self._client.create_collection(name="image_embeddings", metadata={"hnsw:space": "cosine"}, get_or_create=True)

    def run(self, input: Union[ImageWithEmbedding, TextChunk]) -> bool:
        import uuid
        from PIL import Image
        import io
        import numpy as np
        if type(input) == ImageWithEmbedding:
            img_arr = np.array(Image.open(io.BytesIO(input.image_bytes)))
            self._image_collection.upsert(
                ids=[str(uuid.uuid4())],
                embeddings=[input.embedding],
                metadatas=[{"page_number": input.page_number}],
                images=[img_arr]
            )
        elif type(input) == TextChunk:
            self._text_collection.upsert(
                ids=[str(uuid.uuid4())],
                embeddings=[input.embeddings],
                metadatas=[{"page_number": input.page_number}],
                documents=[input.chunk]
            )
        return True
