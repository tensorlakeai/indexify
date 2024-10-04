from typing import Any, List

from indexify import Image
from indexify.functions_sdk.indexify_functions import IndexifyFunction
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
from common_objects import ImageWithEmbedding, TextChunk

image = Image().name("tensorlake/pdf-blueprint-st").run("pip install sentence-transformers")


class DocumentImage(BaseModel):
    page_number: int
    # This is so that we don't have to import PIL.Image at the top of the file
    image: Any

class DocumentImages(BaseModel):
    images: List[DocumentImage]

class TextEmbeddingExtractor(IndexifyFunction):
    name = "text-embedding-extractor"
    description = "Extractor class that captures an embedding model"
    system_dependencies = []
    input_mime_types = ["text"]
    image = image

    def __init__(self):
        super().__init__()
        self.model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

    def run(self, input: TextChunk) -> TextChunk:
        embeddings = self.model.encode(input.chunk)
        input.embeddings = embeddings.tolist()
        return input


class ImageEmbeddingExtractor(IndexifyFunction):
    name = "image-embedding"
    description = "Extractor class that captures an embedding model"
    image=image

    def __init__(self):
        super().__init__()
        self.model = SentenceTransformer("clip-ViT-B-32")

    def run(self, images: DocumentImages) -> List[ImageWithEmbedding]:
        embedding = []
        for image in images.images:
            print(image.image)
            img_emb = self.model.encode(image.image)
            embedding.append(
                ImageWithEmbedding(
                    embedding=img_emb,
                    page_number=image.page_number,
                )
            )
        return embedding
