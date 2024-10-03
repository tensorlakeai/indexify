from typing import List

from indexify import Image
from indexify.functions_sdk.indexify_functions import IndexifyFunction
from inkwell import Document
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer

from common_objects import ImageWithEmbedding, TextChunk

image = Image().name("pdf-blueprint-st").run("pip install sentence-transformers")


class TextEmbeddingExtractor(IndexifyFunction):
    name = "text-embedding-extractor"
    description = "Extractor class that captures an embedding model"
    system_dependencies = []
    input_mime_types = ["text"]
    image = image

    def __init__(self):
        super().__init__()
        self.model = None

    def run(self, input: TextChunk) -> TextChunk:
        if self.model is None:
            from sentence_transformers import SentenceTransformer

            self.model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

        embeddings = self.model.encode(input.chunk)
        input.embeddings = embeddings.tolist()
        return input


class ImageEmbeddingExtractor(IndexifyFunction):
    name = "image-embedding"
    description = "Extractor class that captures an embedding model"

    def __init__(self):
        super().__init__()
        self.model = None

    def run(self, document: Document) -> List[ImageWithEmbedding]:
        from inkwell import PageFragmentType
        from sentence_transformers import SentenceTransformer

        if self.model is None:
            self.model = SentenceTransformer("clip-ViT-B-32")

        embedding = []
        for page in document.pages:
            for fragment in page.page_fragments:
                if (
                    fragment.fragment_type == PageFragmentType.FIGURE
                    or fragment.fragment_type == PageFragmentType.TABLE
                ):
                    image = fragment.content.image
                    img_emb = self.model.encode(image)
                    embedding.append(
                        ImageWithEmbedding(
                            embedding=img_emb,
                            page_number=page.page_number,
                            figure_number=0,
                        )
                    )
        return embedding
