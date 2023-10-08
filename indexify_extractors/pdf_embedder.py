from .base_extractor import ExtractorInfo, Content, Embeddings, Extractor, EmbeddingSchema
from .sentence_transformer import SentenceTransformersEmbedding
from pypdf import PdfReader
from langchain import text_splitter
import json
from io import BytesIO

from typing import List, Any, Union
from pydantic import BaseModel

class PDFEmbeddingInputParams(BaseModel):
    ...

class PDFEmbedder(Extractor):

    def __init__(self, max_context_length: int = 128):
        self._model_context_length: int = max_context_length
        self._model = SentenceTransformersEmbedding(model_name="all-MiniLM-L6-v2")

    def extract(
        self, content: List[Content], params: dict[str, str]
    ) -> List[Embeddings]:
        splitter = text_splitter.RecursiveCharacterTextSplitter()
        embeddings_list = []
        for c in content:
            pdf_file = BytesIO(c.data)
            doc = PdfReader(pdf_file)
            for (i, page) in enumerate(doc.pages):
                text = page.extract_text()
                chunks = splitter.split_text(text)
                embeddings = self._model.embed_ctx(chunks)
                for (i, (chunk, embeddings)) in enumerate(zip(chunks, embeddings)):
                    embeddings_list.append(Embeddings(content_id=c.id, text=chunk, embeddings=embeddings, metadata=json.dumps({"page": i})))
        return embeddings_list
    
    def extract_query_embeddings(self, query: str) -> List[float]:
        return self._model.embed_query(query)

    def info(self) -> ExtractorInfo:
        input_params = PDFEmbeddingInputParams()
        return ExtractorInfo(
            name="PDFEmbedder",
            description="Extractor to embed texts from PDF using SentenceTransformer MiniLM-L6-v2 model.",
            input_params=input_params,
            output_schema=EmbeddingSchema(distance="cosine", dim=384),
        )
 