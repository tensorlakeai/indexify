from typing import List

from indexify import indexify_function
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.indexify_functions import IndexifyFunction
from inkwell import Document, Page
from pydantic import BaseModel

from common_objects import TextChunk


class PDFParser(IndexifyFunction):
    name = "pdf-parse"
    description = "Parser class that captures a pdf file"

    def __init__(self):
        super().__init__()
        from inkwell import Pipeline

        self._pipeline = Pipeline()

    def run(self, input: File) -> Document:
        import tempfile

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".pdf") as f:
            f.write(input.data)
            document: Document = self._pipeline.process(f.name)
        return Document(pages=document.pages)


class Document(BaseModel):
    pages: List[Page]


@indexify_function()
def extract_chunks(document: Document) -> List[TextChunk]:
    """
    Extract chunks from document"""
    from inkwell import PageFragmentType
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
    chunks: List[TextChunk] = []
    for page in document.pages:
        page_text = ""
        for fragment in page.page_fragments:
            # Add the table or figure as a separate chunk, with the text extracted from OCR
            if (
                fragment.fragment_type == PageFragmentType.TABLE
                or fragment.fragment_type == PageFragmentType.FIGURE
            ):
                chunks.append(
                    TextChunk(chunk=fragment.content.text, page_number=page.page_number)
                )

            # Add all the text from the page to the page text, and chunk them later.
            elif fragment.fragment_type == PageFragmentType.TEXT:
                page_text += fragment.content.text

        texts = text_splitter.split_text(page_text)
        for text in texts:
            chunk = TextChunk(chunk=text, page_number=page.page_number)
            chunks.append(chunk)
    return chunks
