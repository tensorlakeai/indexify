from indexify import indexify_function, indexify_router, Graph
from pydantic import BaseModel
from typing import Dict, Union

class PDFFile(BaseModel):
    file: bytes
    service: str # Textract, Unstructured, Inkwell
    labels: Dict[str, str]

class Document(BaseModel):
    text: str
    doc_type: str

@indexify_function()
def process_file_with_textract(pdf: PDFFile) -> Document:
    return Document(...)

@indexify_function()
def process_file_with_inkwell(pdf: PDFFile) -> Document:
    return Document(...)

@indexify_router()
def ingest_pdf(pdf: PDFFile) -> Union[process_file_with_textract, process_file_with_inkwell]:
    if pdf.service == 'Textract':
        return process_file_with_textract
    elif pdf.service == 'Inkwell':
        return process_file_with_inkwell


@indexify_function()
def chunk_and_embed(Document) -> DocumentEmbeddings:
    return DocumentEmbeddings(...)

@indexify_function()
def write_to_db(DocumentEmbeddings) -> None:
    return None

g = Graph()
g.add_node(process_file_with_inkwell)
g.add_node(process_file_with_textract)
g.add_edges(ingest_pdf, [process_file_with_inkwell, process_file_with_textract])
g.add_edge(process_file_with_inkwell, chunk_and_embed)
g.add_edge(process_file_with_textract, chunk_and_embed)