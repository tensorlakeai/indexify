from indexify_extractor_sdk import Content
from io import BytesIO

def load_pdf_as_content(file: str) -> Content:
    with open("extractors_tests/data/test.pdf", "rb") as fh:
        b = fh.read()
    return Content(id=file, content_type="pdf", data=b)