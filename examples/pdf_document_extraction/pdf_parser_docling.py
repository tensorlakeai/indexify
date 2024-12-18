from examples.pdf_document_extraction.common_objects import PDFParserDoclingOutput
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.indexify_functions import IndexifyFunction

from images import inkwell_image_gpu


class PDFParserDocling(IndexifyFunction):
    name = "pdf-parse-docling"
    description = "Parser class that captures a pdf file"
    # Change to gpu_image to use GPU
    image = inkwell_image_gpu

    def __init__(self):
        super().__init__()

    def run(self, file: File) -> PDFParserDoclingOutput:
        from docling.document_converter import DocumentConverter

        import tempfile
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".pdf") as f:
            f.write(file.data)
            converter = DocumentConverter()
            result = converter.convert(f.name)

            texts = []
            for i in range(len(result.pages)):
                page_result = result.document.export_to_markdown(page_no=i+1)
                texts.append(page_result)

            images = []
            for img in result.document.pictures:
                pil_image = img.get_image(result.document)
                # Using docling APIs to avoid confusion.
                b64 = img._image_to_base64(pil_image)
                images.append(b64)

            return PDFParserDoclingOutput(text=texts, images=images)