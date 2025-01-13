from common_objects import PDFParserDoclingOutput
from tensorlake.functions_sdk.data_objects import File
from tensorlake.functions_sdk.functions import TensorlakeCompute

from images import inkwell_image_gpu


class PDFParserDocling(TensorlakeCompute):
    name = "pdf-parse-docling"
    description = "Parser class that captures a pdf file"
    # Change to gpu_image to use GPU
    image = inkwell_image_gpu

    def __init__(self):
        super().__init__()

    def run(self, file: File) -> PDFParserDoclingOutput:
        from docling.datamodel.pipeline_options import PdfPipelineOptions
        IMAGE_RESOLUTION_SCALE = 2.0
        pipeline_options = PdfPipelineOptions()
        pipeline_options.images_scale = IMAGE_RESOLUTION_SCALE
        pipeline_options.generate_page_images = True

        from docling.document_converter import DocumentConverter, PdfFormatOption
        from docling.datamodel.base_models import InputFormat

        import tempfile
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".pdf") as f:
            f.write(file.data)
            converter = DocumentConverter(
                format_options={
                    InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
                }
            )
            result = converter.convert(f.name)

            texts = []
            for i in range(len(result.pages)):
                page_result = result.document.export_to_markdown(page_no=i+1)
                texts.append(page_result)

            images = []
            for element, _level in result.document.iterate_items():
                from docling_core.types.doc import ImageRefMode, PictureItem, TableItem
                if isinstance(element, PictureItem):
                    pil_image = element.get_image(result.document)

                    # Using docling APIs to avoid confusion.
                    b64 = element._image_to_base64(pil_image)
                    images.append(b64)

            return PDFParserDoclingOutput(texts=texts, images=images)
