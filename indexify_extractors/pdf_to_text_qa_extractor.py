from dataclasses import dataclass
from typing import List, Any, Union, Optional
from decimal import Decimal 
from enum import Enum
import json
from typing import Optional
from .base_extractor import Extractor, ExtractorInfo, Content, ExtractedAttributes, ExtractedEmbedding
from pydantic import BaseModel
from transformers import pipeline, AutoTokenizer, AutoModelForQuestionAnswering
from pdf2image import convert_from_path

# For PDF extraction
import PyPDF2

# For image extraction
from PIL import Image
import pytesseract

class EntityExtractionInputParams(BaseModel):
    question: str

class EntityExtractor:
    def __init__(self):
        # Using the question answering pipeline from transformers
        self._model = pipeline("question-answering")

    """
        Helper function, that reads text from well-formatted PDFs
    """
    def _pdf_to_text(self, pdf_path: str) -> str:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfFileReader(file)
            text = ""
            for page_num in range(reader.numPages):
                page = reader.getPage(page_num)
                text += page.extractText()
        return text
    
    """
        Redundant helper function, that uses OCR to turn a PDF into a picture, and then apply OCR to extract text. 
        It adds even more redundancy by rotating the PDF by 90 degrees each time. 
    """
    def _pdf_ocr_to_text(self, pdf_path: str) -> str:
        # Convert PDF to images
        images = convert_from_path(pdf_path)
        text = ""
        for image in images:
            for i in range(3):
                rotated_image = image.rotate(90 * i)
                text += pytesseract.image_to_string(rotated_image)
        return text

    def _image_to_text(self, image_path: str) -> str:
        return pytesseract.image_to_string(Image.open(image_path))

    def extract(self, content_path: str, params: dict[str, Any]) -> List[Union[ExtractedEmbedding, ExtractedAttributes]]:
        # Determine the type of the content and convert to text
        content_text = {}
        if content_path.endswith('.pdf'):
            content_text['pdf_to_text'] = self._pdf_to_text(content_path)
            content_text['pdf_ocr_to_text'] = self._pdf_ocr_to_text(content_path)
        elif content_path.endswith(('.png', '.jpg', '.jpeg')):
            content_text['image_to_text'] = self._image_to_text(content_path)
        else:
            raise ValueError("Unsupported file format")

        extracted_data = []
        for context in content_text.values():
            answer = self._model({
                'question': params['question'],
                'context': context
            })
            # Assuming answer['embedding'] and answer['answer'] are defined in self._model
            extracted_data.append(ExtractedEmbedding(content_id=content_path, text=context, embeddings=answer['embedding']))
            extracted_data.append(ExtractedAttributes(content_id=content_path, json=json.dumps({'answer': answer['answer']})))

        return extracted_data

    def info(self) -> ExtractorInfo:
        schema = {"answer": "string"}
        schema_json = json.dumps(schema)
        return ExtractorInfo(
            name="EntityExtractor",
            description="EntityExtractor",
            output_datatype="answer",
            input_params=json.dumps(EntityExtractionInputParams.schema_json()),
            output_schema=schema_json,
        )