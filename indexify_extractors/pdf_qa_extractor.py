from transformers import LayoutLMv2ForQuestionAnswering, LayoutLMv2Tokenizer
from PIL import Image
import torch
import json 
from typing import List, Any, Union
from .base_extractor import Extractor, ExtractorInfo, Content, ExtractedAttributes, ExtractedEmbedding
from pydantic import BaseModel

class QuestionAnsweringInputParams(BaseModel):
    question: str

class InvoiceQA(Extractor):
    def __init__(self):
        self.model_name = "microsoft/layoutlmv2-base-uncased"
        self.tokenizer = LayoutLMv2Tokenizer.from_pretrained(self.model_name)
        self.model = LayoutLMv2ForQuestionAnswering.from_pretrained(self.model_name)

    def answer_question(self, question: str, image_path: str):
        image = Image.open(image_path)
        encoding = self.tokenizer(question, images=image, padding=True, truncation=True, return_tensors="pt")
        input_ids = encoding["input_ids"]
        attention_mask = encoding["attention_mask"]
        token_type_ids = encoding["token_type_ids"]

        outputs = self.model(input_ids=input_ids, attention_mask=attention_mask, token_type_ids=token_type_ids)
        start_logits = outputs.start_logits
        end_logits = outputs.end_logits

        all_tokens = self.tokenizer.convert_ids_to_tokens(input_ids[0].tolist())
        answer = ' '.join(all_tokens[torch.argmax(start_logits) : torch.argmax(end_logits)+1])
        return answer

    def extract(self, content: List[Content], params: dict[str, Any]) -> List[Union[ExtractedEmbedding, ExtractedAttributes]]:
        extracted_data = []
        for c in content:
            answer = self.answer_question(params['question'], c.data)
            extracted_data.append(ExtractedAttributes(content_id=c.id, json=json.dumps({'answer': answer})))
        return extracted_data

    def info(self) -> ExtractorInfo:
        schema = {"answer": "string"}
        schema_json = json.dumps(schema)
        return ExtractorInfo(
            name="InvoiceQA",
            description="Question Answering for Invoices",
            output_datatype="attributes",
            input_params=json.dumps(QuestionAnsweringInputParams.schema_json()),
            output_schema=schema_json,
        )