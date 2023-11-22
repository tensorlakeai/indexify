from io import BytesIO
import json
import re
import requests
import timeit
from typing import List, Literal

from PIL import Image
from pdf2image import convert_from_bytes
from pydantic import BaseModel
import pytesseract
import torch
from transformers import DonutProcessor, VisionEncoderDecoderModel

from indexify_extractor_sdk import (
    Extractor,
    Feature,
    ExtractorSchema,
    Content,
)


class SimpleInvoiceParserInputParams(BaseModel):
    # No input except the file itself
    ...

class SimpleInvoiceParserExtractor(Extractor):
    def __init__(self):
        super().__init__()
        self.processor = DonutProcessor.from_pretrained("to-be/donut-base-finetuned-invoices")
        self.model = VisionEncoderDecoderModel.from_pretrained("to-be/donut-base-finetuned-invoices")
        # TODO: Is this for example how we would pick it up? Probably the model would still need to be defined by the user i.e. how it should be used
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model.to(self.device)

    def _process_document(self, image):
        # prepare encoder inputs
        pixel_values = self.processor(image, return_tensors="pt").pixel_values

        # prepare decoder inputs
        task_prompt = "<s_cord-v2>"
        decoder_input_ids = self.processor.tokenizer(task_prompt, add_special_tokens=False, return_tensors="pt").input_ids

        # generate answer
        outputs = self.model.generate(
            pixel_values.to(self.device),
            decoder_input_ids=decoder_input_ids.to(self.device),
            max_length=self.model.decoder.config.max_position_embeddings,
            early_stopping=True,
            pad_token_id=self.processor.tokenizer.pad_token_id,
            eos_token_id=self.processor.tokenizer.eos_token_id,
            use_cache=True,
            num_beams=1,
            bad_words_ids=[[self.processor.tokenizer.unk_token_id]],
            return_dict_in_generate=True,
        )

        # postprocess
        sequence = self.processor.batch_decode(outputs.sequences)[0]
        sequence = sequence.replace(self.processor.tokenizer.eos_token, "").replace(self.processor.tokenizer.pad_token, "")
        sequence = re.sub(r"<.*?>", "", sequence, count=1).strip()  # remove first task start token
        return self.processor.token2json(sequence), image

    def extract(
        self, content: List[Content], params: SimpleInvoiceParserInputParams
    ) -> List[List[Content]]:
        content_filebytes = [c.data for c in content]

        # TODO: Right now it only looks at the first image! We should probably flatten it and do it for each page!
        images = [convert_from_bytes(x)[0].convert("RGB") for x in content_filebytes]

        out = []
        for i, x in enumerate(content):
            print("i, x are: ", i, x)
            data = self._process_document(images[i])[0]  # Key 1 includes the image, which we ignore in this case
            out.append(
                [Content.from_text(
                    text="",  # TODO: Diptanu, what do we do for PDFs? Do you want to save the raw bytes too, I feel like this is unnecessary? Also, I felt like these would be stored in a database _before_ processing, not after
                    feature=Feature.metadata(value=data, name="invoice_simple_donut"),
                )]
            )
        return out

    def schemas(self) -> ExtractorSchema:
        """
        Returns a list of options for indexing.
        """
        input_params = SimpleInvoiceParserExtractor()
        # TODO If it's metadata, how do we extract things
        # This extractor does not return any embedding, only a dictionary!
        return ExtractorSchema(
            embedding_schemas={},
            input_params=json.dumps(input_params.model_json_schema()),
        )

