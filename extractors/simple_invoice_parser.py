import sentence_transformer
import 
from transformers import pipeline
from pdf2image import convert_from_bytes
from transformers import DonutProcessor, VisionEncoderDecoderModel

from typing import List, Literal
from pydantic import BaseModel
from indexify_extractor_sdk import (
    Extractor,
    Feature,
    ExtractorSchema,
    EmbeddingSchema,
    Content,
)
import json
from indexify_extractor_sdk.base_embedding import (
    EmbeddingInputParams,
)
from indexify_extractor_sdk.sentence_transformer import SentenceTransformersEmbedding
from lingua import LanguageDetectorBuilder


class SimpleInvoiceParserInputParams(BaseModel):
    # No input except the file itself
    ...

class SimpleInvoiceParserExtractor(Extractor):
    def __init__(self):
        super().__init__()
        self.processor = DonutProcessor.from_pretrained("to-be/donut-base-finetuned-invoices")
        self.model = VisionEncoderDecoderModel.from_pretrained("to-be/donut-base-finetuned-invoices")
        # TODO: Is this for example how we would pick it up? Probably the model would still need to be defined by the user i.e. how it should be used
        device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model.to(device)

    def _query(image):
        # prepare encoder inputs
        pixel_values = processor(image, return_tensors="pt").pixel_values

        # prepare decoder inputs
        task_prompt = "<s_cord-v2>"
        decoder_input_ids = processor.tokenizer(task_prompt, add_special_tokens=False, return_tensors="pt").input_ids

        # generate answer
        outputs = model.generate(
            pixel_values.to(device),
            decoder_input_ids=decoder_input_ids.to(device),
            max_length=model.decoder.config.max_position_embeddings,
            early_stopping=True,
            pad_token_id=processor.tokenizer.pad_token_id,
            eos_token_id=processor.tokenizer.eos_token_id,
            use_cache=True,
            num_beams=1,
            bad_words_ids=[[processor.tokenizer.unk_token_id]],
            return_dict_in_generate=True,
        )

        # postprocess
        sequence = processor.batch_decode(outputs.sequences)[0]
        sequence = sequence.replace(processor.tokenizer.eos_token, "").replace(processor.tokenizer.pad_token, "")
        sequence = re.sub(r"<.*?>", "", sequence, count=1).strip()  # remove first task start token
        # img2.update(visible=False)
        return processor.token2json(sequence), image

    def extract(
        self, content: List[Content], params: LanguageExtractionInputParams
    ) -> List[List[Content]]:
        content_texts = [c.data.decode("utf-8") for c in content]
        out = []
        for i, x in enumerate(content):
            language = self._model.detect_language_of(content_texts[i])
            confidence = self._model.compute_language_confidence(content_texts[i], language)
            # TODO: Could be modified depending on the database we have
            data = {"language": language.name, "score": str(confidence)}
            out.append(
                [Content.from_text(
                    text=content_texts[i],
                    feature=Feature.metadata(value=data, name="language"),
                )]
            )
        return out

    def extract_query_embeddings(self, query: str) -> List[float]:
        # TODO: This will not be implemented for this extractor
        raise NotImplementedError

    def schemas(self) -> ExtractorSchema:
        """
        Returns a list of options for indexing.
        """
        input_params = LanguageExtractionInputParams()
        # TODO If it's metadata, how do we extract things
        # This extractor does not return any embedding, only a dictionary!
        return ExtractorSchema(
            embedding_schemas={},
            input_params=json.dumps(input_params.model_json_schema()),
        )

