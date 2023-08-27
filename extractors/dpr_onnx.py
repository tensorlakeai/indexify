import torch
from optimum.onnxruntime import ORTModelForFeatureExtraction
import torch.nn.functional as F
from transformers import AutoTokenizer, Pipeline
from pathlib import Path
from typing import List


def cls_pooling(model_output, attention_mask):
    return model_output[0][:, 0]


def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[
        0
    ]  # First element of model_output contains all token embeddings
    input_mask_expanded = (
        attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    )
    sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, 1)
    sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)
    return sum_embeddings / sum_mask

class SentenceEmbeddingPipeline(Pipeline):
    def _sanitize_parameters(self, **kwargs):
        # we don't have any hyperameters to sanitize
        preprocess_kwargs = {}
        return preprocess_kwargs, {}, {}

    def preprocess(self, inputs):
        encoded_inputs = self.tokenizer(
            inputs, padding=True, truncation=True, return_tensors="pt"
        )
        return encoded_inputs

    def _forward(self, model_inputs):
        outputs = self.model(**model_inputs)
        return {"outputs": outputs, "attention_mask": model_inputs["attention_mask"]}

    def postprocess(self, model_outputs):
        # Perform pooling
        sentence_embeddings = mean_pooling(
            model_outputs["outputs"], model_outputs["attention_mask"]
        )
        # Normalize embeddings
        sentence_embeddings = F.normalize(sentence_embeddings, p=2, dim=1)
        return sentence_embeddings.squeeze().tolist()



class OnnxDPR:
    def __init__(
        self,
        ctx_model: str = "sentence-transformers/facebook-dpr-ctx_encoder-multiset-base",
        ques_model: str = "sentence-transformers/facebook-dpr-question_encoder-multiset-base",
    ):
        self._ctx_model = ORTModelForFeatureExtraction.from_pretrained(ctx_model, export=True)
        self._ques_model = ORTModelForFeatureExtraction.from_pretrained(ques_model, export=True)
        self._tokenizer_ctx_model = AutoTokenizer.from_pretrained(ctx_model)
        self._tokenizer_ques_model = AutoTokenizer.from_pretrained(ques_model)
        self._ctx_pipeline = SentenceEmbeddingPipeline(self._ctx_model, self._tokenizer_ctx_model)
        self._ques_pipeline = SentenceEmbeddingPipeline(self._ques_model, self._tokenizer_ques_model)

    def generate_embeddings_ctx(self, inputs: List[str]):
        embeddings = self._ctx_pipeline(inputs)
        return embeddings

    def generate_embeddings_query(self, query: str):
        embeddings = self._ques_pipeline(query)
        return embeddings
    


## TODO Move this to proper unit tests later
if __name__ == "__main__":
    dpr = OnnxDPR()
    print(dpr.generate_embeddings_ctx(["What is the capital of England?"]))
    print(dpr.generate_embeddings_query("What is the capital of England?"))


