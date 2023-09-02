from transformers import AutoTokenizer, AutoModel
import torch
import torch.nn.functional as F

from typing import List


def cls_pooling(model_output, attention_mask):
    return model_output[0][:, 0]


class DPREmbeddings:
    def __init__(
        self,
        ctx_model_name="facebook-dpr-ctx_encoder-multiset-base",
        query_model_name="facebook-dpr-question_encoder-multiset-base",
    ) -> None:
        self._ctx_tokenizer = AutoTokenizer.from_pretrained(
            f"sentence-transformers/{ctx_model_name}"
        )
        self._ctx_model = AutoModel.from_pretrained(
            f"sentence-transformers/{ctx_model_name}"
        )
        self._query_tokenizer = AutoTokenizer.from_pretrained(
            f"sentence-transformers/{query_model_name}"
        )
        self._query_model = AutoModel.from_pretrained(
            f"sentence-transformers/{query_model_name}"
        )

    def embed_ctx(self, inputs: List[str]) -> List[List[float]]:
        encoded_input = self._ctx_tokenizer(
            inputs, padding=True, truncation=True, return_tensors="pt"
        )
        with torch.no_grad():
            model_output = self._ctx_model(**encoded_input)
        sentence_embeddings = cls_pooling(model_output, encoded_input["attention_mask"])
        return F.normalize(sentence_embeddings, p=2, dim=1).tolist()

    def embed_query(self, input: str) -> torch.Tensor:
        encoded_input = self._query_tokenizer(
            input, padding=True, truncation=True, return_tensors="pt"
        )
        with torch.no_grad():
            model_output = self._query_model(**encoded_input)
        sentence_embeddings = cls_pooling(model_output, encoded_input["attention_mask"])
        return F.normalize(sentence_embeddings, p=2, dim=1)[0].tolist()

    def tokenizer_encode(self, inputs: List[str]) -> List[List[int]]:
        return self._ctx_tokenizer.batch_encode_plus(inputs)["input_ids"]

    def tokenizer_decode(self, tokens: List[List[int]]) -> List[str]:
        return self._ctx_tokenizer.batch_decode(tokens, skip_special_tokens=True)

    def tokenize(self, inputs: List[str]) -> List[List[str]]:
        result = []
        for input in inputs:
            result.extend(self._tokenize(input))
        return result

    def _tokenize(self, input: str) -> List[str]:
        max_length = self._ctx_tokenizer.model_max_length
        chunks = []
        chunk = []
        chunk_length = 0
        words = input.split(" ")

        for word in words:
            tokens = self._ctx_tokenizer.tokenize(word)
            token_length = len(tokens)

            if chunk_length + token_length <= max_length:
                chunk.append(word)
                chunk_length += token_length
            else:
                chunks.append(" ".join(chunk))
                chunk = [word]
                chunk_length = token_length

        chunks.append(" ".join(chunk))
        return chunks
