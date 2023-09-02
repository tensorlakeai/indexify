from transformers import AutoTokenizer, AutoModel
import torch
import torch.nn.functional as F

from typing import List


def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[
        0
    ]  # First element of model_output contains all token embeddings
    input_mask_expanded = (
        attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    )
    return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(
        input_mask_expanded.sum(1), min=1e-9
    )


class SentenceTransformersEmbedding:
    def __init__(self, model_name) -> None:
        self._model_name = model_name
        self._tokenizer = AutoTokenizer.from_pretrained(
            f"sentence-transformers/{model_name}"
        )
        self._model = AutoModel.from_pretrained(f"sentence-transformers/{model_name}")

    def embed_ctx(self, inputs: List[str]) -> List[List[float]]:
        result = self._embed(inputs)
        return result.tolist()

    def embed_query(self, query: str) -> List[float]:
        result = self._embed([query])
        return result[0].tolist()

    def _embed(self, inputs: List[str]) -> torch.Tensor:
        encoded_input = self._tokenizer(
            inputs, padding=True, truncation=True, return_tensors="pt"
        )
        with torch.no_grad():
            model_output = self._model(**encoded_input)
        sentence_embeddings = mean_pooling(
            model_output, encoded_input["attention_mask"]
        )
        return F.normalize(sentence_embeddings, p=2, dim=1)

    def tokenizer_encode(self, inputs: List[str]) -> List[List[int]]:
        return self._tokenizer.batch_encode_plus(inputs)["input_ids"]

    def tokenizer_decode(self, tokens: List[List[int]]) -> List[str]:
        return self._tokenizer.batch_decode(tokens, skip_special_tokens=True)

    def tokenize(self, inputs: List[str]) -> List[List[str]]:
        result = []
        for input in inputs:
            result.extend(self._tokenize(input))
        return result

    def _tokenize(self, input: str) -> List[str]:
        max_length = self._tokenizer.model_max_length
        chunks = []
        chunk = []
        chunk_length = 0
        words = input.split(" ")

        for word in words:
            tokens = self._tokenizer.tokenize(word)
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
