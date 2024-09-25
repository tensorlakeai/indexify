import os
from hashlib import sha256
from typing import List, Optional


class CacheAwareFunctionWrapper:
    def __init__(self, cache_dir: str):
        self._cache_dir = cache_dir
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

    def _get_key(self, input: bytes) -> str:
        h = sha256()
        h.update(input)
        return h.hexdigest()

    def get(self, graph: str, node_name: str, input: bytes) -> Optional[List[bytes]]:
        key = self._get_key(input)
        dir_path = os.path.join(self._cache_dir, graph, node_name, key)
        if not os.path.exists(dir_path):
            return None

        files = os.listdir(dir_path)
        outputs = []
        for file in files:
            with open(os.path.join(dir_path, file), "rb") as f:
                return f.read()

        return outputs

    def set(
        self,
        graph: str,
        node_name: str,
        input: bytes,
        output: List[bytes],
    ):
        key = self._get_key(input)
        dir_path = os.path.join(self._cache_dir, graph, node_name, key)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        for i, output_item in enumerate(output):
            file_path = os.path.join(dir_path, f"{i}.cbor")
            with open(file_path, "wb") as f:
                f.write(output_item)
