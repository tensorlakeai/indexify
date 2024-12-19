from elasticsearch import Elasticsearch
from typing import Union
import base64
import uuid

from common_objects import ImageWithEmbedding, TextChunk
from indexify.functions_sdk.indexify_functions import IndexifyFunction


class ElasticSearchWriter(IndexifyFunction):
    name = "elastic_Search_writer"

    def __init__(self):
        super().__init__()
        # Connect to Elasticsearch
        self._client = Elasticsearch(
            hosts=["http://localhost:9200"],
            verify_certs=False,
            ssl_show_warn=False
            #basic_auth=("elastic", "your_password")
        )

        # Create indices if they don't exist
        self._create_indices_if_not_exists()

    def _create_indices_if_not_exists(self):
        # Text index mapping
        text_mapping = {
            "mappings": {
                "properties": {
                    "embedding": {
                        "type": "dense_vector",
                        "dims": 768,
                        "index": True,
                        "similarity": "cosine",
                        "index_options": {
                            "type": "hnsw",
                            "m": 16,
                            "ef_construction": 100
                        }
                    },
                    "page_number": {"type": "integer"},
                    "chunk": {"type": "text"}
                }
            }
        }

        # Image index mapping
        image_mapping = {
            "mappings": {
                "properties": {
                    "embedding": {
                        "type": "dense_vector",
                        "dims": 512,
                        "index": True,
                        "similarity": "cosine",
                        "index_options": {
                            "type": "hnsw",
                            "m": 16,
                            "ef_construction": 100
                        }
                    },
                    "page_number": {"type": "integer"},
                    "image_data": {"type": "binary"}
                }
            }
        }

        if not self._client.indices.exists(index="text_embeddings"):
            self._client.indices.create(index="text_embeddings", body=text_mapping)

        if not self._client.indices.exists(index="image_embeddings"):
            self._client.indices.create(index="image_embeddings", body=image_mapping)

    def run(self, input: Union[ImageWithEmbedding, TextChunk]) -> bool:
        try:
            if isinstance(input, ImageWithEmbedding):
                # Convert image bytes to base64 for storage
                image_base64 = base64.b64encode(input.image_bytes).decode('utf-8')

                document = {
                    "embedding": input.embedding,
                    "page_number": input.page_number,
                    "image_data": image_base64
                }

                self._client.index(
                    index="image_embeddings",
                    id=str(uuid.uuid4()),
                    document=document
                )

            elif isinstance(input, TextChunk):
                document = {
                    "embedding": input.embeddings,
                    "page_number": input.page_number,
                    "chunk": input.chunk
                }

                self._client.index(
                    index="text_embeddings",
                    id=str(uuid.uuid4()),
                    document=document
                )

            return True

        except Exception as e:
            print(f"Error indexing document: {str(e)}")
            return False