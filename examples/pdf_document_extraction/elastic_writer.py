from elastic_transport import ApiError
from elasticsearch import Elasticsearch
from typing import Union
import base64
import uuid

from common_objects import ImageWithEmbedding, TextChunk
from tensorlake.functions_sdk.functions import TensorlakeCompute

from images import st_image


class ElasticSearchWriter(TensorlakeCompute):
    name = "elastic_search_writer"
    image = st_image

    def __init__(self):
        super().__init__()
        # Connect to Elasticsearch
        self._client = Elasticsearch(
            hosts=["http://elasticsearch:9200"],  # <User Change>: default is service name in the docker compose file.
            verify_certs=False,
            ssl_show_warn=False,
            #basic_auth=("elastic", "your_password"),
            retry_on_timeout=True,
            max_retries=3,
            request_timeout=5,
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
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
            },
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
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
            },
        }

        try:
            self._client.indices.create(index="text_embeddings", body=text_mapping)
        except ApiError as e:
            if e.status_code == 400 and "resource_already_exists_exception" in str(e):
                print("Text index already exists. Continuing.")
            else:
                raise e

        try:
            self._client.indices.create(index="image_embeddings", body=image_mapping)
        except ApiError as e:
            if e.status_code == 400 and "resource_already_exists_exception" in str(e):
                print("Image index already exists. Continuing.")
            else:
                raise e

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
