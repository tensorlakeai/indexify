Image Retrieval based on natural language is typically done in the following manner today -

1. Embed images with CLIP, embed the query using the same model and do KNN search to retrieve semantically similar images.
2. Describe an image using a Visual LLM such as LLava and GPT-V, embed the description and retrieve images by searching the descriptions.

While these work, semantic search on descriptions or CLIP based algorithms retrieve semantically similar images so they can be less accurate. In addition to that, running these models are expensive. To make retrieval more accurate and cheaper, Indexify in addition to **supporting CLIP and VLLM based extractors**, also supports **SQL based querying of images which are far more cheaper and accurate**. Indexify automatically exposes structured data extracted by object detection and tracking models with a SQL interface.

For SQL based retrieval, you will -

1. Run object detection extractors powered by efficient models like YoloV9 or Grounding Dino(if you want prompt based extraction).
2. You will make SQL queries with predicates to find relevant images from your applications.

In this tutorial we will show you how to do all three image retrieval. We will upload some images of New York City, and query them with Natural Language to find images with skateboards.

### Download Indexify
Download and start Indexify!
```
curl https://getindexify.ai | sh
indexify server -d 
```

### Download and Run Yolo Extractor
=== "Shell"

    ```shell
    indexify-extractor download tensorlake/yolo-extractor
    indexify-extractor join-server
    ```

=== "Docker"

    ```shell
    docker run -d -v /tmp/indexify-blob-storage:/tmp/indexify-blob-storage -p 9500:9500 tensorlake/yolo-extractor join-server --coordinator-addr=host.docker.internal:8950 --ingestion-addr=host.docker.internal:8900 --advertise-addr=0.0.0.0:9500 --workers=1
    ```


### (Optional) How to test Yolo Extractor Locally
Load Yolo Extractor in a notebook or terminal
```python
from indexify_extractor_sdk import load_extractor, Content
extractor, config_cls = load_extractor("indexify_extractors.yolo.yolo_extractor:YoloExtractor")
content = Content.from_file("/path/to/file.jpg")
results =  extractor.extract(content)
print(results)
```

### Create an Extraction Policy
```
extraction_graph_spec = """
name: 'imageknowledgebase'
extraction_policies:
   - extractor: 'tensorlake/yolo-extractor'
     name: 'object_detection'
"""
extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
```

### Upload Files
Let's add some files stored remotely, which we can use for the rest of the tutorial.
```
file_names=["skate.jpg", "congestion.jpg", "bushwick-bred.jpg", "141900.jpg", "132500.jpg", "123801.jpg","120701.jpg", "103701.jpg"]
file_urls = [f"https://extractor-files.diptanu-6d5.workers.dev/images/{file_name}" for file_name in file_names]
for file_url in file_urls:
    client.ingest_remote_file(file_url, "image/png", {})
```
If you have local files, you can upload them by -
```
client.upload_file("yourextractiongraphname", path="../path/to/file")
```


## SQL Based Retrieval 
### Search using SQL
Lets find a image with a skateboard! 
```
result = client.sql_query("select * from ingestion where object_name='skateboard';")
```

### Make OpenAI find the images using Langchain! 
We can make OpenAI generate the SQL query based on a language -
```
chain.invoke("Find the photos with a skateboard?")
```
## Semantic Search with CLIP Embeddings
OpenAI's CLIP embedding model allows searching images with semantically similar description of images. 

### Download and start the Clip Embedding Extractor

=== "Shell"

    ```bash
    indexify-extractor download hub://embedding/clip_embedding
    indexify-extractor join-server
    ```
=== "Docker"

    ```shell
    docker run -d -v /tmp/indexify-blob-storage:/tmp/indexify-blob-storage -p 9501:9501 tensorlake/clip-extractor join-server --coordinator-addr=host.docker.internal:8950 --ingestion-addr=host.docker.internal:8900 --advertise-addr=0.0.0.0:9501 --listen-port=9501
    ```


### Create an Extraction Graph 
```
extraction_graph_spec = """
name: 'imageknowledgebase'
extraction_policies:
   - extractor: 'tensorlake/clip-extractor'
     name: 'clip_embedding'
"""
extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
```
This creates an embedding index `imageknowledgebase.clip_embedding.embedding`. You can also find the index name via the following API - 

```
client.indexes()
```

### Upload Images
Upload some images or search on the images which were already uploaded

### Search
```
client.search_index(name="imageknowledgebase.clip_embedding.embedding", query="skateboard", top_k=2)
```
