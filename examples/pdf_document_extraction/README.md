# PDF Document Extraction and Indexing

The example builds a pipeline that extracts text, tables and figures from a PDF Document. It embeds the text, table and images from the document and writes them into ChromaDB.
This example uses open-source components such Docling for document parsing and ElasticSearch as the vector store, to make it easy to deploy in commercial settings

The pipeline is hosted on a server endpoint in one of the containers. The endpoint can be called from any Python application.

## Tools and Libraries Used 
* **Orchestration**: Indexify
* **Docling**: PDF Parsing
* **Langchain**: Text splitting
* **SentenceTransformers**: Text and Image Embedding
* **VectorDB**: Elastic Search

## Start the Server Endpoint

```bash
docker compose up
```

## Deploy the Graph
The [Graph](workflow.py) has all the code which performs PDF Parsing, embedding and writing the VectorDB. We will deploy the Graph on the server to create an Endpoint for our workflow. 
Make sure to deploy the right graph before running the example.

```bash
pip install indexify tensorlake
```

```bash
python workflow.py
```

This stage deploys the workflow on the server. At this point, you can also open the [UI](http://localhost:8900) to see the deployed Graph.

After this, you can call the endpoint with PDFs to make Indexify start parsing documents.

## Calling the Endpoint 

```python
from indexify import RemoteGraph

graph = RemoteGraph.by_name("Extract_pages_tables_images_pdf")
# Pass in a publicly avaialable PDF from the internet 
invocation_id = graph.run(block_until_done=True, url="https://arxiv.org/pdf/2501.12948")
```

## Outputs 
You can read the output of every function of the Graph. For example, to get the chunks from the documents - 

```python
chunks = graph.output(invocation_id, "chunk_text")
```

## Vector Search

Once the documents are processed, you can query Elastic Search for vector search. Here is some [same code for that](https://github.com/tensorlakeai/indexify/blob/main/examples/pdf_document_extraction/es_retreive.py)

The indexes created in Elastic Search are `text_embeddings` and `image_embeddings`. The port `9200` is forwarded to the host from the container, so you can query the indexes.


## Customization

Copy the folder, modify the code as you like and simply upload the new Graph.

```bash
python workflow.py
```

#### Using Chroma DB 

The ChromaDB tables are populated automatically by the [ChromaDBWriter](https://github.com/tensorlakeai/indexify/blob/main/examples/pdf_document_extraction/chromadb_writer.py) class.
The name of the databases used in the example are `text_embeddings` and `image_embeddings`. The database running inside the container at port `8000` is forwarded to the host for convenience. 

There code to retrieve from ChromaDB is [here](https://github.com/tensorlakeai/indexify/blob/main/examples/pdf_document_extraction/chromadb_retreive.py)

## Using GPU

You have to make a couple of changes to use GPUs for PDF parsing.
1. Uncomment the lines in the `pdf-parser-executor` block which mention uncommenting them would enable GPUs.
2. Use the `gpu_image` in the `PDFParser`, `extract_chunks` and `extract_images` class/functions so that the workflow routes the PDFParser into the GPU enabled image.
