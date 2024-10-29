# PDF Document Extraction and Indexing

The example builds a pipeline that extracts text, tables and figures from a PDF Document. It embeds the text, table and images from the document and writes them into LanceDB.

The pipeline is hosted on a server endpoint in one of the containers. The endpoint can be called from any Python application.

## Start the Server Endpoint

```bash
docker compose up
```

## Deploy the Graph
The [Graph](workflow.py) has all the code which performs PDF Parsing, embedding and writing the VectorDB. We will deploy the Graph on the server to create an Endpoint for our workflow. 
```bash
pip install indexify
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
invocation_id = graph.run(block_until_done=True, url="")
```

## Outputs 
You can read the output of every function of the Graph.

```python
chunks = graph.output(invocation_id, "chunk_text")
```

The lancedb table is populated automatically by the `LanceDBWriter` class.
The name of the database used in the example is `vectordb.lance`. Its created in the folder where the docker compose lives, and bind-mounted into the container for LanceDB to write to it.
You can change the code to make it write anywhere you want.

## Customization

Copy the folder, modify the code as you like and simply upload the new Graph.

```bash
python workflow.py
```

## Using GPU

You have to make a couple of changes to use GPUs for PDF parsing.
1. Uncomment the lines in the `pdf-parser-executor` block which mention uncommenting them would enable GPUs.
2. Use the `gpu_image` in the `PDFParser`, `extract_chunks` and `extract_images` class/functions so that the workflow routes the PDFParser into the GPU enabled image.
