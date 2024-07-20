# Indexify: Effortless Ingestion and Extraction for AI Applications at Any Scale

![Indexify High Level](images/Indexify_KAT.gif)

Indexify is a powerful data framework designed for building ingestion and extraction pipelines for unstructured data. It provides a declarative approach to define pipelines that can perform structured extraction using any AI model or transform ingested data. These pipelines begin processing immediately upon data ingestion, making them ideal for interactive applications and low-latency use cases.

## Key Concepts

Indexify has the following core concepts:

* **Extractors**: Functions that take data from upstream sources and output transformed data, embeddings, or structured data.
* **Extraction Graphs**: Multi-step workflows created by chaining extractors.
* **Namespaces**: Logical abstractions for storing related content, allowing data partitioning based on security and organizational boundaries.
* **Content**: Representation of raw unstructured data (documents, video, images).
* **Vector Indexes**: Automatically created from extractors that return embeddings, enabling semantic search capabilities.
* **Structured Data Tables**: Metadata extracted from content, exposed via SQL queries.

While that looks like a handful the core concept is effectively simple. Indexify provides you with tools to build or use Extractors.

Extractors consume `Content` which contains raw bytes of unstructured data, and they produce a list of Content and features from them.

![Image 4: Extractor_working](https://github.com/user-attachments/assets/ac12fc76-3043-485f-9a8b-6bbffa7d878d)

## How It Works

Indexify empowers you to make the most out of your data by doing accurate, and effortless ingestion and extraction of data at scale. This is done in a 3-step process.

Whether you have one source or 100s of sources, this 3-step process stays the same.

### Step 1: Setup Ingestion Pipelines

Indexify uses a declarative configuration approach. Here's an example of how to set up a pipeline:

```yaml
name: 'pdf-ingestion-pipeline'
extraction_policies:
- extractor: 'tensorlake/markdown'
  name: 'pdf_to_markdown'
- extractor: 'tensorlake/ner'
  name: 'entity_extractor'
  content_source: 'pdf_to_markdown'
- extractor: 'tensorlake/minilm-l6'
  name: 'embedding'
  content_source: 'pdf_to_markdown'
```

1. Extractors are referenced in pipelines and can be customized or created from scratch.
2. Extraction policies are linked using the `content_source` attribute.
3. Indexify provides ready-to-use extractors, and custom extractors can be easily created.

### Step 2: Upload Data

```python
client = IndexifyClient()

files = ["file1.pdf", .... "file100.pdf"]
for file in files:
  client.upload_file("pdf-ingestion-pipeline", file)
```

### Step 3: Retrieve Data

Retrieve extracted data from extractors:

```python
content_ids = [content.id for content in client.list_content("pdf-ingestion-pipeline")]

markdown = client.get_extracted_content(content_ids[0], "pdf-ingestion-pipeline", "pdf_to_markdown")
named_entities = client.get_extracted_content(content_ids[0], "pdf-ingestion-pipeline", "entity_extractor")
```

Search using embeddings:

```python
results = client.search("pdf-ingestion-pipeline.embedding.embedding","Who won the 2017 NBA finals?", k=3)
```

To understand usage of indexify for various use cases we recommend the following starting points.

## Indexify Learning Path 

| Guide | What You'll Build | Key Concepts Covered |
|-------|-------------------|----------------------|
| [Getting Started - Basic](https://docs.getindexify.ai/getting_started/) | A Wikipedia information retrieval system | - Indexify Server setup<br>- Extraction Graphs<br>- Basic Extractors<br>- Data Ingestion<br>- Vector Search |
| [Getting Started - Intermediate](https://docs.getindexify.ai/getting_started_intermediate/) | A tax document processing and Q&A system | - PDF processing<br>- Custom Extractors<br>- Structured Data Extraction<br>- Advanced Querying |
| [Multi-Modal RAG on PDF](https://docs.getindexify.ai/example_code/pdf/indexing_and_rag) | A video content analysis and retrieval system | - Multi-modal data processing<br>- Video frame extraction<br>- Speech-to-text conversion<br>- Cross-modal retrieval |


## Features of Indexify

* Multi-Modal: Process PDFs, Videos, Images, and Audio
* Highly Available and Fault Tolerant: Designed for production-grade reliability
* Local Experience: Runs locally without dependencies
* Real-Time Extraction: Keeps indexes automatically updated
* Incremental Extraction and Selective Deletion
* Multi-Tenant with Namespace Isolation
* Compatible with Various LLM Frameworks (Langchain, DSPy, etc.)
* Scalable from Laptop to Cloud
* Works with Multiple Blob Stores, Vector Stores, and Structured Databases
* Open-Sourced Automation for Kubernetes Deployment


## Building with Indexify

Let us look closely at how to build with Indexify using the 3-step process above. We will start by defining how our Project Directory should look and then move to creating the necessary files. 

??? note "Warning: Template Code"

    The code presented here is simply a template code, for the actual code example, start [here](https://docs.getindexify.ai/getting_started/).


### Project structure

Here's a sample directory structure for a project using Indexify:

```
indexify-tutorial/
│
├── venv/                  # Virtual environment (created by python3 -m venv venv)
│
├── graph.yaml             # Extraction graph definition
├── setup.py               # Script to create the extraction graph
├── ingest.py              # Script to ingest Wikipedia data
├── query.py               # Script to query the indexed data
│
└── indexify               # Indexify server executable (downloaded by curl command)
```

This structure keeps all the components of our tutorial project organized in one place, making it easy to manage and run the different scripts.

### Defining the Extraction Graph

Indexify uses a YAML file to define the extraction graph, which specifies the pipeline for processing data. Let's create a file named `graph.yaml` with the following content:

```yaml
name: "wiki_extraction_pipeline"
extraction_policies:
  - extractor: "tensorlake/openai"
    name: "entity-extractor"
    input_params:
      system_prompt: "Extract entities from text, and return the output in JSON format."
  - extractor: "tensorlake/chunk-extractor"
    name: "chunker"
    input_params:
        chunk_size: 1000
        overlap: 100
  - extractor: "tensorlake/minilm-l6"
    name: "wikiembedding"
    content_source: "chunker"
```

This YAML file defines three extractors:

1. `entity-extractor`: Uses OpenAI's language models to extract named entities from the text.
2. `chunker`: Splits the text into smaller chunks of 1000 characters with a 100-character overlap.
3. `wikiembedding`: Creates embeddings for the chunks, using the output from the chunker.

### Setting Up the Extraction Graph

To set up the extraction graph using the YAML file, create a Python script named `setup.py`:

```python
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

def create_extraction_graph():
    extraction_graph = ExtractionGraph.from_yaml_file("graph.yaml")
    client.create_extraction_graph(extraction_graph)

if __name__ == "__main__":
    create_extraction_graph()
```

This script reads the `graph.yaml` file and creates the extraction graph in Indexify.

### Ingesting Data

To ingest data into the defined extraction pipeline, create a script named `ingest.py`:

```python
from indexify import IndexifyClient, ExtractionGraph
from langchain_community.document_loaders import WikipediaLoader

client = IndexifyClient()

def load_data(player):
    docs = WikipediaLoader(query=player, load_max_docs=1).load()

    for doc in docs:
        client.add_documents("wiki_extraction_pipeline", doc.page_content)

if __name__ == "__main__":
    load_data("Stephen Hawking")
    load_data("Isaac Newton")
```

This script uses the WikipediaLoader to fetch articles and ingest them into the Indexify pipeline defined in the YAML file.

### Querying Indexed Data

To query the processed and indexed data, create a script named `query.py`:

```python
from indexify import IndexifyClient
from openai import OpenAI

client = IndexifyClient()
client_openai = OpenAI()

def query_database(question: str, index: str, top_k=3):
    retrieved_results = client.search_index(name=index, query=question, top_k=top_k)
    context = "\n-".join([item["text"] for item in retrieved_results])
    response = client_openai.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": f"Answer the question, based on the context.\n question: {question} \n context: {context}",
            },
        ],
        model="gpt-3.5-turbo",
    )
    return response.choices[0].message.content

if __name__ == "__main__":
    index_name = "wiki_extraction_pipeline.wikiembedding.embedding"
    indexes = client.indexes()
    print(f"Vector indexes present: {indexes}, querying index: {index_name}")
    print(
        query_database(
            "What accomplishments did Isaac Newton achieve during his lifetime?",
            "wiki_extraction_pipeline.wikiembedding.embedding",
            4,
        )
    )
```

This script demonstrates how to query the indexed data using semantic search and generate answers using OpenAI's GPT-3.5 model.

By defining the extraction graph in the YAML file and using these Python scripts, you can easily set up, ingest data into, and query an Indexify pipeline. This approach allows for flexible and modular data processing, enabling you to customize the pipeline for various use cases by modifying the YAML configuration.

## Next Steps

To continue your journey with Indexify, consider exploring the following topics in order:

| Topics | Subtopics |
|--------|-----------|
| [Getting Started - Basic](https://docs.getindexify.ai/getting_started/) | - Setting up the Indexify Server<br>- Creating a Virtual Environment<br>- Downloading and Setting Up Extractors<br>- Defining Data Pipeline with YAML<br>- Loading Wikipedia Data<br>- Querying Indexed Data<br>- Building a Simple RAG Application |
| [Intermediate Use Case: Unstructured Data Extraction from a Tax PDF](https://docs.getindexify.ai/getting_started_intermediate/) | - Understanding the challenge of tax document processing<br>- Setting up an Indexify pipeline for PDF extraction<br>- Implementing extractors for key tax information<br>- Querying and retrieving processed tax data |
| [Key Concepts of Indexify](https://docs.getindexify.ai/concepts/) | - Extractors<br>  • Transformation<br>  • Structured Data Extraction<br>  • Embedding Extraction<br>  • Combined Transformation, Embedding, and Metadata Extraction<br>- Namespaces<br>- Content<br>- Extraction Graphs<br>- Vector Index and Retrieval APIs<br>- Structured Data Tables |
| [Architecture of Indexify](https://docs.getindexify.ai/architecture/) | - Indexify Server<br>  • Coordinator<br>  • Ingestion Server<br>- Extractors<br>- Deployment Layouts<br>  • Local Mode<br>  • Production Mode |
| [Building a Custom Extractor for Your Use Case](https://docs.getindexify.ai/apis/develop_extractors/) | - Understanding the Extractor SDK<br>- Designing your extractor's functionality<br>- Implementing the extractor class<br>- Testing and debugging your custom extractor<br>- Integrating the custom extractor into your Indexify pipeline |
| [Examples and Use Cases](https://docs.getindexify.ai/examples_index/) | - Document processing and analysis<br>- Image and video content extraction<br>- Audio transcription and analysis<br>- Multi-modal data processing<br>- Large-scale data ingestion and retrieval systems |

Each section builds upon the previous ones, providing a logical progression from practical application to deeper technical understanding and finally to customization and real-world examples.

For more information on how to use Indexify, refer to the [official documentation](https://docs.getindexify.ai/).

Happy coding!
