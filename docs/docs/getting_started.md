# Getting Started

Hello and welcome to this Getting Started guide. We do not assume anything other than a very fundamental knowledge of python.

## Beginner's Guide to Indexify: Building a Wikipedia Information Retrieval System

![Getting-Started-Cover-Image](https://github.com/user-attachments/assets/1ec01ede-9da0-4483-adcd-3ee5c583595b)

In this guide, we'll walk you through creating an online ingestion pipeline for Wikipedia pages. This pipeline will demonstrate how to:

1. Extract structured information (Named Entity Recognition) from web pages using Large Language Models (LLMs)
2. Break down (chunk) the text, create embeddings, and store them in a vector database (LanceDB in this example)

By the end of this tutorial, you'll be able to:

1. Use advanced LLMs like GPT-4 or Mistral to answer questions based on indexed information (Retrieval-Augmented Generation or RAG)
2. Retrieve the Named Entities extracted from the text
3. Use a User Interface to visually debug your pipelines and inspect how pages are broken down into chunks

Let's get started!

## Prerequisites

Before we begin, make sure you have:

1. Python 3.7 or higher installed
2. Basic knowledge of Python programming
3. Familiarity with command-line interfaces
4. An OpenAI API key (for using GPT models)

## Setup

You'll need three separate terminal windows open for this tutorial:

1. Terminal 1: For downloading and running the Indexify Server
2. Terminal 2: For running Indexify extractors (handling structured extraction, chunking, and embedding)
3. Terminal 3: For running Python scripts to load and query data from the Indexify server

We'll use the following notation to indicate which terminal to use:

```bash title="( Terminal X ) Description of Command"
<command goes here>
```

### Understanding Indexify Components

Before we dive in, let's briefly explain the key components of Indexify:

1. **Indexify Server**: The central coordinator and data ingestion API.
2. **Extractors**: Specialized workers designed to perform specific data processing tasks (e.g., embedding data, generating summaries, or extracting features from unstructured data).
3. **Extraction Graph**: A declarative YAML file that chains together extractors into a complex pipeline.


Also before we look into creating pipelines for ingestion and query, it is best to lay out the directory structure of our project.

```plaintext title="Directory Structure"
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

To use this structure:

1. Create a new directory called `indexify-tutorial`.
2. Navigate into this directory in your terminal.
3. Create the virtual environment and activate it (discussed below).
4. Create each of the `.py` and `.yaml` files in the root of this directory (discussed below).
5. Run the curl command to download the Indexify executable into this directory (discussed below).

This structure keeps all the components of our tutorial project organized in one place, making it easy to manage and run the different scripts. 

## Step 1: Setting Up the Indexify Server

To start the indexify server, we have to open up a terminal and put in the download command. 

![Indexify Terminal 1](https://github.com/tensorlakeai/indexify-extractors/assets/44690292/d06ba9c0-a0c6-43d2-90e2-037e537c0f1b)

Let's start by downloading and running the Indexify server:

```bash title="( Terminal 1 ) Download Indexify Server"
curl https://getindexify.ai | sh
./indexify server -d
```

This command creates two important endpoints:

1. Ingestion API: `http://localhost:8900`
2. User Interface: `http://localhost:8900/ui`

The Ingestion API is used for uploading content and retrieving data from indexes and SQL tables, while the User Interface provides a dashboard for visualizing extraction graphs, content, and indexes.

## Step 2: Creating a Virtual Environment

It's good practice to use a virtual environment for Python projects. Let's create one and install the necessary packages:

```bash title="( Terminal 2 ) Install Dependencies"
python3 -m venv venv
source venv/bin/activate
pip3 install indexify-extractor-sdk indexify wikipedia openai langchain_community
```

## Step 3: Setting Up Indexify Extractors

The next step is of course setting up extractors to extract data reliably at scale. Extractors are the very soul of the pipelines at Indexify. Extractors are used for structured extraction from un-structured data of any modality. For example, line items of an invoice as JSON, objects in a video, embedding of text in a PDF, etc.

For the purpose of this tutorial we will be using extractors that have already been created. You can find the full list [here](https://docs.getindexify.ai/apis/develop_extractors/)

![Indexify Terminal 2](https://github.com/tensorlakeai/indexify-extractors/assets/44690292/d06ba9c0-a0c6-43d2-90e2-037e537c0f1b)


Extractors consume `Content` which contains raw bytes of unstructured data, and they produce a list of Content and features from them.

![Extractor_working](https://github.com/user-attachments/assets/ac12fc76-3043-485f-9a8b-6bbffa7d878d)

If you want to read and understand how to build a custom extractor for your own use case, go through the following section. However, if you want to use a built-in available extractor jump to the [next section](#using-available-extractors). 

### Using Custom Extractors

An extractor in Indexify is designed to process unstructured data. It receives data in a `Content` object and transforms it into one or more `Content` objects, optionally adding `Feature` objects during extraction. For example, you could split a PDF into multiple content pieces, each with its text and corresponding embedding or other metadata.

#### Key Concepts

1. **Content**: Represents unstructured data with properties:
   - `data`: Raw bytes of the unstructured data
   - `content_type`: MIME type of the data (e.g., `text/plain`, `image/png`)
   - `Feature`: Optional associated feature (embedding or JSON metadata)

2. **Feature**: Extracted information from unstructured data, such as embeddings or JSON metadata.

#### Building a Custom Extractor

Building a custom extractor is an easy 4 step process. We will walk through this while laying down each step in a sequential manner. To read a full guide on building custom extractors, read the official [documentation for developing extractors](https://docs.getindexify.ai/apis/develop_extractors/).

##### Step 1 of 4: Install the Extractor SDK

First, install the Indexify Extractor SDK:

```bash title="( Terminal 2 ) Install Indexify Server"
pip install indexify-extractor-sdk
```

##### Step 2 of 4: Create a Template

Use the following command to create a template for your new extractor:

```bash  title="( Terminal 2 ) Download Code Template"
curl https://codeload.github.com/tensorlakeai/indexify-extractor-template/tar.gz/main | tar -xz  indexify-extractor-template-main
```

##### Step 3 of 4: Implement the Extractor

In the template, you'll find a `MyExtractor` class in the `custom_extractor.py` file. Implement the `extract` method, which takes a `Content` object and returns a list of `Content` objects.

Basically `extract` method takes a Content object which have the bytes of unstructured data and the mime-type. You can pass a list of JSON, text, video, audio and documents into the extract method. It then returns a list of transformed or derived content, or a list of features.

For instance, in the following example snippet we iterate over a list of content, chunk each content, run a NER model and an embedding model over each chunk and return them as features along with the chunks of text.

```python title="custom_extractor.py"
def extract(self, content: Content) -> List[Content]:
    """
    Extracts features from content.
    """
    output: List[Content] = []
    chunks = content.chunk()
    for chunk in chunks:
        embedding = get_embedding(chunk)
        entities = run_ner_model(chunk)
        embed_chunk = Content.from_text(text=chunk, feature=Feature.embedding(name="text_embedding", values=embedding))
        metadata_chunk = Content.from_text(text=chunk, feature=Feature.metadata(name="metadata", json.dumps(entities))),
        output.append([embed_chunk, metadata_chunk])
    return output
```

##### Step 4 of 4: Define Extractor Properties

Add the following properties to your extractor class:

- `name`: The name of your extractor
- `description`: A detailed description of what your extractor does
- `system_dependencies`: List of dependencies for packaging in a Docker container
- `input_mime_types`: List of input data types your extractor can handle (default is `["text/plain"]`)

##### Step 5: List Dependencies

Create a `requirements.txt` file in your extractor's folder to list any Python dependencies.

#### Deploying Locally

##### Local Installation

Install your extractor locally to make it available to the Indexify server:

```bash
indexify-extractor install-local custom_extractor:MyExtractor 
```

#### Joining with Control Plane

Connect your extractor to the Indexify server to receive content streams:

```bash
indexify-extractor join-server
```

By following these short steps, you can create, and locally deploy custom extractors in Indexify, allowing you to integrate specialized data processing capabilities into your Indexify pipelines.

### Using Available Extractors

As mentioned before, for the purpose of this tutorial, we already have Extractors written, deployed and tested.
Now, let's download three essential extractors:

```bash title="( Terminal 2 ) Download Indexify Extractors"
source venv/bin/activate
indexify-extractor download tensorlake/openai
indexify-extractor download tensorlake/minilm-l6
indexify-extractor download tensorlake/chunk-extractor
indexify-extractor join-server
```

Don't forget to set your OpenAI API key:

```bash
export OPENAI_API_KEY=your_api_key_here
```

Now, let's start all available extractors:

```bash title="( Terminal 2 ) Starting Extractor Workers"
indexify-extractor join-server
```

## Step 4: Defining Our Data Pipeline

We'll use a YAML file to define our data pipeline. This pipeline will take text documents, split them into small chunks, extract entities, and embed the chunks in parallel. The following diagram describes the Indexify end-to-end pipeline

![Extraction Policy Graph](https://github.com/user-attachments/assets/5d4d7a4e-b7c3-401e-965a-e43b22f539a8)

Let us create (or open) a file named `graph.yaml` with the following content:

```yaml title="graph.yaml"
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

This YAML file defines three extraction policies:
1. `entity-extractor`: Extracts named entities from the text
2. `chunker`: Splits the text into smaller chunks
3. `wikiembedding`: Creates embeddings for the chunks

These are the three key extractors are utilized to process and analyze the input content from Wikipedia:

1. OpenAI Extractor (`tensorlake/openai`): This extractor supports various input types including text, PDF, and images. It leverages OpenAI's language models (such as GPT-3.5 Turbo or GPT-4) to perform entity extraction from the input content. The extractor can be customized with specific system and user prompts to tailor its output.

2. Chunk Extractor (`tensorlake/chunk-extractor`): This extractor is responsible for breaking down the input text into smaller, manageable chunks. It offers flexibility in chunk size and overlap, and can use different text splitting strategies (e.g., recursive, markdown, or HTML-based splitting).

3. MiniLM-L6 Extractor (`tensorlake/minilm-l6`): This extractor is likely used for generating embeddings of the text chunks. MiniLM-L6 is a compact and efficient language model that can create high-quality vector representations of text, which are crucial for semantic search and similarity-based retrieval.

These extractors work in concert to transform raw, unstructured input into processed, indexed, and easily retrievable information, forming the backbone of the Indexify pipeline for tasks such as entity recognition, text segmentation, and semantic embedding. You can learn more about different types of available extractors and their usage [here](https://docs.getindexify.ai/apis/extractors/).

## Step 5: Creating the Extraction Graph

Now, let's create a Python script to set up our extraction graph using the YAML file we just created.

Create a file named `setup.py` with the following content:

```python title="setup.py"
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

def create_extraction_graph():
    extraction_graph = ExtractionGraph.from_yaml_file("graph.yaml")
    client.create_extraction_graph(extraction_graph)

if __name__ == "__main__":
    create_extraction_graph()
```

Run this script to create the extraction graph:

```bash title="( Terminal 3) Create Extraction Graph"
source venv/bin/activate
python3 ./setup.py
```

## Step 6: Loading Data

Now that we have our extraction graph set up, let's create (or open) a script to load the Wikipedia data into our pipeline.

![Indexify Terminal 3](https://github.com/tensorlakeai/indexify-extractors/assets/44690292/c0185f07-5033-4e7c-9040-7854b996d430)


Create a file named `ingest.py` with the following content:

```python title="ingest.py"
from indexify import IndexifyClient, ExtractionGraph
from langchain_community.document_loaders import WikipediaLoader

client = IndexifyClient()

def load_data(player):
    docs = WikipediaLoader(query=player, load_max_docs=1).load()

    for doc in docs:
        client.add_documents("wiki_extraction_pipeline", doc.page_content)

if __name__ == "__main__":
    load_data("Kevin Durant")
    load_data("Stephen Curry")
```

Run this script to ingest data into Indexify:

```bash title="( Terminal 3) Ingest Data"
source venv/bin/activate
python3 ./ingest.py
```

## Step 7: Querying Indexify

Now that we have data in our system, let's create a script to query Indexify and retrieve information.

You can query Indexify to - 

1. List ingested content by extraction graph. You can also list content per extraction policy.
2. Get extracted data from any of the extraction policies of an Extraction Graph.
3. Perform semantic search on vector indexes populated by embedding extractors.
4. Run SQL Queries on structured data(not in this tutorial).

For now let us create a file named `query.py` with the following content:

```python title="query.py"
from indexify import IndexifyClient
from openai import OpenAI

client = IndexifyClient()
client_openai = OpenAI()

# Get entities
ingested_content_list = client.list_content("wiki_extraction_pipeline")
content_id = ingested_content_list[0].id
entities = client.get_extracted_content(
    content_id, 
    "wiki_extraction_pipeline", 
    "entity-extractor")

# Get chunks
chunks = client.get_extracted_content(
    content_id, 
    "wiki_extraction_pipeline", 
    "chunker")

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
            "What accomplishments did Kevin durant achieve during his career?",
            "wiki_extraction_pipeline.wikiembedding.embedding",
            4,
        )
    )
```

Run this script to query the indexed data:

```bash title="( Terminal 3 ) Run our RAG query"
OPENAI_API_KEY=your_api_key_here python3 ./query.py
```

You should see a response summarizing Kevin Durant's career accomplishments based on the indexed Wikipedia data.

```plaintext title="Output"
During his career, Kevin Durant has achieved numerous accomplishments, including winning two NBA championships, an NBA Most Valuable Player Award, two Finals MVP Awards, two NBA All-Star Game Most Valuable Player Awards, four NBA scoring titles, the NBA Rookie of the Year Award, and being named to ten All-NBA teams (including six First Teams). He has also been selected as an NBA All-Star 14 times and was named to the NBA 75th Anniversary Team in 2021. Additionally, Durant has won three gold medals in the Olympics as a member of the U.S. men's national team and gold at the 2010 FIBA World Championship
```

## Conclusion

Congratulations! You've successfully set up an Indexify pipeline for ingesting, processing, and querying Wikipedia data. This beginner-friendly guide has walked you through:

1. Setting up the Indexify server and extractors
2. Defining an extraction graph for processing Wikipedia pages
3. Ingesting data into the system
4. Querying the processed data using semantic search and GPT-3.5

Indexify's fault-tolerant design ensures reliability and scalability, making it suitable for mission-critical applications. You can now explore more advanced topics and integrations to further enhance your information retrieval and processing capabilities.

## Next Steps

To continue your journey with Indexify, consider exploring the following topics in order:

| Topics | Subtopics |
|--------|-----------|
| [Intermediate Use Case: Unstructured Data Extraction from a Tax PDF](https://docs.getindexify.ai/getting_started_intermediate/) | - Understanding the challenge of tax document processing<br>- Setting up an Indexify pipeline for PDF extraction<br>- Implementing extractors for key tax information<br>- Querying and retrieving processed tax data |
| [Key Concepts of Indexify](https://docs.getindexify.ai/concepts/) | - Extractors<br>  • Transformation<br>  • Structured Data Extraction<br>  • Embedding Extraction<br>  • Combined Transformation, Embedding, and Metadata Extraction<br>- Namespaces<br>- Content<br>- Extraction Graphs<br>- Vector Index and Retrieval APIs<br>- Structured Data Tables |
| [Architecture of Indexify](https://docs.getindexify.ai/architecture/) | - Indexify Server<br>  • Coordinator<br>  • Ingestion Server<br>- Extractors<br>- Deployment Layouts<br>  • Local Mode<br>  • Production Mode |
| [Building a Custom Extractor for Your Use Case](https://docs.getindexify.ai/apis/develop_extractors/) | - Understanding the Extractor SDK<br>- Designing your extractor's functionality<br>- Implementing the extractor class<br>- Testing and debugging your custom extractor<br>- Integrating the custom extractor into your Indexify pipeline |
| [Examples and Use Cases](https://docs.getindexify.ai/examples_index/) | - Document processing and analysis<br>- Image and video content extraction<br>- Audio transcription and analysis<br>- Multi-modal data processing<br>- Large-scale data ingestion and retrieval systems |

Each section builds upon the previous ones, providing a logical progression from practical application to deeper technical understanding and finally to customization and real-world examples.

Happy coding!
