# Getting Started

In this tutorial, we'll show you how to create a simple RAG application using Indexify that you can use to query Wikipedia about Kevin Durant or any other topic of your choice.

You'll need three different terminals open to complete this tutorial

1. Terminal 1 to download and run the Indexify Server
2. Terminal 2 to run our Indexify extractors which will handle the chunking and embedding of the data we download
3. Terminal 3 to run our python scripts to help load and query data from our Indexify server.

We'll indicate which terminal to run a command in by using the annotation

```bash title="( Terminal 1 ) Description Of Command"
<command goes here>
```

We can think of our Indexify server as a central coordinator and our Extractors ( which we'll set up in a bit ) as specialized workers designed to perform a specific task.

These tasks can range from embedding data, generating summaries or even automatically extracting features from unstructured data. All it takes to chain together these extractors into a complex pipeline is a **a single declarative `.yaml` file**.

## Creating a Virtual Environment

??? note "Want the Source Code?"

    The source code for this tutorial can be found [here](https://github.com/tensorlakeai/indexify-python-client/tree/main/examples/openai-rag) in our example folder

Let's start by creating a new virtual environment before installing the required packages in our virtual environment.

```bash title="( Terminal 1 ) Install Dependencies"
python3 -m venv venv
source venv/bin/activate
pip3 install indexify-extractor-sdk indexify wikipedia openai langchain_community
```

## Indexify Server

Let's first start by downloading the indexify server and running it

```bash title="( Terminal 1 ) Download Indexify Server"
curl https://getindexify.ai | sh
./indexify server -d
```

By doing so, we immediately get the two following endpoints created.

| Endpoint       | Route                           | Description                                                                        |
| -------------- | ------------------------------- | ---------------------------------------------------------------------------------- |
| Ingestion API  | [/](http://localhost:8900)      | The API endpoint for uploading content and retrieving from indexes and SQL Tables. |
| User Interface | [/ui](http://localhost:8900/ui) | Dashboard for extraction graphs, content, and indexes.                             |

## Indexify Extractors

??? info "Extractors"

    Extractors help convert unstructured data into structured data we can query using a vector database or simple SQL. Examples of this could include converting PDF invoices to JSON, labelling objects in a video or even extracting text to embed from PDFs. You can read more about extractors [here](/apis/extractors)

Next, we'll need to download two extractors - one for chunking our pages that we've downloaded from Wikipedia and another that will embed the text chunks that we've generated.

```bash title="( Terminal 2 ) Download Indexify Extractors"
source venv/bin/activate
indexify-extractor download tensorlake/minilm-l6
indexify-extractor download tensorlake/chunk-extractor
```

We can then run all avaliable extractors using the command below.

```python title="( Terminal 2 ) Starting Extractor Workers"
indexify-extractor join-server
```

## Loading in our data

### Defining Our Data Pipeline

Now that we've set up our `Indexify` server and extractors, it's time to define our data pipeline. What we want is a simple pipeline that will take in text documents, split it into individual chunks and then embed it.

We can do so using a simple `.yaml` file as seen below

```yaml title="graph.yaml"
name: "summarize_and_chunk" #(1)!
extraction_policies:
  - extractor: "tensorlake/chunk-extractor"
    name: "chunker" #(2)!
    input_params:
      chunk_size: 1000 #(3)!
      overlap: 100

  - extractor: "tensorlake/minilm-l6"
    name: "wikiembedding"
    content_source: "chunker" #(4)!
```

1. Every data pipeline needs to have a unique name - this is known as a namespace
2. Each extractor is uniquely identified by a single `name`
3. We can configure each extractor using the `input_params` field easily so that it is customized to our needs
4. We can chain together multiple extractors sequentially by specifying a `content_source` for each extractor.

Now that we've defined our pipeline using a `.yaml` file , let's see how we can create our first data pipeline in Indexify using our Indexify SDK.

```python title="setup.py"
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient() #(1)!

def create_extraction_graph():
    with open("graph.yaml", "r") as file:
        extraction_graph_spec = file.read()
        extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec) #(2)!
        client.create_extraction_graph(extraction_graph) #(3)!

if __name__ == "__main__":
    create_extraction_graph()
```

1. We first create an instance of an `Indexify` client so that we can interact with our Indexify server in a typesafe manner
2. We then read in our `.yaml` file which we defined above and create an Extraction Graph using the yaml definition. This is a python object which tells Indexify how to chain together different Extractors together
3. Lastly, we create our new data pipeline with the help of our `Indexify` client.

### Loading in Data

Now that we've written up a simple function to define our extraction graph, let's see how we can update `setup.py` so that we can load in data from wikipedia using our new data pipeline.

=== "New Additions"

    ```python
    from langchain_community.document_loaders import WikipediaLoader

    def load_data():
        docs = WikipediaLoader(query="Kevin Durant", load_max_docs=20).load()

        for doc in docs:
            client.add_documents("summarize_and_chunk", doc.page_content)

    if __name__ == "__main__":
        load_data()
    ```

=== "Full Code"

    ```python hl_lines="2 12-16"
    from indexify import IndexifyClient, ExtractionGraph
    from langchain_community.document_loaders import WikipediaLoader

    client = IndexifyClient()

    def create_extraction_graph():
        with open("graph.yaml", "r") as file:
            extraction_graph_spec = file.read()
            extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
            client.create_extraction_graph(extraction_graph)

    def load_data():
        docs = WikipediaLoader(query="Kevin Durant", load_max_docs=20).load()

        for doc in docs:
            client.add_documents("summarize_and_chunk", doc.page_content)

    if __name__ == "__main__":
        create_extraction_graph()
        load_data()
    ```

We can then run this code to create our new extraction graph and load in the data into our data pipeline. Once we use the add the documents, all we need to do is to let Indexify handle all of the batching and storage.

```bash title="( Terminal 3) Create Data Pipeline using Extraction Graph and load data"
source venv/bin/activate
python3 ./setup.py
```

## Query Indexify

Now that we've loaded our data into Indexify, we can then query our list of downloaded text chunks with some RAG. Create a file `query.py` and add the following code -

```python
def query_database(question: str, index: str, top_k=3):
    retrieved_results = client.search_index(name=index, query=question, top_k=top_k) #(1)!
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
    print(
        query_database(
            "What accomplishments did Kevin durant achieve during his career?",
            "summarize_and_chunk.wikiembedding.embedding",
            4,
        )
    )
```

1. All we need to do to query our database is to use the `.search_index` method and we can get the top `k` elements which are closest in semantic meaning to the user's query.

When we run this file, we get the following output

```bash title="( Terminal 3 ) Run our RAG query"
>> OPENAI_API_KEY=<MY_API_KEY> python3 ./query.py
During his career, Kevin Durant has achieved numerous accomplishments, including winning two NBA championships, an NBA Most Valuable Player Award, two Finals MVP Awards, two NBA All-Star Game Most Valuable Player Awards, four NBA scoring titles, the NBA Rookie of the Year Award, and being named to ten All-NBA teams (including six First Teams). He has also been selected as an NBA All-Star 14 times and was named to the NBA 75th Anniversary Team in 2021. Additionally, Durant has won three gold medals in the Olympics as a member of the U.S. men's national team and gold at the 2010 FIBA World Championship
```

## Next Steps

Now that you have learnt how to build a basic RAG application using Indexify, you can head over to learning more advanced topics

- Learn how to extract text, tables and images from PDF documents.
- See how you can reteive extracted data from Langchain or DSPy.
- Deploying Indexify server and extractors on Kubernetes, when you are ready to take your app to production.
- Observability and understanding performance of Retrieval and Extraction processes.
