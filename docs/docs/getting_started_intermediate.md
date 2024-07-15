# Getting Started - Intermediate

Hello and welcome to the Intermediate Getting Started guide. Please go through the [Getting Started Basic](https://docs.getindexify.ai/getting_started/) guide for Indexify if you haven't already.

## California Tax Calculation Example

In this example, we will make an LLM (Large Language Model) answer how much someone would be paying in taxes in California, based on their income. We will ingest and extract information from a PDF containing CA tax laws, the LLM will refer to the extracted data for response synthesis.

<img src="https://github.com/user-attachments/assets/79839395-3a42-49a7-86a6-1bc9c4e407f9" alt="Image description" height="1024">


This seemingly simple example demonstrates the power and flexibility of Indexify. In a real-world scenario, you could easily scale this approach to ingest and extract information from hundreds of state-specific documents, creating a robust tax consultation system.

## Prerequisites
Before we begin, ensure you have the following:

- Python 3.11 or older installed (Indexify currently requires this version)
- Basic familiarity with Python or TypeScript programming
- An OpenAI API key (for using GPT models)
- Command-line interface experience

## Indexify Server

Download the indexify server and run it

```bash title="( Terminal 1 ) Download Indexify Server"
curl https://getindexify.ai | sh
./indexify server -d
```
Once running, the server provides two key endpoints:

- `http://localhost:8900` - The main API endpoint for data ingestion and retrieval
- `http://localhost:8900/ui` - A user interface for monitoring and debugging your Indexify pipelines

## Download the Extractors

Extractors are specialized components in Indexify that process and transform data. For our tax law application, we'll need three specific extractors:

- Marker Extractor: Converts PDF documents to Markdown format
- Chunk Extractor: Splits text into manageable chunks
- MiniLM-L6 Extractor: Generates embeddings for text chunks


??? note "Want the Source Code?"

    The source code for this tutorial can be found [here](https://github.com/tensorlakeai/indexify/tree/main/examples/getting_started/website/intermediate) in our example folder

Before we begin, let's download the extractors

!!! note "Python Versions"

    Indexify requires python 3.11 or older at this present moment to work.

```bash title="( Terminal 2 ) Download Indexify Extractors"
python3 -m venv venv
source venv/bin/activate
pip3 install indexify-extractor-sdk indexify wikipedia openai
indexify-extractor download tensorlake/marker
indexify-extractor download tensorlake/minilm-l6
indexify-extractor download tensorlake/chunk-extractor
```

## Start the Extractors

Run the Indexify Extractor server in a separate terminal

```bash title="( Terminal 2 ) Join the server"
indexify-extractor join-server
```

You'll be able to verify that the three extractors are available by going to
the [dashboard](http://localhost:8900/ui/default) and looking at the extractors
section.

## Install the libraries

Don't forget to install the necessary dependencies before running the rest of this tutorial.

=== "Python"

    ```bash
    pip3 install indexify openai
    ```

=== "Typescript"

    ```bash
    npm install axios getindexify openai
    ```

## Extraction Graph Setup

Set up an extraction graph to process the PDF documents. The extraction graph defines the sequence of operations that will be performed on our input data (the tax law PDF). Let's set it up:

=== "Python"

    ``` python
    from indexify import ExtractionGraph, IndexifyClient

    client = IndexifyClient()

    extraction_graph_spec = """
    name: 'pdfqa'
    extraction_policies:
      - extractor: 'tensorlake/marker' # (2)!
        name: 'mdextract'
      - extractor: 'tensorlake/chunk-extractor' #(3)!
        name: 'chunker'
        input_params:
            chunk_size: 1000
            overlap: 100
        content_source: 'mdextract'
      - extractor: 'tensorlake/minilm-l6' #(4)!
        name: 'pdfembedding'
        content_source: 'chunker'
    """

    extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
    client.create_extraction_graph(extraction_graph)
    ```

=== "Typescript"

    ```javascript
    import { ExtractionGraph, IndexifyClient } from "getindexify"

    (async () => {
        const client = await IndexifyClient.createClient();

        const graph = ExtractionGraph.fromYaml(`
        name: 'pdfqa'
        extraction_policies:
          - extractor: 'tensorlake/marker'
            name: 'mdextract'
          - extractor: 'tensorlake/chunk-extractor'
            name: 'chunker'
            input_params:
                chunk_size: 1000
                overlap: 100
            content_source: 'mdextract'
          - extractor: 'tensorlake/minilm-l6'
            name: 'pdfembedding'
            content_source: 'chunker'
        `)

        await client.createExtractionGraph(graph)
      })()
    ```

This extraction graph, named 'pdfqa', defines a three-step process:

1. The `tensorlake/marker` extractor converts the PDF into Markdown format.
2. The `tensorlake/chunk-extractor` splits the Markdown text into chunks of 1000 characters with a 100-character overlap.
3. The `tensorlake/minilm-l6` extractor generates embeddings for each chunk, enabling semantic search capabilities.

The following diagram expresses the pipeline in detail.

![Indexify Extractors Presentation](https://github.com/user-attachments/assets/80149ab7-e698-47a3-b853-7add9a7b60d6)

## Document Ingestion

Add the PDF document to the "pdfqa" extraction graph:

=== "Python"

    ```python
    import requests
    from indexify import IndexifyClient

    client = IndexifyClient()

    response = requests.get("https://arev.assembly.ca.gov/sites/arev.assembly.ca.gov/files/publications/Chapter_2B.pdf")
    with open("taxes.pdf", 'wb') as file:
        file.write(response.content)

    client.upload_file("pdfqa", "taxes.pdf")
    ```

=== "TypeScript"

    ```typescript
    import axios from 'axios';
    import { promises as fs } from 'fs';
    import { IndexifyClient } from "getindexify";

    (async () => {
        const client = await IndexifyClient.createClient();

        const url = "https://arev.assembly.ca.gov/sites/arev.assembly.ca.gov/files/publications/Chapter_2B.pdf";
        const filePath = "taxes.pdf";

        const response = await axios.get(url, { responseType: 'arraybuffer' });
        await fs.writeFile(filePath, response.data);

        await client.uploadFile("pdfqa", filePath);
    })();
    ```
This code does the following:

1. Downloads the California tax law PDF from a public URL
2. Saves the PDF locally as "taxes.pdf"
3. Uploads the PDF to Indexify, associating it with our 'pdfqa' extraction graph

Once uploaded, Indexify will automatically process the PDF through our defined extraction graph.

## Prompting and Context Retrieval Function

We can use the same prompting and context retrieval function defined above to get context for the LLM based on the question.

!!! note "OpenAI API"

    You'll want to have exported `OPENAI_API_KEY` and set to your API key before running these scripts.

=== "Python"

    ```python
    from openai import OpenAI
    from indexify import IndexifyClient

    client = IndexifyClient()
    client_openai = OpenAI()

    def get_context(question: str, index: str, top_k=3):
        results = client.search_index(name=index, query=question, top_k=3)
        context = ""
        for result in results:
            context = context + f"content id: {result['content_id']} \n\n passage: {result['text']}\n"
        return context

    def create_prompt(question, context):
        return f"Answer the question, based on the context.\n question: {question} \n context: {context}"

    question = "What are the tax brackets in California and how much would I owe on an income of $24,000?"
    context = get_context(question, "pdfqa.pdfembedding.embedding")

    prompt = create_prompt(question, context)

    chat_completion = client_openai.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": prompt,
            }
        ],
        model="gpt-3.5-turbo",
    )

    print(chat_completion.choices[0].message.content)
    ```

=== "Typescript"

    ```typescript
    import { ExtractionGraph, IndexifyClient } from "getindexify";
    import { OpenAI } from "openai";

    (async () => {
      const client = await IndexifyClient.createClient();
      const openai = new OpenAI();

      const getContext = async (question: string, index: string, topK=3) => {
        const results = await client.searchIndex(index, question, topK)

        return results.reduce((ctx, result) =>
          ctx + `content id: {${result.content_id}} \n\n passage: {${result.text}}\n`,
        "")
      }

      const createPrompt = (question: string, context: string) =>
        `Answer the question, based on the context.\n question: ${question} \n context: ${context}`

      const question = "What are the tax brackets in California and how much would I owe on an income of $24,000?"
      const context = await getContext(question, "pdfqa.pdfembedding.embedding")

      const prompt = createPrompt(question, context)

      console.log(await openai.chat.completions.create({
        model: "gpt-3.5-turbo",
        messages: [
          { role: "user", content: prompt }
        ]
      }))
    })()
    ```
This code does the following:

1. Defines a `get_context` function that retrieves relevant passages from our processed PDF based on the question.
2. Creates a `create_prompt` function that formats the question and context for the LLM.
3. Uses OpenAI's GPT-3.5-turbo model to generate an answer based on the provided context.

The example question asks about California tax brackets and calculates taxes for a $24,000 income. The LLM uses the context provided by our Indexify pipeline to formulate an accurate response as shown below.

!!! note "Response"

    Based on the provided information, the tax rates and brackets for California are as follows:

    - $0 - $11,450: 10% of the amount over $0
    - $11,450 - $43,650: $1,145 plus 15% of the amount over $11,450
    - $43,650 - $112,650: $5,975 plus 25% of the amount over $43,650
    - $112,650 - $182,400: $23,225 plus 28% of the amount over $112,650
    - $182,400 - $357,700: $42,755 plus 33% of the amount over $182,400
    - $357,700 and above: $100,604 plus 35% of the amount over $357,700

    For an income of $24,000, you fall within the $0 - $43,650 bracket. To calculate your tax liability, you would need to determine the tax owed on the first $11,450 at 10%, the tax owed on $11,450 - $24,000 at 15%, and add those together.

    Given that $24,000 falls within the $0 - $43,650 bracket, you would need to calculate the following:

    - Tax on first $11,450: $11,450 x 10% = $1,145
    - Tax on next $12,550 ($24,000 - $11,450): $12,550 x 15% = $1,882.50

    Therefore, your total tax liability would be $1,145 + $1,882.50 = $3,027.50.

## Conclusion

This intermediate guide demonstrates how to use Indexify to create a sophisticated question-answering system for California tax laws. By ingesting a PDF, extracting and processing its content, and using an LLM for answer generation, we've created a flexible and powerful tool that could be easily expanded to cover more complex scenarios or multiple documents.

The key strengths of this approach include:
1. Automatic processing of complex documents (PDFs)
2. Efficient chunking and embedding of text for quick retrieval
3. Use of up-to-date, specific information for answer generation
4. Scalability to handle multiple documents or more complex queries

By following this guide, you've taken a significant step in leveraging Indexify's capabilities for real-world applications. As you continue to explore, consider how you might apply these techniques to other domains or expand the system to handle more diverse types of queries and documents

## Next Steps

| Topics | Subtopics |
|--------|-----------|
| [Key Concepts of Indexify](https://docs.getindexify.ai/concepts/) | - Extractors<br>  • Transformation techniques<br>  • Structured Data Extraction methods<br>  • Advanced Embedding Extraction<br>  • Combining Transformation, Embedding, and Metadata Extraction<br>- Namespaces and data organization<br>- Content management strategies<br>- Designing complex Extraction Graphs<br>- Advanced Vector Index and Retrieval API usage<br>- Optimizing Structured Data Tables |
| [Architecture of Indexify](https://docs.getindexify.ai/architecture/) | - Deep dive into the Indexify Server<br>  • Coordinator functionality and optimization<br>  • Ingestion Server scalability<br>- Advanced Extractor configurations<br>- Deployment Layouts<br>  • Optimizing Local Mode for development<br>  • Scaling Production Mode for high-volume applications |
| [Building Custom Extractors for Your Use Case](https://docs.getindexify.ai/apis/develop_extractors/) | - Understanding the Extractor SDK in depth<br>- Designing extractors for specific data types or industries<br>- Implementing advanced extractor classes<br>- Strategies for testing and debugging complex extractors<br>- Integrating custom extractors into large-scale Indexify pipelines |
| [Advanced Examples and Use Cases](https://docs.getindexify.ai/examples_index/) | - Multi-lingual document processing and analysis<br>- Real-time image and video content extraction systems<br>- Audio transcription and sentiment analysis pipelines<br>- Creating multi-modal data processing systems<br>- Implementing large-scale, distributed data ingestion and retrieval systems<br>- Building domain-specific question-answering systems (e.g., legal, medical, financial) |

