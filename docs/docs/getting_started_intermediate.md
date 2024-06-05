# Getting Started - Intermediate

In this example, we will make an LLM answer how much someone would be paying in taxes in California, based on their income. We will ingest and extract information from a PDF containing CA tax laws, the LLM will refer to the extracted data for response synthesis.

While this example is simple, if you were building a production application on tax laws, you can ingest and extract information from 100s of state specific documents.

## Indexify Server

Download the indexify server and run it

```bash title="( Terminal 1 ) Download Indexify Server"
curl https://getindexify.ai | sh
./indexify server -d
```

## Download the Extractors

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

Set up an extraction graph to process the PDF documents

=== "Python"

    ``` python
    from indexify import ExtractionGraph

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

The process begins by setting the name of the extraction graph to "pdfqa". Next, the `tensorlake/marker` extractor is used to convert the PDF document into a Markdown format. Following this, the text extracted by the `tensorlake/marker` extractor is chunked by specifying a `content_source` of `mdextract`. Finally, an embedding step is added to the pipeline, allowing each chunk to be embedded and searchable through semantic search.

## Document Ingestion

Add the PDF document to the "pdfqa" extraction graph:

=== "Python"

    ```python
    import requests

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
