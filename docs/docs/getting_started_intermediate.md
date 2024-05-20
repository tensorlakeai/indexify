# Getting Started - Intermediate 

In this example, we will make an LLM answer how much someone would be paying in taxes in California, based on their income. We will ingest and extract information from a PDF containing CA tax laws, the LLM will refer to the extracted data for response synthesis. 

While this example is simple, if you were building a production application on tax laws, you can ingest and extract information from 100s of state specific documents.

##### Extraction Graph Setup

Set up an extraction graph to process the PDF documents -

- Set the name of the extraction graph to "pdfqa".
- The first stage of the graph converts the PDF document into Markdown. We use the extractor `tensorlake/marker`, which uses a popular Open Source PDF to markdown converter model.
- The text is then chunked into smaller fragments. Chunking makes retrieval and processing by LLMs efficient.
- The chunks are then embedded to make them searchable.
- Each stage has of the pipeline is named and connected to their upstream extractors using the field `content_source`

=== "Python"
    ```python
    from indexify import ExtractionGraph

    extraction_graph_spec = """
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
    """

    extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
    client.create_extraction_graph(extraction_graph)
    ```
=== "TypeScript"
    ```typescript
    import { ExtractionGraph } from "getindexify";
    
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
    `);
    await client.createExtractionGraph(graph);
    ```
##### Document Ingestion

Add the PDF document to the "pdfqa" extraction graph
=== "Python"
    ```python
    import requests

    response = requests.get("https://arev.assembly.ca.gov/sites/arev.assembly.ca.gov/files/publications/Chapter_2B.pdf")
    with open("taxes.pdf", 'wb') as file:
        file.write(response.content)

    client.upload_file("taxes", "taxes.pdf")
    ```
=== "TypeScript"
    ```typescript
    import axios from 'axios';
    import * as fs from 'fs';

    const url = "https://arev.assembly.ca.gov/sites/arev.assembly.ca.gov/files/publications/Chapter_2B.pdf";
    const filePath = "taxes.pdf";

    await axios.get(url, { responseType: 'arraybuffer' })
    .then(response => {
        fs.writeFile(filePath, response.data);
    })

    await client.uploadFile("taxes", "taxes.pdf");
    ```
##### Prompting and Context Retrieval Function
We can use the same prompting and context retrieval function defined above to get context for the LLM based on the question.

=== "Python"
    ```python
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
    const prompt = createPrompt(question, context);

    clientOpenAI.chat.completions
      .create({
        messages: [
          {
            role: "user",
            content: prompt,
          },
        ],
        model: "gpt-3.5-turbo",
      })
      .then((chatCompletion) => {
        console.log(chatCompletion.choices[0].message.content);
      })
      .catch((error) => {
        console.error(error);
      });
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
