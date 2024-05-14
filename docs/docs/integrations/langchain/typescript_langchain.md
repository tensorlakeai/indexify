# Langchain TypeScript

Indexify complements LangChain by providing a robust platform for indexing large volume of multi-modal content such as PDFs, raw text, audio and video. It provides a retriever API to retrieve context for LLMs.

You can use our LangChain retriever in TypeScript from our langchain package `@getindexify/langchain` to begin retrieving your data.

#### Installation
```shell
npm install @getindexify/langchain
```

Below is an example

```typescript
import { ChatOpenAI } from "@langchain/openai";
import {
  RunnableSequence,
  RunnablePassthrough,
} from "@langchain/core/runnables";
import { PromptTemplate } from "@langchain/core/prompts";
import { StringOutputParser } from "@langchain/core/output_parsers";
import { IndexifyClient } from "getindexify";
import { IndexifyRetriever } from "@getindexify/langchain";
import { formatDocumentsAsString } from "langchain/util/document";

(async () => {
  // setup client
  const client = await IndexifyClient.createNamespace("testlangchain");
  const client = await IndexifyClient.createClient();
  const extractionPolicy = {
    "tensorlake/minilm-l6",
    name: `minilml6`,
  };
  const resp = await client.createExtractionGraph(
    "myextractiongraph",
    extractionPolicy
  );
  // add documents
  await client.addDocuments("myextractiongraph", "Lucas is in Los Angeles, California");

  // wait for content to be processed
  await new Promise((r) => setTimeout(r, 5000));

  // setup indexify retriever
  const retriever = new IndexifyRetriever(client, {
    name: "myextractiongraph.minilml6.embedding",
    topK: 9,
  });

  // use openai chat and set up langchain prompt template
  const model = new ChatOpenAI({});

  const prompt =
    PromptTemplate.fromTemplate(`Answer the question based only on the following context:
  {context}
  
  Question: {question}`);

  const chain = RunnableSequence.from([
    {
      context: retriever.pipe(formatDocumentsAsString),
      question: new RunnablePassthrough(),
    },
    prompt,
    model,
    new StringOutputParser(),
  ]);

  // ask question
  const query = "Where is Lucas?";
  console.log(`Question: ${query}`)
  const result = await chain.invoke(query);
  console.log(result)
})();
```