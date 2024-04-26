# Text Extractors

#### [Summarization](https://github.com/tensorlakeai/indexify-extractors/tree/main/text/summarization)
This is a BART based Summary Extractor that can convert text from Audio, PDF and other files to their summaries. For summary extraction we use facebook/bart-large-cnn which is a strong summarization model trained on English news articles. Excels at generating factual summaries. For chunking we use FastRecursiveTextSplitter along with support for LangChain based text splitters.

#### [LLM-Summary](https://github.com/tensorlakeai/indexify-extractors/tree/main/text/llm-summary)
This is a LLM based Summary Extractor that can convert text from Audio, PDF and other files to their summaries. For summary extraction we use h2oai/h2o-danube2-1.8b-chat which is a strong <3B parameter Large Language Model suitable even for low end machines. For chunking we use FastRecursiveTextSplitter along with support for LangChain based text splitters.

#### [Chunking](https://github.com/tensorlakeai/indexify-extractors/tree/main/text/chunking)
ChunkExtractor splits text into smaller chunks. The input to this extractor can be from any source which produces text, with a mime type text/plain. The chunk extractor can be configured to use one of the many chunking strategies available. We use Langchain under the hood in this extractor.

#### [LLM](https://github.com/tensorlakeai/indexify-extractors/tree/main/text/llm)
This is a LLM based Extractor that supports multiple LLMs. The extractor uses OpenAI by default, with ability to use other LLMs as well and works on the Content of previous extractor as message, however we can manually overwrite prompt and message. We support Gemini 1.5 Pro which has 1 million token context window that can process vast amounts of information in one go — including 11 hours of audio transcript, codebases with over 30,000 lines of code or over 700,000 words. We also support using any open source LLM from Hugging Face if you want to run locally.

#### [Schema](https://github.com/tensorlakeai/indexify-extractors/tree/main/text/schema)
This is a LLM based Schema Extractor that supports multiple LLMs. It accepts a user provided JSON Schema and extracts information from text passed into it to the schema. The extractor uses OpenAI by default, with ability to use other LLMs as well. We support Gemini 1.5 Pro which has 1 million token context window that can process vast amounts of information in one go — including 11 hours of audio transcript, codebases with over 30,000 lines of code or over 700,000 words. We also support using any open source LLM from Hugging Face if you want to run locally.

This is insprired by Instructor from @jxnlco. We support instructor too by pickling the user provided pydantic model into the Instructor extractor. This extractor doesn't depend on pickling and only uses JSON schema which is easier to work with in distributed production deployments.