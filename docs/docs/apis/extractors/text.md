# Text Extractors

#### [Gemini Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/text/gemini)
This is an extractor that supports multiple kinds of input documents like text, pdf and images and returns output in text using Gemini from Google. This extractor supports various Gemini models like 1.5 Pro and 1.5 Flash and works on the Content of previous extractor as message, however we can manually overwrite prompt and message. Gemini has 1 million token context window that can process vast amounts of information in one go — including 11 hours of audio transcript, codebases with over 30,000 lines of code or over 700,000 words.

##### Input Params
```
    model_name: Optional[str] = Field(default='gemini-1.5-flash-latest')
    key: Optional[str] = Field(default=None)
    system_prompt: str = Field(default='You are a helpful assistant.')
    user_prompt: Optional[str] = Field(default=None)
```
##### Input Data Types
```["text/plain", "application/pdf", "image/jpeg", "image/png"]```

#### [Mistral AI Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/text/mistralai)
This is an extractor that supports text input documents and returns output in text using Mistral AI. This extractor supports various Mistral AI models like mistral-large-latest and works on the Content of previous extractor as message, however we can manually overwrite prompt and message.

##### Input Params
```
    model_name: Optional[str] = Field(default='mistral-large-latest')
    key: Optional[str] = Field(default=None)
    system_prompt: str = Field(default='You are a helpful assistant.')
    user_prompt: Optional[str] = Field(default=None)
```
##### Input Data Types
```["text/plain"]```

#### [Ollama Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/text/ollama)
This is a Ollama based Extractor that supports multiple Ollama models. The extractor uses Llama3 by default, with ability to use other Ollamas as well and works on the Content of previous extractor as message, however we can manually overwrite prompt and message. We support any open source Ollama models from Hugging Face if you want to run locally.

##### Input Params
```
    model_name: Optional[str] = Field(default='llama3')
    system_prompt: str = Field(default='You are a helpful assistant.')
    user_prompt: Optional[str] = Field(default=None)
```
##### Input Data Types
```["text/plain"]```

#### [OpenAI Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/text/openai)
This is an extractor that supports multiple kinds of input documents like text, pdf and images and returns output in text using OpenAI. This extractor supports various OpenAI models like 3.5 Turbo and 4o and works on the Content of previous extractor as message, however we can manually overwrite prompt and message.

##### Input Params
```
    model_name: Optional[str] = Field(default='gpt-3.5-turbo')
    key: Optional[str] = Field(default=None)
    system_prompt: str = Field(default='You are a helpful assistant.')
    user_prompt: Optional[str] = Field(default=None)
```
##### Input Data Types
```["text/plain", "application/pdf", "image/jpeg", "image/png"]```

#### [NER Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/text/ner)
bert-base-NER is a fine-tuned BERT model that is ready to use for Named Entity Recognition and achieves state-of-the-art performance for the NER task. It has been trained to recognize four types of entities: location (LOC), organizations (ORG), person (PER) and Miscellaneous (MISC). Specifically, this model is a bert-base-cased model that was fine-tuned on the English version of the standard CoNLL-2003 Named Entity Recognition dataset.

##### Input Params
```
    model_name: Optional[str] = Field(default="dslim/bert-base-NER")
```
##### Input Data Types
```["text/plain"]```

#### [Summarization](https://github.com/tensorlakeai/indexify-extractors/tree/main/text/summarization)
This is a BART based Summary Extractor that can convert text from Audio, PDF and other files to their summaries. For summary extraction we use facebook/bart-large-cnn which is a strong summarization model trained on English news articles. Excels at generating factual summaries. For chunking we use FastRecursiveTextSplitter along with support for LangChain based text splitters.

##### Input Params
```
    max_length: int = 3000
    min_length: int = 30
    chunk_method: str = "indexify"
```
##### Input Data Types
```["text/plain"]```

#### [LLM-Summary](https://github.com/tensorlakeai/indexify-extractors/tree/main/text/llm-summary)
This is a LLM based Summary Extractor that can convert text from Audio, PDF and other files to their summaries. For summary extraction we use h2oai/h2o-danube2-1.8b-chat which is a strong <3B parameter Large Language Model suitable even for low end machines. For chunking we use FastRecursiveTextSplitter along with support for LangChain based text splitters.

##### Input Params
```
    max_length: int = 130
    chunk_method: str = "indexify"
```
##### Input Data Types
```["text/plain"]```

#### [Chunking](https://github.com/tensorlakeai/indexify-extractors/tree/main/text/chunking)
ChunkExtractor splits text into smaller chunks. The input to this extractor can be from any source which produces text, with a mime type text/plain. The chunk extractor can be configured to use one of the many chunking strategies available. We use Langchain under the hood in this extractor.

##### Input Params
```
    overlap: int = 0
    chunk_size: int = 100
    text_splitter: Literal["char", "recursive", "markdown", "html"] = "recursive"
    headers_to_split_on: List[str] = []
```
##### Input Data Types
```["text/plain"]```

#### [LLM](https://github.com/tensorlakeai/indexify-extractors/tree/main/text/llm)
This is a LLM based Extractor that supports multiple LLMs. The extractor uses OpenAI by default, with ability to use other LLMs as well and works on the Content of previous extractor as message, however we can manually overwrite prompt and message. We support Gemini 1.5 Pro which has 1 million token context window that can process vast amounts of information in one go — including 11 hours of audio transcript, codebases with over 30,000 lines of code or over 700,000 words. We also support using any open source LLM from Hugging Face if you want to run locally.

##### Input Params
```
    service: str = Field(default='openai')
    model_name: Optional[str] = Field(default='gpt-3.5-turbo')
    key: Optional[str] = Field(default=None)
    prompt: str = Field(default='You are a helpful assistant.')
    query: Optional[str] = Field(default=None)
```
##### Input Data Types
```["text/plain"]```

#### [Schema](https://github.com/tensorlakeai/indexify-extractors/tree/main/text/schema)
This is a LLM based Schema Extractor that supports multiple LLMs. It accepts a user provided JSON Schema and extracts information from text passed into it to the schema. The extractor uses OpenAI by default, with ability to use other LLMs as well. We support Gemini 1.5 Pro which has 1 million token context window that can process vast amounts of information in one go — including 11 hours of audio transcript, codebases with over 30,000 lines of code or over 700,000 words. We also support using any open source LLM from Hugging Face if you want to run locally.

This is inspired by Instructor from @jxnlco. We support instructor too by pickling the user provided pydantic model into the Instructor extractor. This extractor doesn't depend on pickling and only uses JSON schema which is easier to work with in distributed production deployments.

##### Input Params
```
    service: str = Field(default='openai')
    model_name: Optional[str] = Field(default='gpt-3.5-turbo')
    key: Optional[str] = Field(default=None)
    schema_config: Dict = Field(default={
        'properties': {'name': {'title': 'Name', 'type': 'string'}},
        'required': ['name'],
        'title': 'User',
        'type': 'object'
    }, alias='schema')
    example_text: Optional[str] = Field(default=None)
    data: Optional[str] = Field(default=None)
    additional_messages: str = Field(default='Extract information in JSON according to this schema and return only the output.')
```
##### Input Data Types
```["text/plain"]```