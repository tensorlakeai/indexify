# LLM Extractors

#### [Gemini Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/text/gemini)
The Gemini extractors supports various Gemini models like 1.5 Pro and 1.5 Flash. It supports input documents like text, pdf and images and returns output in text. The system and user prompts can be configured using the input parameter configuration. The input to the extractor is appended into the user prompt. 

##### Input Params
**model_name**(default:'gemini-1.5-flash-latest'): Name of the gemini model to use.

**key**(default: None): API Key

**system_prompt**(default: "You are a helpful assistant"): Default system prompt.

**user_prompt**(default: None): User prompt

##### Input Data Types
```["text/plain", "application/pdf", "image/jpeg", "image/png"]```

#### [Mistral AI Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/text/mistralai)
This Mistral extractor supports various Mistral AI models like mistral-large-latest. The system and user prompts can be configured using the input parameter configuration. The input to the extractor is appended into the user prompt.

##### Input Params
**model_name**(default:'mistral-large-latest'): Name of the Mistral model to use.

**key**:(default: None): API Key

**system_prompt**(default: "You are a helpful assistant"): Default system prompt.

**user_prompt**(default: None): User prompt

##### Input Data Types
```["text/plain"]```

#### [Ollama Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/text/ollama)
The extractor provides access to local LLMs using the Ollama API. It Llama3 by default. The system and user prompts can be configured using the input parameter configuration. The input to the extractor is appended into the user prompt.

##### Input Params
**model_name**(default='llama3'): Model Name

**system_prompt**(default='You are a helpful assistant.'): = System prompt 

**user_prompt**(default=None): = User prompt


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

#### [Schema](https://github.com/tensorlakeai/indexify-extractors/tree/main/text/schema)
The schema extractors enables structured extraction using LLMs. It accepts a user provided JSON Schema and extracts information from text based on the schema. The extractor uses OpenAI by default, with ability to use other LLMs as well. 

This is inspired by Instructor from @jxnlco. 

##### Input Params
**service**(default='openai'): = LLM Service to use. Options: 

**model_name**(default='gpt-3.5-turbo'): LLM to use.

**key**(default=None) = API key of the service

**schema_config**(default=None): The JSON Schema to use for structured extraction.

**example_text**(default=None): TBD

**data**: TBD

**additional_messages**:(default: None): TBD
```
##### Input Data Types
```["text/plain"]```
