# Langchain Python

Indexify complements LangChain by providing a robust platform for indexing large volume of multi-modal content such as PDFs, raw text, audio and video. It provides a retriever API to retrieve context for LLMs.

### Install the Indexify Langchain retriever package - 
```bash
pip install indexify-langchain
```

Add some raw text or documents in Indexify following the [Getting Started Guide](https://docs.getindexify.ai/getting_started/)

### Instantiate the Retriever

```python
# Initialize retriever
params = {"name": "minilml6.embedding", "top_k": 9}
retriever = IndexifyRetriever(client=client, params=params)
```

Here we are initializing the retriever to retrieve from the `minilml6.embedding` index and asking for top 9 results to be returned.

### Setup Chat Prompt Template
```python
from langchain.prompts import ChatPromptTemplate

template = """Answer the question based only on the following context:
  {context}
  
  Question: {question}"""
prompt = ChatPromptTemplate.from_template(template)
```

Create some prompt templates.

### Ask llm question with retriever context
```python
from langchain_openai import ChatOpenAI
from langchain.schema.runnable import RunnablePassthrough
from langchain.schema.output_parser import StrOutputParser

llm = ChatOpenAI(model_name="gpt-3.5-turbo", temperature=0)

rag_chain = (
    {"context": retriever, "question": RunnablePassthrough()}
    | prompt
    | llm
    | StrOutputParser()
)
```
Pass in the retriever created above into the chain so that langchain uses that for retreival.

### Ask LLM Question
```python
query = "Where is Lucas?"
print(rag_chain.invoke(query))
```
After that you can just query the chain
