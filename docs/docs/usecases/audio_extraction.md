# Audio Extraction

We are going to build a pipeline where we will upload a few episodes of the "All In" podcast and ask questions about them. We will also show how you can also get the transcription directly.

### Install the Indexify Extractor SDK, Langchain Retriever and the Indexify Client
```bash
pip install indexify-extractor-sdk indexify-langchain indexify
```

### Start the Indexify Server
```bash
indexify server -d
```

### Download an Embedding Extractor
On another terminal start the embedding extractor which we will use to index text from the wikiepdia page.

=== "Shell"

    ```bash
    indexify-extractor download hub://embedding/minilm-l6
    indexify-extractor join-server minilm-l6.minilm_l6:MiniLML6Extractor
    ```
  
=== "Docker"

    ```shell
    docker run -v /tmp/indexify-blob-storage:/tmp/indexify-blob-storage tensorlake/minilm-l6 join-server --coordinator-addr=host.docker.internal:8950 --ingestion-addr=host.docker.internal:8900
    ```

### Download a Speech To Text Extractor
Install ffmpeg on your machine 
=== "MacOS"
    ```bash
    brew install ffmpeg
    ```
=== "Ubuntu"
    ```bash
    apt install ffmpeg
    ```

On another terminal start a Whisper based Speech To Text Extractor
=== "Shell"

    ```bash
    indexify-extractor download hub://audio/whisper-asr
    indexify-extractor join-server whisper-asr.whisper_extractor:WhisperExtractor 
    ```
=== "Docker"

    ```shell
    docker run -v /tmp/indexify-blob-storage:/tmp/indexify-blob-storage tensorlake/whisper-asr join-server --coordinator-addr=host.docker.internal:8950 --ingestion-addr=host.docker.internal:8900
    ```


### Create Extraction Policies
Instantiate the Indexify Client 
```python
from indexify import IndexifyClient
client = IndexifyClient()
```

First, create a policy to transcribe audio to text.
```python
client.add_extraction_policy(extractor='tensorlake/whisper-asr', name="audio-transcription")
```

Second, from the transcribed audio create an embedding based index.
```python
client.add_extraction_policy(extractor='tensorlake/minilm-l6', name="transcription-embedding", content_source="audio-transcription")
```

### Upload an Audio File
```python
import requests
req = requests.get("https://extractor-files.diptanu-6d5.workers.dev/ALLIN-E167.mp3")

with open('ALLIN-E167.mp3','wb') as f:
    f.write(req.content)
```

```python
client.upload_file(path="ALLIN-E167.mp3")
```

### What is happening behind the scenes
Indexify automatically reacts when ingestion happens and evaluates all the existing policies and invokes appropriate extractors for extraction. When the whisper extractor finishes transcribing the podcast, it automatically fires off the embedding extractor to chunk and extract embedding and populate an index. 

You can upload 100s of audio files parallely into Indexify and it will handle transcription of the audio files and indexing the transcripts automatically. You can run many instances of the extractors for speeding up extraction, and Indexify's in-built scheduler will distribute the work transparently. 

### Perform RAG

Initialize the Langchain Retreiver.
```python
from indexify_langchain import IndexifyRetriever
params = {"name": "transcription-embedding.embedding", "top_k": 3}
retriever = IndexifyRetriever(client=client, params=params)
```

Now create a chain to prompt OpenAI with data retreived from Indexify to create a simple Q and A bot
```python
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import ChatOpenAI
```

```python
template = """Answer the question based only on the following context:
{context}

Question: {question}
"""
prompt = ChatPromptTemplate.from_template(template)

model = ChatOpenAI()

chain = (
    {"context": retriever, "question": RunnablePassthrough()}
    | prompt
    | model
    | StrOutputParser()
)
```
Now ask any question about KD -
```python
chain.invoke("What is Chamath's firm called?")
```

```bash
"Chamath's firm is called social capital"
```


