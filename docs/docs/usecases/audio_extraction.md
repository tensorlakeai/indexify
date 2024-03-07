# Audio Extraction

We are going to build a pipeline where we will upload a few episodes of the "All In" podcast and ask questions about them. We will also show how you can also get the transcription directly.

### Install the Indexify Extractor SDK and the Indexify Client
```bash
pip install indexify-extractor-sdk indexify
```

### Start the Indexify Server
```bash
indexify server -d
```

### Download an Embedding Extractor
On another terminal start the embedding extractor which we will use to index text from the wikiepdia page.
```bash
indexify-extractor download hub://embedding/minilm-l6
indexify-extractor join minilm_l6:MiniLML6Extractor
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
```bash
indexify-extractor download hub://whisper-asr
indexify-extractor join whisper:WhisperExtractor
```

### Create Extraction Policies
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
import urllib.request

urllib.request.urlretrieve(filename="ALLIN-E167.mp3", url="https://content.libsyn.com/p/5/d/f/5df17f8350f43745/ALLIN-E167.mp3?c_id=168165938&cs_id=168165938&destination_id=1928300&response-content-type=audio%2Fmpeg&Expires=1708741176&Signature=P6FSLybeGf4~lPTP5n1w0rVSYsSW7hraj0AqMd6DcMHAwNKGc2h7Zpka2rD0mXDB4VovIPPS1WgpUl30~cMv9eICU6NZGeypWAh9I~vRSB7siFoZwfl~~RbXME-ovRGXu2kSsQdSlx4pynuECYsnu03YvNdBTGEvxROfGXOWd6jrTYL5tVrPDrJYDpDnP~LwrrLfzBT7~CD~s1vvKnPBzrAKFA-KiZ~40GvuLAFOHl77JPk5u5tPk1mO~jTwEKiOmjBwPWkpf359gGys4ozaOBKoeYZeWEOlJDfHT8OHXvLZUjAdqzx95WellT8hWRs85irqZ4uTaWYwbkhT2QHN3A__&Key-Pair-Id=K1YS7LZGUP96OI")
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
params = {"name": "minilml6.embedding", "top_k": 3}
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


