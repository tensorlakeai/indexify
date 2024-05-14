# RAG on Videos

In this tutorial we will build a RAG application to answer questions about topics from a video. We will ingest the [State of The Union address from President Biden](https://www.youtube.com/watch?v=cplSUhU2avc) and build Q&A bot to answer questions.

At the end of the tutorial your application will be able to answer the following -
```text
Q: Whats biden doing to save climate?
A: Biden is taking significant action on climate by cutting carbon emissions in half by 2030 ...
```

Indexify can extract information from videos, including key scenes in a video, audio of the video, the transcripts and also detects all the objects of interest in a video. All of these are done through Indexify's open source extractors. 

We will be using the following extractors - 

1. [Audio Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/video/audio-extractor) - It will extract audio from ingested videos.
2. [Whisper Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/whisper-asr) - It will extract transcripts of the audio.
3. [Mini LM L6 Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/embedding/minilm-l6) - A Sentence Transformer to extract embedding from the audio extractor.

The Q&A will be powered by Langchain and OpenAI. We will create a Indexify Retriever and pass it to Langchain to retrieve the relevant text of the questions based on semantic search.

### Download Indexify and the necessary extractors
```bash
curl https://www.tensorlake.ai | sh

pip install indexify-extractor-sdk
indexify-extractor download hub://audio/whisper-asr
indexify-extractor download hub://video/audio-extractor
indexify-extractor download hub://text/chunking
indexify-extractor download hub://embedding/minilm-l6
```

### Start Indexify and the necessary extractors in the terminal
Start Indexify Server in the local dev mode.
```bash
indexify server -d
```
Start the audio extractor
=== "Shell"

    ```bash
    indexify-extractor join-server
    ```
=== "Docker"

    ```shell
    docker run -d -v /tmp/indexify-blob-storage:/tmp/indexify-blob-storage -p 9500:9500 tensorlake/audio-extractor join-server --coordinator-addr=host.docker.internal:8950 --ingestion-addr=host.docker.internal:8900 --advertise-addr=0.0.0.0:9500 --listen-port=9500
    ```

Start the minilm embedding extractor
=== "Shell"

    ```shell
    indexify-extractor join-server
    ```
=== "Docker"

    ```shell
    docker run -d -v /tmp/indexify-blob-storage:/tmp/indexify-blob-storage -p 9501:9501 tensorlake/minilm-l6 join-server --coordinator-addr=host.docker.internal:8950 --ingestion-addr=host.docker.internal:8900 --advertise-addr=0.0.0.0:9501 --listen-port=9501
    ```

Start the whisper extractor
=== "Shell"

    ```bash
    indexify-extractor join-server 
    ```
=== "Docker"

    ```shell
    docker run -d -v /tmp/indexify-blob-storage:/tmp/indexify-blob-storage -p 9502:9502 tensorlake/whisper-asr join-server --workers=1 --coordinator-addr=host.docker.internal:8950 --ingestion-addr=host.docker.internal:8900 --advertise-addr=0.0.0.0:9502 --listen-port=9502
    ```

Start the chunk extractor
=== "Shell"

    ```bash
    indexify-extractor join-server
    ```
=== "Docker"

    ```shell
    docker run -d -v /tmp/indexify-blob-storage:/tmp/indexify-blob-storage -p 9503:9503 tensorlake/chunk-extractor join-server --coordinator-addr=host.docker.internal:8950 --ingestion-addr=host.docker.internal:8900 --advertise-addr=0.0.0.0:9503 --listen-port=9503
    ```


### Download the Libraries
```bash
pip install pytube indexify indexify-langchain
```

```python
from pytube import YouTube
import os

yt = YouTube("https://www.youtube.com/watch?v=cplSUhU2avc")
file_name = "state_of_the_union_2024.mp4"
if not os.path.exists(file_name):
    yt.streams.filter(progressive=True, file_extension='mp4').order_by('resolution').desc().first().download(filename=file_name)
```

### Create the Extraction Policies
Instantiate the Indexify client.
```python
from indexify import IndexifyClient
client = IndexifyClient()
```

Next create two extraction policies, the first instructs indexify to extract audio from every video that is ingested by applying the `tensorlake/audio-extractor` on the videos.
Second, the extracted audio are passed through the `tensorlake/whisper-asr` extractor, we set the source of the policy to the `audio_clips_of_videos` policy so only audio clips extracted by that specific policy is evaluated by this policy.
Third, we pass the transcripts though a `tensorlake/minilm-l6` extractor to extract embedding. 

```python
extraction_graph_spec = """
name: 'videoknowledgebase'
extraction_policies:
   - extractor: 'tensorlake/audio-extractor'
     name: 'audio_clips_of_videos'
   - extractor: 'tensorlake/whisper-asr'
     name: 'audio_transcription'
     content_source: 'audio_clips_of_videos'
   - extractor: 'tensorlake/chunk-extractor'
     name: 'transcription_chunks'
     content_source: 'audio_transcription'
   - extractor: 'tensorlake/minilm-l6'
     name: 'transcript_embedding'
     content_source: 'transcription_chunks'
"""
extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)
```


### Upload the Video
```
client.upload_file("videoknowledgebase", path=file_name)
```

### Perform RAG
Create the Indexify Langchain Retriever
```python
from indexify_langchain import IndexifyRetriever
params = {"name": "videoknowledgebase.transcription_embedding.embedding", "top_k": 50}
retriever = IndexifyRetriever(client=client, params=params)
```

Create the Langchain Q and A chain to ask questions about the video
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

Now ask questions on the video.
```python
chain.invoke("Whats biden doing to save climate and the evidences he provides?")
```

Answer:
```text
Biden is taking significant action on climate by cutting carbon emissions in half by 2030, creating clean energy jobs, launching the Climate Corps, and working towards environmental justice. He mentions that the world is facing a climate crisis and that all Americans deserve the freedom to be safe. Biden also mentions that America is safer today than when he took office and provides statistics on murder rates and violent crime decreasing.
```
