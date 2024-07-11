# Videos Extraction and Understanding

Indexify supports videos, and you could build complex pipelines that extracts information from videos. Some of the capabilities provided by Indexify extractors are - 

1. [Key Frame Extraction](https://github.com/tensorlakeai/indexify-extractors/tree/main/video/keyframes)
2. [Audio Extraction from videos](https://github.com/tensorlakeai/indexify-extractors/tree/main/video/audio-extractor) 
3. [Tracking objects in a video](https://github.com/tensorlakeai/indexify-extractors/tree/main/video/tracking) 
4. Video Description using Visual LLMs

## Video RAG Example
To demonstrate video understanding capabilities, we will build a RAG application to answer questions about topics from a video. We will ingest the [State of The Union address from President Biden](https://www.youtube.com/watch?v=cplSUhU2avc) video and build a Q&A bot to answer questions.

At the end of the tutorial your application will be able to answer the following questions.

```text
Q: Whats biden doing to save climate?
A: Biden is taking significant action on climate by cutting carbon emissions in half by 2030 ...
```


We will extract the audio from the video, transcribe it, and index it. Then we will use the extracted information to answer questions about the video.

We will be using the following extractors:

1. [Audio Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/video/audio-extractor) - It will extract audio from ingested videos.
2. [Whisper Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/whisper-asr) - It will extract transcripts of the audio.
3. [Chunk Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/text/chunking) - It will chunk the transcripts into smaller parts.
4. [Mini LM L6 Extractor](https://github.com/tensorlakeai/indexify-extractors/tree/main/embedding/minilm-l6) - A Sentence Transformer to extract embedding from the audio extractor.

The Q&A will be powered by Langchain and OpenAI. We will use Indexify Retriever and pass it to Langchain to retrieve the relevant text of the questions based on semantic search.

### Download Indexify and Extractors

```bash
# Indexify server.
curl https://getindexify.ai | sh

# Indexify extractors.
pip install indexify-extractor-sdk
indexify-extractor download tensorlake/whisper-asr
indexify-extractor download tensorlake/audio-extractor 
indexify-extractor download tensorlake/chunk-extractor 
indexify-extractor download tensorlake/minilm-l6
```

### Start Indexify and Extractors in Terminal

We need to use 2 terminals to start the Indexify server and the extractors.

=== "Terminal 1"

    ```bash
    indexify server -d
    ```

=== "Terminal 2"

    ```bash
    indexify-extractor join-server
    ```

### Download the Libraries

```bash
pip install pytube indexify indexify-langchain langchain-openai
```

### Download the Video

```python
from pytube import YouTube
import os

yt = YouTube("https://www.youtube.com/watch?v=cplSUhU2avc")
file_name = "state_of_the_union_2024.mp4"
if not os.path.exists(file_name):
    video = yt.streams.filter(progressive=True, file_extension="mp4").order_by("resolution").desc().first()
    video.download(filename=file_name)
```

### Create the Extraction Policies

```python
from indexify import IndexifyClient
client = IndexifyClient()
```

Next, we create an extraction graph with 4 extraction policies:

```yaml title="graph.yaml"
name: "videoknowledgebase"
extraction_policies:
  - extractor: "tensorlake/audio-extractor" #(1)!
    name: "audio_clips_of_videos"
  - extractor: "tensorlake/whisper-asr" #(2)!
    name: "audio_transcription"
    content_source: "audio_clips_of_videos" #(5)!
  - extractor: "tensorlake/chunk-extractor" #(3)!
    name: "transcription_chunks"
    content_source: "audio_transcription"
  - extractor: "tensorlake/minilm-l6" #(4)!
    name: "transcript_embedding"
    content_source: "transcription_chunks"
```

1. We extract the audio from every video that is ingested by using the `tensorlake/audio-extractor` on the videos.
2. The extracted audio are passed through the `tensorlake/whisper-asr` extractor to be transcribed.
3. We pass the transcripts to the `tensorlake/chunk-extractor` to chunk the transcripts into smaller parts.
4. We process the transcript chunks through `tensorlake/minilm-l6` extractor to extract the vector embedding and index them.
5. The `content_source` parameter is used to specify the source of the content for the extraction policy. Typically, when creating a pipeline of multiple extractors, the output of one extractor is used as the input for the next extractor.

```py
with open("graph.yaml", "r") as file:
  extraction_graph_spec = file.read()
  extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
  client.create_extraction_graph(extraction_graph)
```

### Upload the Video

```
client.upload_file("videoknowledgebase", path=file_name)
```

Without needing to do anything, Indexify will automatically start the extraction process on the video. This is because Indexify will evaluate any data that is uploaded to it against the extraction graph and start the extraction process if the data matches the graph specification.

### Perform RAG

Create the Indexify Langchain Retriever. This retriever is automatically created by Indexify Extractor MiniLM-L6 and after the extraction process is completed, we can use it to retrieve the relevant text based on the questions.

```python
from indexify_langchain import IndexifyRetriever

params = {
    "name": "videoknowledgebase.transcription_embedding.embedding",
    "top_k": 50
}

retriever = IndexifyRetriever(client=client, params=params)
```

Create the Langchain Q&A chain to ask questions about the video.

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

Now, we can ask questions related to the video.

```python
chain.invoke("Whats biden doing to save climate and the evidences he provides?")
```

Answer:

```text
Biden is taking significant action on climate by cutting carbon emissions in half by 2030, creating clean energy jobs, launching the Climate Corps, and working towards environmental justice. He mentions that the world is facing a climate crisis and that all Americans deserve the freedom to be safe. Biden also mentions that America is safer today than when he took office and provides statistics on murder rates and violent crime decreasing.
```
