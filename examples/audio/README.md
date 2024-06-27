# Audio Processing Pipelines

Indexify is capable of ingesting and processing audios. You can build pipelines that perform one or more of these functions - 
- [Transcription](transcription)
- [Summarization](summarization)
- [Topic Extraction](topic_extraction)
- [Indexing](indexing)

Each of these examples are organized as - 
1. Extraction Graph/Pipeline Description
2. Pipeline Setup Script
3. Upload audio files, and retrieve the artifacts of the pipeline such as transcripts, summary, topics, etc.


### Transcription Models 
Indexify provides the following transcription extractors out of the box - 
- OpenAI Whisper
- Apple's Whisper MLX(MacOS)
- Groq Whisper API
- DistilWhisper x PyAnnotate based speaker diarization

You can list all the ASR extractors by running the command `indexfiy-extractor list`

There are a ton of other ASR APIs out there, which we haven't integrated with. We would love the community to contribute or you can write a custom extractor to call into any API.

### Indexing 
The indexing example uses LanceDB by default, but you can swap it out with any other VectorDB that Indexfy supports - Qdrant or PgVector
We are using the embedding model - minilm-l6 in this example, you can list the other available extractor by running the command - `indexfiy-extractor list`
