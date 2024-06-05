# 🎧 Audio Extraction: Real Time Speech Recognition Pipelines 

You can build real time pipelines with Indexify that incorporates speech, build applications that retrieve information from the audio. We describe the possible tasks and provide some examples -

## 🌟 What Can You Achieve with Indexify?

With Indexify, you can accomplish the following with your audio files:

1. 🔍 **Speech-to-Text:** Easily convert spoken words into written text, enabling you to analyze and search through your audio content effortlessly. Say goodbye to manual transcription and hello to automated efficiency!
2. 📚 **Audio Indexing:** Build comprehensive indexes on vector stores and structured stores by combining audio extractors with chunking, embedding, and structured data extractors. Create a searchable knowledge base that's always at your fingertips!
3. 🤖 **Audio Q&A:** Leverage the power of LLMs to query your audio indexes and get accurate answers to your questions. It's like having a personal assistant that understands your audio files inside out!

## 🔧 The Extraction Pipeline: A Three-Stage Journey

To unlock the full potential of your audio files, we've designed a seamless three-stage extraction pipeline that will take you from raw audio to actionable insights:

1. 🎤 **Content Extraction Stage:** Start by extracting raw content from your audio files using extractors like `tensorlake/whisper-mlx`, `tensorlake/whisper-asr`, or `tensorlake/asrdiarization`. These extractors will convert the spoken words into text, laying the foundation for further analysis.
2. ✂️ **Content to Chunk Extraction Stage:** Break down the extracted text into manageable chunks using extractors like `text/chunking`. This stage helps organize your content into coherent and contextually relevant pieces, making it easier to process and understand.
3. 🧠 **Chunk to Embedding Extraction Stage:** Convert the chunks into vector embeddings using extractors like `embedding/minilm-l6` or `embedding/arctic`. By transforming your content into numerical representations, you enable powerful similarity search and retrieval capabilities.

By chaining these stages together, you can create a powerful pipeline that enables question answering using the RAG (Retrieval-Augmented Generation) approach. Watch as your audio files come to life, ready to answer any question you throw at them! 🚀

## 🌈 Explore the Audio Extractor Landscape

We offer a range of audio extractors to suit your specific needs. Here's a quick overview of our extractor lineup:

| Extractor                | Output Type | Best For                                                                             | Example Usage                                                                                                                                  |
|--------------------------|-------------|--------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| tensorlake/whisper-mlx   | text        | macOS devices                                                                        |   |
| tensorlake/whisper-asr   | text        | Regular devices                                                                      | [Audio RAG](../examples/audio_rag.ipynb), [Audio Transcription](../examples/audio_transcription.ipynb)                                                            |
| tensorlake/asrdiarization | text        | Multi-speaker conversations, assisted generation            | [ASR Diarization Colab Notebook](https://colab.research.google.com/drive/1aW6DdAkxTQWZcCe1fS0QCVZ6GeQFji2S?usp=sharing)                       |

### Choosing the Right Extractor

When selecting an audio extractor, consider your specific requirements and the nature of your audio files:

- If you need advanced features like speaker diarization and speculative decoding, `tensorlake/asrdiarization` is the way to go. It leverages state-of-the-art models to handle complex audio scenarios and deliver rich insights.
- For straightforward speech-to-text conversion on macOS devices, `tensorlake/whisper-mlx` is a great choice. It provides accurate transcription results optimized for the macOS environment.
- If you're working with general-purpose audio files on regular devices, `tensorlake/whisper-asr` offers reliable and efficient transcription capabilities. It's compatible with a wide range of operating systems and can handle various audio qualities.

Remember, you can always experiment with different extractors and compare their results to find the one that best suits your needs. Indexify provides the flexibility to switch between extractors seamlessly, allowing you to explore and leverage the strengths of each one.

## 🚀 Get Started with Audio Extraction

You can test it locally and unlock the secrets hidden within your audio files:

1. Download an Audio Extractor:
   ```bash
   indexify-extractor download tensorlake/whisper-asr
   indexify-extractor join-server whisper-asr.whisper_extractor:WhisperExtractor
   ```

2. (Optional) Load it in a notebook or terminal:
   ```python
   from indexify_extractor_sdk import load_extractor, Content
   extractor, config_cls = load_extractor("indexify_extractors.whisper-asr.whisper_extractor:WhisperExtractor")
   content = Content.from_file("/path/to/audio.mp3")
   results = extractor.extract(content,params={})
   print(results)
   ```

## 🌐 Continuous Audio Extraction for Applications

We've made it incredibly easy to integrate Indexify into your workflow. Get ready to supercharge your audio processing capabilities! 🔋

1. Start the Indexify Server and Extraction Policies:
   ```bash
   curl https://getindexify.ai | sh
   ./indexify server -d
   ```

2. Start a long-running Audio Extractor:
   ```bash
   indexify-extractor download hub://audio/whisper-asr
   indexify-extractor join-server whisper-asr.whisper_extractor:WhisperExtractor
   ```

3. Create an Extraction Graph:
   ```python
   from indexify import IndexifyClient
   client = IndexifyClient()

   extraction_graph_spec = """
   name: 'audioknowledgebase'
   extraction_policies:
      - extractor: 'tensorlake/whisper-asr'
        name: 'my-audio-extractor'
   """
   extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
   client.create_extraction_graph(extraction_graph)
   ```

4. Upload Audio Files from your application:
   ```python
   from indexify import IndexifyClient  
   client = IndexifyClient()
   content_id = client.upload_file("audioknowledgebase", "/path/to/audio.mp3")
   ```

5. Inspect the extracted content:
   ```python
   extracted_content = client.get_extracted_content(content_id=content_id)
   print(extracted_content)  
   ```

With just a few lines of code, you can use data locked in audio files in your applications. Example use-cases: automated transcription, intelligent audio search, and effortless question answering.

## 📚 Explore More Examples

Check out this inspiring example to showcase the power of audio extraction:

- [ASR Diarization Colab Notebook](https://colab.research.google.com/drive/1aW6DdAkxTQWZcCe1fS0QCVZ6GeQFji2S?usp=sharing): Experience the state-of-the-art ASR + diarization + speculative decoding capabilities. 🎙️🗣️
