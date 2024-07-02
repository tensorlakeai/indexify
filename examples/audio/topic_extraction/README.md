# Topic Extraction 

1. The extraction graph creates an endpoint which accepts audio files and transcribes them using OpenAI's Whisper model
2. The transcription is fed into an LLM for Topic Extraction.
3. The transcription is fed into a summarization model to summarize the entire transcript.

## Code Reference

1. `graph.yaml` - contains the extraction graph.
2. `setup_graph.py` - Sets up the extraction graph in Indexify Server
3. `upload_and_retrieve.py` - Uploads audio into the extraction graph, waits for extraction and finally retrieves from the endpoint.

## Download Indexify Server

```bash
curl https://getindexify.ai | sh
```

## Download Indexify Extractors 
```bash
virtualenv ve
source ve/bin/activate

pip install indexify-extractor-sdk
indexify-extractor download tensorlake/whisper-asr
indexify-extractor download tensorlake/summarization
indexify-extractor download tensorlake/openai
```

## Setup the Graph 
```bash
python setup_graph.py
```

## Upload Data and Retrieve 
The next step is to upload an audio file and retrieve the transcript

```bash
python upload_and_retrieve.py
```

