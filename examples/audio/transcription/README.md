# Audio Transcription 

1. The extraction graph creates an endpoint which accepts audio files and transcribes them using OpenAI's Whisper model
2. You can continously transcribe audio with this pipeline by uploading audio files to indexify server.
3. You can run 1000s of instances of the extractors to parallely transcribe audio in a fault tolerant manner.

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
```

## Setup the Graph 
```bash
python setup_graph.py
```

## Upload Data and Retreive 
The next step is to upload an audio file and retreive the transcript

```bash
python upload_and_retreive.py
```

