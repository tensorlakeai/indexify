# YouTube Video Summarizer with Indexify

This project demonstrates how to build a YouTube video summarization pipeline using Indexify. The pipeline downloads a YouTube video, extracts audio, transcribes it, classifies the content, and generates a summary based on the classification.

## Features

- YouTube video download
- Audio extraction from video
- Speech-to-text transcription using Faster Whisper
- Content classification using Llama.cpp (e.g. job interview, sales call)
- Generate summaries based on conversation type

## Prerequisites

- Python 3.9+
- Docker and Docker Compose (for containerized setup)

## Installation and Usage

### Option 1: Local Installation

1. Clone this repository:
   ```
   git clone https://github.com/tensorlakeai/indexify
   cd indexify/examples/video_summarization
   ```

2. Create a virtual environment and activate it:
   ```
   python -m venv venv
   source venv/bin/activate
   ```

3. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Run the main script:
   ```
   python workflow.py
   ```

### Option 2: Using Docker Compose

1. Clone this repository:
   ```
   git clone https://github.com/tensorlakeai/indexify
   cd indexify/examples/video_summarization
   ```

2. Ensure Docker and Docker Compose are installed on your system.

3. Build and start the services:
   ```
   docker-compose up --build
   ```

   This command will build the application image and run the YouTube video summarizer pipeline.

## How it Works

1. **Video Processing:**
   - Video Download: Uses `pytube` to download the YouTube video.
   - Audio Extraction: Extracts audio from the video using `pydub`.
   - Transcription: Converts speech to text using Faster Whisper.

2. **Content Analysis:**
   - Classification: Uses Llama.cpp to classify the content of the transcription.
   - Summarization: Generates a summary based on the classification (job interview or sales call) using Llama.cpp.

## Indexify Graph Structure

The project uses the following Indexify graph:

```
download_youtube_video -> extract_audio_from_video -> transcribe_audio -> classify_meeting_intent -> route_transcription_to_summarizer -> summarize_job_interview
                                                                                                                                       -> summarize_sales_call
```

## Customization

- Modify the `youtube_url` in the `main()` function to process different videos.
- Adjust the classification logic in `classify_meeting_intent()` to handle more content types.
- Fine-tune the prompts in the summarization functions for better results.
- Experiment with different Llama model variants for improved performance.
