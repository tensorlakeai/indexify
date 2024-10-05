# YouTube Video Summarizer with Indexify

This project demonstrates how to use Indexify to create a pipeline for downloading YouTube videos, extracting audio, transcribing the content, classifying the type of conversation, and generating a summary. It showcases the power of Indexify in creating complex, multi-step data processing workflows.

## Features

- Download YouTube videos
- Extract audio from video files
- Transcribe audio to text
- Classify conversation intent (e.g., job interview, sales call)
- Generate summaries based on conversation type

## Prerequisites

- Python 3.9+
- Indexify library
- FFmpeg (for audio extraction)
- CUDA-capable GPU (optional, for faster processing)

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/youtube-video-summarizer.git
   cd youtube-video-summarizer
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

1. Set up environment variables:
   ```
   export INDEXIFY_API_KEY=your_indexify_api_key
   ```

2. Run the main script:
   ```
   python youtube_summarizer.py
   ```

3. The script will process a sample YouTube video. To use a different video, modify the `youtube_url` variable in the `main()` function.

## How it Works

1. **Video Download**: The script downloads a YouTube video using PyTubeFix.
2. **Audio Extraction**: It extracts the audio track from the video using PyDub.
3. **Transcription**: The audio is transcribed using the Faster Whisper model.
4. **Intent Classification**: The transcript is classified to determine the type of conversation (e.g., job interview, sales call) using a Llama model.
5. **Summarization**: Based on the classification, an appropriate summarization model (also Llama-based) generates a summary of the conversation.

## Customization

- Modify the `YoutubeURL` class to include additional parameters for video selection.
- Adjust the classification categories in the `classify_meeting_intent` function.
- Extend the summarization types by adding new functions and updating the router.
