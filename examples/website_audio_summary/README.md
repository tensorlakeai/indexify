# Website Summarizer and TTS Generator

This project demonstrates how to build a pipeline that scrapes a website, summarizes its content, and generates a text-to-speech (TTS) audio file using Indexify.

## Features

- Web scraping using httpx
- Content summarization using OpenAI's GPT-4
- Text-to-speech generation using ElevenLabs
- Indexify for workflow orchestration

## Prerequisites

- Docker and Docker Compose
- OpenAI API key
- ElevenLabs API key

## Installation and Usage

### Option 1: Local Installation

1. Clone this repository:
   ```
   git clone https://github.com/tensorlakeai/indexify
   cd indexify/examples/website_audio_summary
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

4. Create a `.env` file in the project directory with your API keys:
   ```
   OPENAI_API_KEY=your_openai_api_key_here
   ELEVENLABS_API_KEY=your_elevenlabs_api_key_here
   ```

5. Run the main script:
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

3. Create a `.env` file in the project directory with your API keys:
   ```
   OPENAI_API_KEY=your_openai_api_key_here
   ELEVENLABS_API_KEY=your_elevenlabs_api_key_here
   ```

4. Build and start the services:
   ```
   docker-compose up --build
   ```

   This command will build the application image and run the YouTube video summarizer pipeline.

## How it Works

1. **Web Scraping:** The pipeline starts by scraping the content of a specified URL.
2. **Summarization:** The scraped content is then summarized using OpenAI's GPT-4 model.
3. **Text-to-Speech:** Finally, the summary is converted to speech using ElevenLabs' TTS service.

## Indexify Graph Structure

The project uses an Indexify graph with the following structure:

```
scrape_website -> summarize_website -> generate_tts
```

## Customization

- Modify the `url` variable in the `main()` function of `website_summarizer.py` to process different websites.
- Adjust the summarization prompt in the `summarize_website()` function for different summarization styles.
- Change the voice in the `generate_tts()` function to use a different ElevenLabs voice.
