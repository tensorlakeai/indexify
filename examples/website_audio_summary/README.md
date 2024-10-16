# Tensorlake Daily Website Summarizer

This project demonstrates how to build a website summarization pipeline using Indexify. The pipeline scrapes a website, summarizes its content, and generates an audio version of the summary.

## Features

- Website content scraping
- Content summarization using OpenAI's GPT-4
- Text-to-speech generation using ElevenLabs


## How it Works

We define functions to do the following tasks - 
1. **Website Scraping:**
   - Uses `httpx` to fetch the content of a given URL.
2. **Content Summarization:**
   - Utilizes OpenAI's GPT-4 to generate a concise summary of the website content.
3. **Text-to-Speech Generation:**
   - Employs ElevenLabs' API to convert the summary into an audio file.

## Graph Structure

The functions are laid out in the graph as follows:

```
scrape_website -> summarize_website -> generate_tts
```

## Prerequisites

- Python 3.9+
- Docker and Docker Compose (for containerized setup)
- OpenAI API key
- ElevenLabs API key

## Installation and Usage

### Option 1: Local Installation - In Process

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

4. Set up environment variables:
   ```
   export OPENAI_API_KEY=your_openai_api_key
   export ELEVENLABS_API_KEY=your_elevenlabs_api_key
   ```

5. Run the main script:
   ```
   python workflow.py --mode in-process-run
   ```

### Option 2: Using Docker Compose - Deployed Graph

1. Clone this repository:
   ```
   git clone https://github.com/tensorlakeai/indexify
   cd indexify/examples/website_audio_summary
   ```

2. Build the Docker images:
   ```
   indexify-cli build-image workflow.py scrape_website
   indexify-cli build-image workflow.py summarize_website
   indexify-cli build-image workflow.py generate_tts
   ```

3. Create a `.env` file in the project directory and add your API keys:
   ```
   OPENAI_API_KEY=your_openai_api_key
   ELEVENLABS_API_KEY=your_elevenlabs_api_key
   ```

4. Start the services:
   ```
   docker-compose up --build
   ```

5. Deploy the graph:
   ```
   python workflow.py --mode remote-deploy
   ```

6. Run the workflow:
   ```
   python workflow.py --mode remote-run
   ```

## Customization

- Modify the `url` variable in the `run_workflow()` function to summarize different websites.
- Adjust the summarization prompt in `summarize_website()` for different summary styles.
- Change the voice in `generate_tts()` to use different ElevenLabs voices.
