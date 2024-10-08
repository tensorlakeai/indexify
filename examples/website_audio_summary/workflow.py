import logging
import os
from typing import List
import httpx
from pydantic import BaseModel, Field
from indexify import RemoteGraph
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import indexify_function
from indexify.functions_sdk.image import Image

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Data Models
class WebsiteData(BaseModel):
    url: str = Field(..., description="URL of the website")
    content: str = Field(..., description="Content of the website")

class SummarizeWebsite(BaseModel):
    summary: str = Field(..., description="Summary of the website content")

class Audio(BaseModel):
    file: File = Field(..., description="Audio file of the generated TTS")

# Define custom images
scraper_image = (
    Image()
    .name("tensorlake/scraper-image")
    .run("pip install httpx")
)

openai_image = (
    Image()
    .name("tensorlake/openai-image")
    .run("pip install openai")
)

elevenlabs_image = (
    Image()
    .name("tensorlake/elevenlabs-image")
    .run("pip install elevenlabs")
)

@indexify_function(image=scraper_image)
def scrape_website(url: str) -> WebsiteData:
    """Scrape the website content."""
    try:
        response = httpx.get(f"https://r.jina.ai/{url}")
        response.raise_for_status()
        return WebsiteData(url=url, content=response.text)
    except Exception as e:
        logging.error(f"Error scraping website: {e}")
        raise

@indexify_function(image=openai_image)
def summarize_website(website_data: WebsiteData) -> SummarizeWebsite:
    """Summarize the website content."""
    try:
        import openai
        client = openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful assistant that summarizes website content in a form which is hearable as a podcast. Remove all the marketing content and only keep the core content in the summary. Call it Tensorlake Daily, don't add words such as host, pause, etc. The transcript of the summary is going to be fed into a TTS model which will add the pauses.",
                },
                {
                    "role": "user",
                    "content": f"Summarize the following content: {website_data.content}",
                },
            ],
            max_tokens=3000,
            temperature=0.7,
        )
        summary = response.choices[0].message.content.strip()
        return SummarizeWebsite(summary=summary)
    except Exception as e:
        logging.error(f"Error summarizing website: {e}")
        raise

@indexify_function(image=elevenlabs_image)
def generate_tts(summary: SummarizeWebsite) -> Audio:
    """Generate TTS for the summary using elevenlabs."""
    try:
        import elevenlabs
        from elevenlabs import save

        voice = "Rachel"  # You can choose a different voice if needed
        client = elevenlabs.ElevenLabs(api_key=os.environ.get("ELEVENLABS_API_KEY"))
        audio = client.generate(text=summary.summary, voice=voice)
        save(audio, "tensorlake-daily.mp3")
        with open("tensorlake-daily.mp3", "rb") as f:
            return Audio(file=File(data=f.read()))
    except Exception as e:
        logging.error(f"Error generating TTS: {e}")
        raise

def create_graph() -> Graph:
    g = Graph(name="tensorlake-daily-website-summarizer", start_node=scrape_website)
    g.add_edge(scrape_website, summarize_website)
    g.add_edge(summarize_website, generate_tts)
    return g

def deploy_graphs(server_url: str):
    graph = create_graph()
    RemoteGraph.deploy(graph, server_url=server_url)
    logging.info("Graph deployed successfully")

def run_workflow(mode: str, server_url: str = 'http://localhost:8900'):
    if mode == 'in-process-run':
        graph = create_graph()
    elif mode == 'remote-run':
        graph = RemoteGraph.by_name("tensorlake-daily-website-summarizer", server_url=server_url)
    else:
        raise ValueError("Invalid mode. Choose 'in-process-run' or 'remote-run'.")

    url = "https://www.cidrap.umn.edu/avian-influenza-bird-flu/h5n1-avian-flu-virus-detected-wastewater-10-texas-cities"
    logging.info(f"Processing URL: {url}")
    
    invocation_id = graph.run(block_until_done=True, url=url)
    output: List[Audio] = graph.output(invocation_id, "generate_tts")
    
    if output:
        with open("tensorlake-daily-saved.mp3", "wb") as f:
            f.write(output[0].file.data)
        logging.info("Audio file saved as tensorlake-daily-saved.mp3")
    else:
        logging.warning("No output found")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run Tensorlake Daily Website Summarizer")
    parser.add_argument('--mode', choices=['in-process-run', 'remote-deploy', 'remote-run'], required=True, 
                        help='Mode of operation: in-process-run, remote-deploy, or remote-run')
    parser.add_argument('--server-url', default='http://localhost:8900', help='Indexify server URL for remote mode or deployment')
    args = parser.parse_args()

    try:
        if args.mode == 'remote-deploy':
            deploy_graphs(args.server_url)
        elif args.mode in ['in-process-run', 'remote-run']:
            run_workflow(args.mode, args.server_url)
        logging.info("Operation completed successfully!")
    except Exception as e:
        logging.error(f"An error occurred during execution: {str(e)}")
