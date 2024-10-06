import logging
from typing import List
import httpx
from pydantic import BaseModel
from indexify import RemoteGraph
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import indexify_function
from indexify.functions_sdk.image import Image

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class WebsiteData(BaseModel):
    url: str
    content: str

class SummarizeWebsite(BaseModel):
    summary: str

class Audio(BaseModel):
    file: File

# Define custom images
scraper_image = (
    Image()
    .name("scraper-image")
    .base_image("python:3.9-slim")
    .run("pip install httpx")
)

openai_image = (
    Image()
    .name("openai-image")
    .base_image("python:3.9-slim")
    .run("pip install openai")
)

elevenlabs_image = (
    Image()
    .name("elevenlabs-image")
    .base_image("python:3.9-slim")
    .run("pip install elevenlabs")
)

@indexify_function(image=scraper_image)
def scrape_website(url: str) -> WebsiteData:
    """Scrape the website content."""
    response = httpx.get(f"https://r.jina.ai/{url}")
    return WebsiteData(url=url, content=response.content.decode("utf-8"))

@indexify_function(image=openai_image)
def summarize_website(website_data: WebsiteData) -> SummarizeWebsite:
    """Summarize the website content."""
    import openai
    client = openai.OpenAI()
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

@indexify_function(image=elevenlabs_image)
def generate_tts(summary: SummarizeWebsite) -> Audio:
    """Generate TTS for the summary using elevenlabs."""
    import elevenlabs
    from elevenlabs import save

    voice = "Rachel"  # You can choose a different voice if needed
    client = elevenlabs.ElevenLabs()
    audio = client.generate(text=summary.summary, voice=voice)
    save(audio, "tensorlake-daily.mp3")
    with open("tensorlake-daily.mp3", "rb") as f:
        return Audio(file=File(data=f.read()))

def create_graph() -> Graph:
    g = Graph(name="tensorlake-daily-website-summarizer", start_node=scrape_website)
    g.add_edge(scrape_website, summarize_website)
    g.add_edge(summarize_website, generate_tts)
    return g

def main():
    graph = create_graph()
    # remote_graph = RemoteGraph.deploy(graph)  # Uncomment to deploy remotely
    
    url = "https://www.cidrap.umn.edu/avian-influenza-bird-flu/h5n1-avian-flu-virus-detected-wastewater-10-texas-cities"
    logging.info(f"Processing URL: {url}")
    
    invocation_id = graph.run(block_until_done=True, url=url)
    output: List[Audio] = graph.output(invocation_id, "generate_tts")
    
    if len(output) > 0:
        with open("tensorlake-daily-saved.mp3", "wb") as f:
            f.write(output[0].file.data)
        logging.info("Audio file saved as tensorlake-daily-saved.mp3")
    else:
        logging.warning("No output found")

if __name__ == "__main__":
    main()
