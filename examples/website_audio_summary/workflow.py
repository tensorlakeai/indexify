from typing import List

import httpx
from pydantic import BaseModel

from indexify import create_client
from indexify.functions_sdk.data_objects import File
from indexify.functions_sdk.graph import Graph
from indexify.functions_sdk.indexify_functions import indexify_function


class WebsiteData(BaseModel):
    url: str
    content: str


@indexify_function()
def scrape_website(url: str) -> WebsiteData:
    """
    Scrape the website content.
    """
    response = httpx.get(f"https://r.jina.ai/{url}")
    return WebsiteData(url=url, content=response.content.decode("utf-8"))


class SummarizeWebsite(BaseModel):
    summary: str


@indexify_function()
def summarize_website(website_data: WebsiteData) -> SummarizeWebsite:
    """
    Summarize the website content.
    """
    import openai

    client = openai.OpenAI()
    response = client.chat.completions.create(
        model="gpt-4o",
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


class Audio(BaseModel):
    file: File


@indexify_function()
def generate_tts(summary: str) -> Audio:
    """
    Generate TTS for the summary using elevenlabs.
    """
    import elevenlabs
    from elevenlabs import save

    voice = "Rachel"  # You can choose a different voice if needed
    client = elevenlabs.ElevenLabs()
    audio = client.generate(text=summary, voice=voice)
    save(audio, "tensorlake-daily.mp3")
    with open("tensorlake-daily.mp3", "rb") as f:
        return Audio(file=File(data=f.read()))


if __name__ == "__main__":
    g = Graph(name="tensorlake-daily-website-summarizer", start_node=scrape_website)
    g.add_edge(scrape_website, summarize_website)
    g.add_edge(summarize_website, generate_tts)
    client = create_client()
    client.register_compute_graph(g)
    invocation_id = client.invoke_graph_with_object(
        g.name,
        block_until_done=True,
        url="https://www.cidrap.umn.edu/avian-influenza-bird-flu/h5n1-avian-flu-virus-detected-wastewater-10-texas-cities",
    )
    output: List[Audio] = client.graph_outputs(
        g.name, invocation_id, fn_name="generate_tts"
    )
    if len(output) > 0:
        with open("tensorlake-daily-saved.mp3", "wb") as f:
            f.write(output[0].file.data)
    else:
        print("No output found")
