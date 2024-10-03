from indexify import indexify_function, Graph
from pydantic import BaseModel

class Audio(BaseModel):
    file: bytes

@indexify_function()
def scrape_website(url: str) -> str:
    import requests
    return requests.get(f"http://r.jina.ai/{url}").text

@indexify_function()
def summarize_text(text: str) -> str:
    from openai import OpenAI
    completion = OpenAI().chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Generate a summary of this website. Don't add asterisks or any other markdown to the text. Keep the summary short. Write something funny and light-hearted about the topic."},
            {"role": "user", "content": text},
        ],
    )
    return completion.choices[0].message.content

@indexify_function()
def create_audio(summary: str) -> Audio:
    import elevenlabs
    from elevenlabs import save

    voice = "Rachel"  # You can choose a different voice if needed
    client = elevenlabs.ElevenLabs()
    audio = client.generate(text=summary, voice=voice)
    save(audio, "tensorlake-daily.mp3")
    with open("tensorlake-daily.mp3", "rb") as f:
        return Audio(file=f.read())
    return None

g = Graph(name="website-summarizer", start_node=scrape_website)
g.add_edge(scrape_website, summarize_text)
g.add_edge(summarize_text, create_audio)


if __name__ == "__main__":
    #g.run(url="https://en.wikipedia.org/wiki/Golden_State_Warriors")
    from indexify import RemoteGraph
    RemoteGraph.deploy(g, server_url="http://localhost:8900")
    graph = RemoteGraph.by_name(name="website-summarizer", server_url="http://localhost:8900")
    invocation_id = graph.run(block_until_done=True, url="https://en.wikipedia.org/wiki/Golden_State_Warriors")
    summary = graph.get_output(invocation_id, "summarize_text")
    print(summary)
    audio = graph.get_output(invocation_id, "create_audio")
    from elevenlabs import play
    play(audio[0].file)
