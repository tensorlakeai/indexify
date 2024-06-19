from indexify import IndexifyClient, ExtractionGraph
import requests
client = IndexifyClient()

extraction_graph_spec = """
name: 'audiosummary'
extraction_policies:
   - extractor: 'tensorlake/whisper-asr'
     name: 'transcription'
   - extractor: 'tensorlake/summarization'
     name: 'summarizer'
     input_params:
        max_length: 400
        min_length: 300
        chunk_method: str = 'recursive'
     content_source: 'transcription'
   - extractor: 'tensorlake/minilm-l6'
     name: 'minilml6'
     content_source: 'summarizer'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)

with open("sample.mp3", 'wb') as file:
  file.write((requests.get("https://extractor-files.diptanu-6d5.workers.dev/sample-000009.mp3")).content)
content_id = client.upload_file("audiosummary", "sample.mp3")

client.wait_for_extraction(content_id)
print("transcription ----")
print(client.get_extracted_content(content_id, "audiosummary", "transcription"))
print("summary ----")
print(client.get_extracted_content(content_id, "audiosummary", "summarizer"))


context = client.search_index(name="audiosummary.minilml6.embedding", query="President of America", top_k=1)




