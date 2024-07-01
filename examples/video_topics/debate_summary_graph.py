from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'debate_summarizer'
extraction_policies:
  - extractor: 'tensorlake/audio-extractor'
    name: 'video_to_audio'
  - extractor: 'tensorlake/asrdiarization'
    name: 'speech_recognition'
    content_source: 'video_to_audio'
  - extractor: 'tensorlake/mistral'
    name: 'topic_extraction'
    input_params:
      model_name: 'mistral-large-latest'
      key: 'YOUR_MISTRAL_API_KEY'
      system_prompt: 'Extract the main topics discussed in this debate transcript. List each topic as a brief phrase or title.'
    content_source: 'speech_recognition'
  - extractor: 'tensorlake/mistral'
    name: 'topic_summarization'
    input_params:
      model_name: 'mistral-large-latest'
      key: 'YOUR_MISTRAL_API_KEY'
      system_prompt: 'Summarize the discussion on the main topics from the debate transcript. Provide key points and arguments from both sides.'
    content_source: 'speech_recognition'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)