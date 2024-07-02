from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'pdf_translator_gpt4o'
extraction_policies:
  - extractor: 'tensorlake/openai'
    name: 'pdf_to_french'
    input_params:
      model_name: 'gpt-4o'
      key: 'YOUR_OPENAI_API_KEY'
      system_prompt: 'Translate the content of the following PDF from English to French. Maintain the original formatting and structure as much as possible. Provide the translation in plain text format.'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)