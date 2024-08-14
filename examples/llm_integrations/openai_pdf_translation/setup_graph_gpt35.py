from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'pdf_translator_gpt35'
extraction_policies:
  - extractor: 'tensorlake/pdfextractor'
    name: 'pdf_to_text'
  - extractor: 'tensorlake/openai'
    name: 'text_to_french'
    input_params:
      model: 'gpt-3.5-turbo'
      api_key: 'YOUR_OPENAI_API_KEY'
      system_prompt: 'You are a professional translator. Translate the following English text to French. Maintain the original formatting and structure as much as possible.'
    content_source: 'pdf_to_text'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)