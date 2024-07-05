from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'pdf_text_extractor'
extraction_policies:
  - extractor: 'tensorlake/marker'
    name: 'pdf_to_text'
    input_params:
      batch_multiplier: 2
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)