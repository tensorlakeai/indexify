from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'pdf_chunker'
extraction_policies:
  - extractor: 'tensorlake/marker'
    name: 'pdf_to_text'
  - extractor: 'tensorlake/chunk-extractor'
    name: 'text_to_chunks'
    input_params:
      text_splitter: 'recursive'
      chunk_size: 1000
      overlap: 200
    content_source: 'pdf_to_text'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)