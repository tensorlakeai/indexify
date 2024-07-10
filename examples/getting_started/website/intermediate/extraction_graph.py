from indexify import ExtractionGraph, IndexifyClient

client = IndexifyClient()

extraction_graph_spec = """
name: 'pdfqa'
extraction_policies:
  - extractor: 'tensorlake/marker' # (2)!
    name: 'mdextract'
  - extractor: 'tensorlake/chunk-extractor' #(3)!
    name: 'chunker'
    input_params:
        chunk_size: 1000
        overlap: 100
    content_source: 'mdextract'
  - extractor: 'tensorlake/minilm-l6' #(4)!
    name: 'pdfembedding'
    content_source: 'chunker'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)