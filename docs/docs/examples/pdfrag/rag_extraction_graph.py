from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'rag_pipeline'
extraction_policies:
  - extractor: 'tensorlake/pdfextractor'
    name: 'pdf_to_text'
  - extractor: 'tensorlake/chunk-extractor'
    name: 'text_to_chunks'
    input_params:
      text_splitter: 'recursive'
      chunk_size: 1000
      overlap: 200
    content_source: 'pdf_to_text'
  - extractor: 'tensorlake/minilm-l6'
    name: 'chunks_to_embeddings'
    content_source: 'text_to_chunks'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)