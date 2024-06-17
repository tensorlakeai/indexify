from indexify import IndexifyClient, ExtractionGraph
client = IndexifyClient()

extraction_graph_spec = """
name: 'sportsknowledgebase'
extraction_policies:
   - extractor: 'tensorlake/minilm-l6'
     name: 'minilml6'
"""
extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph) 
print("indexes", client.indexes())

content_ids = client.add_documents("sportsknowledgebase", ["Adam Silver is the NBA Commissioner", "Roger Goodell is the NFL commisioner"])
client.wait_for_extraction(content_ids)

context = client.search_index(name="sportsknowledgebase.minilml6.embedding", query="NBA commissioner", top_k=1)
print(context)
