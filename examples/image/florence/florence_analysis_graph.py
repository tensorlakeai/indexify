from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'florence_image_analyzer'
extraction_policies:
  - extractor: 'tensorlake/florence'
    name: 'detailed_caption'
    input_params:
      task_prompt: '<MORE_DETAILED_CAPTION>'
  - extractor: 'tensorlake/florence'
    name: 'object_detection'
    input_params:
      task_prompt: '<OD>'
  - extractor: 'tensorlake/florence'
    name: 'referring_expression_segmentation'
    input_params:
      task_prompt: '<REFERRING_EXPRESSION_SEGMENTATION>'
      text_input: 'a green car'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)