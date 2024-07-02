from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

extraction_graph_spec = """
name: 'yolo_detector'
extraction_policies:
  - extractor: 'tensorlake/yolo-extractor'
    name: 'image_object_detection'
    input_params:
      model_name: 'yolov8n.pt'
      conf: 0.25
      iou: 0.7
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)