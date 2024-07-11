from indexify import IndexifyClient, ExtractionGraph
import requests

client = IndexifyClient()

extraction_graph_spec = """
name: 'imageknowledgebase'
extraction_policies:
   - extractor: 'tensorlake/yolo-extractor'
     name: 'object_detection'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)

with open("sample.jpg", 'wb') as file:
  file.write((requests.get("https://extractor-files.diptanu-6d5.workers.dev/people-standing.jpg")).content)
content_id = client.upload_file("imageknowledgebase", "sample.jpg")

client.wait_for_extraction(content_id)
detections = client.get_extracted_content(content_id, "imageknowledgebase", "object_detection")
print("objects detected ---")
print(detections)

