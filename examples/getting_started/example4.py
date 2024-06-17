pdfextractorfrom indexify import IndexifyClient, ExtractionGraph
import requests
client = IndexifyClient()

extraction_graph_spec = """
name: 'pdfqa'
extraction_policies:
   - extractor: 'tensorlake/pdf-extractor'
     name: 'docextractor'
"""

extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
client.create_extraction_graph(extraction_graph)


with open("sample.pdf", 'wb') as file:
  file.write((requests.get("https://extractor-files.diptanu-6d5.workers.dev/scientific-paper-example.pdf")).content)
content_id = client.upload_file("pdfqa", "sample.pdf")

client.wait_for_extraction(content_id)
print(client.get_extracted_content(content_id))

