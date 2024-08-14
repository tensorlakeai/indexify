import os
from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the YAML file relative to the script's location
yaml_file_path = os.path.join(script_dir, "graph_mm.yaml")

extraction_graph = ExtractionGraph.from_yaml_file(yaml_file_path)
client.create_extraction_graph(extraction_graph)