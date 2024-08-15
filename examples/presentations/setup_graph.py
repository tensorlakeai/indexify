from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

def setup_graph(file_path):
    extraction_graph = ExtractionGraph.from_yaml_file(file_path)
    client.create_extraction_graph(extraction_graph)

if __name__ == "__main__":
    setup_graph("graph.yaml")
