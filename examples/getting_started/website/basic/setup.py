from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient()

def create_extraction_graph():
    extraction_graph = ExtractionGraph.from_yaml_file("graph.yaml")
    client.create_extraction_graph(extraction_graph)

if __name__ == "__main__":
    create_extraction_graph()

