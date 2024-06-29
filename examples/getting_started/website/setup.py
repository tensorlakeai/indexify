from indexify import IndexifyClient, ExtractionGraph

client = IndexifyClient() #(1)!

def create_extraction_graph():
    extraction_graph = ExtractionGraph.from_yaml_file("graph.yaml") #(2)!
    client.create_extraction_graph(extraction_graph) #(3)!

if __name__ == "__main__":
    create_extraction_graph()
