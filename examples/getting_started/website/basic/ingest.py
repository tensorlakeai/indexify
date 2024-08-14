from indexify import IndexifyClient, ExtractionGraph
from langchain_community.document_loaders import WikipediaLoader

client = IndexifyClient()

def load_data(player):
    docs = WikipediaLoader(query=player, load_max_docs=1).load()

    for doc in docs:
        client.add_documents("wiki_extraction_pipeline", doc.page_content)

if __name__ == "__main__":
    load_data("Kevin Durant")
    load_data("Stephen Curry")

