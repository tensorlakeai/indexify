# %%
from indexify import IndexifyClient, Repository

repo = Repository()
repo.add_documents([
    {"text": "Indexify is amazing!"},
    {"text": "Indexify is a retrieval service for LLM agents!"},
    {"text": "Kevin Durant is the best basketball player in the world."}
])

# %%
client = IndexifyClient()
print(client.extractors)

# %%
# FIXME: this throws a 500 error if the binding exists
repo.bind_extractor("EntityExtractor", index_name="entityindex")

# FIXME: this throws a 500 error if the binding exists
# FIXME: also throws a 500 if the index name doesn't exist
#         -> b'index `embeddingindex` not found'
repo.bind_extractor("MiniLML6", index_name="embeddingindex")

# %%
print(repo.extractor_bindings)

# %%
attributes = repo.query_attribute("entityindex")
print('Attributes:', *attributes, sep='\n')

# %%
search_results = repo.search_index("embeddingindex", "sports", 3)
print('Search results:', *search_results, sep='\n')

# %%
repo.add_documents(
    {"text": "Steph Curry is also an amazing player!"}
)
search_results = repo.search_index("embeddingindex", "sports", 3)
print('Updated search results:', *search_results, sep='\n')
