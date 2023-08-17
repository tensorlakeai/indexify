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
print(repo.extractors)

# %%
attributes = repo.query_attribute("entityindex")
print('Attributes: ')
for a in attributes:
    print(a)

# %%
search_results = repo.search_index("embeddingindex", "Indexify", 10)
print('Search results: ')
for r in search_results:
    print(r)

# %%
# FIXME: this is not running the bound extractor automatically
repo.add_documents(
    {"text": "Steph Curry is also an amazing player!"}
)
search_results = repo.search_index("embeddingindex", "basketball", 10)
print('Updated search results: ')
for r in search_results:
    print(r)
