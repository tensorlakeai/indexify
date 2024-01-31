# %%
from indexify import IndexifyClient, Repository

repo = Repository()
repo.add_documents(
    [
        "Indexify is amazing!",
        "Indexify is a retrieval service for LLM agents!",
        "Kevin Durant is the best basketball player in the world.",
    ]
)

# %%
client = IndexifyClient()
print(client.extractors)

# %%
# FIXME: this throws a 500 error if the binding exists
repo.bind_extractor("EntityExtractor", index_name="entities")

# FIXME: this throws a 500 error if the binding exists
# FIXME: also throws a 500 if the index name doesn't exist
#         -> b'index `embeddings` not found'
repo.bind_extractor("MiniLML6", index_name="embeddings")

# %%
print(repo.extractor_bindings)

# %%
attributes = repo.query_attribute("entities")
print("Attributes:", *attributes, sep="\n")

# %%
search_results = repo.search_index("embeddings", "sports", 3)
print("Search results:", *search_results, sep="\n")

# %%
repo.add_documents({"text": "Steph Curry is also an amazing player!"})
search_results = repo.search_index("embeddings", "sports", 3)
print("Updated search results:", *search_results, sep="\n")

# %%
repo.add_documents(
    [
        {
            "text": "The Cayuga was launched in 2245.",
            "labels": {"url": "https://memory-alpha.fandom.com/wiki/USS_Cayuga"},
        },
    ]
)

# %%
repo.bind_extractor(
    "MiniLML6",
    index_name="star_trek_embeddings",
    include=dict(url="https://memory-alpha.fandom.com/wiki/USS_Cayuga"),
)

print(repo.extractor_bindings)

# %%
search_results = repo.search_index("star_trek_embeddings", "starships", 3)
print("Star Trek results:", *search_results, sep="\n")
