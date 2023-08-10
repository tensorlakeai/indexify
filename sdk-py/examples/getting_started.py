# %%
from indexify import IndexifyClient, Repository

repo = Repository()
repo.add_documents([
    {"text": "Indexify is amazing!", "metadata": {"topic": "llm"}},
    {"text": "Indexify is a retrieval service for LLM agents!", "metadata": {"topic": "ai"}},
    {"text": "Kevin Durant is the best basketball player in the world.", "metadata": {"topic": "nba"}}
])

# %%
client = IndexifyClient()
print(client.extractors)

# %%
# FIXME: this throws a 500 error
repo.add_extractor("EntityExtractor", index_name="entityindex", filter={"content_type": "text"})

# FIXME: this throws a 500 error
repo.add_extractor("MiniLML6", index_name="embeddingindex", filter={"content_type": "text"})

# %%
print(repo.extractors)

# %%
attributes = repo.query_attribute("entityindex")
print(attributes)

# %%
search_results = repo.search_index("embeddingindex", "Indexify", 10)
print(search_results)

# # %%
# import random
# lucky_number = random.randint(1, 100)
# doc = {
#     # "text": "Indexify is amazing!",
#     "text": f"My lucky number is {lucky_number}!",
#     "metadata": {
#         "key": "k1",
#         "lucky_number": lucky_number,
#     }
# }

# # # %%
# repo.add_documents(doc)
