# %%
from indexify import Repository

# %%
repo = Repository()

# %%
doc = {
    "text": "Indexify is amazing!",
    "metadata": {
        "key": "k1"
    }
}

# %%
repo.add_documents(doc)
