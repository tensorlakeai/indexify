# %%
from indexify import Repository

# %%
repo = Repository()

# %%
d = {
    "text": "Indexify is amazing!", 
    "metadata": {
        "key": "k1"
    } 
}

# %%
repo.add_documents(d)
