# %%
from indexify import Repository
import random

# %%
repo = Repository(name='my_repo')

# %%
doc = {
    # "text": "Indexify is amazing!",
    "text": f"My lucky number is {random.randint(1, 100)}!",
    "metadata": {
        "key": "k1"
    }
}

# %%
repo.add_documents(doc)
