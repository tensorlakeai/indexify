# %%
from indexify import Repository, list_repositories
import random

# %%
repo = Repository(name='my_repo')
print(list_repositories())

# %%
lucky_number = random.randint(1, 100)
doc = {
    # "text": "Indexify is amazing!",
    "text": f"My lucky number is {lucky_number}!",
    "metadata": {
        "key": "k1",
        "lucky_number": lucky_number,
    }
}

# %%
repo.add_documents(doc)
