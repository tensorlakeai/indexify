# Getting Started

Indexify is very easy to get started with Docker Compose.

### Clone the Repository
```
git clone https://github.com/diptanu/indexify.git
```

### Start the Service using Docker Compose
```
docker compose up indexify
```
This starts the Indexify server at port `8900` and additionally starts a postgres server for storing metadata and qdrant for storing embeddings.

That's it! Let's explore some basic document storage and retrieval APIs

Data Repositories are logical buckets that holds content. Indexify starts with a default data repository, we can start adding texts to it straight away.

#### Add some Texts
=== "curl"

    ```
    curl -v -X POST http://localhost:8900/repository/add_texts
    -H "Content-Type: application/json"
    -d '{
            "documents": [
            {"text": "Indexify is amazing!", 
            "metadata":{"key": "k1"}
            }
        ]}'
    ```

The default data repository is configured to have an extractor which populates an index for searching content.

#### Query the Index
=== "curl"
    ```
    curl -v -X GET http://localhost:8900/index/search
    -H "Content-Type: application/json"
    -d '{
            "index": "default/default",
            "query": "good", 
            "k": 1
        }'
    ```


#### Start Using Memory
- Create Memory Session
Memory is usually stored for interactions of an agent with a user or in a given context. Related messages are grouped in Indexify as a `Session`, so first create a session!
=== "curl"
    ```
    curl -X POST http://localhost:8900/memory/create
    -H "Content-Type: application/json" 
    -d '{}'
    ```
    You can optionally pass in a `session-id` while creating a session if you would
    like to use a user-id or any other application id to retrieve and search for memory.


- Add Memory Events
=== "curl"
    ```
    curl -X POST http://localhost:8900/memory/add
    -H "Content-Type: application/json" 
    -d '{
            "session_id": "77569cf7-8f4c-4f4b-bcdb-aa54355eee13",
            "messages": [
                {
                "role": "human",
                "text": "Indexify is amazing!",
                "metadata": {}
                },
                {
                "role": "ai",
                "text": "How are you planning on using Indexify?!"
                "metadata": {}
                }
        ]}'
    ```


- Retrieve All Memory Events
You can retrieve all the previously stored messages in Indexify for a given session.
=== "curl"
    ```
    curl -X GET http://localhost:8900/memory/get
    -H "Content-Type: application/json" 
    -d '{
            "session_id": "77569cf7-8f4c-4f4b-bcdb-aa54355eee13"
        }
    ```


- Retrieve using search
Now, search for something specific! Every memory session comes with a default index, you could also
add more extractors to a session, and add more than one index or extract other specific information
from the messages(like named entities - places, names, etc).
=== "curl"
    ```
    curl -X GET http://localhost:8900/memory/search
    -H "Content-Type: application/json" 
    -d '{
            "session_id": "77569cf7-8f4c-4f4b-bcdb-aa54355eee13",
            "query": "indexify"
        }
    ```
