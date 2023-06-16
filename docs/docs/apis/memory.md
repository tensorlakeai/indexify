# Memory APIs

Store and Retrieve long term memory of agent interactions using the Memory APIs, therefore making your LLM agents stateless and remember information related to the current context by searching through memory.

Create a new Memory Session to begin logging memory events for a user or a co-pilot session, and from there log messages to the session.


## Create Memory Session
=== "curl"
    ``` console
    curl -X POST http://localhost:8900/memory/create
    -H "Content-Type: application/json" -d '{}'
    
    ```

#### Output 
``` json
{
    "session_id":"81a46dcf-7808-48f2-87f9-e5a83c474519"
}
```
### Request Body
* `session_id`(optional): ID of the memory session. Default - Creates a UUID if it's not provided.
* `repository`(optional): Name of the repository where the session should be created. Default - `default`
* `extractor`(optional): An extractor to use for enriching the messages of the session. Default- Vector Index with the default embedding model.
* `metadata`(optional): Key/Value pairs of opaque metadata. The value can be any valid json.

## Add Memory
=== "curl"
    ```console
    curl -X POST http://localhost:8900/memory/add
    -H "Content-Type: application/json" 
    -d '{"session_id": "77569cf7-8f4c-4f4b-bcdb-aa54355eee13", "messages": [{"role": "Human", "text": "Indexify is amazing!", "metadata": {}}]}'
    ```
### Request Body
* `session_id` - Memory session identifier.
* `messages` -
    * `role` - Role (ie: `human`, `ai`).
    * `text` - Conversation text.
    * `metadata` - Key/Value pair of metadata associated with the text. 

## Retrieve Memory
=== "curl"
    ```
    curl -X GET http://localhost:8900/memory/get
    -H "Content-Type: application/json"
    -d '{
            "session_id": "de7970cb-68db-4c6d-9c2b-96755dc6f9e3"
        }'
    ```
### Request Body
* `session_id` - UUID corresponding to memory session.

## Search Memory
=== "curl"
    ```
    curl -X GET http://localhost:8900/memory/search
    -H "Content-Type: application/json"
    -d '{
            "session_id": "de7970cb-68db-4c6d-9c2b-96755dc6f9e3",
            "query": "fruit",
            "k": 10
        }'
    ```
### Request Body
* `session_id` - UUID corresponding to memory session.
* `query` - Query string.
* `k` - Top k responses.
