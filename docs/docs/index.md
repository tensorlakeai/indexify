# Welcome to Indexify

Indexify is a knowledge and memory retrieval service for Large Language Models. It facilitates LLMs and Agents to respond to queries related to private data that lives in 
various private databases and documents. LLM applications can use Indexify to provide context and memory to an LLM about a query in real time, and let the service deal with ingesting external data and agent memory reliably and keeping indexes udpated. 

The service also facilitates efficient execution of fine tuned/pre-trained embedding and other retreival models and expose them over APIs. Several state of the art retreival algorithms are implemented to provide a batteries-included retrieval experience.

## Why use Indexify
* **Knowledge Base for LLMs:** Provide LLM based applications and agents with knowledge and context from documents and structured data to improve accuracy of inference queries.
* **Improved Embedding Generation:** Out of the box support for state of the art embedding models and improve them further by fine tuning them on the target knowledge base which will be used for queries.
* **Memory Engine for Co-Pilot agents:** Provide LLM agents and co-pilot temporal context by storage and retreival of relevant memory for personalization and improved accuracy during a co-pilot session.
* **Real Time Data:** Indexify will keep indexes updated if the source of the data is known, such as a S3 bucket or a database, so LLM models can answer queries that require real time information about the world.
* **Secure Access:** Indexes can be put behind RBAC so that sensistive data is exposed to only certain applications. 

## Next Steps
Read the [Getting Started](getting_started.md) to learn how to use Indexify.
