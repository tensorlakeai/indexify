# Indexify

![Tests](https://github.com/diptanu/indexify/actions/workflows/test.yaml/badge.svg?branch=main)

Indexify is a knowledge and memory retrieval service for Large Language Models. It faciliates in-context learning of LLMs by providing relevant context in a prompt or expsing relevant memory to AI agents.
It also faciliates efficient execution of fine tuned/pre-trained embedding models and expose them over APIs. Several state of the art retreival algorithms are implemented to provide a batteries-included retrieval experience.

Currently for production use-case, the embedding generation APIs are stable, while the other features are coming along.

## Why Use Indexify
1. Create indexes from documents and search them to retrieve relevant context for Large Language Model based inference.
2. Efficient execution of pre-trained and fine-tuned embedding models in a standalone service.
3. APIs to retreive context from indexes or generate embeddings from any application runtime.

## Getting Started

To get started follow our [documentation](https://getindexify.ai/getting_started/).

## Documentation

Our comprehensive documentation is available - https://getindexify.ai

## Contributions
Please open an issue to discuss new features, or join our Discord group. Contributions are welcome, there are a bunch of open tasks we could use help with! 


## Coming Soon 
* Suppport for hardware acceleration, and using ONNX runtime for faster execution on CPUs.
* Retrieval strategies for dense embeddings, and a plugin mechanism to add new strategies.
* Resource Usage of Embedding Models.
* Asynchronous Embedding Generation for large corpuses of documents.
* Real Time Index updates
* Retreival of temporal context.

## Contact 
Join the Discord Server - https://discord.gg/mrXrq3DmV8 <br />
Email - diptanuc@gmail.com, Kanchi.shah957@gmail.com
