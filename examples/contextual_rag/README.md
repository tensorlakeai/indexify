# Contextual RAG

This example demonstrates setting up a Contextual RAG pipeline, introduced in [this](https://www.anthropic.com/news/contextual-retrieval) Anthropic blogpost.

We will be building a simple contextual chunker which uses the Prompt Caching feature supported by Anthropic's Claude API, and write the data for querying
to a local lancedb vector store.

This pipeline can be run locally for quick testing and the lancedb vector store can be queried from any application.

## Run Application Locally

1. Simple install a new virutal environment for python dependencies.
2. Use the requirements file to install them, `pip install -r requirements.txt`.
3. Run the python workflow file, `python workflow.py`

## Explanation of the Workflow
The workflow involves the following steps,
1. `generate_chunk_contexts` - Chunks the input document, and generates contexts using Anthropic's model API.
2. `TextEmbeddingExtractor` - Computes an embedding for the chunk contexts.
3. `LanceDBWriter` - Writes the embeddings to the vectore store. 

## Customization

Copy the folder, modify the code as you like and simply test the new Graph.

This example calls for a local lancedb vector store to write the contextual embedding data. We use a local instance (written to disk) to
demonstrate how Indexify pipelines work. For a production deployment we would replace the `LanceDBWriter` to call the production
deployment of the vector store.

This example also relies on `sentence-transformers/all-MiniLM-L6-v2` to perform the embedding. We can replace this to use services like
OpenAI or Amazon Bedrock, or run a custom executor that can be deployed as needed on a GPU accelerated resource.