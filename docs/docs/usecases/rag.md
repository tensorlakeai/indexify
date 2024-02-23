# RAG

Here we show an example of building a RAG application with Indexify. We are going to upload content about Kevin Durant from Wikipedia and ask questions about KD's career.

### Install the Indexify Extractor SDK
```bash
pip install indexify-extractor-sdk
```

### Start the Indexify Server
```bash
indexify server -d
```

### Download an Embedding Extractor
On another terminal start the embedding extractor which we will use to index text from the wikiepdia page.
```bash
indexify-extractor download hub://embedding/minilm-l6
```

### Upload Content
We will use the lanchain wikipedia loader to download content from wikipedia and upload to Indexify
```python
```

### Create an Extraction Policy 

### Perform RAG


