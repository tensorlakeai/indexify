# Extractors and Executors

Extractors are used to extract information from the documents in a data repository. The extracted information can be structured (entities, keywords, etc.) or unstructured (embeddings) in nature, and is stored in an index for retrieval.

Extractors are typically built from a AI model and some additional pre and post processing of content. 

Extractors can be parameterized as well when a binding of the extractor is created for a repository. Extractors parameterized differently can create different variations of indexes. For example, you could change the text splitting strategy of the `MiniLML6` extractor based on the nature of the documents being indexed.

The following extractors are available out of the box:

### Embedding Extractors
Indexify bundles a few different Embedding Extractors to embed documents. In addition to the models themselves, the text splitting algorithm can be configured. If you want to use an embedding model besides the one directly supported here, please send a pull request or file an issue.

#### OpenAI Text Embedding 

#### Instructor XL

#### MiniLML6 Extractor

#### DPR Extractor

#### MPNet Extractor

### NER Extractor
We use the `span-marker-xlm-roberta-base-multinerd` model for NER. 
