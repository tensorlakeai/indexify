# Available Extractors

Indexify comes with some pre-built extractors that you can download and deploy with your installation. We keep these extractors always updated with new releases of models and our internal code.

## Embedding Extractors
#### [ColBERT](https://github.com/tensorlakeai/indexify-extractors/tree/main/embedding-extractors/colbert)
This ColBERTv2-based extractor is a Python class that encapsulates the functionality to convert text inputs into vector embeddings using the ColBERTv2 model. It leverages ColBERTv2's transformer-based architecture to generate context-aware embeddings suitable for various natural language processing tasks.


#### [E5](https://github.com/tensorlakeai/indexify-extractors/tree/main/embedding-extractors/e5_embedding)
A good small and fast general model for similarity search or downstream enrichments.
Based on [E5_Small_V2](https://huggingface.co/intfloat/e5-small-v2) which only works for English texts. Long texts will be truncated to at most 512 tokens.


#### [Hash](https://github.com/tensorlakeai/indexify-extractors/tree/main/embedding-extractors/hash-embedding)
This extractor extractors an "identity-"embedding for a piece of text, or file. It uses the sha256 to calculate the unique embeding for a given text, or file. This can be used to quickly search for duplicates within a large set of data.


#### [Jina](https://github.com/tensorlakeai/indexify-extractors/tree/main/embedding-extractors/jina_base_en)
This extractor extractors an embedding for a piece of text.
It uses the huggingface [Jina model](https://huggingface.co/jinaai/jina-embeddings-v2-base-en) which is an English, monolingual embedding model supporting 8192 sequence length. It is based on a Bert architecture (JinaBert) that supports the symmetric bidirectional variant of ALiBi to allow longer sequence length.

#### [MiniLML6 - Sentence Transformer](https://github.com/tensorlakeai/indexify-extractors/tree/main/embedding-extractors/minilm-l6)
This extractor extractors an embedding for a piece of text.
It uses the huggingface [MiniLM-6 model](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2), which is a tiny but very robust emebdding model for text.

#### [MPnet - Sentence Transformer](https://github.com/tensorlakeai/indexify-extractors/tree/main/embedding-extractors/mpnet)
This is a sentence embedding extractor based on the [MPNET Multilingual Base V2](https://huggingface.co/sentence-transformers/paraphrase-multilingual-mpnet-base-v2).
This is a sentence-transformers model: It maps sentences & paragraphs to a 768 dimensional dense vector space and can be used for tasks like clustering or semantic search.
It's best use case is paraphrasing, but it can also be used for other tasks.

#### [OpenAI Embedding](https://github.com/tensorlakeai/indexify-extractors/tree/main/embedding-extractors/openai-embedding)
This extractor extracts an embedding for a piece of text.
It uses the OpenAI text-embedding-ada-002 model.

#### [SciBERT Uncased](https://github.com/tensorlakeai/indexify-extractors/tree/main/embedding-extractors/scibert)
This is the pretrained model presented in [SciBERT: A Pretrained Language Model for Scientific Text](https://www.aclweb.org/anthology/D19-1371/), which is a BERT model trained on scientific text.
Works best with scientific text embedding extraction.


## Audio
#### [Whisper](https://github.com/tensorlakeai/indexify-extractors/tree/main/whisper-asr)
This extractor converts extracts transcriptions from audio. The entire text and
chunks with timestamps are represented as metadata of the content.


## Video
#### [Frames](https://github.com/tensorlakeai/indexify-extractors/tree/main/video/frames)
Dissect a video into its individual frames and generate JPEG image content for each extracted frame. Comes with frame frequency and scene detection configuration that can be changed with input parameters.


## Invoice
#### [Donut CORD](https://github.com/tensorlakeai/indexify-extractors/tree/main/invoice-extractor/donut_cord)
This extractor parses pdf or image form of invoice which is provided in JSON format. It uses the pre-trained [donut cord fine-tune model from huggingface](https://huggingface.co/naver-clova-ix/donut-base-finetuned-cord-v2).
This model is specially good at extracting list of product and its price from invoice.

#### [Donut Invoice](https://github.com/tensorlakeai/indexify-extractors/tree/main/invoice-extractor/donut_invoice)
This extractor parses some invoice-related data from a PDF.
It uses the pre-trained [donut model from huggingface](https://huggingface.co/docs/transformers/model_doc/donut).


## Web Extractors

#### [Wikipedia](https://github.com/tensorlakeai/indexify-extractors/tree/main/web-extractors/wikipedia)
Extract text content from wikipedia html pages.