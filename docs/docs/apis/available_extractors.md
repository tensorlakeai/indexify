# Available Extractors

Indexify comes with some pre-built extractors that you can download and deploy with your installation. We keep these extractors always updated with new releases of models and our internal code.

## Embedding Extractors
<ul>
  <li>
    <details>
    <summary>ColBERT</summary>
      <ul>
        <li><p>This extractor extracts an embedding for a piece of text. It uses the huggingface <a href='https://huggingface.co/colbert-ir/colbertv2.0'>Colbert v2 model </a> It encodes each passage into a matrix of token-level embeddings. Then at search time, it embeds every query into another matrix and efficiently finds passages that contextually match the query using scalable vector-similarity (MaxSim) operators.</p></li>
      </ul>
    </details>
  </li>
</ul>
<ul>
  <li>
    <details>
    <summary>E5</summary>
      <ul>
        <li><p>A good small and fast general model for similarity search or downstream enrichments. Based on <a href='https://huggingface.co/intfloat/e5-small-v2'>E5_Small_V2 </a> which only works for English texts. Long texts will be truncated to at most 512 tokens.</p></li>
      </ul>
    </details>
  </li>
</ul>
<ul>
  <li>
    <details>
    <summary>Hash</summary>
      <ul>
        <li><p>This extractor extractors an "identity-"embedding for a piece of text, or file. It uses the sha256 to calculate the unique embeding for a given text, or file. This can be used to quickly search for duplicates within a large set of data.</p></li>
      </ul>
    </details>
  </li>
</ul>
<ul>
  <li>
    <details>
    <summary>Jina</summary>
      <ul>
        <li><p>This extractor extractors an embedding for a piece of text. It uses the huggingface <a href = 'https://huggingface.co/jinaai/jina-embeddings-v2-base-en'>Jina model</a> which is an English, monolingual embedding model supporting 8192 sequence length. It is based on a Bert architecture (JinaBert) that supports the symmetric bidirectional variant of ALiBi to allow longer sequence length.</p></li>
      </ul>
    </details>
  </li>
</ul>
<ul>
  <li>
    <details>
    <summary>Sentence Transformers - MiniLML6, MPnet</summary>
      <ul>
        <li><p>MiniLML6 - This extractor extractors an embedding for a piece of text. It uses the huggingface MiniLM-6 model, which is a tiny but very robust emebdding model for text.</p></li>
        <li><p>MPnet - This is a sentence embedding extractor based on the <a href = 'https://huggingface.co/sentence-transformers/paraphrase-multilingual-mpnet-base-v2'>MPNET Multilingual Base V2</a>. This is a sentence-transformers model: It maps sentences & paragraphs to a 768 dimensional dense vector space and can be used for tasks like clustering or semantic search. It's best use case is paraphrasing, but it can also be used for other tasks.</p></li>
      </ul>
    </details>
  </li>
</ul>
<ul>
  <li>
    <details>
    <summary>Scibert</summary>
      <ul>
        <li><p>This is the pretrained model presented in <a href = 'https://www.aclweb.org/anthology/D19-1371/'> SciBERT: A Pretrained Language Model for Scientific Text </a>, which is a BERT model trained on scientific text. Works best with scientific text embedding extraction.</p></li>
      </ul>
    </details>
  </li>
</ul>

## Audio
<ul>
  <li>
    <details>
    <summary>Whisper</summary>
      <ul>
        <li><p>This extractor converts extracts transcriptions from audio. The entire text and chunks with timestamps are represented as metadata of the content.</p></li>
      </ul>
    </details>
  </li>
</ul>

## Invoice
<ul>
  <li>
    <details>
    <summary>Chord</summary>
      <ul>
        <li><p>This extractor parses pdf or image form of invoice which is provided in JSON format. It uses the pre-trained <a href ='https://huggingface.co/naver-clova-ix/donut-base-finetuned-cord-v2'> donut cord fine-tune model from huggingface </a>. This model is specially good at extracting list of product and its price from invoice.</p></li>
      </ul>
    </details>
  </li>
</ul>
<ul>
  <li>
    <details>
    <summary>Donut</summary>
      <ul>
        <li><p>TThis extractor parses some invoice-related data from a PDF. It uses the pre-trained <a href='https://huggingface.co/docs/transformers/model_doc/donut'>donut model from huggingface</a>.</p></li>
      </ul>
    </details>
  </li>
</ul>

* Nougat

## Web Extractors
* Wikipedia
