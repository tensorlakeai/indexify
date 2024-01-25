# Extractors

Extractors are used for structured extraction from un-structured data of any modality. For example, line items of an invoice as JSON, objects in a video, embedding of text in a PDF, etc. 

## Extractor Input and Ouput 

Extractors consume `Content` which contains the raw data and they product a list of Content which can either produce more content and features from them. For example, a PDF document can produce 10 `Content` each being a chunk of text and the corresponding embeddings of the chunk.

A single extractor can produce more than one indexed features, so you can expect some extractors to produce many different JSON metadata or embedding from a single document or other content.

![High Level Concept](../images/Content_AI_Content.png)

Extractors are typically built from a AI model and some additional pre and post processing of content.

## Running Extractors

### From Source Code
```shell
indexify extractor extract --extractor-path </path/to/extractor_file.py:ClassNameOfExtractor> --text "hello world"
```
This will invoke the extractor in `extractor_file.py` and create a `Content` with *hello world* as the payload. 

If you want to pass in the contents of a file into the payload use `--file </path/to/file>`

### From Packaged Containers

Extractors are packaged in docker containers so they can be tested locally for evaluation and also deployed in production with ease.

```shell
indexify extractor extract --name diptanu/minilm-l6-extractor --text "hello world"
```
