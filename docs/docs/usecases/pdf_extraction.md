# 📄 PDF Extraction!

Indexify provides extractors that extract text, images, and tables from PDF documents. Some extractors also convert PDFs to markdown documents. You can build complex pipelines that can extract and write tabular information from PDF documents in structured stores or extract embedding from texts in the documents. PDF is a complex document type; we offer many different extractors suitable to various use cases.

![PDF Extraction High Level](../images/PDF_Usecase.png)

## 🌟 What Can You Achieve with Indexify?

With Indexify, you can accomplish the following with your PDFs:

1. 🔍 **Data Extraction:** Easily extract specific information from PDFs, such as fields from tax documents, healthcare records, invoices, and receipts. Use Cases: Automate Manual Data Entry
2. 📚 **Document Indexing:** Build searchable indexes on vector stores and structured stores by combining PDF extractors with chunking, embedding, and structured data extractors. Use Cases: RAG Applications, Assitants, etc.
3. 🤖 **Document Q&A:** Get Answers to specific Questions from Documents without creating indexing. 

## 🔧 The Extraction Pipeline: A Three-Stage Process

PDF Extraction Pipelines are usually composed of three stages. You can use one or more of these stages depending on your usecase - 

1. 📄 **Content Extraction Stage:** Start by extracting raw content from your PDFs using extractors like `pdf/pdf-extractor` or `pdf/markdown`. These extractors will retrieve text, images, and tables from your documents.
2. ✂️ **Content to Chunk Extraction Stage:** Break down the extracted content into manageable chunks using extractors like `text/chunking`. This stage helps organize your content into coherent and contextually relevant pieces, making it easier to process and understand.
3. 🧠 **Chunk to Embedding Extraction Stage:** Convert the chunks into vector embeddings using extractors like `embedding/minilm-l6` or `embedding/arctic`. 

## 🎞️ Image Extraction
If you would like to extract images from PDF, the best extractor to use is `tensorlake/pdf-extractor` It automatically extracts images from documents and writes them into blob stores. Once images are extracted, you could create pipelines for many downstream image tasks - Embedding using CLIP for semantic search, visual understading of images using GPT4V, Cog or Moondream, or object detection using YOLO.Indexify provides extractors for all these downstream tasks.

## 📈 Table Extraction
Tables are automatically extracted by `tensorlake/pdf-extractor` as JSON metadata. You can query the metadata associated with documents by calling the Retrieval APIs. 

## 🌈 Explore the PDF Extractor Landscape

We offer a wide range of PDF extractors to suit your specific needs. Whether you're dealing with invoices, scientific papers, or scanned documents, we've got you covered. Here's a quick overview of our extractor lineup:

| Extractor                                  | Output Type        | Output Example                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | Best For                        | Example Usage                                                                                                                                                                                                                                      |
|-------------------------------------------|--------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| tensorlake/layoutlm-document-qa-extractor | metadata           | [Feature(feature_type='metadata', name='metadata', value={'query': 'What is the invoice total?', 'answer': '$93.00', 'page': 0, 'score': 0.9743825197219849}, comment=None)]                                                                                                                                                                                                                                                                                                    | Invoices Question Answering     | [Schema based HOA Documents](../examples/HOA_Invoice_Data_Extraction.ipynb)                                                                                                                                                                        |
| tensorlake/pdf-extractor                  | text, image, table | [Content(content_type='text/plain', data=b'I love playing football.', features=[Feature(feature_type='metadata', name='text', value={'page': 1}, comment=None)], labels={})]                                                                                                                                                                                                                                                                                                    | Scientific Papers, Tabular Info | [Schema based HOA Documents](../examples/HOA_Invoice_Data_Extraction.ipynb), [Multi-state Terms Documents](../examples/Sixt.ipynb), [Scientific Journals](../examples/Scientific_Journals.ipynb), [SEC 10-K docs](../examples/SEC_10_K_docs.ipynb) |
| tensorlake/ocrmypdf                       | text               | [Content(content_type='text/plain', data=b'I love playing football.', features=[Feature(feature_type='metadata', value={'page': 1}, comment=None)], labels={})]                                                                                                                                                                                                                                                                                                                 | Photocopied/Scanned PDFs on CPU |                                                                                                                                                                                                                                                    |
| tensorlake/easyocr                        | text               | [Content(content_type='text/plain', data=b'I love playing football.', features=[Feature(feature_type='metadata', name='text', value={'page': 1}, comment=None)], labels={})]                                                                                                                                                                                                                                                                                                    | Photocopied/Scanned PDFs on GPU |                                                                                                                                                                                                                                                    |
| tensorlake/unstructuredio                 | text               |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |                                 |                                                                                                                                                                                                                                                    |
| tensorlake/markdown                       | text, table        | [Content(content_type='text/plain', data=b'I love playing football.', features=[Feature(feature_type='metadata', name='text', value={'language': 'English', 'filetype': 'pdf', 'toc': [], 'pages': 1, 'ocr_stats': {'ocr_pages': 0, 'ocr_failed': 0, 'ocr_success': 0}, 'block_stats': {'header_footer': 2, 'code': 0, 'table': 0, 'equations': {'successful_ocr': 0, 'unsuccessful_ocr': 0, 'equations': 0}}, 'postprocess_stats': {'edit': {}}}, comment=None)], labels={})] | Structured & formatted PDF      |                                                                                                                                                                                                                                                    |

## 🚀 Get Started with PDF Extraction

You can test it locally:

1. Download a PDF Extractor:
   ```bash
   indexify-extractor download hub://pdf/pdf-extractor
   indexify-extractor join-server pdf-extractor.pdf_extractor:PDFExtractor
   ```

2. Load it in a notebook or terminal:
   ```python
   from indexify_extractor_sdk import load_extractor, Content
   extractor, config_cls = load_extractor("pdf-extractor.pdf_extractor:PDFExtractor")
   content = Content.from_file("/path/to/file.pdf")
   results =  extractor.extract(content)
   print(results)
   ```


## 🌐 Continuous PDF Extraction for Applications

We've made it incredibly easy to integrate Indexify into your workflow. Get ready to supercharge your document processing capabilities! 🔋

1. Start the Indexify Server:
   ```bash
   curl https://tensorlake.ai | sh
   ./indexify server -d
   ```

2. Start a long-running PDF Extractor:
   ```bash
   indexify-extractor download hub://pdf/pdf-extractor
   indexify-extractor join-server pdf-extractor.pdf_extractor:PDFExtractor
   ```

3. Create an Extraction Graph:
   ```python
   from indexify import IndexifyClient
   client = IndexifyClient()
   extraction_graph_spec = """
   name: 'pdfknowledgebase'
   extraction_policies:
      - extractor: 'tensorlake/pdf-extractor'
        name: 'my-pdf-extractor'
   """
   extraction_graph = ExtractionGraph.from_yaml(extraction_graph_spec)
   client.create_extraction_graph(extraction_graph)
   ```

4. Upload PDFs from your application:
   ```python
   from indexify import IndexifyClient
   client = IndexifyClient()
   content_id = client.upload_file("pdfknowledgebase", "/path/to/pdf.file")
   ```

5. Inspect the extracted content:
   ```python
   extracted_content = client.get_extracted_content(content_id=content_id)
   print(extracted_content)
   ```

With just a few lines of code, you can use data locked in PDFs in your applications. Example use-cases: automated data entry, intelligent document search, and effortless question answering. 

## 📚 Explore More Examples

We've curated a collection of inspiring examples to showcase the versatility of PDF extraction. Check out these notebooks:

- [Invoices](../examples/Invoices.ipynb): Extract and analyze invoice data like a pro! 💰📊
- [Terms and Condition Documents of Car Rental](../examples/Terms_and_Condition_Documents_of_Car_Rental.ipynb): Navigate the complex world of car rental agreements with ease. 🚗📜
- [Terms and Conditions Documents of Health Care Benefits](../examples/Terms_and_Conditions_Documents_of_Health_Care_Benefits.ipynb): Demystify health care benefits and make informed decisions. 🏥📄

