# 📄 PDF Extraction: Unleash the Power of Your Documents! 

Welcome to the world of PDF extraction with Indexify! 🎉 We're here to revolutionize the way you interact with your documents, making it easier than ever to extract valuable insights and perform powerful question answering. Get ready to dive into a realm of endless possibilities! 💡

![PDF Extraction High Level](../images/PDF_Usecase.png)

## 🌟 What Can You Achieve with Indexify?

With Indexify, you can accomplish the following with your PDFs:

1. 🔍 **Data Extraction:** Easily extract specific information from PDFs, such as fields from tax documents, healthcare records, invoices, and receipts. Say goodbye to manual data entry and hello to automated efficiency!
2. 📚 **Document Indexing:** Build comprehensive indexes on vector stores and structured stores by combining PDF extractors with chunking, embedding, and structured data extractors. Create a searchable knowledge base that's always at your fingertips!
3. 🤖 **Document Q&A:** Leverage the power of LLMs to query your document indexes and get accurate answers to your questions. It's like having a personal assistant that knows your documents inside out!

## 🔧 The Extraction Pipeline: A Three-Stage Journey

To unlock the full potential of your PDFs, we've designed a seamless three-stage extraction pipeline that will take you from raw documents to actionable insights:

1. 📄 **Content Extraction Stage:** Start by extracting raw content from your PDFs using extractors like `pdf/pdf-extractor` or `pdf/markdown`. These extractors will retrieve text, images, and tables from your documents, laying the foundation for further analysis.
2. ✂️ **Content to Chunk Extraction Stage:** Break down the extracted content into manageable chunks using extractors like `text/chunking`. This stage helps organize your content into coherent and contextually relevant pieces, making it easier to process and understand.
3. 🧠 **Chunk to Embedding Extraction Stage:** Convert the chunks into vector embeddings using extractors like `embedding/minilm-l6` or `embedding/arctic`. By transforming your content into numerical representations, you enable powerful similarity search and retrieval capabilities.

By chaining these stages together, you can create a powerful pipeline that enables question answering using the RAG (Retrieval-Augmented Generation) approach. Watch as your documents come to life, ready to answer any question you throw at them! 🚀

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

You can test it locally and unlock the secrets hidden within your documents:

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

1. Start the Indexify Server and Extraction Policies:
   ```bash
   curl https://tensorlake.ai | sh
   ./indexify server -d
   ```

2. Start a long-running PDF Extractor:
   ```bash
   indexify-extractor download hub://pdf/pdf-extractor
   indexify-extractor join-server pdf-extractor.pdf_extractor:PDFExtractor
   ```

3. Create an Extraction Policy:
   ```python
   from indexify import IndexifyClient
   client = IndexifyClient()
   client.create_extraction_policy(extractor="tensorlake/pdf-extractor", name="my-pdf-extractor")
   ```

4. Upload PDFs from your application:
   ```python
   from indexify import IndexifyClient
   client = IndexifyClient()
   content_id = client.upload_file("/path/to/pdf.file")
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

