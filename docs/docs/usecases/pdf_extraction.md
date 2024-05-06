# 📄 PDF Extraction: Unleash the Power of Your Documents! 📊

Welcome to the world of PDF extraction with Indexify! 🎉 We're here to revolutionize the way you interact with your documents, making it easier than ever to extract valuable insights and perform powerful question answering. Get ready to dive into a realm of endless possibilities! 💡

![PDF Extraction High Level](../images/PDF_Usecase.png)

## 🌟 What Can You Achieve with Indexify?

## Explore various PDF Extractors
| Extractors                                | Input Type | Output Type        | Output Example                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | Best For                        | Example Usage                                                                                                                                                                                                                                      |
|-------------------------------------------|------------|--------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| tensorlake/layoutlm-document-qa-extractor | query, pdf | metadata           | [Feature(feature_type='metadata', name='metadata', value={'query': 'What is the invoice total?', 'answer': '$93.00', 'page': 0, 'score': 0.9743825197219849}, comment=None)]                                                                                                                                                                                                                                                                                                   | Invoices Question Answering     | [Schema based HOA Documents](../examples/HOA_Invoice_Data_Extraction.ipynb)                                                                                                                                                                        |
| tensorlake/pdf-extractor                  | pdf        | text, image, table | [Content(content_type='text/plain', data=b'I love playing football.', features=[Feature(feature_type='metadata', name='text', value={'page': 1}, comment=None)], labels={})]                                                                                                                                                                                                                                                                                                   | Scientific Papers, Tabular Info | [Schema based HOA Documents](../examples/HOA_Invoice_Data_Extraction.ipynb), [Multi-state Terms Documents](../examples/Sixt.ipynb), [Scientific Journals](../examples/Scientific_Journals.ipynb), [SEC 10-K docs](../examples/SEC_10_K_docs.ipynb) |
| tensorlake/ocrmypdf                       | pdf        | text               | [Content(content_type='text/plain', data=b'I love playing football.', features=[Feature(feature_type='metadata', value={'page': 1}, comment=None)], labels={})]                                                                                                                                                                                                                                                                                                                | Photocopied/Scanned PDFs on CPU |                                                                                                                                                                                                                                                    |
| tensorlake/easyocr                        | pdf, image | text               | [Content(content_type='text/plain', data=b'I love playing football.', features=[Feature(feature_type='metadata', name='text', value={'page': 1}, comment=None)], labels={})]                                                                                                                                                                                                                                                                                                   | Photocopied/Scanned PDFs on GPU |                                                                                                                                                                                                                                                    |
| tensorlake/unstructuredio                 | pdf, image | text               |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |                                 |                                                                                                                                                                                                                                                    |
| tensorlake/markdown                       | pdf        | text, table        | [Content(content_type='text/plain', data=b'I love playing football.', features=[Feature(feature_type='metadata', name='text', value={'language': 'English', 'filetype': 'pdf', 'toc': [], 'pages': 1, 'ocr_stats': {'ocr_pages': 0, 'ocr_failed': 0, 'ocr_success': 0}, 'block_stats': {'header_footer': 2, 'code': 0, 'table': 0, 'equations': {'successful_ocr': 0, 'unsuccessful_ocr': 0, 'equations': 0}}, 'postprocess_stats': {'edit': {}}}, comment=None)], labels={})] | Structured & formatted PDF      |                                                                                                                                                                                                                                                    |

## How to Test PDF Extraction Locally
Download a PDF Extractor
```bash
indexify-extractor download hub://pdf/pdf-extractor
indexify-extractor join-server
```

Ready to dive in and experience the magic of PDF extraction? Here's how you can test it locally and unlock the secrets hidden within your documents:

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

Watch as the extractor works its magic, revealing the hidden gems within your PDFs. It's like having a superhero sidekick that can read and understand your documents in the blink of an eye! 🦸‍♀️📄

Start a long running PDF Extractor 
```bash
indexify-extractor download hub://pdf/pdf-extractor
indexify-extractor join-server
```

Want to harness the power of PDF extraction in your own applications? We've made it incredibly easy to integrate Indexify into your workflow. Get ready to supercharge your document processing capabilities! 🔋

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

With just a few lines of code, you can unleash the full potential of PDF extraction in your applications. Imagine the possibilities: automated data entry, intelligent document search, and effortless question answering. The sky's the limit! 🚀🌟

## 📚 Explore More Examples

But wait, there's more! We've curated a collection of inspiring examples to showcase the versatility and power of PDF extraction. Check out these notebooks and witness the magic in action:

- [Invoices](../examples/Invoices.ipynb): Extract and analyze invoice data like a pro! 💰📊
- [Terms and Condition Documents of Car Rental](../examples/Terms_and_Condition_Documents_of_Car_Rental.ipynb): Navigate the complex world of car rental agreements with ease. 🚗📜
- [Terms and Conditions Documents of Health Care Benefits](../examples/Terms_and_Conditions_Documents_of_Health_Care_Benefits.ipynb): Demystify health care benefits and make informed decisions. 🏥📄

These examples are just the tip of the iceberg. With Indexify, the possibilities are endless. Dive in, explore, and discover how PDF extraction can revolutionize the way you work with documents! 🌈📚

---

Are you ready to embark on this exciting journey of PDF extraction? Let's unlock the full potential of your documents together and make data-driven decisions like never before! 🎉🚀

If you have any questions, feedback, or just want to share your amazing PDF extraction stories, we're here for you. Reach out to our friendly support team, and let's make document magic happen! ✨💬

Happy extracting! 📄🔍🎉