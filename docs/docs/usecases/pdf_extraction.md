# PDF Extraction

PDF is a complex document type, and they can contain text, images and tabular data. Depending on the document type the strategy to extract information from PDF could vary. 

With Indexify, you can -

1. **Perform Data Extraction on PDFs:** Extract specific information from PDFs, such as fields from tax documents, healthcare records, invoices and receipts.
2. **Index PDFs:** - Add chunking, embedding extractors, structured data extractors in a pipeline after data extraction to build indexes on vector stores and structured stores. LLMs can then query these indexes for Document Q and A.

Below is an overview of what you can achieve by combining the PDF Extractors, with Embedding, Chunking and other structured data extractors. We have some examples as well below.
![PDF Extraction High Level](../images/PDF_Usecase.png)

We have developed a PDF extractor which can extract text, images and tables from PDF documents. Additionaly, you can use many other PDF extraction libraries which we have 
packaged as an extractor. You can try out all the various extractors and see which one works best for your use-case.


## Extractors
* [**tensorlake/pdf-extractor**](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/pdf-extractor) - Extract text, images and tables as strings, bytes and json respectively using this extractor.
* [**tensorlake/ocrmypdf**](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/ocrmypdf) - Extract text content from image based pdf files using this ocrmypdf based extractor.
* [**tensorlake/unstructuredio**](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/unstructuredio) - This extractor uses unstructured.io to extract pieces of pdf document into separate plain text content data.
* [**tensorlake/layoutlm-document-qa-extractor**](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/layoutlm_document_qa) - This is a fine-tuned version of the multi-modal [LayoutLM](https://aka.ms/layoutlm) model for the task of question answering on documents. It has been fine-tuned using both the SQuAD2.0 and [DocVQA](https://www.docvqa.org/) datasets.
* [**tensorlake/easyocr**](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/ocrpdf-gpu) - Uses EasyOCR to extract text from PDFs.

## How to Test PDF Extraction Locally
Download a PDF Extractor
```bash
indexify-extractor download hub://pdf/pdf-extractor
indexify-extractor join-server pdf-extractor.pdf_extractor:PDFExtractor
```

Load it in a notebook or terminal
```python
from indexify_extractor_sdk import load_extractor, Content
extractor, config_cls = load_extractor("pdf-extractor.pdf_extractor:PDFExtractor")
content = Content.from_file("/path/to/file.pdf")
results =  extractor.extract(content)
print(results)
```

## Continuous PDF Extraction for Applications

#### Start Indexify Server and Extraction Policies

Download and Start the Indexify Server 
```bash
curl https://tensorlake.ai | sh
./indexify server -d
```

Start a long running PDF Extractor 
```bash
indexify-extractor download hub://pdf/pdf-extractor
indexify-extractor join-server pdf-extractor.pdf_extractor:PDFExtractor
```

```python
from indexify import IndexifyClient
client = IndexifyClient()
client.create_extraction_policy(extractor="tensorlake/pdf-extractor", name="my-pdf-extractor")
```

##### Upload PDFs from your application 


Inspect the extracted content
```python
from indexify import IndexifyClient
client = IndexifyClient()
content_id = client.upload_file("/path/to/pdf.file")
### Read back the extracted content 
extracted_content = client.get_extracted_content(content_id=content_id)
print(extracted_content)
```

## Explore various PDF Extractors
| Extractors                                | Input Type | Output Type        | Output Example                                                                                                                                                               | Best For                        | Example Usage                                                                                                                                                                                                                                      |
|-------------------------------------------|------------|--------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| tensorlake/layoutlm-document-qa-extractor | query, pdf | metadata           | [Feature(feature_type='metadata', name='metadata', value={'query': 'What is the invoice total?', 'answer': '$93.00', 'page': 0, 'score': 0.9743825197219849}, comment=None)] | Invoices Question Answering     | [Schema based HOA Documents](../examples/HOA_Invoice_Data_Extraction.ipynb)                                                                                                                                                                        |
| tensorlake/pdf-extractor                  | pdf        | text, image, table | [Content(content_type='text/plain', data=b'I love playing football.', features=[Feature(feature_type='metadata', name='text', value={'page': 1}, comment=None)], labels={})] | Scientific Papers, Tabular Info | [Schema based HOA Documents](../examples/HOA_Invoice_Data_Extraction.ipynb), [Multi-state Terms Documents](../examples/Sixt.ipynb), [Scientific Journals](../examples/Scientific_Journals.ipynb), [SEC 10-K docs](../examples/SEC_10_K_docs.ipynb) |
| tensorlake/ocrmypdf                       | pdf        | text               | [Content(content_type='text/plain', data=b'I love playing football.', features=[Feature(feature_type='metadata', value={'page': 1}, comment=None)], labels={})]              | Photocopied/Scanned PDFs        |                                                                                                                                                                                                                                                    |
| tensorlake/ocrpdf-gpu                     | pdf        | text               | [Content(content_type='text/plain', data=b'I love playing football.', features=[Feature(feature_type='metadata', name='text', value={'page': 1}, comment=None)], labels={})] | Photocopied/Scanned PDFs on GPU |                                                                                                                                                                                                                                                    |
| tensorlake/unstructuredio                 | pdf, image | text               |                                                                                                                                                                              |                                 |                                                                                                                                                                                                                                                    |

## Other Examples 

### Invoices
[Notebook for Invoices](../examples/Invoices.ipynb)

### Terms and Condition Documents of Car Rental
[Notebook for Documents of Car Rental](../examples/Terms_and_Condition_Documents_of_Car_Rental.ipynb)

### Terms and Conditions Documents of Health Care Benefits
[Notebook for Documents of Health Care](../examples/Terms_and_Conditions_Documents_of_Health_Care_Benefits.ipynb)
