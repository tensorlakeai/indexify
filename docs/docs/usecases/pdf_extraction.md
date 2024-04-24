# PDF Extraction

PDF is a complex document type, and they can contain text, images and tabular data. Depending on the document type the strategy to extract information from PDF could vary. 

With Indexify, you can -
1. Perform Data Extraction on PDFs: Extract specific information from PDFs, such as fields from tax documents, healthcare records, invoices and receipts. Once you create these pipelines, Indexify will continously extract when documents are uploaded.
2. Index PDFs - Extract text, images and tables(in the form of JSON) and add chunking, embedding extractors, structured data extractors in the pipeline to build indexes on vector stores and structured stores. LLMs can then query these indexes for Document Q and A.

Below is an overview of what you can achieve by combining the PDF Extractors, with Embedding, Chunking and other structured data extractors. We have some examples as well below.
![PDF Extraction High Level](../images/PDF_Usecase.png)

We have developed a PDF extractor which can extract text, images and tables from PDF documents. Additionaly, you can use many other PDF extraction libraries which we have 
packaged as an extractor. You can try out all the various extractors and see which one works best for your use-case.


## Extractors
* [**tensorlake/pdf-extractor**](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/pdf-extractor) - A combined PDF extractor which can extract text, image and tables. This combines many different models in a single package to make it easier to work with PDFs. We recommend starting here.
* [**tensorlake/ocrmypdf**](https://github.com/tensorlakeai/indexify-extractors/tree/main/pdf/ocrmypdf) - Uses the ocrmypdf library which uses tessarect under the hood to extract text from PDFs.
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
client.upload_file("/path/to/pdf.file")
extracted_content = client.get_extracted_content(content_id=content_id)
print(extracted_content)
```

## Data Model of tensorlake/pdf-extractor
### Text
Extracts text from PDFs as `Content` with text in the `data` attribute and the mime type is set to `text/plain`. 

### Image 
Extracts images from PDFs as `Content` with bytes in the `data` attribute and the mime type is set to `image/png`. 

### Tables
Tables are extracted as JSON 

### Metadata
Every `Content` will have `page_number` as a metadata. 



## Examples 

### Invoices
[Notebook for Invoices](../examples/Invoices.ipynb)

### Scientific Journals
[Notebook for Scientific Journals](../examples/Scientific_Journals.ipynb)

### SEC 10-K docs
[Notebook for SEC 10-K docs](../examples/SEC_10_K_docs.ipynb)

### Terms and Condition Documents of Car Rental
[Notebook for Documents of Car Rental](../examples/Terms_and_Condition_Documents_of_Car_Rental.ipynb)

### Terms and Conditions Documents of Health Care Benefits
[Notebook for Documents of Health Care](../examples/Terms_and_Conditions_Documents_of_Health_Care_Benefits.ipynb)
