# PDF Extraction

PDF is a complex document type, and they can contain text, images and tabular data. Depending on the document type the strategy to extract information from PDF could vary. 
We have developed a PDF extractor which can extract text, images and tables from PDF documents. Additionaly, you can use many other PDF extraction libraries which we have 
packaged as an extractor. You can try out all the various extractors and see which one works best for your use-case.

## Extractors
* tensorlake/pdf-extractor - A combined PDF extractor which can extract text, image and tables
* tensorlake/ocrmypdf - Uses the ocrmypdf library which uses tessarect under the hood to extract text from PDFs.
* tensorlake/easyocr - Uses EasyOCR to extract text from PDFs.

## Output Data Model
### Text
We extract text from PDFs as `Content` with text in the `data` attribute and the mime type is set to `text/plain`. 

### Image 
We extract images from PDFs as `Content` with bytes in the `data` attribute and the mime type is set to `image/png`. 

### Tables
Tables are extracted as JSON 

### Metadata
Every `Content` will have `page_number` as a metadata. 


## How to Test PDF Extraction

Start the Indexify Server 
```bash
indexify server -d
```

Run a PDF Extractor 
```bash
indexify-extractor join pdf_extractor.PDFExtactor
```

Upload a PDF 

```python
client = IndexifyClient()
content_id = client.upload_file("foo.pdf")
client.create_extraction_policy(...)
```

Inspect the extracted content
```python
extracted_content = client.derived_content_of(content_id=content_id)
```



## Examples 

### Invoices

### Scientific Journals

### SEC 10-K docs

### Terms and Condition Documents of Car Rental

### Terms and Conditions Documents of Health Care Benefits
